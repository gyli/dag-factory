"""Validator for dagfactory lint command."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from packaging.version import InvalidVersion, Version

from dagfactory import parameters
from dagfactory._yaml import load_yaml_file, load_yaml_string
from dagfactory.constants import DEFAULTS_FILE_NAMES
from dagfactory.dagbuilder import DagBuilder
from dagfactory.dagfactory import SYSTEM_PARAMS, _DagFactory


@dataclass
class ValidationIssue:
    """A single validation finding for a DAG inside a YAML file."""

    file: Path
    dag_id: Optional[str]
    severity: str  # "error" or "warning"
    message: str
    path: str = ""

    def render(self) -> str:
        location = f"{self.dag_id}" if self.dag_id else "<file>"
        if self.path:
            location = f"{location}.{self.path}"
        return f"[{location}] {self.message}"


@dataclass
class FileValidationResult:
    """Aggregated validation findings for one YAML file."""

    file: Path
    issues: List[ValidationIssue] = field(default_factory=list)

    @property
    def errors(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.severity == "warning"]


def _make_result(file_path: Path, severity: str, message: str) -> FileValidationResult:
    """Build a FileValidationResult holding a single ValidationIssue."""
    r = FileValidationResult(file=file_path)
    r.issues.append(ValidationIssue(file=file_path, dag_id=None, severity=severity, message=message))
    return r


# ---------------------------------------------------------------------------
# JSON Schema validator extension
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Loader interception (schema-only mode)
# ---------------------------------------------------------------------------
class _LintDagFactory(_DagFactory):
    """_DagFactory subclass that skips DAG generation and markdown serialization."""

    def _generate_dags(self, globals):  # noqa: A002 (matches parent signature)
        return

    @staticmethod
    def _serialise_config_md(dag_name, dag_config, default_config):
        return ""


class _LintDagBuilder(DagBuilder):
    """A real DagBuilder with only ``build`` stubbed out.

    Subclassing means lint resolves each DAG through ``get_dag_params``, the
    very method the runtime uses, so the two cannot merge defaults differently.
    """

    def build(self):
        # build_dags returns {"dag_id": ..., "dag": ...}; the dag value is
        # discarded (validators don't need a real Airflow DAG object).
        return {"dag_id": self.dag_name, "dag": None}


def _intercept_dag_factory() -> Iterator[List[_LintDagFactory]]:
    """Swap _DagFactory in its module; yield a list that captures every instantiation."""
    import dagfactory.dagfactory as df_module

    captured: List[_LintDagFactory] = []

    class _Capturing(_LintDagFactory):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            captured.append(self)

    original = df_module._DagFactory
    df_module._DagFactory = _Capturing
    try:
        yield captured
    finally:
        df_module._DagFactory = original


@contextmanager
def _intercept_dag_builder() -> Iterator[List[_LintDagBuilder]]:
    """Swap DagBuilder in its module; yield a list that captures every instantiation."""
    import dagfactory.dagfactory as df_module

    captured: List[_LintDagBuilder] = []

    class _Capturing(_LintDagBuilder):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            captured.append(self)

    original = df_module.DagBuilder
    df_module.DagBuilder = _Capturing
    try:
        yield captured
    finally:
        df_module.DagBuilder = original


def _force_strict_mode() -> Iterator[None]:
    """Temporarily enable dagfactory.settings.strict_mode.

    ``build_dags()`` no longer raises on a per-DAG build failure by default —
    it returns ``(dags, first_build_error)`` and only raises through
    ``_generate_dags`` when strict_mode is on. Lint's build-mode path needs
    that exception to surface failures, so it forces strict_mode for the
    duration of the import.
    """
    from dagfactory import settings as df_settings

    original = df_settings.strict_mode
    df_settings.strict_mode = True
    try:
        yield
    finally:
        df_settings.strict_mode = original


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------
class DagParameterValidator:
    """Validate DAG Factory YAML configs against :data:`dagfactory.parameters.PARAM_METADATA`."""

    def __init__(
        self,
        airflow_version: str = "3",
        schema_only: bool = True,
        defaults_config_path: Optional[str] = None,
    ) -> None:
        try:
            self.airflow_version = Version(str(airflow_version))
        except InvalidVersion as exc:
            raise ValueError(f"airflow_version must be a PEP440 version string, got: {airflow_version!r}") from exc

        self.schema_only = schema_only
        self.defaults_config_path = defaults_config_path

    # ------------------------------------------------------------------
    # Entry points
    # ------------------------------------------------------------------

    def validate_yaml_file(self, yaml_file_path: Path) -> List[FileValidationResult]:
        """Lint a YAML file as a complete DAG config.

        The YAML's own ``default`` block is applied, and so is the external
        ``defaults.yml`` chain: the file path goes to the same factory the
        runtime uses, which walks up to the defaults root exactly as it does
        under Airflow. ``defaults_config_path`` sets that root.
        """
        yaml_file_path = yaml_file_path.resolve()
        if yaml_file_path.name in DEFAULTS_FILE_NAMES:
            return [
                _make_result(
                    yaml_file_path,
                    "warning",
                    f"Skipping {yaml_file_path.name} — defaults file, not a DAG config.",
                )
            ]
        try:
            config = load_yaml_file(str(yaml_file_path))
        except Exception as exc:
            return [
                _make_result(
                    yaml_file_path,
                    "error",
                    f"Failed to load YAML: {type(exc).__name__}: {exc}",
                )
            ]
        return [self._validate_yaml_config(config, yaml_file_path, config_filepath=str(yaml_file_path))]

    def validate_yaml_content(
        self, yaml_content: str, source_label: str = "<inline yaml>"
    ) -> List[FileValidationResult]:
        """Lint an inline YAML string.

        Parsed via the same ``load_yaml_string`` helper as ``validate_yaml_file``
        (env-var expansion, ``__type__`` casting, ``__and__``/``__or__``/``__join__``
        flattening), so this validates the same shape dag-factory actually builds.
        """
        source = Path(source_label)
        try:
            config = load_yaml_string(yaml_content)
        except Exception as exc:
            return [
                _make_result(
                    source,
                    "error",
                    f"Failed to parse YAML: {type(exc).__name__}: {exc}",
                )
            ]
        return [self._validate_yaml_config(config, source)]

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------
    def _validate_yaml_config(
        self, config: Any, source: Path, config_filepath: Optional[str] = None
    ) -> FileValidationResult:
        """Validate a parsed YAML dict; routes through metadata or build mode.

        With *config_filepath* the factory resolves the external
        ``defaults.yml`` chain by walking up from the file. Inline content has
        no location, so its defaults come from ``defaults_config_path`` alone.
        """
        result = FileValidationResult(file=source)

        if not isinstance(config, dict):
            result.issues.append(
                ValidationIssue(
                    file=source,
                    dag_id=None,
                    severity="error",
                    message="Top-level YAML must be a mapping.",
                )
            )
            return result

        dag_entries = {k: v for k, v in config.items() if k not in SYSTEM_PARAMS}
        if not dag_entries:
            result.issues.append(
                ValidationIssue(
                    file=source,
                    dag_id=None,
                    severity="warning",
                    message="No DAG entries found in YAML (only reserved top-level keys found).",
                )
            )
            return result
        # Non-DAG YAML (top-level values aren't mappings) — skip with one warning.
        if not any(isinstance(v, dict) for v in dag_entries.values()):
            result.issues.append(
                ValidationIssue(
                    file=source,
                    dag_id=None,
                    severity="warning",
                    message="No DAG entries found (top-level values are not mappings); "
                    "looks like a config-only YAML, skipping.",
                )
            )
            return result

        factory_kwargs: Dict[str, Any] = (
            {"config_filepath": config_filepath} if config_filepath else {"config_dict": config}
        )
        if self.defaults_config_path:
            factory_kwargs["defaults_config_path"] = self.defaults_config_path

        if not self.schema_only:
            try:
                _, first_build_error = _DagFactory(**factory_kwargs).build_dags()
            except Exception as exc:
                result.issues.append(
                    ValidationIssue(
                        file=source,
                        dag_id=None,
                        severity="error",
                        message=f"Failed to build DAGs: {type(exc).__name__}: {exc}",
                    )
                )
                return result
            if first_build_error is not None:
                dag_name, exc = first_build_error
                result.issues.append(
                    ValidationIssue(
                        file=source,
                        dag_id=dag_name,
                        severity="error",
                        message=f"Failed to build DAG: {type(exc).__name__}: {exc}",
                    )
                )
            return result

        # Schema mode: build via lint factory under DagBuilder intercept,
        # then validate each captured config against the JSON schema.
        with _intercept_dag_builder() as builders:
            try:
                _LintDagFactory(**factory_kwargs).build_dags()
            except Exception as exc:
                result.issues.append(
                    ValidationIssue(
                        file=source,
                        dag_id=None,
                        severity="error",
                        message=f"dag-factory failed to process YAML: {type(exc).__name__}: {exc}",
                    )
                )
        for builder in builders:
            try:
                merged = builder.get_dag_params()
            except Exception as exc:
                result.issues.append(
                    ValidationIssue(
                        file=source,
                        dag_id=builder.dag_name,
                        severity="error",
                        message=f"Failed to resolve config: {type(exc).__name__}: {exc}",
                    )
                )
                continue
            # This entry point never resolves the external defaults.yml chain
            # (see validate_yaml_file's docstring), so a DAG that legitimately
            # gets `tasks`/`start_date` from there would otherwise be flagged
            # with a false-positive error.
            self._validate_dag(source, builder.dag_name, merged, result)
        return result

    def _find_dag_objects(module) -> Dict[str, object]:
        """Collect any Airflow DAG instances registered in the module globals."""
        try:
            from airflow.sdk.definitions.dag import DAG
        except ImportError:
            from airflow.models import DAG
        return {name: obj for name, obj in vars(module).items() if isinstance(obj, DAG)}

    def iter_issues(
        self,
        config: Dict[str, Any],
        dag_id: Optional[str] = None,
        file_path: Optional[Path] = None,
    ) -> Iterator[ValidationIssue]:
        """Yield the findings for one fully resolved DAG config.

        A thin wrapper over :func:`dagfactory.parameters.check`, which is what
        ``DagBuilder.build`` calls too, so lint and build report the same facts
        from the same table.
        """
        for severity, path, message in parameters.check(config, self.airflow_version):
            yield ValidationIssue(
                file=file_path if file_path is not None else Path("<config>"),
                dag_id=dag_id,
                severity=severity,
                message=message,
                path=path,
            )

    def _validate_dag(
        self,
        file_path: Path,
        dag_id: str,
        merged: Dict[str, Any],
        result: FileValidationResult,
    ) -> None:
        """Append the findings for one merged DAG config to *result*."""
        result.issues.extend(self.iter_issues(merged, dag_id=dag_id, file_path=file_path))
