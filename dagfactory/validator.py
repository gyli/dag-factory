"""Validator for dagfactory lint command."""

from __future__ import annotations

import datetime
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from jsonschema import Draft202012Validator, ValidationError, validators
from packaging.version import InvalidVersion, Version

from dagfactory._yaml import load_yaml_file, load_yaml_string
from dagfactory.constants import DEFAULTS_FILE_NAMES
from dagfactory.dagbuilder import DagBuilder
from dagfactory.dagfactory import SYSTEM_PARAMS, _DagFactory
from dagfactory.schema import load_schema, satisfies_max_version, satisfies_min_version


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
def _is_string_or_date(checker, instance):
    return isinstance(instance, (str, datetime.date, datetime.datetime))


def _is_object_or_python_instance(checker, instance):
    """Match a JSON object (dict) OR any non-JSON-primitive Python instance.

    dag-factory's function cast_with_type materialises __type__ directives
    in YAML into real Python objects (e.g. CronTriggerTimetable,
    Dataset, timedelta, callables), so we widen "object" to
    match dicts AND any Python value that isn't a JSON primitive.
    """
    if isinstance(instance, bool):
        return False
    if isinstance(instance, (str, int, float, list, type(None))):
        return False
    return True


def _x_airflow_min_version(validator, value, instance, schema):
    if not satisfies_min_version(value, validator.airflow_version):
        yield ValidationError(
            f"was introduced in Airflow {value} and is not valid for the configured "
            f"Airflow {validator.airflow_version}."
        )


def _x_airflow_max_version(validator, value, instance, schema):
    if not satisfies_max_version(value, validator.airflow_version):
        yield ValidationError(
            f"is not supported past Airflow {value} and was removed before the configured "
            f"Airflow {validator.airflow_version}."
        )


def _x_deprecated_since(validator, value, instance, schema):
    if validator.airflow_version >= Version(value):
        yield ValidationError(f"is deprecated as of Airflow {value}.")


def _x_dagfactory_supported(validator, value, instance, schema):
    if value is False:
        yield ValidationError(
            "is a valid Airflow DAG argument but is not currently wired through dag-factory; "
            "the value will be silently ignored at runtime "
            "(see github.com/astronomer/dag-factory/issues/696)."
        )


def _x_mutually_exclusive(validator, value, instance, schema):
    if not isinstance(instance, dict):
        return
    for group in value or []:
        fields = group.get("fields", [])
        present = [f for f in fields if instance.get(f) is not None]
        if len(present) > 1:
            base = group.get("message") or "Mutually exclusive fields set together."
            yield ValidationError(f"{base} (set: {', '.join(present)})")


def _x_required_anywhere(validator, value, instance, schema):
    if not isinstance(instance, dict):
        return
    for group in value or []:
        fields = group.get("fields", [])
        if not any(_path_present(instance, f) for f in fields):
            base = group.get("message") or f"At least one of these fields must be set: {', '.join(fields)}."
            yield ValidationError(base)


def _path_present(config: Dict[str, Any], dotted_path: str) -> bool:
    """Return True if *dotted_path* (e.g. ``default_args.start_date``) resolves to a non-None value."""
    current: Any = config
    for part in dotted_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]
    return current is not None


# Extend the validator with custom keywords for the DAG Factory schema annotations.
# We have to allow date and time type values as strings since dag-factory uses yaml.FullLoader,
# which enriches the value types. If we want to avoid this, we will have to use a loader function
# different from the one used by dag-factory, which might also not be ideal.
_LINT_TYPE_CHECKER = Draft202012Validator.TYPE_CHECKER.redefine("string", _is_string_or_date).redefine(
    "object", _is_object_or_python_instance
)
_LintValidatorClass = validators.extend(
    Draft202012Validator,
    type_checker=_LINT_TYPE_CHECKER,
    validators={
        "x-airflow-min-version": _x_airflow_min_version,
        "x-airflow-max-version": _x_airflow_max_version,
        "x-deprecated-since": _x_deprecated_since,
        "x-dagfactory-supported": _x_dagfactory_supported,
        "x-mutually-exclusive": _x_mutually_exclusive,
        "x-required-anywhere": _x_required_anywhere,
    },
)

# Keywords whose violations should be surfaced as warnings rather than errors.
# TODO: x-dagfactory-supported should be a temporary field and can be removed once issue #696 is resolved.
_WARNING_KEYWORDS = {"x-deprecated-since", "x-dagfactory-supported"}

#: Findings that say the key does not exist on the configured Airflow at all.
_VERSION_RANGE_KEYWORDS = {"x-airflow-min-version", "x-airflow-max-version"}


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

    Subclassing rather than re-implementing means lint resolves each DAG's
    config through the very method the runtime uses, ``get_dag_params``, so
    the two cannot merge defaults differently.
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
    """Validate DAG Factory YAML files against the bundled JSON schema."""

    def __init__(
        self,
        airflow_version: str = "3",
        schema: Optional[Dict[str, Any]] = None,
        schema_only: bool = True,
        defaults_config_path: Optional[str] = None,
    ) -> None:
        try:
            self.airflow_version = Version(str(airflow_version))
        except InvalidVersion as exc:
            raise ValueError(f"airflow_version must be a PEP440 version string, got: {airflow_version!r}") from exc

        self.schema_only = schema_only
        self.defaults_config_path = defaults_config_path
        self.schema = schema if schema is not None else load_schema()
        # Subclass the extended validator class so we can attach airflow_version
        # as a class attribute (instances are slotted and reject arbitrary
        # attribute assignment). The keyword functions read it off the validator.
        validator_cls = type(
            "_DagParameterValidator",
            (_LintValidatorClass,),
            {"airflow_version": self.airflow_version},
        )
        self._validator = validator_cls(self.schema)

    # ------------------------------------------------------------------
    # Entry points
    # ------------------------------------------------------------------

    def validate_yaml_file(self, yaml_file_path: Path) -> List[FileValidationResult]:
        """Lint a YAML file as a complete DAG config.

        The YAML's own top-level ``default`` block is applied, and so is the
        external ``defaults.yml`` chain: the file path is handed to the same
        factory the runtime uses, which walks up to the defaults root exactly
        as it does when Airflow loads the DAG. Pass ``defaults_config_path`` to
        the validator to set that root explicitly.

        Files named ``defaults.yml`` / ``defaults.yaml`` are skipped.
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
        self,
        config: Any,
        source: Path,
        config_filepath: Optional[str] = None,
    ) -> FileValidationResult:
        """Validate a parsed YAML dict; routes through schema or build mode.

        When *config_filepath* is given the factory is built from the path, so
        the external ``defaults.yml`` chain is resolved by walking up from it,
        just as at runtime. Inline content has no location to walk up from, so
        its defaults come from ``defaults_config_path`` alone.
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
            self._validate_dag(source, builder.dag_name, merged, result)
        return result

    def iter_issues(
        self,
        config: Dict[str, Any],
        dag_id: Optional[str] = None,
        file_path: Optional[Path] = None,
    ) -> Iterator[ValidationIssue]:
        """Yield the schema findings for one fully resolved DAG config.

        This is the single place the schema is applied. ``dagfactory lint``
        reaches it through :meth:`validate_yaml_file` and friends, and
        ``DagBuilder.build`` calls it directly, so a config that lints clean is
        one that builds without schema complaints.

        The config must already be resolved: defaults merged, values cast.
        Whatever is missing here is missing for real.
        """
        errors = list(self._validator.iter_errors(config))

        # A key the running Airflow no longer accepts does not also need to be
        # called deprecated: report that it is gone and stop there.
        out_of_range = {tuple(error.absolute_path) for error in errors if error.validator in _VERSION_RANGE_KEYWORDS}

        for error in errors:
            if error.validator == "x-deprecated-since" and tuple(error.absolute_path) in out_of_range:
                continue
            severity = "warning" if error.validator in _WARNING_KEYWORDS else "error"
            yield ValidationIssue(
                file=file_path if file_path is not None else Path("<config>"),
                dag_id=dag_id,
                severity=severity,
                message=error.message,
                path=".".join(str(part) for part in error.absolute_path),
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
