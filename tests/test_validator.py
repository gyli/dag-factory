"""Tests for dagfactory.validator: imports_dagfactory helper + DagParameterValidator."""

import textwrap
from pathlib import Path

from dagfactory.validator import DagParameterValidator


def _write_loader(tmp_path: Path, source: str, name: str = "loader.py") -> Path:
    p = tmp_path / name
    p.write_text(textwrap.dedent(source))
    return p


# ---------------------------------------------------------------------------
# validate_python_loader — schema mode
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# validate_yaml_file
# ---------------------------------------------------------------------------
def test_validate_yaml_file_catches_removed_field(tmp_path):
    """`schedule_interval` in YAML triggers the schema's removed-in-AF3 error."""
    p = tmp_path / "dag.yml"
    p.write_text(
        "my_dag:\n"
        "  schedule_interval: '@daily'\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="3.1").validate_yaml_file(p)
    rendered = " ".join(i.render() for i in results[0].errors)
    assert "schedule_interval" in rendered


def test_validate_yaml_file_applies_internal_default_block(tmp_path):
    """YAML mode DOES merge the YAML's own `default` block (it's part of the same file)."""
    p = tmp_path / "dag.yml"
    p.write_text(
        "default:\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "my_dag:\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="3").validate_yaml_file(p)
    assert not results[0].errors


def test_validate_yaml_file_skips_defaults_yml(tmp_path):
    """defaults.yml files are dag-factory infrastructure, not standalone DAGs."""
    p = tmp_path / "defaults.yml"
    p.write_text("default_args:\n  start_date: '2025-01-01'\n  owner: alice\n")
    results = DagParameterValidator(airflow_version="3").validate_yaml_file(p)
    assert not results[0].errors
    assert any("defaults file" in i.message for i in results[0].warnings)


def test_validate_yaml_file_skips_non_dag_yaml(tmp_path):
    """A YAML with no DAG-shaped entries (e.g. dataset config list) is skipped."""
    p = tmp_path / "datasets.yml"
    p.write_text("datasets:\n  - name: d1\n    uri: s3://x\n")
    results = DagParameterValidator(airflow_version="3").validate_yaml_file(p)
    assert not results[0].errors
    assert any("config-only" in i.message for i in results[0].warnings)


def test_validate_yaml_file_missing_start_date_is_an_error(tmp_path):
    """With the defaults chain resolved, a genuinely absent start_date is an error.

    This used to be softened to a warning because lint could not see
    defaults.yml. It can now, so a missing value means missing everywhere.
    """
    p = tmp_path / "dag.yml"
    p.write_text("my_dag:\n  tasks:\n    - task_id: t\n      operator: x\n")
    results = DagParameterValidator(airflow_version="3", defaults_config_path=str(tmp_path)).validate_yaml_file(p)
    assert any("start_date" in e.message for e in results[0].errors)


def test_validate_yaml_file_start_date_from_defaults_is_accepted(tmp_path):
    """The same DAG is clean once defaults.yml supplies start_date."""
    (tmp_path / "defaults.yml").write_text('default_args:\n  start_date: "2024-01-01"\n')
    p = tmp_path / "dag.yml"
    p.write_text("my_dag:\n  tasks:\n    - task_id: t\n      operator: x\n")
    results = DagParameterValidator(airflow_version="3", defaults_config_path=str(tmp_path)).validate_yaml_file(p)
    assert not results[0].errors, [e.render() for e in results[0].errors]


def test_validate_yaml_file_missing_tasks_is_an_error(tmp_path):
    """Same for a `tasks` list that no defaults file supplies."""
    p = tmp_path / "dag.yml"
    p.write_text("my_dag:\n  default_args:\n    start_date: '2025-01-01'\n")
    results = DagParameterValidator(airflow_version="3", defaults_config_path=str(tmp_path)).validate_yaml_file(p)
    assert any("tasks" in e.message for e in results[0].errors)


def test_validate_yaml_content_reports_a_genuinely_missing_field(tmp_path):
    """Nothing is softened: what the validator was given has no start_date.

    Inline YAML has no location to walk up from, so unless a defaults root is
    supplied there is no defaults.yml to satisfy the requirement.
    """
    results = DagParameterValidator(airflow_version="3", defaults_config_path=str(tmp_path)).validate_yaml_content(
        "my_dag:\n  tasks:\n    - task_id: t\n      operator: x\n"
    )
    assert any("start_date" in e.message for e in results[0].errors)


def test_validate_yaml_content_resolves_defaults_from_the_supplied_root(tmp_path):
    """Point --defaults-path at the root and inline content is clean."""
    (tmp_path / "defaults.yml").write_text('default_args:\n  start_date: "2024-01-01"\n')
    results = DagParameterValidator(airflow_version="3", defaults_config_path=str(tmp_path)).validate_yaml_content(
        "my_dag:\n  tasks:\n    - task_id: t\n      operator: x\n"
    )
    assert not results[0].issues, [i.render() for i in results[0].issues]


# ---------------------------------------------------------------------------
# validate_yaml_content
# ---------------------------------------------------------------------------
def test_validate_yaml_content_catches_schema_error():
    yaml_text = (
        "my_dag:\n"
        "  catchup: 'yes'\n"  # not a boolean
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="3").validate_yaml_content(yaml_text)
    rendered = " ".join(i.render() for i in results[0].errors)
    assert "catchup" in rendered and "boolean" in rendered


def test_validate_yaml_content_with_label():
    yaml_text = "my_dag:\n  default_args: {start_date: '2025-01-01'}\n  tasks: [{task_id: t, operator: x}]\n"
    results = DagParameterValidator(airflow_version="3").validate_yaml_content(
        yaml_text, source_label="editor:buffer.yml"
    )
    assert results[0].file == Path("editor:buffer.yml")


def test_validate_yaml_content_parse_error():
    results = DagParameterValidator(airflow_version="3").validate_yaml_content("key: [unclosed\n")
    assert results[0].errors
    assert "Failed to parse YAML" in results[0].errors[0].message


def test_validate_yaml_content_expands_join_directive():
    """--yaml-content shares load_yaml_string with the file path, so __join__/
    __and__/__or__ directives are flattened the same way dag-factory actually
    builds them, not left as raw dicts that would fail schema validation."""
    yaml_text = (
        "my_dag:\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "    owner:\n"
        "      __join__: ['team-', 'data']\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="3").validate_yaml_content(yaml_text)
    assert not results[0].errors


# ---------------------------------------------------------------------------
# Custom x-* JSON Schema keywords
# ---------------------------------------------------------------------------
def test_x_deprecated_since_is_a_warning_not_error():
    """x-deprecated-since (schedule_interval) is a warning once the configured
    Airflow version reaches the deprecation, not an error."""
    yaml_text = (
        "my_dag:\n"
        "  schedule_interval: '@daily'\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="2.4").validate_yaml_content(yaml_text)
    assert not results[0].errors
    assert any("deprecated" in w.message for w in results[0].warnings)


def test_x_dagfactory_supported_false_is_a_warning_not_error():
    """x-dagfactory-supported: false (template_undefined) is a warning — the
    value is valid Airflow but silently ignored by dag-factory at runtime."""
    yaml_text = (
        "my_dag:\n"
        "  template_undefined: 'AllowUndefined'\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="3").validate_yaml_content(yaml_text)
    assert not results[0].errors
    assert any("not currently wired through dag-factory" in w.message for w in results[0].warnings)


def test_x_mutually_exclusive_is_an_error():
    """x-mutually-exclusive (schedule vs schedule_interval vs timetable) errors
    when more than one of the group is set."""
    yaml_text = (
        "my_dag:\n"
        "  schedule: '@daily'\n"
        "  schedule_interval: '@daily'\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="2").validate_yaml_content(yaml_text)
    assert any("Only one of" in e.message for e in results[0].errors)


def test_x_airflow_min_version_is_an_error():
    """x-airflow-min-version (allowed_run_types, introduced in 3.2) errors when
    the configured Airflow predates it."""
    yaml_text = (
        "my_dag:\n"
        "  allowed_run_types: ['backfill']\n"
        "  default_args:\n"
        "    start_date: '2025-01-01'\n"
        "  tasks:\n"
        "    - task_id: t\n"
        "      operator: x\n"
    )
    results = DagParameterValidator(airflow_version="3.0").validate_yaml_content(yaml_text)
    assert any("was introduced in Airflow" in e.message for e in results[0].errors)
