"""Tests for dagfactory lint's config checking."""

from pathlib import Path

from packaging.version import Version

from dagfactory import parameters
from dagfactory.lint import is_dag_config, lint_file

AF3 = Version("3.0.0")


def _write(tmp_path: Path, name: str, text: str) -> Path:
    p = tmp_path / name
    p.write_text(text)
    return p


VALID = """
my_dag:
  start_date: 2024-01-01
  tasks:
    t:
      operator: airflow.providers.standard.operators.bash.BashOperator
      bash_command: echo hi
"""


class TestIsDagConfig:
    def test_a_mapping_of_dags_is_a_config(self):
        assert is_dag_config({"my_dag": {"tasks": {}}})

    def test_a_values_file_is_not(self):
        assert not is_dag_config({"key": "value"})

    def test_a_list_is_not(self):
        assert not is_dag_config([1, 2, 3])

    def test_only_reserved_keys_is_not(self):
        assert not is_dag_config({"default": {"default_args": {}}})


class TestLintFile:
    def test_a_valid_config_is_clean(self, tmp_path):
        result = lint_file(_write(tmp_path, "dag.yml", VALID), AF3, str(tmp_path))
        assert not result.findings, [f.render() for f in result.findings]

    def test_non_dag_yaml_is_skipped_silently(self, tmp_path):
        """A values file is not a dag-factory config; flagging it would be noise."""
        result = lint_file(_write(tmp_path, "values.yml", "key: value\n"), AF3, str(tmp_path))
        assert not result.findings

    def test_unknown_key_is_a_warning(self, tmp_path):
        text = VALID + "  nonsense_key: 1\n"
        result = lint_file(_write(tmp_path, "dag.yml", text), AF3, str(tmp_path))
        assert any("nonsense_key" in f.message for f in result.warnings)
        assert not result.errors

    def test_wrong_type_is_an_error(self, tmp_path):
        text = VALID.replace("  tasks:", "  catchup: maybe\n  tasks:")
        result = lint_file(_write(tmp_path, "dag.yml", text), AF3, str(tmp_path))
        assert any("catchup" in f.message for f in result.errors)

    def test_parameter_removed_in_this_airflow_is_an_error(self, tmp_path):
        text = VALID.replace("  tasks:", "  timetable: {}\n  tasks:")
        result = lint_file(_write(tmp_path, "dag.yml", text), AF3, str(tmp_path))
        assert any("timetable" in f.message and "removed" in f.message for f in result.errors)

    def test_same_parameter_is_fine_on_airflow_2(self, tmp_path):
        text = VALID.replace("  tasks:", "  timetable: {}\n  tasks:")
        result = lint_file(_write(tmp_path, "dag.yml", text), Version("2.10.0"), str(tmp_path))
        assert not any("timetable" in f.message for f in result.errors)

    def test_deprecated_parameter_is_a_warning(self, tmp_path):
        text = VALID.replace("  tasks:", "  concurrency: 4\n  tasks:")
        result = lint_file(_write(tmp_path, "dag.yml", text), AF3, str(tmp_path))
        assert any("concurrency" in f.message for f in result.warnings)
        assert not result.errors

    def test_missing_start_date_is_an_error(self, tmp_path):
        text = VALID.replace("  start_date: 2024-01-01\n", "")
        result = lint_file(_write(tmp_path, "dag.yml", text), AF3, str(tmp_path))
        assert any("start_date" in f.message for f in result.errors)

    def test_start_date_from_defaults_yml_is_accepted(self, tmp_path):
        """The defaults chain is resolved exactly as it is at runtime."""
        _write(tmp_path, "defaults.yml", 'default_args:\n  start_date: "2024-01-01"\n')
        text = VALID.replace("  start_date: 2024-01-01\n", "")
        result = lint_file(_write(tmp_path, "dag.yml", text), AF3, str(tmp_path))
        assert not result.errors, [f.render() for f in result.errors]

    def test_every_dag_in_a_file_is_checked(self, tmp_path):
        text = VALID + VALID.replace("my_dag:", "other_dag:").replace("  catchup", "  catchup")
        text = text.replace("other_dag:\n  start_date", "other_dag:\n  catchup: nope\n  start_date")
        result = lint_file(_write(tmp_path, "dag.yml", text), AF3, str(tmp_path))
        assert {f.dag_id for f in result.errors} == {"other_dag"}

    def test_findings_render_with_dag_and_path(self, tmp_path):
        text = VALID.replace("  tasks:", "  catchup: maybe\n  tasks:")
        result = lint_file(_write(tmp_path, "dag.yml", text), AF3, str(tmp_path))
        assert "[my_dag.catchup]" in result.errors[0].render()

    def test_unparseable_config_is_reported_not_raised(self, tmp_path):
        result = lint_file(_write(tmp_path, "dag.yml", "my_dag:\n  tasks: [unclosed\n"), AF3, str(tmp_path))
        assert result.errors

    def test_severity_split_matches_the_metadata(self, tmp_path):
        text = VALID.replace("  tasks:", "  concurrency: 4\n  catchup: maybe\n  tasks:")
        result = lint_file(_write(tmp_path, "dag.yml", text), AF3, str(tmp_path))
        assert {f.severity for f in result.errors} == {parameters.ERROR}
        assert {f.severity for f in result.warnings} == {parameters.WARNING}


class TestLintAgreesWithBuild:
    def test_lint_uses_the_same_check_the_builder_uses(self, tmp_path):
        """The point of the exercise: one function, two callers."""
        import inspect

        from dagfactory.dagbuilder import DagBuilder

        assert "parameters.check" in inspect.getsource(DagBuilder.validate_config)
        assert "parameters.check" in inspect.getsource(lint_file)


BASH = "airflow.providers.standard.operators.bash.BashOperator"

TASKS = f"""
my_dag:
  start_date: 2024-01-01
  tasks:
    a:
      operator: {BASH}
      bash_command: echo hi
"""


class TestLintChecksTasks:
    def test_a_bad_task_parameter_is_reported(self, tmp_path):
        text = TASKS + "      retries: many\n"
        result = lint_file(_write(tmp_path, "dag.yml", text), AF3, str(tmp_path))
        assert any("retries" in f.message for f in result.errors)

    def test_an_unimportable_operator_is_reported(self, tmp_path):
        text = TASKS.replace(BASH, "airflow.operators.bash.NoSuchOperator")
        result = lint_file(_write(tmp_path, "dag.yml", text), AF3, str(tmp_path))
        assert any("Cannot import" in f.message for f in result.errors)

    def test_a_missing_dependency_is_reported(self, tmp_path):
        text = TASKS + "      dependencies: [ghost]\n"
        result = lint_file(_write(tmp_path, "dag.yml", text), AF3, str(tmp_path))
        assert any("does not exist" in f.message for f in result.errors)

    def test_a_cycle_is_reported(self, tmp_path):
        text = TASKS + f"""    b:
      operator: {BASH}
      bash_command: echo
      dependencies: [a]
"""
        text = text.replace("      bash_command: echo hi\n", "      bash_command: echo hi\n      dependencies: [b]\n")
        result = lint_file(_write(tmp_path, "dag.yml", text), AF3, str(tmp_path))
        assert any("Cycle detected" in f.message for f in result.errors)

    def test_operator_arguments_are_not_mistaken_for_typos(self, tmp_path):
        text = TASKS + "      env: {A: '1'}\n      cwd: /tmp\n"
        result = lint_file(_write(tmp_path, "dag.yml", text), AF3, str(tmp_path))
        assert not result.findings, [f.render() for f in result.findings]

    def test_dev_dags_report_nothing_about_unrecognised_task_keys(self):
        """Task keys are the operator's business, not ours."""
        from pathlib import Path

        root = Path("dev/dags")
        files = [p for p in sorted(root.rglob("*.yml")) if p.name != "defaults.yml"]
        spurious = [
            finding.render()
            for f in files
            for finding in lint_file(f, AF3, "dev/dags").findings
            if "is not an argument" in finding.message
        ]
        assert not spurious, spurious
