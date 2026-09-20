"""Tests for the shared schema metadata that drives building and linting."""

import json

import pytest
from packaging.version import Version

from dagfactory import schema


class TestSchemaLoading:
    def test_schema_is_valid_json_and_loaded_once(self):
        assert isinstance(schema.load_schema(), dict)
        assert schema.load_schema() is schema.load_schema()

    def test_schema_path_ships_with_the_package(self):
        assert schema.SCHEMA_PATH.exists()
        json.loads(schema.SCHEMA_PATH.read_text())


class TestDagArgumentNames:
    def test_includes_ordinary_dag_arguments(self):
        names = schema.dag_argument_names()
        for key in ("dag_id", "catchup", "max_active_runs", "default_args", "start_date"):
            assert key in names

    def test_excludes_dagfactory_only_keys(self):
        """`tasks` and friends are dag-factory's own; they must never reach DAG()."""
        names = schema.dag_argument_names()
        for key in ("tasks", "task_groups", "doc_md_file_path", "doc_md_python_callable_name"):
            assert key not in names

    def test_excludes_keys_dagfactory_interprets_itself(self):
        names = schema.dag_argument_names()
        for key in schema.HANDLED_BY_DAGFACTORY:
            assert key not in names

    def test_every_name_is_a_root_property_of_the_schema(self):
        root = schema.load_schema()["properties"]
        assert schema.dag_argument_names() <= set(root)


class TestVersionPredicates:
    @pytest.mark.parametrize(
        "bound,version,within",
        [
            ("2", "2.10.5", True),  # major-only bound allows all of Airflow 2
            ("2", "3.0.0", False),
            ("3.0", "3.0.3", True),  # minor bound allows the whole 3.0 series
            ("3.0", "3.1.0", False),
            ("3.1.2", "3.1.2", True),  # full version bound is inclusive
            ("3.1.2", "3.1.3", False),
        ],
    )
    def test_max_bound_read_at_written_precision(self, bound, version, within):
        assert schema.satisfies_max_version(bound, Version(version)) is within

    @pytest.mark.parametrize(
        "bound,version,within",
        [
            ("3", "3.0.0", True),
            ("3", "2.10.0", False),
            ("2.9", "2.9.0", True),
            ("2.9", "2.8.9", False),
        ],
    )
    def test_min_bound_read_at_written_precision(self, bound, version, within):
        assert schema.satisfies_min_version(bound, Version(version)) is within

    def test_builder_and_linter_share_the_predicates(self):
        """The lint keywords must not re-implement the comparison."""
        import inspect

        from dagfactory import validator

        source = inspect.getsource(validator._x_airflow_max_version)
        assert "satisfies_max_version" in source


class TestUnsupportedReason:
    def test_supported_key_has_no_reason(self):
        assert schema.unsupported_reason("catchup", Version("3.0.0")) is None

    def test_key_removed_in_airflow_3(self):
        reason = schema.unsupported_reason("timetable", Version("3.0.0"))
        assert reason is not None and "timetable" in reason

    def test_key_not_yet_introduced(self):
        reason = schema.unsupported_reason("deadline", Version("3.0.0"))
        assert reason is not None and "introduced" in reason

    def test_key_not_wired_through_dagfactory(self):
        reason = schema.unsupported_reason("fail_fast", Version("3.0.0"))
        assert reason is not None and "not wired through" in reason


class TestTransforms:
    def test_every_transform_names_a_real_property(self):
        root = schema.load_schema()["properties"]
        for key in schema.TRANSFORMS:
            assert key in root, f"TRANSFORMS has '{key}', which the schema does not declare"

    def test_transformed_keys_are_actually_forwarded(self):
        """A transform on a key the schema calls unsupported would never run."""
        names = schema.dag_argument_names()
        for key in schema.TRANSFORMS:
            assert key in names, f"'{key}' has a transform but is not forwarded to DAG()"
            assert schema.annotations(key).get("x-dagfactory-supported") is not False

    def test_every_transform_is_callable(self):
        for key, fn in schema.TRANSFORMS.items():
            assert callable(fn), key


class TestAliases:
    def test_concurrency_defers_to_max_active_tasks(self):
        assert schema.deprecated_in_favor_of("concurrency") == "max_active_tasks"

    def test_ordinary_key_has_no_alias(self):
        assert schema.deprecated_in_favor_of("catchup") is None

    def test_every_alias_target_is_itself_a_dag_argument(self):
        names = schema.dag_argument_names()
        for key in names:
            target = schema.deprecated_in_favor_of(key)
            if target:
                assert target in names, f"'{key}' defers to '{target}', which is not forwarded"
