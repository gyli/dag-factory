"""The committed JSON Schema must stay in step with the parameter registry.

``dag_parameters.json`` is generated from :mod:`dagfactory.parameters` but is
committed, because IDEs and ``dagfactory lint`` read the file directly. These
tests are what stops the file and the registry drifting apart.
"""

import json

import pytest

from dagfactory.parameters import DAG_PARAMS, DAG_PARAMS_BY_KEY, Handling
from dagfactory.schemas import generate


def test_committed_schema_matches_a_fresh_generation():
    committed = generate.SCHEMA_PATH.read_text()
    assert committed == generate.render(), (
        "dag_parameters.json is out of date with dagfactory/parameters.py. "
        "Run: python -m dagfactory.schemas.generate"
    )


def test_committed_schema_is_valid_json():
    json.loads(generate.SCHEMA_PATH.read_text())


class TestGeneratedProperties:
    @pytest.fixture(scope="class")
    def schema(self):
        return generate.build_schema()

    def test_every_registry_key_with_a_fragment_is_generated(self, schema):
        generated = schema["$defs"]["dag_only"]["properties"]
        expected = {param.key for param in DAG_PARAMS if param.json_schema is not None}
        assert set(generated) == expected

    def test_every_generated_property_has_a_root_reference(self, schema):
        for key in schema["$defs"]["dag_only"]["properties"]:
            assert schema["properties"][key] == {"$ref": f"#/$defs/dag_only/properties/{key}"}

    def test_every_root_property_is_known(self, schema):
        """A root property either comes from the registry or from the chassis."""
        chassis = json.loads(generate.CHASSIS_PATH.read_text())
        known = set(DAG_PARAMS_BY_KEY) | set(chassis["properties"])
        assert set(schema["properties"]) <= known

    def test_unsupported_flag_is_derived_from_handling(self, schema):
        for key, definition in schema["$defs"]["dag_only"]["properties"].items():
            is_ignored = DAG_PARAMS_BY_KEY[key].handling is Handling.IGNORED
            assert ("x-dagfactory-supported" in definition) is is_ignored
            if is_ignored:
                assert definition["x-dagfactory-supported"] is False

    def test_version_annotations_come_from_the_registry(self, schema):
        for key, definition in schema["$defs"]["dag_only"]["properties"].items():
            param = DAG_PARAMS_BY_KEY[key]
            assert definition.get("x-airflow-min-version") == param.min_version
            assert definition.get("x-airflow-max-version") == param.max_version
            assert definition.get("x-deprecated-since") == param.deprecated_since

    def test_chassis_sections_are_copied_through_untouched(self, schema):
        chassis = json.loads(generate.CHASSIS_PATH.read_text())
        for section in ("types", "_shared", "task_only", "default_args", "dagfactory_only"):
            assert schema["$defs"][section] == chassis["$defs"][section]
        for key in ("x-mutually-exclusive", "x-required-anywhere", "dependentRequired", "required"):
            assert schema[key] == chassis[key]


class TestBuildAndLintAgree:
    """The point of the exercise: one fact, one place."""

    def test_params_the_builder_forwards_are_never_marked_unsupported(self):
        from dagfactory.parameters import BUILD_PARAMS

        for param in BUILD_PARAMS:
            assert param.handling is Handling.KWARG

    def test_no_max_version_on_a_deprecated_alias(self):
        """Bounds describe the argument a key maps to, not the key itself.

        ``concurrency`` is rewritten to ``max_active_tasks``, so it stays usable
        on Airflow 3 even though Airflow 3 dropped the ``concurrency`` kwarg.
        """
        for param in DAG_PARAMS:
            if param.deprecated_in_favor_of and param.max_version:
                target = DAG_PARAMS_BY_KEY[param.deprecated_in_favor_of]
                assert target.max_version == param.max_version, (
                    f"'{param.key}' is bounded at {param.max_version} but maps to " f"'{target.key}', which is not"
                )
