"""The committed JSON Schema must stay in step with the parameter registry.

``dag_parameters.json`` is generated from :mod:`dagfactory.parameters` but is
committed, because IDEs and ``dagfactory lint`` read the file directly. These
tests are what stops the file and the registry drifting apart.
"""

import json

import pytest

from dagfactory.parameters import PARAMS, PARAMS_BY_KEY, Handling
from dagfactory.schemas import generate


def _is_ref(node):
    """True for a pointer at another section's definition.

    A bare `$ref` into `#/$defs/types/...` is a type alias, not a pointer, so
    `dagrun_timeout: {"$ref": ".../types/timedelta_like"}` still counts as a
    definition in its own right.
    """
    if not (isinstance(node, dict) and set(node) == {"$ref"}):
        return False
    return node["$ref"].startswith(("#/$defs/dag/", "#/$defs/task/", "#/$defs/default_args/"))


def _definitions(schema):
    """Every generated property definition, wherever it is defined."""
    for section in ("dag", "task", "default_args"):
        for key, definition in schema["$defs"][section]["properties"].items():
            if not _is_ref(definition):
                yield key, definition


def _refs(node):
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "$ref" and isinstance(value, str) and value.startswith("#/"):
                yield value
            else:
                yield from _refs(value)
    elif isinstance(node, list):
        for value in node:
            yield from _refs(value)


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

    def test_every_param_is_defined_in_exactly_one_section(self, schema):
        sections = ("dag", "task", "default_args")
        for param in PARAMS:
            properties = {s: schema["$defs"][s]["properties"] for s in sections}
            homes = [s for s in sections if param.key in properties[s] and not _is_ref(properties[s][param.key])]
            assert homes == [generate.section_of(param)], f"{param.key} defined in {homes}"

    def test_every_section_lists_every_param_in_its_scope(self, schema):
        from dagfactory.parameters import Scope

        for scope, section in ((Scope.DAG, "dag"), (Scope.TASK, "task"), (Scope.DEFAULT_ARGS, "default_args")):
            listed = set(schema["$defs"][section]["properties"])
            assert listed == {param.key for param in PARAMS if scope in param.scope}

    def test_every_root_property_references_its_definition(self, schema):
        for key, value in schema["properties"].items():
            assert value == {"$ref": f"#/$defs/{generate.section_of(PARAMS_BY_KEY[key])}/properties/{key}"}

    def test_no_dangling_refs(self, schema):
        for ref in _refs(schema):
            node = schema
            for part in ref[2:].split("/"):
                assert part in node, f"dangling $ref {ref}"
                node = node[part]

    def test_root_properties_are_exactly_the_dag_scoped_params(self, schema):
        from dagfactory.parameters import Scope

        expected = {param.key for param in PARAMS if Scope.DAG in param.scope}
        assert set(schema["properties"]) == expected

    def test_unsupported_flag_is_derived_from_handling(self, schema):
        for key, definition in _definitions(schema):
            is_ignored = PARAMS_BY_KEY[key].handling is Handling.IGNORED
            assert ("x-dagfactory-supported" in definition) is is_ignored
            if is_ignored:
                assert definition["x-dagfactory-supported"] is False

    def test_version_annotations_come_from_the_registry(self, schema):
        for key, definition in _definitions(schema):
            param = PARAMS_BY_KEY[key]
            assert definition.get("x-airflow-min-version") == param.min_version
            assert definition.get("x-airflow-max-version") == param.max_version
            assert definition.get("x-deprecated-since") == param.deprecated_since

    def test_reusable_types_come_from_the_registry(self, schema):
        from dagfactory.parameters import TYPES

        assert schema["$defs"]["types"] == TYPES

    def test_cross_field_rules_are_placed_by_scope(self, schema):
        from dagfactory.parameters import CROSS_FIELD_RULES, Scope

        for key, value in CROSS_FIELD_RULES[Scope.DAG].items():
            assert schema[key] == value
        for key, value in CROSS_FIELD_RULES[Scope.DEFAULT_ARGS].items():
            assert schema["$defs"]["default_args"][key] == value

    def test_required_is_derived_from_the_registry(self, schema):
        assert schema["required"] == [p.key for p in PARAMS if p.required_in_yaml]


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
        for param in PARAMS:
            if param.deprecated_in_favor_of and param.max_version:
                target = PARAMS_BY_KEY[param.deprecated_in_favor_of]
                assert target.max_version == param.max_version, (
                    f"'{param.key}' is bounded at {param.max_version} but maps to " f"'{target.key}', which is not"
                )
