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

    def test_required_is_derived_from_the_registry(self, schema):
        from dagfactory.parameters import Scope

        # A key satisfiable in more than one scope is x-required-anywhere instead.
        expected = [p.key for p in PARAMS if p.required_in_yaml and p.scope is Scope.DAG]
        assert schema["required"] == expected


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


class TestDerivedCrossFieldRules:
    """The rules that span keys are derived too; only their wording is written."""

    @pytest.fixture(scope="class")
    def schema(self):
        return generate.build_schema()

    def test_every_exclusive_group_is_declared_and_has_members(self):
        from dagfactory.parameters import EXCLUSIVE_GROUPS

        members = {}
        for param in PARAMS:
            if param.exclusive_group:
                members.setdefault(param.exclusive_group, []).append(param.key)
        assert set(members) == set(EXCLUSIVE_GROUPS), "a group is declared but unused, or used but undeclared"
        for name, keys in members.items():
            assert len(keys) > 1, f"exclusive group '{name}' has a single member"

    def test_exclusive_groups_match_their_members(self, schema):
        rules = {tuple(sorted(rule["fields"])): rule for rule in schema["x-mutually-exclusive"]}
        groups = {}
        for param in PARAMS:
            if param.exclusive_group:
                groups.setdefault(param.exclusive_group, []).append(param.key)
        for keys in groups.values():
            assert tuple(sorted(keys)) in rules

    def test_deprecated_aliases_are_exclusive_without_a_group(self, schema):
        pairs = {tuple(sorted(rule["fields"])) for rule in schema["x-mutually-exclusive"]}
        for param in PARAMS:
            if param.deprecated_in_favor_of:
                assert tuple(sorted([param.key, param.deprecated_in_favor_of])) in pairs
                # The pairing comes from deprecated_in_favor_of, not a group.
                assert param.exclusive_group is None

    def test_dependent_required_comes_from_requires(self, schema):
        from dagfactory.parameters import Scope

        for scope, node in ((Scope.DAG, schema), (Scope.DEFAULT_ARGS, schema["$defs"]["default_args"])):
            expected = {p.key: list(p.requires) for p in PARAMS if p.requires and scope in p.scope}
            assert node["dependentRequired"] == expected

    def test_requires_only_names_known_keys(self):
        for param in PARAMS:
            for other in param.requires:
                assert other in PARAMS_BY_KEY, f"'{param.key}' requires unknown key '{other}'"

    def test_required_anywhere_lists_every_scope_the_key_allows(self, schema):
        from dagfactory.parameters import Scope

        for rule in schema["x-required-anywhere"]:
            key = rule["fields"][0]
            param = PARAMS_BY_KEY[key]
            expected = [key] if Scope.DAG in param.scope else []
            if Scope.DEFAULT_ARGS in param.scope:
                expected.append(f"default_args.{key}")
            assert rule["fields"] == expected

    def test_no_rule_names_an_unknown_key(self, schema):
        for rule in schema["x-mutually-exclusive"]:
            for field in rule["fields"]:
                assert field in PARAMS_BY_KEY
