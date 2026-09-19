"""Tests for the canonical DAG parameter registry."""

import pytest
from packaging.version import parse as parse_version

from dagfactory import parameters
from dagfactory.parameters import PARAMS, PARAMS_BY_KEY, Param


class TestParam:
    def test_target_key_defaults_to_key(self):
        assert Param("catchup").target_key == "catchup"

    def test_target_key_follows_deprecation(self):
        assert Param("concurrency", deprecated_in_favor_of="max_active_tasks").target_key == "max_active_tasks"

    def test_unbounded_param_applies_to_any_version(self):
        param = Param("catchup")
        for release in ("2.9.0", "3.0.0", "99.0.0"):
            assert param.unsupported_reason(parse_version(release)) is None

    def test_min_version_is_inclusive(self):
        param = Param("x", min_version="2.9.0")
        assert param.unsupported_reason(parse_version("2.9.0")) is None
        assert param.unsupported_reason(parse_version("2.8.9")) is not None

    def test_max_version_is_exclusive(self):
        param = Param("x", max_version="3.0.0")
        assert param.unsupported_reason(parse_version("2.10.5")) is None
        assert param.unsupported_reason(parse_version("3.0.0")) is not None

    def test_reason_names_the_key_and_both_versions(self):
        reason = Param("orientation", max_version="3.0.0").unsupported_reason(parse_version("3.2.1"))
        assert "orientation" in reason
        assert "3.0.0" in reason
        assert "3.2.1" in reason

    def test_malformed_bound_fails_at_construction(self):
        with pytest.raises(Exception):
            Param("x", min_version="not-a-version")

    def test_params_are_frozen(self):
        with pytest.raises(Exception):
            Param("x").key = "y"


class TestRegistry:
    def test_keys_are_unique(self):
        keys = [param.key for param in PARAMS]
        assert len(keys) == len(set(keys))

    def test_lookup_table_matches_the_list(self):
        assert set(PARAMS_BY_KEY) == {param.key for param in PARAMS}
        assert len(PARAMS_BY_KEY) == len(PARAMS)

    def test_deprecated_entries_precede_their_canonical_key(self):
        order = {param.key: index for index, param in enumerate(PARAMS)}
        for param in PARAMS:
            if param.deprecated_in_favor_of:
                assert (
                    param.deprecated_in_favor_of in order
                ), f"'{param.key}' defers to unknown key '{param.deprecated_in_favor_of}'"
                assert order[param.key] < order[param.deprecated_in_favor_of], (
                    f"'{param.key}' must come before '{param.deprecated_in_favor_of}' so the "
                    "canonical value wins when both are set"
                )

    def test_version_bounds_are_wellformed(self):
        for param in PARAMS:
            if param.min_version and param.max_version:
                assert parse_version(param.min_version) < parse_version(param.max_version)

    def test_exactly_one_required_param(self):
        assert [param.key for param in PARAMS if param.required] == ["dag_id"]

    def test_transforms_are_callable(self):
        for param in PARAMS:
            if param.transform is not None:
                assert callable(param.transform)

    def test_registry_is_the_only_definition_dagbuilder_reads(self):
        from dagfactory import dagbuilder

        assert dagbuilder.BUILD_PARAMS is parameters.BUILD_PARAMS


class TestHandling:
    def test_build_params_are_exactly_the_kwarg_entries(self):
        from dagfactory.parameters import BUILD_PARAMS, Handling

        assert BUILD_PARAMS == [p for p in PARAMS if p.handling is Handling.KWARG]

    def test_default_handling_is_kwarg(self):
        from dagfactory.parameters import Handling

        assert Param("x").handling is Handling.KWARG

    def test_custom_and_ignored_keys_are_not_forwarded(self):
        from dagfactory.parameters import BUILD_PARAMS, Handling

        forwarded = {p.key for p in BUILD_PARAMS}
        for param in PARAMS:
            if param.handling is not Handling.KWARG:
                assert param.key not in forwarded

    def test_schedule_is_handled_outside_build_dag_kwargs(self):
        from dagfactory.parameters import Handling

        # configure_schedule() owns these, so _build_dag_kwargs must not touch them.
        assert PARAMS_BY_KEY["schedule"].handling is Handling.CUSTOM
        assert PARAMS_BY_KEY["schedule_interval"].handling is Handling.CUSTOM

    def test_user_defined_macros_is_supported(self):
        from dagfactory.parameters import Handling

        # Support landed in main after #732 wrote the schema by hand; deriving
        # the annotation from `handling` is what keeps the two in step.
        param = PARAMS_BY_KEY["user_defined_macros"]
        assert param.handling is Handling.KWARG
        assert param.transform is not None

    def test_keys_dagfactory_interprets_itself_are_not_forwarded(self):
        from dagfactory.parameters import BUILD_PARAMS

        forwarded = {p.key for p in BUILD_PARAMS}
        for key in ("schedule", "schedule_interval", "doc_md_file_path", "doc_md_python_callable_file"):
            assert key not in forwarded


class TestVersionFacts:
    def test_timetable_is_bounded_to_airflow_2(self):
        # Airflow 3's DAG has no `timetable` kwarg; forwarding it raises TypeError.
        assert PARAMS_BY_KEY["timetable"].max_version == "3.0.0"

    def test_deprecated_alias_is_not_bounded_by_the_kwarg_it_replaces(self):
        # `concurrency` is rewritten to max_active_tasks, which Airflow 3 still takes.
        concurrency = PARAMS_BY_KEY["concurrency"]
        assert concurrency.max_version is None
        assert concurrency.target_key == "max_active_tasks"

    def test_deprecated_since_must_parse(self):
        with pytest.raises(Exception):
            Param("x", deprecated_since="whenever")


class TestExclusiveGroups:
    """Airflow raises for mutually exclusive arguments; so do we."""

    def test_every_group_is_declared_and_has_members(self):
        from dagfactory.parameters import EXCLUSIVE_GROUPS

        members = {}
        for param in PARAMS:
            if param.exclusive_group:
                members.setdefault(param.exclusive_group, []).append(param.key)
        assert set(members) == set(EXCLUSIVE_GROUPS)
        for name, keys in members.items():
            assert len(keys) > 1, f"exclusive group '{name}' has a single member"

    def test_one_key_of_a_group_is_fine(self):
        parameters.check_exclusive_groups({"dag_id": "d", "schedule": "@daily"})

    def test_two_keys_of_a_group_raises(self):
        from dagfactory.exceptions import DagFactoryConfigException

        with pytest.raises(DagFactoryConfigException, match="Only one of"):
            parameters.check_exclusive_groups({"schedule": "@daily", "timetable": {}})

    def test_doc_md_sources_are_exclusive(self):
        from dagfactory.exceptions import DagFactoryConfigException

        with pytest.raises(DagFactoryConfigException, match="single source"):
            parameters.check_exclusive_groups({"doc_md": "x", "doc_md_file_path": "/a"})

    def test_a_deprecated_alias_is_not_an_exclusive_group(self):
        # Airflow allows concurrency and max_active_tasks together, so this
        # must not raise; _build_dag_kwargs warns and the deprecated one wins.
        parameters.check_exclusive_groups({"concurrency": 5, "max_active_tasks": 7})
        assert PARAMS_BY_KEY["concurrency"].exclusive_group is None
