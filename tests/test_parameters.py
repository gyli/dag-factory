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

        assert dagbuilder.PARAMS is parameters.PARAMS


class TestVersionFacts:
    def test_timetable_is_bounded_to_airflow_2(self):
        # Airflow 3's DAG has no `timetable` kwarg, so forwarding it raises
        # TypeError rather than being ignored.
        assert PARAMS_BY_KEY["timetable"].max_version == "3.0.0"
