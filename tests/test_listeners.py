from types import SimpleNamespace

import pytest

from dagfactory.listeners.runtime_event import is_dagfactory_dag


@pytest.mark.parametrize(
    "tags,expected",
    [
        (["dagfactory"], True),
        (["dagfactory", "team-a"], True),
        (["team-a"], False),
        ([], False),
        (None, False),
    ],
)
def test_is_dagfactory_dag(tags, expected):
    assert is_dagfactory_dag(SimpleNamespace(tags=tags)) is expected


def test_is_dagfactory_dag_without_dag():
    # dag_run.get_dag() returns None when the DAG is no longer in the DagBag.
    assert is_dagfactory_dag(None) is False
    assert is_dagfactory_dag() is False
