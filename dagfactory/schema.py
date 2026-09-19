"""Access to the bundled JSON Schema, shared by DAG building and ``dagfactory lint``.

``schemas/dag_parameters.json`` is hand maintained and is the single description
of the configuration dag-factory accepts. This module loads it once and answers
the questions the DAG builder asks of it: which keys reach the ``DAG``
constructor, which are gated to particular Airflow versions, and which are
deprecated. :mod:`dagfactory.validator` reads the same file to report the same
facts as lint diagnostics, so the two cannot disagree.

The one thing the schema cannot hold is Python: a handful of values need a
callable applied before they reach Airflow. Those live in :data:`TRANSFORMS`,
keyed by the same property names, and a test asserts every key is a property
the schema declares.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Dict, FrozenSet, Optional

from packaging.version import Version

from dagfactory.utils import resolve_user_defined_macros

SCHEMA_PATH = Path(__file__).parent / "schemas" / "dag_parameters.json"

#: Sections of ``$defs`` a root property may point at and still be an Airflow
#: DAG argument. ``default_args`` is itself a ``DAG`` argument, so the root key
#: that references that section counts. Keys under ``dagfactory_only`` are
#: dag-factory's own and are never forwarded.
_DAG_ARGUMENT_SECTIONS = ("dag_only", "_shared", "default_args")

#: DAG-level keys dag-factory interprets itself rather than forwarding.
#: ``schedule`` and ``schedule_interval`` go through ``configure_schedule``;
#: ``tags`` is applied to the DAG after construction; ``timezone`` only informs
#: date parsing. Everything else the schema lists is passed straight through.
HANDLED_BY_DAGFACTORY: FrozenSet[str] = frozenset({"schedule", "schedule_interval", "tags", "timezone", "owner"})

#: Values needing a callable applied before they reach the DAG constructor.
TRANSFORMS: Dict[str, Callable[[Any], Any]] = {
    "user_defined_macros": resolve_user_defined_macros,
}


@lru_cache(maxsize=1)
def load_schema() -> Dict[str, Any]:
    """The bundled schema, parsed once per process."""
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def _section_of(schema: Dict[str, Any], key: str) -> Optional[str]:
    """Which ``$defs`` section a root property points at."""
    ref = schema["properties"].get(key, {}).get("$ref", "")
    parts = ref.split("/")
    return parts[2] if len(parts) > 2 and parts[1] == "$defs" else None


def _definition(schema: Dict[str, Any], key: str) -> Dict[str, Any]:
    """The property definition a root key resolves to."""
    section = _section_of(schema, key)
    if section is None:
        return schema["properties"].get(key, {})
    return schema["$defs"][section]["properties"].get(key, {})


@lru_cache(maxsize=1)
def dag_argument_names() -> FrozenSet[str]:
    """Root keys that map to an Airflow ``DAG`` argument.

    Excludes dag-factory's own keys and the ones it interprets itself, so what
    remains is exactly the set ``_build_dag_kwargs`` may forward.
    """
    schema = load_schema()
    return frozenset(
        key
        for key in schema["properties"]
        if _section_of(schema, key) in _DAG_ARGUMENT_SECTIONS and key not in HANDLED_BY_DAGFACTORY
    )


@lru_cache(maxsize=None)
def annotations(key: str) -> Dict[str, Any]:
    """The ``x-*`` annotations on a root key, empty if it declares none."""
    definition = _definition(load_schema(), key)
    return {name: value for name, value in definition.items() if name.startswith("x-")}


def satisfies_min_version(bound: Any, airflow_version: Version) -> bool:
    """Whether *airflow_version* is at or above an ``x-airflow-min-version`` bound.

    Bounds are written with as many components as the author needed: ``"3"``
    means any Airflow 3, ``"2.9"`` means 2.9 or later. This is the one reading
    of those annotations; :mod:`dagfactory.validator` and the DAG builder both
    call it rather than each interpreting the strings themselves.
    """
    text = str(bound).strip()
    if "." not in text:
        return airflow_version.major >= int(text)
    return airflow_version >= Version(text)


def satisfies_max_version(bound: Any, airflow_version: Version) -> bool:
    """Whether *airflow_version* is still within an ``x-airflow-max-version`` bound.

    The bound is inclusive and read at the precision it is written: ``"2"``
    allows any Airflow 2, ``"3.0"`` allows 3.0.x.
    """
    text = str(bound).strip()
    parsed = Version(text)
    if "." not in text:
        return airflow_version.major <= parsed.major
    if parsed.micro == 0 and text.count(".") == 1:
        return (airflow_version.major, airflow_version.minor) <= (parsed.major, parsed.minor)
    return airflow_version <= parsed


def deprecated_in_favor_of(key: str) -> Optional[str]:
    """The key that supersedes this one, if the schema names one."""
    return annotations(key).get("x-deprecated-in-favor-of")


def unsupported_reason(key: str, airflow_version: Version) -> Optional[str]:
    """Why *key* does not apply to *airflow_version*, or None if it does.

    Uses the same version predicates the linter applies, so a parameter the
    builder skips is one lint reports, and vice versa.
    """
    declared = annotations(key)

    minimum = declared.get("x-airflow-min-version")
    if minimum is not None and not satisfies_min_version(minimum, airflow_version):
        return (
            f"'{key}' was introduced in Airflow {minimum} (installed: {airflow_version}). "
            "The parameter will be ignored."
        )

    maximum = declared.get("x-airflow-max-version")
    if maximum is not None and not satisfies_max_version(maximum, airflow_version):
        return (
            f"'{key}' is not supported past Airflow {maximum} (installed: {airflow_version}). "
            "The parameter will be ignored."
        )

    if declared.get("x-dagfactory-supported") is False:
        return f"'{key}' is a valid Airflow DAG argument but is not wired through dag-factory."

    return None
