"""Canonical facts about the configuration keys dag-factory understands.

This module is the single source of truth for per-parameter metadata: which
Airflow versions a key applies to, whether it is deprecated, whether it is
required, and how its raw YAML value is turned into a constructor argument.

Two consumers read it:

* ``dagfactory.dagbuilder`` iterates :data:`PARAMS` to decide which keys to
  forward to the ``DAG`` constructor.
* the JSON Schema used by ``dagfactory lint`` is generated from it, so the lint
  rules and the build behaviour cannot drift apart.

Version bounds are a half-open interval ``[min_version, max_version)``, written
as PEP 440 strings. The upper bound is exclusive on purpose: ``"3.0.0"`` reads
unambiguously as "gone in 3.0", whereas an inclusive bound spelled ``"2"`` has
to be interpreted against however many components the author happened to write.

Conflicting keys follow Airflow, which treats two cases differently:

* Genuinely mutually exclusive arguments raise. Airflow 2's ``DAG`` raises
  ``ValueError("At most one allowed for args 'schedule_interval',
  'timetable', and 'schedule'.")``, and Airflow 3's ``BaseSensorOperator``
  raises for ``soft_fail``/``never_fail``. :data:`EXCLUSIVE_GROUPS` does the
  same, with ``DagFactoryConfigException``.
* A deprecated alias does not raise. Airflow 2 warns and then assigns
  ``max_active_tasks = concurrency``, so setting both is legal and the
  deprecated value wins. ``deprecated_in_favor_of`` reproduces that.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional

from packaging.version import Version, parse as parse_version

from dagfactory.exceptions import DagFactoryConfigException
from dagfactory.utils import resolve_user_defined_macros


class Handling(Enum):
    """What dag-factory does with a configuration key."""

    #: Forwarded to the ``DAG`` constructor by ``DagBuilder._build_dag_kwargs``.
    KWARG = "kwarg"
    #: dag-factory interprets the key itself (``schedule``, ``doc_md_file_path``).
    CUSTOM = "custom"


@lru_cache(maxsize=None)
def _parse(bound: str) -> Version:
    return parse_version(bound)


@dataclass(frozen=True)
class Param:
    """A DAG-level configuration key and everything dag-factory knows about it.

    :param key: the key as it appears in YAML, and the ``DAG`` constructor kwarg
        it maps to (unless ``deprecated_in_favor_of`` redirects it).
    :param min_version: lowest Airflow version that accepts this key, inclusive.
    :param max_version: lowest Airflow version that no longer accepts it, exclusive.
    :param deprecated_in_favor_of: the canonical key that supersedes this one.
        Setting both is allowed and the deprecated value wins, matching
        Airflow, which warns and assigns ``max_active_tasks = concurrency``
        rather than refusing the pair.
    :param exclusive_group: name of an :data:`EXCLUSIVE_GROUPS` entry. Setting
        more than one key of a group raises, matching Airflow's own handling of
        mutually exclusive arguments.
    :param required: raise if the key is absent from the DAG config.
    :param transform: applied to the raw YAML value before it reaches the
        ``DAG`` constructor.
    """

    key: str
    handling: Handling = Handling.KWARG
    min_version: Optional[str] = None
    max_version: Optional[str] = None
    deprecated_in_favor_of: Optional[str] = None
    exclusive_group: Optional[str] = None
    required: bool = False
    transform: Optional[Callable[[Any], Any]] = None

    def __post_init__(self) -> None:
        # Parse eagerly, so a malformed bound fails at import time rather than
        # silently never matching at DAG build time.
        for bound in (self.min_version, self.max_version):
            if bound is not None:
                _parse(bound)

    @property
    def target_key(self) -> str:
        """The ``DAG`` constructor kwarg this parameter writes to."""
        return self.deprecated_in_favor_of or self.key

    def unsupported_reason(self, airflow_version: Version) -> Optional[str]:
        """Return why this key does not apply to *airflow_version*, or None if it does."""
        if self.min_version is not None and airflow_version < _parse(self.min_version):
            return (
                f"DAG parameter '{self.key}' requires Airflow >= {self.min_version} "
                f"(installed: {airflow_version}). The parameter will be ignored."
            )
        if self.max_version is not None and airflow_version >= _parse(self.max_version):
            return (
                f"DAG parameter '{self.key}' is not supported in Airflow >= {self.max_version} "
                f"(installed: {airflow_version}). The parameter will be ignored."
            )
        return None


# Order is presentational. Alias precedence is explicit in _build_dag_kwargs,
# which applies deprecated keys last so they win, as Airflow does.
PARAMS: List[Param] = [
    Param("dag_id", required=True),
    Param("dag_display_name"),
    Param("description"),
    Param("concurrency", deprecated_in_favor_of="max_active_tasks"),
    Param("max_active_tasks"),
    Param("catchup"),
    Param("max_active_runs"),
    Param("dagrun_timeout"),
    Param("default_view", max_version="3.0.0"),
    Param("orientation", max_version="3.0.0"),
    Param("template_searchpath"),
    Param("render_template_as_native_obj"),
    Param("sla_miss_callback", max_version="3.1.0"),
    Param("on_success_callback"),
    Param("on_failure_callback"),
    Param("default_args"),
    Param("doc_md", exclusive_group="doc_md"),
    Param("access_control"),
    Param("is_paused_upon_creation"),
    Param("params"),
    Param("start_date"),
    Param("end_date"),
    # Airflow 3 dropped the `timetable` kwarg; a Timetable instance goes via
    # `schedule` instead. Forwarding it on Airflow 3 raises TypeError.
    Param("timetable", max_version="3.0.0", exclusive_group="schedule"),
    Param("user_defined_macros", transform=resolve_user_defined_macros),
    # Interpreted by dag-factory rather than forwarded. Listed here so the
    # mutually exclusive checks can see them.
    Param("schedule", Handling.CUSTOM, exclusive_group="schedule"),
    Param("schedule_interval", Handling.CUSTOM, max_version="3.0.0", exclusive_group="schedule"),
    Param("doc_md_file_path", Handling.CUSTOM, exclusive_group="doc_md"),
    Param("doc_md_python_callable_file", Handling.CUSTOM, exclusive_group="doc_md"),
]

#: Mutually exclusive groups. Membership is derived from the ``exclusive_group``
#: field on each entry; only the wording lives here. ``{fields}`` interpolates
#: the derived member list.
EXCLUSIVE_GROUPS: Dict[str, str] = {
    "schedule": "Only one of {fields} may be set.",
    "doc_md": "Pick a single source for `doc_md`: an inline string, a file path, or a python callable.",
}

PARAMS_BY_KEY: Dict[str, Param] = {param.key: param for param in PARAMS}

if len(PARAMS_BY_KEY) != len(PARAMS):
    raise RuntimeError("PARAMS contains duplicate keys")


#: The subset ``DagBuilder._build_dag_kwargs`` forwards to the DAG constructor.
BUILD_PARAMS: List[Param] = [param for param in PARAMS if param.handling is Handling.KWARG]


def check_exclusive_groups(config: Dict[str, Any]) -> None:
    """Raise if a config sets more than one key of a mutually exclusive group.

    Mirrors Airflow, which raises rather than silently picking a winner. Note
    that a deprecated alias is deliberately *not* handled here: Airflow allows
    ``concurrency`` and ``max_active_tasks`` together and lets the deprecated
    one win, so that pairing warns instead of raising.
    """
    members: Dict[str, List[str]] = {}
    for param in PARAMS:
        if param.exclusive_group and param.key in config:
            members.setdefault(param.exclusive_group, []).append(param.key)

    for group, keys in members.items():
        if len(keys) > 1:
            listed = ", ".join(f"`{key}`" for key in keys)
            raise DagFactoryConfigException(f"{EXCLUSIVE_GROUPS[group].format(fields=listed)} (set: {', '.join(keys)})")
