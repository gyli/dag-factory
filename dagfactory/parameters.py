"""Canonical facts about the configuration keys dag-factory understands.

This module is the single source of truth for per-parameter metadata: which
Airflow versions a key applies to, whether it is deprecated, whether
dag-factory actually does anything with it, and how its raw YAML value becomes
a constructor argument.

Two consumers read it:

* :mod:`dagfactory.dagbuilder` iterates :data:`BUILD_PARAMS` to decide which
  keys to forward to the ``DAG`` constructor.
* ``dagfactory/schemas/dag_parameters.json``, used by ``dagfactory lint`` and by
  IDEs, is generated from :data:`DAG_PARAMS` by
  :mod:`dagfactory.schemas.generate`. A test asserts the committed file is
  byte-identical to a fresh generation, so lint rules cannot drift away from
  build behaviour.

Two conventions worth stating, because the two consumers used to disagree
about both:

Version bounds are the half-open interval ``[min_version, max_version)``,
written as PEP 440 strings. The upper bound is exclusive: ``"3.0.0"`` reads
unambiguously as "gone in 3.0", whereas an inclusive bound spelled ``"2"``
needs a rule about how many components the author wrote.

Bounds describe :attr:`DagParam.target_key`, the Airflow argument the key
ultimately feeds, not the YAML key itself. ``concurrency`` is therefore
unbounded even though Airflow 3 dropped the ``concurrency`` kwarg, because
dag-factory rewrites it to ``max_active_tasks``, which Airflow 3 still accepts.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from typing import Any, Callable, Dict, List, Mapping, Optional

from packaging.version import Version, parse as parse_version

from dagfactory.utils import resolve_user_defined_macros

#: Issue tracking the DAG arguments dag-factory does not yet forward.
UNSUPPORTED_ARGS_ISSUE = "https://github.com/astronomer/dag-factory/issues/696"


class Handling(str, Enum):
    """What dag-factory does with a configuration key."""

    #: Forwarded to the ``DAG`` constructor by ``DagBuilder._build_dag_kwargs``.
    KWARG = "kwarg"
    #: dag-factory interprets the key itself (``schedule``, ``tags``, ...).
    CUSTOM = "custom"
    #: A valid Airflow DAG argument that dag-factory silently drops.
    IGNORED = "ignored"


@lru_cache(maxsize=None)
def _parse(bound: str) -> Version:
    return parse_version(bound)


@dataclass(frozen=True)
class DagParam:
    """A DAG-level configuration key and everything dag-factory knows about it.

    :param key: the key as it appears in YAML.
    :param handling: see :class:`Handling`.
    :param min_version: lowest Airflow version accepting this key, inclusive.
    :param max_version: lowest Airflow version no longer accepting it, exclusive.
    :param deprecated_since: Airflow version that deprecated the key.
    :param deprecated_in_favor_of: the canonical key that supersedes this one.
        Entries carrying this must appear *before* the canonical entry in
        :data:`DAG_PARAMS`, so the canonical value wins when both are set.
    :param required: raise if the key is absent from the DAG config.
    :param transform: applied to the raw YAML value before it reaches the
        ``DAG`` constructor.
    :param json_schema: JSON Schema fragment describing accepted values. ``None``
        means the fragment is hand-maintained in the schema chassis instead,
        which is the case for keys shared with ``default_args``.
    :param description: prose for the generated schema. Notes that follow from
        other fields (deprecation, missing support) are generated, not written
        here.
    """

    key: str
    handling: Handling = Handling.KWARG
    min_version: Optional[str] = None
    max_version: Optional[str] = None
    deprecated_since: Optional[str] = None
    deprecated_in_favor_of: Optional[str] = None
    required: bool = False
    transform: Optional[Callable[[Any], Any]] = None
    json_schema: Optional[Mapping[str, Any]] = None
    description: Optional[str] = None

    def __post_init__(self) -> None:
        # Parse eagerly, so a malformed bound fails at import time rather than
        # silently never matching at DAG build time.
        for bound in (self.min_version, self.max_version, self.deprecated_since):
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


# Order matters twice over: a deprecated key must precede the key it defers to,
# so the canonical value overwrites the deprecated one when a config sets both;
# and the generated schema lists properties in this order.
DAG_PARAMS: List[DagParam] = [
    DagParam(
        "dag_id",
        required=True,
        description="When omitted, dag-factory uses the YAML key as the dag_id.",
        json_schema={"type": "string", "pattern": "^[A-Za-z0-9._-]+$"},
    ),
    DagParam("dag_display_name", json_schema={"type": "string"}),
    DagParam("description", json_schema={"type": "string"}),
    DagParam(
        "schedule",
        Handling.CUSTOM,
        json_schema={
            "anyOf": [
                {"type": "string"},
                {"type": "integer", "minimum": 0},
                {"type": "number"},
                {"type": "array"},
                {"type": "object"},
                {"type": "null"},
            ]
        },
    ),
    DagParam(
        "schedule_interval",
        Handling.CUSTOM,
        max_version="3.0.0",
        deprecated_since="2.4",
        description="Use `schedule` instead.",
        json_schema={
            "anyOf": [
                {"type": "string"},
                {"type": "integer", "minimum": 0},
                {"type": "number"},
                {"type": "object"},
                {"type": "null"},
            ]
        },
    ),
    DagParam(
        "timetable",
        max_version="3.0.0",
        description="Airflow 3+ accepts a Timetable instance via `schedule` instead of a separate kwarg.",
        json_schema={"type": "object"},
    ),
    DagParam("catchup", json_schema={"type": "boolean"}),
    DagParam(
        "concurrency",
        deprecated_since="2.2",
        deprecated_in_favor_of="max_active_tasks",
        json_schema={"type": "integer"},
    ),
    DagParam("max_active_tasks", json_schema={"type": "integer", "minimum": 1}),
    DagParam("max_active_runs", json_schema={"type": "integer", "minimum": 1}),
    DagParam("dagrun_timeout", json_schema={"$ref": "#/$defs/types/timedelta_like"}),
    DagParam(
        "default_view",
        max_version="3.0.0",
        description="The Airflow 3 UI does not honour this setting.",
        json_schema={
            "type": "string",
            "enum": ["grid", "graph", "duration", "gantt", "landing_times", "tree"],
        },
    ),
    DagParam(
        "orientation",
        max_version="3.0.0",
        json_schema={"type": "string", "enum": ["LR", "TB", "RL", "BT"]},
    ),
    DagParam("template_searchpath", json_schema={"$ref": "#/$defs/types/string_or_string_array"}),
    DagParam("render_template_as_native_obj", json_schema={"type": "boolean"}),
    DagParam(
        "sla_miss_callback",
        max_version="3.1.0",
        deprecated_since="2.0",
        json_schema={"$ref": "#/$defs/types/callback"},
    ),
    # The fragments for the next five live under $defs/_shared, because these
    # keys are also accepted inside default_args.
    DagParam("on_success_callback"),
    DagParam("on_failure_callback"),
    DagParam("default_args"),
    DagParam("start_date"),
    DagParam("end_date"),
    DagParam("doc_md", json_schema={"type": "string"}),
    DagParam("access_control", json_schema={"type": "object"}),
    DagParam("is_paused_upon_creation", json_schema={"type": "boolean"}),
    DagParam("params", json_schema={"type": "object"}),
    DagParam("user_defined_macros", transform=resolve_user_defined_macros, json_schema={"type": "object"}),
    DagParam("tags", Handling.CUSTOM, json_schema={"type": "array", "items": {"type": "string"}}),
    DagParam("template_undefined", Handling.IGNORED, json_schema={"type": "string"}),
    DagParam("user_defined_filters", Handling.IGNORED, json_schema={"type": "object"}),
    DagParam(
        "max_consecutive_failed_dag_runs",
        Handling.IGNORED,
        min_version="2.9",
        json_schema={"type": "integer", "minimum": 0},
    ),
    DagParam("auto_register", Handling.IGNORED, min_version="2.7", json_schema={"type": "boolean"}),
    DagParam("fail_fast", Handling.IGNORED, json_schema={"type": "boolean"}),
    DagParam("owner_links", Handling.IGNORED, min_version="2.3", json_schema={"type": "object"}),
    DagParam("jinja_environment_kwargs", Handling.IGNORED, json_schema={"type": "object"}),
    DagParam(
        "allowed_run_types",
        Handling.IGNORED,
        min_version="3.2",
        json_schema={"type": "array", "items": {"type": "string"}},
    ),
    DagParam(
        "deadline",
        Handling.IGNORED,
        min_version="3.1",
        description="Replacement for the removed SLA feature.",
        json_schema={"oneOf": [{"type": "array"}, {"type": "object"}, {"type": "null"}]},
    ),
]

DAG_PARAMS_BY_KEY: Dict[str, DagParam] = {param.key: param for param in DAG_PARAMS}

if len(DAG_PARAMS_BY_KEY) != len(DAG_PARAMS):
    raise RuntimeError("DAG_PARAMS contains duplicate keys")

#: The subset ``DagBuilder._build_dag_kwargs`` forwards to the DAG constructor.
BUILD_PARAMS: List[DagParam] = [param for param in DAG_PARAMS if param.handling is Handling.KWARG]
