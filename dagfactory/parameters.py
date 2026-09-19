"""Canonical facts about every configuration key dag-factory understands.

This module is the single source of truth. Nothing about a parameter is
written down anywhere else:

* :mod:`dagfactory.dagbuilder` iterates :data:`BUILD_PARAMS` to decide which
  keys to forward to the ``DAG`` constructor.
* ``dagfactory/schemas/dag_parameters.json``, used by ``dagfactory lint`` and by
  IDEs, is generated in full from :data:`PARAMS` by
  :mod:`dagfactory.schemas.generate`. A test asserts the committed file is
  byte-identical to a fresh generation, so lint rules cannot drift away from
  build behaviour.

Three conventions, each of which the two hand-written definitions this
replaces used to disagree about:

**Version bounds** are the half-open interval ``[min_version, max_version)``,
written as PEP 440 strings. The upper bound is exclusive: ``"3.0.0"`` reads
unambiguously as "gone in 3.0", whereas an inclusive bound spelled ``"2"``
needs a rule about how many components the author wrote.

**Bounds describe** :attr:`Param.target_key`, the Airflow argument the key
ultimately feeds, not the YAML key itself. ``concurrency`` is therefore
unbounded even though Airflow 3 dropped the ``concurrency`` kwarg, because
dag-factory rewrites it to ``max_active_tasks``, which Airflow 3 still accepts.

**Support is derived, never declared.** ``x-dagfactory-supported: false`` in
the generated schema comes from :attr:`Handling.IGNORED`, so a key cannot be
wired up in the builder while the linter still calls it unsupported.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, Flag, auto
from functools import lru_cache
from typing import Any, Callable, Dict, List, Mapping, Optional

from packaging.version import Version, parse as parse_version

from dagfactory.utils import resolve_user_defined_macros

#: Issue tracking the DAG arguments dag-factory does not yet forward.
UNSUPPORTED_ARGS_ISSUE = "https://github.com/astronomer/dag-factory/issues/696"


class Scope(Flag):
    """Where in a config a key may appear. A key may be valid in several."""

    #: At the top level of a DAG's YAML.
    DAG = auto()
    #: Inside a task definition.
    TASK = auto()
    #: Inside the ``default_args`` mapping.
    DEFAULT_ARGS = auto()


class Origin(Enum):
    """Whether the key is an Airflow argument or a dag-factory invention."""

    AIRFLOW = "airflow"
    DAGFACTORY = "dagfactory"


class Handling(str, Enum):
    """What dag-factory does with a configuration key."""

    #: Forwarded to the ``DAG`` constructor by ``DagBuilder._build_dag_kwargs``.
    KWARG = "kwarg"
    #: dag-factory interprets the key itself (``schedule``, ``tasks``, ...).
    CUSTOM = "custom"
    #: A valid Airflow argument that dag-factory silently drops.
    IGNORED = "ignored"


@lru_cache(maxsize=None)
def _parse(bound: str) -> Version:
    return parse_version(bound)


@dataclass(frozen=True)
class Param:
    """A configuration key and everything dag-factory knows about it.

    :param key: the key as it appears in YAML.
    :param scope: where the key may appear; see :class:`Scope`.
    :param handling: see :class:`Handling`.
    :param origin: see :class:`Origin`.
    :param min_version: lowest Airflow version accepting this key, inclusive.
    :param max_version: lowest Airflow version no longer accepting it, exclusive.
    :param deprecated_since: Airflow version that deprecated the key.
    :param deprecated_in_favor_of: the canonical key that supersedes this one.
        Entries carrying this must appear *before* the canonical entry in
        :data:`PARAMS`, so the canonical value wins when both are set.
    :param required: the builder needs this key present in the resolved config.
        Distinct from ``required_in_yaml``: ``dag_id`` is required here because
        ``_build_dag_kwargs`` cannot proceed without it, but dag-factory fills
        it in from the YAML key, so an author need not write it.
    :param required_in_yaml: the author must write this key; the linter reports
        its absence.
    :param transform: applied to the raw YAML value before it reaches the
        ``DAG`` constructor.
    :param json_schema: JSON Schema fragment describing accepted values.
    :param description: prose for the generated schema. Notes that follow from
        other fields (deprecation, missing support) are generated, not written
        here.
    """

    key: str
    scope: Scope = Scope.DAG
    handling: Handling = Handling.KWARG
    origin: Origin = Origin.AIRFLOW
    min_version: Optional[str] = None
    max_version: Optional[str] = None
    deprecated_since: Optional[str] = None
    deprecated_in_favor_of: Optional[str] = None
    required: bool = False
    required_in_yaml: bool = False
    transform: Optional[Callable[[Any], Any]] = None
    json_schema: Optional[Mapping[str, Any]] = None
    description: Optional[str] = None

    def __post_init__(self) -> None:
        # Parse eagerly, so a malformed bound fails at import time rather than
        # silently never matching at DAG build time.
        for bound in (self.min_version, self.max_version, self.deprecated_since):
            if bound is not None:
                _parse(bound)
        if self.handling is Handling.KWARG and Scope.DAG not in self.scope:
            raise ValueError(f"'{self.key}' is not a DAG-level key, so it cannot be forwarded to DAG()")

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


#: Reusable JSON Schema fragments, referenced by ``$ref`` from param entries.
TYPES: Dict[str, Any] = {
    "callback": {
        "description": "Python callable spec: import-path string, dict spec, or list of either.",
        "oneOf": [{"type": "string"}, {"type": "object"}, {"type": "array"}],
    },
    "timedelta_like": {
        "description": "Duration: string ('5 minutes'), seconds as integer or float, or a {seconds: N}-style dict.",
        "anyOf": [
            {"type": "string"},
            {"type": "integer", "minimum": 0},
            {"type": "number"},
            {"type": "object"},
        ],
    },
    "string_or_string_array": {
        "oneOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}],
    },
    "mapping_or_list": {"oneOf": [{"type": "array"}, {"type": "object"}]},
}

CALLBACK = {"$ref": "#/$defs/types/callback"}
TIMEDELTA_LIKE = {"$ref": "#/$defs/types/timedelta_like"}
STRING_OR_STRING_ARRAY = {"$ref": "#/$defs/types/string_or_string_array"}
MAPPING_OR_LIST = {"$ref": "#/$defs/types/mapping_or_list"}

TASK_LEVEL = Scope.TASK | Scope.DEFAULT_ARGS
DAG_AND_DEFAULTS = Scope.DAG | Scope.DEFAULT_ARGS

# Order matters twice over: a deprecated key must precede the key it defers to,
# so the canonical value overwrites the deprecated one when a config sets both;
# and the generated schema lists properties in this order.
PARAMS: List[Param] = [
    # ------------------------------------------------------------------
    # DAG-level, forwarded to DAG()
    # ------------------------------------------------------------------
    Param(
        "dag_id",
        required=True,
        description="When omitted, dag-factory uses the YAML key as the dag_id.",
        json_schema={"type": "string", "pattern": "^[A-Za-z0-9._-]+$"},
    ),
    Param("dag_display_name", json_schema={"type": "string"}),
    Param("description", json_schema={"type": "string"}),
    Param(
        "timetable",
        max_version="3.0.0",
        description="Airflow 3+ accepts a Timetable instance via `schedule` instead of a separate kwarg.",
        json_schema={"type": "object"},
    ),
    Param("catchup", json_schema={"type": "boolean"}),
    Param(
        "concurrency",
        deprecated_since="2.2",
        deprecated_in_favor_of="max_active_tasks",
        json_schema={"type": "integer"},
    ),
    Param("max_active_tasks", json_schema={"type": "integer", "minimum": 1}),
    Param("max_active_runs", json_schema={"type": "integer", "minimum": 1}),
    Param("dagrun_timeout", json_schema=TIMEDELTA_LIKE),
    Param(
        "default_view",
        max_version="3.0.0",
        description="The Airflow 3 UI does not honour this setting.",
        json_schema={"type": "string", "enum": ["grid", "graph", "duration", "gantt", "landing_times", "tree"]},
    ),
    Param("orientation", max_version="3.0.0", json_schema={"type": "string", "enum": ["LR", "TB", "RL", "BT"]}),
    Param("template_searchpath", json_schema=STRING_OR_STRING_ARRAY),
    Param("render_template_as_native_obj", json_schema={"type": "boolean"}),
    Param("sla_miss_callback", max_version="3.1.0", deprecated_since="2.0", json_schema=CALLBACK),
    Param("doc_md", json_schema={"type": "string"}),
    Param("access_control", json_schema={"type": "object"}),
    Param("is_paused_upon_creation", json_schema={"type": "boolean"}),
    Param("params", json_schema={"type": "object"}),
    Param("user_defined_macros", transform=resolve_user_defined_macros, json_schema={"type": "object"}),
    Param("default_args", json_schema={"$ref": "#/$defs/default_args"}),
    # ------------------------------------------------------------------
    # DAG-level, interpreted by dag-factory rather than forwarded
    # ------------------------------------------------------------------
    Param(
        "schedule",
        handling=Handling.CUSTOM,
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
    Param(
        "schedule_interval",
        handling=Handling.CUSTOM,
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
    Param("tags", handling=Handling.CUSTOM, json_schema={"type": "array", "items": {"type": "string"}}),
    # ------------------------------------------------------------------
    # DAG-level, valid Airflow arguments dag-factory does not wire through
    # ------------------------------------------------------------------
    Param("template_undefined", handling=Handling.IGNORED, json_schema={"type": "string"}),
    Param("user_defined_filters", handling=Handling.IGNORED, json_schema={"type": "object"}),
    Param(
        "max_consecutive_failed_dag_runs",
        handling=Handling.IGNORED,
        min_version="2.9",
        json_schema={"type": "integer", "minimum": 0},
    ),
    Param("auto_register", handling=Handling.IGNORED, min_version="2.7", json_schema={"type": "boolean"}),
    Param("fail_fast", handling=Handling.IGNORED, json_schema={"type": "boolean"}),
    Param("owner_links", handling=Handling.IGNORED, min_version="2.3", json_schema={"type": "object"}),
    Param("jinja_environment_kwargs", handling=Handling.IGNORED, json_schema={"type": "object"}),
    Param(
        "allowed_run_types",
        handling=Handling.IGNORED,
        min_version="3.2",
        json_schema={"type": "array", "items": {"type": "string"}},
    ),
    Param(
        "deadline",
        handling=Handling.IGNORED,
        min_version="3.1",
        description="Replacement for the removed SLA feature.",
        json_schema={"oneOf": [{"type": "array"}, {"type": "object"}, {"type": "null"}]},
    ),
    # ------------------------------------------------------------------
    # Valid on the DAG body and inside default_args
    # ------------------------------------------------------------------
    Param(
        "start_date",
        DAG_AND_DEFAULTS,
        description="May be set on the DAG body or under default_args.",
        json_schema={"type": "string"},
    ),
    Param(
        "end_date",
        DAG_AND_DEFAULTS,
        description="May be set on the DAG body or under default_args.",
        json_schema={"type": "string"},
    ),
    Param("on_success_callback", DAG_AND_DEFAULTS, json_schema=CALLBACK),
    Param("on_failure_callback", DAG_AND_DEFAULTS, json_schema=CALLBACK),
    # ------------------------------------------------------------------
    # dag-factory's own keys
    # ------------------------------------------------------------------
    Param(
        "tasks",
        handling=Handling.CUSTOM,
        origin=Origin.DAGFACTORY,
        required_in_yaml=True,
        description="Mapping or list of task definitions; not an Airflow DAG argument.",
        json_schema=MAPPING_OR_LIST,
    ),
    Param(
        "task_groups",
        handling=Handling.CUSTOM,
        origin=Origin.DAGFACTORY,
        description="Mapping or list of task group definitions; not an Airflow DAG argument.",
        json_schema=MAPPING_OR_LIST,
    ),
    Param(
        "timezone",
        DAG_AND_DEFAULTS,
        handling=Handling.CUSTOM,
        origin=Origin.DAGFACTORY,
        description="Timezone for parsing start_date / end_date; not passed to Airflow's DAG().",
        json_schema={"type": "string"},
    ),
    Param(
        "doc_md_file_path",
        handling=Handling.CUSTOM,
        origin=Origin.DAGFACTORY,
        description="Path to a markdown file whose contents are assigned to dag.doc_md.",
        json_schema={"type": "string"},
    ),
    Param(
        "doc_md_python_callable_file",
        handling=Handling.CUSTOM,
        origin=Origin.DAGFACTORY,
        description="Path to a Python file containing a callable that returns markdown.",
        json_schema={"type": "string"},
    ),
    Param(
        "doc_md_python_callable_name",
        handling=Handling.CUSTOM,
        origin=Origin.DAGFACTORY,
        description="Name of the callable inside doc_md_python_callable_file.",
        json_schema={"type": "string"},
    ),
    Param(
        "doc_md_python_arguments",
        handling=Handling.CUSTOM,
        origin=Origin.DAGFACTORY,
        description="Keyword arguments passed to doc_md_python_callable_name.",
        json_schema={"type": "object"},
    ),
    # ------------------------------------------------------------------
    # Task-level, also accepted inside default_args
    # ------------------------------------------------------------------
    Param("owner", Scope.DEFAULT_ARGS, handling=Handling.CUSTOM, json_schema={"type": "string"}),
    Param("email", TASK_LEVEL, handling=Handling.CUSTOM, json_schema=STRING_OR_STRING_ARRAY),
    Param("email_on_failure", TASK_LEVEL, handling=Handling.CUSTOM, json_schema={"type": "boolean"}),
    Param("email_on_retry", TASK_LEVEL, handling=Handling.CUSTOM, json_schema={"type": "boolean"}),
    Param("retries", TASK_LEVEL, handling=Handling.CUSTOM, json_schema={"type": "integer", "minimum": 0}),
    Param("retry_delay", TASK_LEVEL, handling=Handling.CUSTOM, json_schema=TIMEDELTA_LIKE),
    Param("retry_exponential_backoff", TASK_LEVEL, handling=Handling.CUSTOM, json_schema={"type": "boolean"}),
    Param("max_retry_delay", TASK_LEVEL, handling=Handling.CUSTOM, json_schema=TIMEDELTA_LIKE),
    Param("depends_on_past", TASK_LEVEL, handling=Handling.CUSTOM, json_schema={"type": "boolean"}),
    Param("wait_for_downstream", TASK_LEVEL, handling=Handling.CUSTOM, json_schema={"type": "boolean"}),
    Param("queue", TASK_LEVEL, handling=Handling.CUSTOM, json_schema={"type": "string"}),
    Param("pool", TASK_LEVEL, handling=Handling.CUSTOM, json_schema={"type": "string"}),
    Param("pool_slots", TASK_LEVEL, handling=Handling.CUSTOM, json_schema={"type": "integer", "minimum": 1}),
    Param("priority_weight", TASK_LEVEL, handling=Handling.CUSTOM, json_schema={"type": "integer"}),
    Param("weight_rule", TASK_LEVEL, handling=Handling.CUSTOM, json_schema={"type": "string"}),
    Param("execution_timeout", TASK_LEVEL, handling=Handling.CUSTOM, json_schema=TIMEDELTA_LIKE),
    Param("trigger_rule", TASK_LEVEL, handling=Handling.CUSTOM, json_schema={"type": "string"}),
    Param(
        "sla",
        TASK_LEVEL,
        handling=Handling.CUSTOM,
        max_version="3.1.0",
        deprecated_since="2.0",
        json_schema=TIMEDELTA_LIKE,
    ),
    Param("on_retry_callback", TASK_LEVEL, handling=Handling.CUSTOM, json_schema=CALLBACK),
    Param("on_execute_callback", TASK_LEVEL, handling=Handling.CUSTOM, json_schema=CALLBACK),
]

#: Rules that span more than one key, so they belong to no single entry,
#: keyed by the scope whose subschema carries them.
CROSS_FIELD_RULES: Dict[Scope, Dict[str, Any]] = {
    Scope.DAG: {
        "dependentRequired": {
            "doc_md_python_callable_file": ["doc_md_python_callable_name"],
            "doc_md_python_callable_name": ["doc_md_python_callable_file"],
            "doc_md_python_arguments": ["doc_md_python_callable_name"],
        },
        "x-mutually-exclusive": [
            {
                "fields": ["schedule", "schedule_interval", "timetable"],
                "message": "Only one of `schedule`, `schedule_interval`, or `timetable` may be set.",
            },
            {
                "fields": ["concurrency", "max_active_tasks"],
                "message": "`concurrency` is a deprecated alias for `max_active_tasks`; do not set both.",
            },
            {
                "fields": ["doc_md", "doc_md_file_path", "doc_md_python_callable_file"],
                "message": "Pick a single source for `doc_md`: an inline string, a file path, or a python callable.",
            },
        ],
        "x-required-anywhere": [
            {
                "fields": ["start_date", "default_args.start_date"],
                "message": "A `start_date` must be set on the DAG body or under default_args.",
            },
        ],
    },
    Scope.DEFAULT_ARGS: {
        "dependentRequired": {"retry_exponential_backoff": ["max_retry_delay"]},
    },
}

PARAMS_BY_KEY: Dict[str, Param] = {param.key: param for param in PARAMS}

if len(PARAMS_BY_KEY) != len(PARAMS):
    raise RuntimeError("PARAMS contains duplicate keys")

#: The subset ``DagBuilder._build_dag_kwargs`` forwards to the DAG constructor.
BUILD_PARAMS: List[Param] = [param for param in PARAMS if param.handling is Handling.KWARG]
