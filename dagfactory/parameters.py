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
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional

from packaging.version import Version, parse as parse_version

from dagfactory.utils import resolve_user_defined_macros


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
        Entries carrying this must appear *before* the canonical entry in
        :data:`PARAMS`, so that the canonical value wins when both are set.
    :param required: raise if the key is absent from the DAG config.
    :param transform: applied to the raw YAML value before it reaches the
        ``DAG`` constructor.
    """

    key: str
    min_version: Optional[str] = None
    max_version: Optional[str] = None
    deprecated_in_favor_of: Optional[str] = None
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


# Order matters: a deprecated key must precede the key it defers to, so that the
# canonical value overwrites the deprecated one when a config sets both.
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
    Param("doc_md"),
    Param("access_control"),
    Param("is_paused_upon_creation"),
    Param("params"),
    Param("start_date"),
    Param("end_date"),
    # Airflow 3 dropped the `timetable` kwarg; a Timetable instance goes via
    # `schedule` instead. Forwarding it on Airflow 3 raises TypeError.
    Param("timetable", max_version="3.0.0"),
    Param("user_defined_macros", transform=resolve_user_defined_macros),
]

PARAMS_BY_KEY: Dict[str, Param] = {param.key: param for param in PARAMS}

if len(PARAMS_BY_KEY) != len(PARAMS):
    raise RuntimeError("PARAMS contains duplicate keys")
