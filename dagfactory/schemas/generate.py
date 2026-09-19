"""Generates ``dag_parameters.json`` from the canonical parameter registry.

The schema is a mix of two things. The DAG-level property definitions come from
:data:`dagfactory.parameters.DAG_PARAMS`, so the lint rules and the DAG build
behaviour cannot drift apart. Everything else, the reusable type definitions,
task and ``default_args`` properties, and the cross-field rules, is hand
maintained in ``_chassis.json`` and copied through untouched.

``dag_parameters.json`` is committed to the repository: IDEs point at it
directly, and ``dagfactory lint`` reads it without importing Airflow.
``tests/test_schema_generation.py`` fails if it falls out of date.

Regenerate with::

    python -m dagfactory.schemas.generate
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from dagfactory.parameters import DAG_PARAMS, UNSUPPORTED_ARGS_ISSUE, DagParam, Handling

HERE = Path(__file__).parent
CHASSIS_PATH = HERE / "_chassis.json"
SCHEMA_PATH = HERE / "dag_parameters.json"

DAG_ONLY_REF = "#/$defs/dag_only/properties/{key}"

_UNSUPPORTED_NOTE = f"Not yet wired through dag-factory (see {UNSUPPORTED_ARGS_ISSUE})."


def _description(param: DagParam) -> str:
    """Prose for one property: the author's sentence plus any generated notes."""
    sentences = []
    if param.description:
        sentences.append(param.description)
    if param.deprecated_in_favor_of:
        sentences.append(f"Deprecated alias for {param.deprecated_in_favor_of}.")
    if param.handling is Handling.IGNORED:
        sentences.append(_UNSUPPORTED_NOTE)
    return " ".join(sentences)


def property_for(param: DagParam) -> Dict[str, Any]:
    """The JSON Schema property definition for one registry entry."""
    definition: Dict[str, Any] = dict(param.json_schema or {})

    description = _description(param)
    if description:
        definition["description"] = description

    if param.min_version:
        definition["x-airflow-min-version"] = param.min_version
    if param.max_version:
        definition["x-airflow-max-version"] = param.max_version
    if param.deprecated_since:
        definition["x-deprecated-since"] = param.deprecated_since
    if param.handling is Handling.IGNORED:
        definition["x-dagfactory-supported"] = False

    return definition


def build_schema() -> Dict[str, Any]:
    """Returns the full schema: chassis plus the generated DAG-level properties."""
    schema: Dict[str, Any] = json.loads(CHASSIS_PATH.read_text())

    generated = {param.key: property_for(param) for param in DAG_PARAMS if param.json_schema is not None}
    schema["$defs"]["dag_only"]["properties"] = generated

    # Root properties reference the definitions above. Registry order wins for
    # the generated keys; chassis-owned keys keep their hand-written position.
    root: Dict[str, Any] = {key: {"$ref": DAG_ONLY_REF.format(key=key)} for key in generated}
    for key, value in schema["properties"].items():
        root.setdefault(key, value)
    schema["properties"] = root

    return schema


def render() -> str:
    # Keys are sorted to match the repository's pretty-format-json hook, so a
    # regeneration and a `pre-commit run` cannot fight over the same file.
    return json.dumps(build_schema(), indent=2, sort_keys=True) + "\n"


def write() -> None:
    SCHEMA_PATH.write_text(render())


if __name__ == "__main__":  # pragma: no cover
    write()
    print(f"wrote {SCHEMA_PATH}")
