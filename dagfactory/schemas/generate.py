"""Generates ``dag_parameters.json`` from the canonical parameter registry.

Every property in the schema comes from :data:`dagfactory.parameters.PARAMS`.
There is no hand-maintained JSON to keep in step: the document envelope and the
section layout live here, the parameters live in Python, and the lint rules and
the DAG build behaviour therefore cannot drift apart.

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

from dagfactory.parameters import (
    CROSS_FIELD_RULES,
    PARAMS,
    TYPES,
    UNSUPPORTED_ARGS_ISSUE,
    Handling,
    Origin,
    Param,
    Scope,
)

SCHEMA_PATH = Path(__file__).parent / "dag_parameters.json"

SCHEMA_ID = "https://github.com/astronomer/dag-factory/schemas/dag_parameters.json"
TITLE = "DAG Factory DAG Parameters"
DESCRIPTION = (
    "Schema for the parameters DAG Factory accepts in YAML configuration. Most properties map to "
    "airflow.sdk.DAG or BaseOperator arguments; dag-factory's own conventions are marked as such in "
    "their descriptions. Generated from dagfactory/parameters.py -- edit that, then run "
    "`python -m dagfactory.schemas.generate`. See "
    "https://astronomer.github.io/dag-factory/latest/features/cli/ for the custom x-* annotations "
    "used by the lint command."
)

#: Where a parameter's definition is written. Everything else refers to it.
#: A key valid in several scopes is defined once, in the first that applies.
_SECTION_BY_SCOPE = [(Scope.DAG, "dag"), (Scope.TASK, "task"), (Scope.DEFAULT_ARGS, "default_args")]

_UNSUPPORTED_NOTE = f"Not yet wired through dag-factory (see {UNSUPPORTED_ARGS_ISSUE})."


def section_of(param: Param) -> str:
    """The ``$defs`` section holding this parameter's definition."""
    for scope, section in _SECTION_BY_SCOPE:
        if scope in param.scope:
            return section
    raise ValueError(f"'{param.key}' belongs to no scope")


def ref_to(param: Param) -> Dict[str, str]:
    return {"$ref": f"#/$defs/{section_of(param)}/properties/{param.key}"}


def _description(param: Param) -> str:
    """Prose for one property: the author's sentence plus any generated notes."""
    sentences = []
    if param.origin is Origin.DAGFACTORY:
        sentences.append("(dag-factory only)")
    if param.description:
        sentences.append(param.description)
    if param.deprecated_in_favor_of:
        sentences.append(f"Deprecated alias for {param.deprecated_in_favor_of}.")
    if param.handling is Handling.IGNORED:
        sentences.append(_UNSUPPORTED_NOTE)
    return " ".join(sentences)


def property_for(param: Param) -> Dict[str, Any]:
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
    """Returns the complete schema, derived entirely from the registry."""
    defs: Dict[str, Any] = {"types": TYPES}
    for _, section in _SECTION_BY_SCOPE:
        defs[section] = {"properties": {}}

    for param in PARAMS:
        defs[section_of(param)]["properties"][param.key] = property_for(param)

    # A section a key is valid in but not defined in refers back to the definition.
    for scope, section in _SECTION_BY_SCOPE:
        properties = defs[section]["properties"]
        for param in PARAMS:
            if scope in param.scope and param.key not in properties:
                properties[param.key] = ref_to(param)

    # default_args is validated as a subschema in its own right; the other
    # sections are definition bags whose members are referenced individually.
    defs["default_args"].update({"type": "object", "additionalProperties": True})
    defs["default_args"].update(CROSS_FIELD_RULES.get(Scope.DEFAULT_ARGS, {}))

    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": SCHEMA_ID,
        "title": TITLE,
        "description": DESCRIPTION,
        "type": "object",
        "$defs": defs,
        "properties": {param.key: ref_to(param) for param in PARAMS if Scope.DAG in param.scope},
        "additionalProperties": True,
        "required": [param.key for param in PARAMS if param.required_in_yaml],
    }
    schema.update(CROSS_FIELD_RULES.get(Scope.DAG, {}))
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
