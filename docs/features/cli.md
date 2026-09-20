# DAG Factory CLI documentation

After installing DAG Factory, the CLI can be invoked using the `dagfactory` command.

## Commands summary

| Command   | Description                                                          |
| --------- | -------------------------------------------------------------------- |
| `lint`    | Validate dag-factory YAML configs (full build by default; `--schema-only` checks against the parameter metadata instead) |
| `convert` | Convert YAML file(s) from Airflow 2 to 3 in the terminal or in-place |

For more details about the available commands, run `dagfactory --help`.

## Base command usage

```bash
dagfactory [OPTIONS]
```

### Flags

| Flag        | Alias | Description                                        |
| ----------- | ----- | -------------------------------------------------- |
| `--version` |       | Show the installed version of DAG Factory and exit |
| `--help`    | `-h`  | Show this message and exit                         |

#### Identify the CLI version

```bash
dagfactory --version
```

## `lint` command

Validate DAG parameters end-to-end. The lint command accepts three input modes and two validation strategies.

### Input modes

| Input                | Example                                                  | What is validated                                                                                                                                |
| -------------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Python loader (`.py`) | `dagfactory lint dags/loader.py`                         | The loader is imported and every `load_yaml_dags(...)` invocation is captured. dag-factory's own defaults handling (defaults.yml chain, `defaults_config_dict`, etc.) runs end-to-end. |
| YAML file (`.yml`/`.yaml`) | `dagfactory lint dags/my_dag.yml`                  | Each top-level DAG entry is validated as a self-contained config. The file's own `default:` block is applied; no external defaults are merged. Since `start_date`/`tasks` are commonly supplied via an external `defaults.yml`, a DAG missing them here is reported as a warning, not an error. |
| Inline YAML          | `dagfactory lint --yaml-content "$(cat my_dag.yml)"`     | Same semantics as a YAML file, supplied as a string. Useful for editor / IDE integration.                                                          |

When a directory is passed, the walker finds all `.py` files that import `dagfactory` and lints them. Files named `defaults.yml`/`defaults.yaml` are recognised as dag-factory infrastructure and skipped with a warning. Pass `--lint-yaml-in-dir` to also include `.yml`/`.yaml` files alongside the loaders, while this should be an uncommon case, as the .py files should cover all DAGs already. If a directory has YAML configs but no `.py` loaders and `--lint-yaml-in-dir` isn't passed, lint exits non-zero rather than silently checking nothing.

### Validation strategies

| Strategy                       | Behaviour                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Build mode (default)**       | Runs the full dag-factory + Airflow pipeline. Catches operator typos, missing required args, dependency cycles, conflicting schedules, bad date strings — anything that would fail at DAG-build time. Requires every operator package referenced in the YAML to be importable in the lint environment.                                                                                                                                                              |
| **Metadata mode (`--schema-only`)** | Intercepts dag-factory before any DAG is built and checks the resolved configs against `PARAM_METADATA`. Cheaper and runs cleanly in environments that don't have every operator installed. Detects table-level issues only: removed-in-AF3 fields, deprecated parameters, missing required fields, type mismatches, unknown keys, etc.                                                                                                                                  |

### Examples

Lint a single Python loader (full build):

```bash
dagfactory lint dev/dags/airflow3/example_dag_factory.py
```

Lint a YAML config against Airflow 2 (schema-only):

```bash
dagfactory lint --schema-only --airflow-version 2 dev/dags/airflow2/example_params.yml
```

Lint inline YAML supplied from a script or editor:

```bash
dagfactory lint --schema-only --yaml-content "$(cat my_dag.yml)"
```

Lint everything under a folder, including the YAML files (Every DAG would be linted twice, one with .py file and the other one with YAML file):

```bash
dagfactory lint --lint-yaml-in-dir dev/dags/airflow3
```

## One table, two consumers

`dagfactory/parameters.py` holds a single dict, `PARAM_METADATA`, describing
every configuration key dag-factory accepts: its types, the Airflow versions it
applies to, whether it is deprecated, and whether dag-factory forwards it.
Both consumers read it, so their rules cannot drift apart:

- `dagfactory lint` renders `parameters.check()` findings as diagnostics.
- `DagBuilder.build()` calls the same `check()` on every resolved config before
  constructing the DAG, and decides from the same table which keys reach the
  `DAG` constructor.

Version ranges are the half-open interval `[min_version, max_version)`, written
as PEP 440 strings. The upper bound is exclusive, so `"3.0.0"` reads as "gone in
3.0" with no ambiguity about how many components were written.

The table also holds what a JSON document could not: `transform` is a Python
callable applied to a value before it reaches Airflow.

### Validation while building

Validation runs on each DAG's fully resolved config — after `defaults.yml` and
the `default:` block are merged — and before the `DAG` is created. Findings are
logged: warnings as warnings, errors as errors. The DAG is still built, so a
stale entry cannot take a deployment down.

```ini
[dag_factory]
strict_mode = True        # a validation error stops the build
validate_on_build = False # skip validation entirely
```

A parameter the installed Airflow does not accept is dropped with a warning
rather than passed through. That is a change in behaviour: previously
`timetable` on Airflow 3 reached `DAG()` and raised
`TypeError: DAG.__init__() got an unexpected keyword argument 'timetable'`, so
the DAG did not appear at all.

### No IDE integration

This approach has no JSON Schema file, so editors cannot validate dag-factory
YAML as you type. Feedback comes from `dagfactory lint` only.

## `convert`  command

Given a path to either a directory containing YAML files or to a path to a single YAML file, tries to convert them from Airflow 2 to 3. By default, displays the necessary changes in the terminal (default). If using the flag `--override`, changes the original files with the necessary changes.

### Example

```bash
 dagfactory convert dev/dags/airflow3
```

Output:

```bash
No changes needed: dev/dags/airflow3/example_params.yml
─────────────────────────────────────────────────── Diff for dev/dags/airflow3/example_customize_operator.yml ───────────────────────────────────────────────────
--- dev/dags/airflow3/example_customize_operator.yml
+++ dev/dags/airflow3/example_customize_operator.yml (converted)
@@ -11,7 +11,7 @@
   schedule: 0 3 * * *
   tasks:
   - task_id: begin
-    operator: airflow.operators.empty.EmptyOperator
+    operator: airflow.providers.standard.operators.empty.EmptyOperator
Tried to convert 10 files, converted 1 file, no errors found.
```
