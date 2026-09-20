# DAG Factory CLI documentation

After installing DAG Factory, the CLI can be invoked using the `dagfactory` command.

## Commands summary

| Command   | Description                                                          |
| --------- | -------------------------------------------------------------------- |
| `lint`    | Validate dag-factory YAML configs (full build by default; `--schema-only` validates against the bundled schema instead) |
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
| YAML file (`.yml`/`.yaml`) | `dagfactory lint dags/my_dag.yml`                  | Each top-level DAG entry is validated as a self-contained config. The file's own `default:` block **and** the external `defaults.yml` chain are applied, by handing the file path to the same factory the runtime uses. Set the search root with `--defaults-path`; it defaults to Airflow's `dags_folder`. |
| Inline YAML          | `dagfactory lint --yaml-content "$(cat my_dag.yml)"`     | The string must be a **complete DAG config**: one or more top-level DAG entries with everything they need to build. A string has no location on disk, so the `defaults.yml` chain cannot be walked up from it — pass `--defaults-path` to give it a root, or anything inherited from defaults is reported as missing. |

When a directory is passed, every `.yml`/`.yaml` file under it is linted. Files named `defaults.yml`/`defaults.yaml` are dag-factory infrastructure rather than DAG configs, so they are not linted in their own right; their contents are still merged into the DAGs that inherit from them.

Linting a Python loader is not supported. The loaders exist to call `load_yaml_dags(...)`; the configuration that can actually be wrong lives in the YAML, and the defaults chain those loaders used to be needed for is now resolved directly from the YAML's path.

Nothing is softened. Whatever the validator is handed is what it checks, so a
`start_date` that no resolved `defaults.yml` supplies is an error rather than a
warning. If a config legitimately inherits one, point `--defaults-path` at the
root that holds it.

### Validation strategies

| Strategy                       | Behaviour                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Build mode (default)**       | Runs the full dag-factory + Airflow pipeline. Catches operator typos, missing required args, dependency cycles, conflicting schedules, bad date strings — anything that would fail at DAG-build time. Requires every operator package referenced in the YAML to be importable in the lint environment.                                                                                                                                                              |
| **Schema mode (`--schema-only`)** | Intercepts dag-factory before any DAG is built and validates the resolved configs against the bundled JSON schema. Cheaper and runs cleanly in environments that don't have every operator installed. Detects schema-level issues only: removed-in-AF3 fields, deprecated parameters, missing required fields, type mismatches, etc.                                                                                                                                  |

### Examples

Lint a single YAML config (full build):

```bash
dagfactory lint dev/dags/airflow3/example_dag_factory.yml
```

Lint a YAML config against Airflow 2 (schema-only):

```bash
dagfactory lint --schema-only --airflow-version 2 dev/dags/airflow2/example_params.yml
```

Lint inline YAML supplied from a script or editor, with a defaults root so
inherited values resolve:

```bash
dagfactory lint --schema-only --defaults-path dags/ --yaml-content "$(cat my_dag.yml)"
```

Lint every config under a folder:

```bash
dagfactory lint dev/dags/airflow3
```

Point the defaults search at a specific root, as dag-factory does at runtime:

```bash
dagfactory lint --defaults-path dev/dags dev/dags/example_dag_factory.yml
```

## Using the JSON schema in your IDE

The JSON schema that powers `dagfactory lint --schema-only` is bundled as a regular JSON file and can be wired into any editor that supports JSON Schema for YAML validation (VS Code via the YAML extension, JetBrains IDEs natively, Neovim with `yaml-language-server`, etc.). This gives you interactive feedback on dag-factory YAML files while you type — no Python toolchain in the loop.

The schema lives at `dagfactory/schemas/dag_parameters.json` in the installed package. To find its absolute path on your machine:

```bash
python -c "from importlib.resources import files; print(files('dagfactory.schemas') / 'dag_parameters.json')"
```

### VS Code (YAML extension)

Add to `.vscode/settings.json`:

```json
{
  "yaml.schemas": {
    "/absolute/path/to/dagfactory/schemas/dag_parameters.json": [
      "dags/**/*.yml",
      "dags/**/*.yaml"
    ]
  }
}
```

### JetBrains IDEs (PyCharm, IntelliJ, etc.)

Settings → Languages & Frameworks → Schemas and DTDs → JSON Schema Mappings → add a mapping from the schema file to your DAG YAML directory.

### Notes on standalone use

The standalone schema validates the static structure of a YAML file: required fields, value types, removed/deprecated parameters, and dag-factory-specific conventions. Used directly by an editor it cannot apply external defaults or run dag-factory's loader, so cross-file constraints and operator-import errors are only caught by `dagfactory lint`.

## One schema, two consumers

`dagfactory/schemas/dag_parameters.json` is hand maintained and is the only
description of the configuration dag-factory accepts. Both the linter and the
DAG builder read it, so the rules cannot drift apart:

- `dagfactory lint` reports its findings as errors and warnings.
- `DagBuilder.build()` validates every resolved config against the same schema,
  through the same code path, before constructing the DAG. It also decides from
  the schema which keys reach the `DAG` constructor, which Airflow versions
  accept them, and which are deprecated aliases.

A parameter's Airflow version range is therefore stated once. `x-airflow-min-version`
is read at the precision it is written (`"3"` means any Airflow 3, `"2.9"` means
2.9 or later) and `x-airflow-max-version` is inclusive in the same way. Both the
lint keywords and the builder call the same predicates in `dagfactory/schema.py`.

The one thing the schema cannot express is Python. A few values need a callable
applied before they reach Airflow; those live in `TRANSFORMS` in
`dagfactory/schema.py`, keyed by the same property names, and a test asserts
every key is a property the schema declares.

### Validation while building

Validation runs on each DAG's fully resolved config — after `defaults.yml` and
the `default:` block are merged — and before the `DAG` object is created.
Findings are logged: warnings as warnings, errors as errors. The DAG is still
built, so a stale annotation cannot take a deployment down.

Set `strict_mode` to make a schema error stop the build instead:

```ini
[dag_factory]
strict_mode = True
```

To turn build-time validation off entirely:

```ini
[dag_factory]
validate_on_build = False
```

A parameter the installed Airflow no longer accepts is dropped with a warning
rather than passed through. That is a change in behaviour: previously
`timetable` on Airflow 3 reached `DAG()` and raised
`TypeError: DAG.__init__() got an unexpected keyword argument 'timetable'`,
so the DAG did not appear at all. It now builds without the unsupported
parameter. Use `strict_mode` if you would rather the DAG fail loudly.

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
