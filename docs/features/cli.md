# DAG Factory CLI documentation

After installing DAG Factory, the CLI can be invoked using the `dagfactory` command.

## Commands summary

| Command    | Args   | Flags        | Description                                                          |
| ---------- | ------ | ------------ | -------------------------------------------------------------------- |
| `lint`     | `path` | `--verbose`, `--ignore`, `--airflow-version`, `--defaults-path` | Check YAML syntax and DAG parameters against the parameter metadata |
| `convert`  | `path` | `--override` | Convert YAML file(s) from Airflow 2 to 3 in the terminal or in-place |

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

Display the DAG Factory version (both the CLI and the library share the same version number):

```bash
dagfactory --version
```

Output:

```bash
DAG Factory 1.0.0a1
```

#### Check all the commands available in the CLI

Find out more about the DAG Factory command line:

```bash
dagfactory --help
```

Output:

```bash
Usage: dagfactory [OPTIONS]

DAG Factory: Dynamically build Apache Airflow DAGs from YAML files

Options:
  -v, --version  Show the version and exit.
  -h, --help     Show this message and exit.
```

## `lint` command

Check that the given file, or every YAML file under the given directory, is
valid YAML **and** a valid dag-factory config.

Each DAG is resolved the way the runtime resolves it — the external
`defaults.yml` chain, the file's own `default:` block, and the same
`DagBuilder.get_dag_params()` the builder calls — and then checked against the
parameter metadata. That reports:

- keys that are not dag-factory parameters, and keys used at the wrong level
- values of the wrong type, outside an enum, below a minimum, or not matching a pattern
- parameters the configured Airflow does not accept, in either direction
- deprecated parameters, and deprecated aliases that have a replacement
- required fields that nothing supplies, including through `defaults.yml`
- keys that may not be set together, and keys that require a companion

Tasks are checked too, with task-level semantics:

- task parameters of the wrong type, or gone in the configured Airflow
- operators and decorators that cannot be imported
- keys the operator does not accept, once it has been imported successfully
- dependencies naming a task or task group that does not exist
- cycles in the dependency graph

A task's keys are mostly operator arguments rather than dag-factory
parameters, so an unrecognised key is only reported when the operator imported
and does not accept it. If the operator cannot be imported, that is the only
thing reported for that task — there is no signature to check the rest
against.

`--airflow-version` checks against a version other than the installed one, so
an Airflow 2 deployment can be checked from an Airflow 3 environment.
`--defaults-path` sets the root to search for `defaults.yml`, as dag-factory
does at runtime; it defaults to Airflow's `dags_folder`.

Files named `defaults.yml` / `defaults.yaml` are dag-factory infrastructure
rather than DAG configs, so they are not linted in their own right. A YAML file
that defines no DAGs is left alone too.

### One table, two consumers

`dagfactory/parameters.py` holds a single dict, `PARAM_METADATA`, describing
every configuration key dag-factory accepts. `lint` reports what it says, and
`DagBuilder.build()` calls the same `parameters.check()` on every resolved
config before constructing the DAG — so a fact is stated once.

The two do not run exactly the same checks. `check()` covers the DAG body and
`default_args`, which is everything Airflow accepts silently: a parameter this
Airflow dropped, a key at the wrong level, a misspelling, a bad type on a DAG
argument. Those build a subtly wrong DAG without complaint, so the builder
checks them too.

Task-level problems are `lint`'s alone, through `check_tasks()`. Building a DAG
already raises a clear error for each of them — `ImportError` for an operator
that cannot be imported, `TypeError` for a bad or unrecognised argument,
`KeyError` for a missing dependency, `ValueError` for a cycle — so the builder
does not repeat the work, which would only report every problem twice.

Version ranges are the half-open interval `[min_version, max_version)` in PEP
440, where the upper bound is exclusive: `"3.0.0"` means gone in 3.0.

Validation during a build is logged rather than raised, so a stale entry cannot
take a deployment down:

```ini
[dag_factory]
strict_mode = True        # a validation error stops the build
validate_on_build = False # skip validation entirely
```

### Example

```bash
 dagfactory lint some/dir --verbose
```

Output:

```bash
                           DAG Factory: YAML Lint Results
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ File                 ┃ Status       ┃ Error Message                               ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ some/dir/v.yml       │ OK           │                                             │
├──────────────────────┼────────------┼---------------------------------────────────┤
│ some/dir/b.yaml      │ Syntax Error │ mapping values are not allowed here         │
│                      │              │   in "<unicode string>", line 2, column 7:  │
│                      │              │       host: localhost                       │
│                      │              │           ^                                 │
├──────────────────────┼──────────────┼─────────────────────────────────────────────┤
│ some/dir/a.yml       │ Syntax Error │ while parsing a flow sequence               │
│                      │              │   in "<unicode string>", line 2, column 5:  │
│                      │              │       - [orange, mango                      │
│                      │              │         ^                                   │
│                      │              │ expected ',' or ']', but got '<stream end>' │
│                      │              │   in "<unicode string>", line 3, column 1:  │
│                      │              │                                             │
│                      │              │     ^                                       │
└──────────────────────┴──────────────┴─────────────────────────────────────────────┘
Analysed 3 files, found 2 invalid YAML files.
```

## `convert`  command

Given a path to either a directory containing YAML files or to a path to a single YAML file, tries to convert them from Airflow 2 to 3. By default, displays the necessary changes in the terminal (default). If using the flag `--override`, changes the original files with the necessary changes.

### Example

```bash
 dagfactory convert dev/dags/airflow3
```

Output:

```bash
No changes needed: dev/dags/airflow3/example_params.yml
No changes needed: dev/dags/airflow3/example_dag_factory_multiple_config.yml
No changes needed: dev/dags/airflow3/example_task_group.yml
No changes needed: dev/dags/airflow3/example_dag_factory_default_args.yml
─────────────────────────────────────────────────── Diff for dev/dags/airflow3/example_customize_operator.yml ───────────────────────────────────────────────────
--- dev/dags/airflow3/example_customize_operator.yml
+++ dev/dags/airflow3/example_customize_operator.yml (converted)
@@ -11,7 +11,7 @@
   schedule: 0 3 * * *
   tasks:
   - task_id: begin
-    operator: airflow.operators.empty.EmptyOperator
+    operator: airflow.providers.standard.operators.empty.EmptyOperator
   - task_id: make_bread_1
     operator: customized.operators.breakfast_operators.MakeBreadOperator
     bread_type: Sourdough
@@ -30,7 +30,7 @@
     - make_bread_1
     - make_bread_2
   - task_id: end
-    operator: airflow.operators.empty.EmptyOperator
+    operator: airflow.providers.standard.operators.empty.EmptyOperator
     dependencies:
     - begin
     - make_bread_1
No changes needed: dev/dags/airflow3/example_custom_py_object_dag.yml
No changes needed: dev/dags/airflow3/example_taskflow.yml
No changes needed: dev/dags/airflow3/example_jinja2_template_dag.yml
No changes needed: dev/dags/airflow3/example_dag_factory_default_config.yml
No changes needed: dev/dags/airflow3/example_dynamic_task_mapping.yml
Tried to convert 10 files, converted 1 file, no errors found.
```
