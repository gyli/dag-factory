# taskq — an overnight work queue for Claude Code

Queue work during the day, let a scheduled job drain it at 3am, read the branches
over coffee. Nothing merges itself; nothing touches your checkout while you sleep.

```
.claude/queue/
  taskq.py       the queue: add, prioritise, transition, harvest, report
  nightly.sh     the runner: worktree → headless claude → commit → push
  config.json    scoring weights and run budget — tune these
  EXECUTOR.md    the prompt every unattended run receives
  state/         your tasks, run reports, board (git-ignored, local only)
```

## Quickstart

```bash
# 1. queue something, and clear it for unattended execution
python3 .claude/queue/taskq.py add "Give YAML parse errors a line number" \
  --value 8 --effort 2 --confidence 0.8 --autonomy auto \
  --file dagfactory/_yaml.py --verify "pytest tests/ -x -q" --body -
# (type the acceptance criteria, then ctrl-D)

# 2. see what tonight would do — changes nothing
.claude/queue/nightly.sh --dry-run

# 3. try one task in the foreground before you trust it with the night
.claude/queue/nightly.sh --max-tasks 1

# 4. schedule it
.claude/queue/nightly.sh --print-cron
```

In a Claude session you mostly use the slash commands instead: `/queue`,
`/queue-add`, `/queue-groom`, `/queue-run`.

## How a task is prioritised

```
        (value + due_urgency + aging) × confidence × risk_factor × retry_penalty
score = ───────────────────────────────────────────────────────────────────────
                              max(effort, 0.25)
```

Every input is a stored field, so `taskq.py show <id>` can always explain a score.

| Input | Meaning | Why it moves the score |
|---|---|---|
| `value` 1–10 | worth of the outcome | the numerator |
| `effort` hours | honest estimate | divides — cheap wins float up |
| `confidence` 0–1 | how well specified it is *now* | a vague task at 2am burns quota for nothing |
| `risk` low/med/high | blast radius if it goes wrong unattended | ×1.0 / ×0.9 / ×0.75 |
| `due` | a real date | ramps to +4.0 inside 14 days, full weight once overdue |
| aging | time since it was queued | +0.15/day, capped at +3.0, so nothing starves |
| `attempts` | failed tries | ×0.6 each, so a task that keeps failing stops hogging the night |

Tune any of it in `config.json`; nothing is hard-coded in the runner.

## Statuses

`proposed` → harvested, waiting for your triage. Never runs on its own.
`ready` → eligible. Runs unattended **only** if `autonomy` is `auto`.
`blocked` → waiting on a dependency, or a run stopped and asked for you.
`running` → a run has it right now.
`done` / `cancelled` → closed. `gc` archives them after 30 days.
`parked` → failed `max_attempts` times. Needs you before it runs again.

## Autonomy — the safety dial

- `auto` — unambiguous goal, contained change, a command proves it worked.
- `supervised` — real work, but it needs your judgement. Never picked by the
  nightly run; use `/queue-run` when you are at the keyboard.
- `manual` — should never run unattended at all.

Harvested tasks always land `supervised` and `proposed`. Promotion to `auto` is
always your call.

## What a night looks like

For each task, highest score first, until the task or time budget runs out:

1. `git worktree add` a fresh worktree on `claude/auto/<id>-<slug>` off the base
   branch. Your main checkout is never touched — you can keep working in it.
2. `claude -p` with the rendered `EXECUTOR.md` prompt, capped at
   `task_timeout_min`.
3. Commit anything left uncommitted, push the branch with retries. **No PR is
   opened and nothing is merged.**
4. Record the outcome and write `state/runs/<date>/<id>.md`.
5. Remove the worktree and move to the next task.

Retries get their own branch (`…-a2`) rather than force-pushing over an attempt
you may already have looked at.

The run stops early and requeues the current task **unpenalised** if it sees a
usage limit — so hitting your quota costs you nothing but the rest of the night.

In the morning: `state/runs/<date>/SUMMARY.md`, then
`git branch --list 'claude/auto/*'`.

## Scheduling

`.claude/queue/nightly.sh --print-cron` prints both a crontab line and a launchd
plist with the right paths filled in. cron is fine on Linux; on a Mac that sleeps,
launchd is more reliable (and consider `caffeinate`). The machine must be awake and
your `claude` CLI logged in — the run is a local process, not a cloud job.

Only one run happens at a time; a second invocation sees the lock and exits quietly.

## Permissions, honestly

Unattended runs cannot answer permission prompts, so the runner passes
`--permission-mode acceptEdits --allowedTools Bash Edit Write Read Glob Grep`.
That is a broad Bash grant, contained by the worktree but not sandboxed. Two
levers:

- Narrow it: `export TASKQ_CLAUDE_ARGS="--permission-mode acceptEdits --allowedTools Bash(pytest:*) Bash(git:*) Edit Read Glob Grep"`
- Widen it: `TASKQ_YOLO=1` uses `--dangerously-skip-permissions`. Only on a machine
  you would hand the keys to anyway.

Start with a couple of low-risk tasks and read the diffs before you trust a full night.

## Where state lives

`state/` is git-ignored by design: nightly runs mutate it constantly, and having
that in your history would mean merge conflicts and dirty checkouts. Point it
elsewhere (shared between clones, or backed up) with:

```bash
export TASKQ_HOME=~/.taskq/dag-factory
```

The tooling itself is committed, so it travels with the repo.

## Command reference

| | |
|---|---|
| `add <title> [fields]` | queue a task (`--body -` reads stdin) |
| `list [--status S] [--all]` | tasks by score |
| `next [--for-auto] [--format id]` | what runs next — the runner's own selector |
| `show <id>` | one task plus its score breakdown |
| `edit <id> [fields]` | change any field |
| `promote / block / cancel / requeue <id>` | move it around by hand |
| `done <id> --branch --commit` / `fail <id>` | record an outcome (the runner does this) |
| `harvest [--apply]` | scan the repo for candidate work |
| `board` / `stats` | regenerate `state/QUEUE.md` / queue health |
| `gc [--days N]` | archive old closed tasks |
| `prompt <id>` | render the exact prompt a run would receive |

## Harvesting

`harvest` scans tracked files for `TODO`/`FIXME`/`XXX`/`HACK`, skipped and xfailed
tests, and `# type: ignore` suppressions, and files anything new as `proposed`.
Candidates are fingerprinted by content, not line number, so re-running after the
code shifts does not create duplicates, and identical suppressions in one file
collapse into a single task. Add or remove scanners in `config.json`.

## Troubleshooting

**Nothing ran.** `nightly.sh --dry-run` shows the planned order; if it is empty,
you likely have no `ready` + `auto` tasks (`taskq.py list` will show them sitting
as `proposed` or `supervised`).

**A task keeps failing.** Read `state/runs/<date>/<id>.md`. Nearly always the task
description was underspecified — fix the body with `edit --body -`, then `promote`
it back from `parked`.

**A run died mid-task.** The lock clears itself on the next run if the process is
gone. Stray worktrees are cleaned by `git worktree prune`, which every run does.
