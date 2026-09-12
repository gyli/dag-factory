#!/usr/bin/env python3
"""taskq - a priority-ordered work queue for unattended Claude Code runs.

State lives under a queue home (default: .claude/queue/state, override with
TASKQ_HOME). Tooling is committed; state is local and git-ignored, so nightly
runs never dirty your checkout or fight with branches.

Run `taskq.py --help` for the command list, or read .claude/queue/README.md.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / "config.json"
EXECUTOR_TEMPLATE = HERE / "EXECUTOR.md"

OPEN_STATUSES = ("proposed", "ready", "blocked", "running")
CLOSED_STATUSES = ("done", "cancelled", "parked")
ALL_STATUSES = OPEN_STATUSES + CLOSED_STATUSES

DEFAULT_CONFIG = {
    "scoring": {
        "age_rate": 0.15,
        "age_cap": 3.0,
        "due_max": 4.0,
        "due_horizon_days": 14,
        "min_effort": 0.25,
        "retry_penalty": 0.6,
        "risk_factor": {"low": 1.0, "medium": 0.9, "high": 0.75},
    },
    "run": {
        "max_tasks": 6,
        "task_timeout_min": 45,
        "stop_after_min": 300,
        "branch_prefix": "claude/auto/",
        "default_verify": [],
    },
    "harvest": {
        "scanners": ["todo", "skipped_test", "type_ignore"],
        "paths": ["."],
        "exclude": ["\\.claude/", "^dev/include/", "\\.lock$"],
        "max_new": 20,
    },
}


# --------------------------------------------------------------------------- #
# paths / io
# --------------------------------------------------------------------------- #
def repo_root() -> Path:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, check=True, cwd=HERE,
        )
        return Path(out.stdout.strip())
    except Exception:
        return HERE.parent.parent


def queue_home() -> Path:
    env = os.environ.get("TASKQ_HOME")
    return Path(env).expanduser().resolve() if env else HERE / "state"


HOME = queue_home()
TASKS_DIR = HOME / "tasks"
RUNS_DIR = HOME / "runs"
BOARD_PATH = HOME / "QUEUE.md"


def ensure_dirs() -> None:
    TASKS_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)


def load_config() -> dict:
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    if CONFIG_PATH.exists():
        user = json.loads(CONFIG_PATH.read_text())
        for section, values in user.items():
            if isinstance(values, dict) and isinstance(cfg.get(section), dict):
                cfg[section].update(values)
            else:
                cfg[section] = values
    return cfg


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def task_path(task_id: str) -> Path:
    return TASKS_DIR / f"{task_id}.json"


def load_task(task_id: str) -> dict:
    path = task_path(task_id)
    if not path.exists():
        die(f"no such task: {task_id}")
    return json.loads(path.read_text())


def save_task(task: dict) -> None:
    task["updated_at"] = now_iso()
    ensure_dirs()
    task_path(task["id"]).write_text(json.dumps(task, indent=2, sort_keys=True) + "\n")


def all_tasks() -> list[dict]:
    ensure_dirs()
    tasks = []
    for path in sorted(TASKS_DIR.glob("*.json")):
        try:
            tasks.append(json.loads(path.read_text()))
        except json.JSONDecodeError:
            warn(f"skipping unreadable task file: {path.name}")
    return tasks


def next_id(tasks: list[dict]) -> str:
    highest = 0
    for task in tasks:
        match = re.fullmatch(r"t-(\d+)", task.get("id", ""))
        if match:
            highest = max(highest, int(match.group(1)))
    return f"t-{highest + 1:04d}"


def die(message: str) -> "None":
    print(f"taskq: {message}", file=sys.stderr)
    raise SystemExit(1)


def warn(message: str) -> None:
    print(f"taskq: {message}", file=sys.stderr)


def log_event(task: dict, event: str, note: str = "") -> None:
    task.setdefault("history", []).append(
        {"ts": now_iso(), "event": event, "note": note}
    )


# --------------------------------------------------------------------------- #
# scoring
# --------------------------------------------------------------------------- #
def score_task(task: dict, cfg: dict, now: datetime | None = None) -> tuple[float, dict]:
    """Priority score. Higher wins.

        score = (value + due_urgency + aging) * confidence * risk * retry_penalty
                -------------------------------------------------------------
                                      max(effort, min_effort)

    Everything that feeds it is a stored field, so a score is always explainable.
    """
    s = cfg["scoring"]
    now = now or datetime.now(timezone.utc)

    value = float(task.get("value", 5))
    effort = max(float(task.get("effort", 1.0)), float(s["min_effort"]))
    confidence = float(task.get("confidence", 0.7))
    risk = s["risk_factor"].get(task.get("risk", "low"), 1.0)

    created = parse_ts(task.get("created_at")) or now
    age_days = max((now - created).total_seconds() / 86400.0, 0.0)
    aging = min(age_days * float(s["age_rate"]), float(s["age_cap"]))

    urgency = 0.0
    due_raw = task.get("due")
    if due_raw:
        try:
            due_dt = datetime.fromisoformat(due_raw).replace(tzinfo=timezone.utc)
        except ValueError:
            due_dt = None
        if due_dt:
            horizon = float(s["due_horizon_days"])
            days_left = (due_dt - now).total_seconds() / 86400.0
            if days_left <= 0:
                urgency = float(s["due_max"])
            elif days_left < horizon:
                urgency = float(s["due_max"]) * (1.0 - days_left / horizon)

    attempts = int(task.get("attempts", 0))
    penalty = float(s["retry_penalty"]) ** attempts

    numerator = (value + urgency + aging) * confidence * risk * penalty
    score = numerator / effort

    breakdown = {
        "value": round(value, 2),
        "urgency": round(urgency, 2),
        "aging": round(aging, 2),
        "confidence": round(confidence, 2),
        "risk_factor": round(risk, 2),
        "retry_penalty": round(penalty, 2),
        "effort": round(effort, 2),
        "score": round(score, 2),
    }
    return score, breakdown


def deps_met(task: dict, by_id: dict[str, dict]) -> bool:
    for dep in task.get("depends_on", []):
        blocker = by_id.get(dep)
        if blocker is None or blocker.get("status") != "done":
            return False
    return True


def eligible(task: dict, by_id: dict[str, dict], for_auto: bool) -> bool:
    if task.get("status") != "ready":
        return False
    if not deps_met(task, by_id):
        return False
    if for_auto:
        if task.get("autonomy", "supervised") != "auto":
            return False
        if int(task.get("attempts", 0)) >= int(task.get("max_attempts", 2)):
            return False
    return True


def ranked(tasks: list[dict], cfg: dict, for_auto: bool = False) -> list[tuple[float, dict]]:
    by_id = {t["id"]: t for t in tasks}
    scored = [
        (score_task(t, cfg)[0], t) for t in tasks if eligible(t, by_id, for_auto)
    ]
    scored.sort(key=lambda pair: (-pair[0], pair[1]["id"]))
    return scored


# --------------------------------------------------------------------------- #
# commands
# --------------------------------------------------------------------------- #
def cmd_add(args) -> None:
    ensure_dirs()
    tasks = all_tasks()
    body = args.body
    if body == "-":
        body = sys.stdin.read().strip()
    task = {
        "id": next_id(tasks),
        "title": args.title,
        "body": body or "",
        "status": args.status,
        "value": args.value,
        "effort": args.effort,
        "confidence": args.confidence,
        "risk": args.risk,
        "autonomy": args.autonomy,
        "tags": args.tag or [],
        "depends_on": args.depends_on or [],
        "due": args.due,
        "verify": args.verify or load_config()["run"]["default_verify"],
        "files": args.file or [],
        "source": args.source,
        "fingerprint": args.fingerprint,
        "attempts": 0,
        "max_attempts": args.max_attempts,
        "created_at": now_iso(),
        "history": [],
        "result": None,
    }
    log_event(task, "created", args.source)
    save_task(task)
    if args.quiet:
        print(task["id"])
    else:
        score, _ = score_task(task, load_config())
        print(f"added {task['id']}  score {score:.2f}  [{task['status']}]  {task['title']}")


def cmd_edit(args) -> None:
    task = load_task(args.id)
    changed = []
    simple = {
        "title": args.title, "body": args.body, "value": args.value,
        "effort": args.effort, "confidence": args.confidence, "risk": args.risk,
        "autonomy": args.autonomy, "due": args.due, "status": args.status,
        "max_attempts": args.max_attempts,
    }
    for field, value in simple.items():
        if value is not None:
            task[field] = value
            changed.append(field)
    for field, value in (("tags", args.tag), ("depends_on", args.depends_on),
                         ("verify", args.verify), ("files", args.file)):
        if value:
            task[field] = value
            changed.append(field)
    if not changed:
        die("nothing to change")
    log_event(task, "edited", ", ".join(changed))
    save_task(task)
    print(f"{task['id']} updated: {', '.join(changed)}")


def _transition(task_id: str, status: str, note: str, **extra) -> dict:
    task = load_task(task_id)
    task["status"] = status
    task.update(extra)
    log_event(task, status, note)
    save_task(task)
    return task


def cmd_promote(args) -> None:
    for task_id in args.ids:
        task = _transition(task_id, "ready", args.note or "promoted")
        print(f"{task['id']} -> ready")


def cmd_start(args) -> None:
    task = _transition(args.id, "running", args.note or "")
    print(f"{task['id']} -> running")


def cmd_done(args) -> None:
    task = load_task(args.id)
    task["status"] = "done"
    task["result"] = {
        "branch": args.branch,
        "commit": args.commit,
        "report": args.report,
        "finished_at": now_iso(),
    }
    log_event(task, "done", args.note or "")
    save_task(task)
    print(f"{task['id']} -> done")


def cmd_fail(args) -> None:
    task = load_task(args.id)
    task["attempts"] = int(task.get("attempts", 0)) + 1
    limit = int(task.get("max_attempts", 2))
    if task["attempts"] >= limit:
        task["status"] = "parked"
        outcome = f"parked after {task['attempts']} attempt(s)"
    else:
        task["status"] = "ready"
        outcome = f"requeued (attempt {task['attempts']}/{limit}, score penalised)"
    task["result"] = {"report": args.report, "failed_at": now_iso()}
    log_event(task, "failed", args.note or "")
    save_task(task)
    print(f"{task['id']} -> {task['status']}: {outcome}")


def cmd_requeue(args) -> None:
    """Put a task back in line without counting an attempt against it.

    For runs cut short by something that is not the task's fault - a usage
    limit, a machine going to sleep - so its score is not penalised."""
    task = _transition(args.id, "ready", args.note or "requeued")
    print(f"{task['id']} -> ready (attempts unchanged: {task.get('attempts', 0)})")


def cmd_block(args) -> None:
    task = _transition(args.id, "blocked", args.note or "")
    print(f"{task['id']} -> blocked")


def cmd_cancel(args) -> None:
    task = _transition(args.id, "cancelled", args.note or "")
    print(f"{task['id']} -> cancelled")


def _status_filter(args) -> list[str]:
    if args.status:
        return args.status
    return ["proposed", "ready", "blocked", "running"] if not args.all else list(ALL_STATUSES)


def cmd_list(args) -> None:
    cfg = load_config()
    tasks = all_tasks()
    by_id = {t["id"]: t for t in tasks}
    wanted = _status_filter(args)
    rows = []
    for task in tasks:
        if task.get("status") not in wanted:
            continue
        if args.tag and not set(args.tag) & set(task.get("tags", [])):
            continue
        score, _ = score_task(task, cfg)
        rows.append((score, task))
    rows.sort(key=lambda pair: (-pair[0], pair[1]["id"]))
    if args.format == "json":
        print(json.dumps([{**t, "score": round(s, 2)} for s, t in rows], indent=2))
        return
    if not rows:
        print("queue is empty")
        return
    print(f"{'ID':<8} {'SCORE':>6}  {'STATUS':<9} {'AUTO':<11} {'EFF':>5}  TITLE")
    for score, task in rows:
        flag = "" if deps_met(task, by_id) else " (deps pending)"
        print(
            f"{task['id']:<8} {score:>6.2f}  {task.get('status',''):<9} "
            f"{task.get('autonomy',''):<11} {float(task.get('effort',1)):>5.1f}  "
            f"{task.get('title','')}{flag}"
        )
    print(f"\n{len(rows)} task(s).")


def cmd_next(args) -> None:
    cfg = load_config()
    rows = ranked(all_tasks(), cfg, for_auto=args.for_auto)
    if not rows:
        if args.format == "id":
            return
        print("nothing eligible")
        raise SystemExit(2)
    picked = rows[: args.limit]
    if args.format == "id":
        for _, task in picked:
            print(task["id"])
    elif args.format == "json":
        print(json.dumps([{**t, "score": round(s, 2)} for s, t in picked], indent=2))
    else:
        for score, task in picked:
            print(f"{task['id']}  {score:6.2f}  {task['title']}")


def cmd_show(args) -> None:
    cfg = load_config()
    task = load_task(args.id)
    if args.format == "json":
        score, breakdown = score_task(task, cfg)
        print(json.dumps({**task, "score": round(score, 2), "score_breakdown": breakdown}, indent=2))
        return
    score, breakdown = score_task(task, cfg)
    print(f"{task['id']}  {task['title']}")
    print(f"  status     {task.get('status')}   autonomy {task.get('autonomy')}   "
          f"attempts {task.get('attempts', 0)}/{task.get('max_attempts', 2)}")
    print(f"  score      {score:.2f}  <- {breakdown}")
    for field in ("tags", "depends_on", "files", "verify", "due", "source"):
        if task.get(field):
            print(f"  {field:<10} {task[field]}")
    if task.get("body"):
        print("\n" + task["body"].rstrip() + "\n")
    if task.get("result"):
        print(f"  result     {json.dumps(task['result'])}")
    for entry in task.get("history", [])[-8:]:
        print(f"  {entry['ts']}  {entry['event']:<9} {entry.get('note','')}")


def cmd_prompt(args) -> None:
    task = load_task(args.id)
    cfg = load_config()
    template = EXECUTOR_TEMPLATE.read_text() if EXECUTOR_TEMPLATE.exists() else "{TITLE}\n\n{BODY}\n"
    verify = task.get("verify") or cfg["run"]["default_verify"]
    fields = {
        "{ID}": task["id"],
        "{TITLE}": task.get("title", ""),
        "{BODY}": task.get("body", "") or "(no further detail supplied)",
        "{TAGS}": ", ".join(task.get("tags", [])) or "none",
        "{FILES}": "\n".join(f"- {f}" for f in task.get("files", [])) or "- (not specified; find them)",
        "{VERIFY}": "\n".join(f"   - `{v}`" for v in verify) or "   - (none specified; use the repo's own checks)",
        "{RISK}": task.get("risk", "low"),
        "{EFFORT}": str(task.get("effort", 1)),
        "{BRANCH}": args.branch or (cfg["run"]["branch_prefix"] + task["id"]),
        "{ATTEMPT}": str(int(task.get("attempts", 0)) + 1),
        "{REPO}": str(repo_root()),
    }
    for key, value in fields.items():
        template = template.replace(key, value)
    print(template)


def cmd_board(args) -> None:
    cfg = load_config()
    tasks = all_tasks()
    by_id = {t["id"]: t for t in tasks}
    lines = [
        "# Task queue",
        "",
        f"_Generated by taskq at {now_iso()}. Do not edit by hand._",
        "",
    ]
    auto_ready = ranked(tasks, cfg, for_auto=True)
    lines += ["## Next up tonight (autonomous)", ""]
    if auto_ready:
        lines += ["| # | ID | Score | Effort | Title |", "|---|----|-------|--------|-------|"]
        for index, (score, task) in enumerate(auto_ready[: cfg["run"]["max_tasks"]], 1):
            lines.append(
                f"| {index} | {task['id']} | {score:.2f} | {float(task.get('effort', 1)):.1f}h | {task.get('title','')} |"
            )
    else:
        lines.append("_Nothing is queued for autonomous execution._")
    lines.append("")

    groups = {
        "Ready (needs you at the wheel)": lambda t: t.get("status") == "ready"
        and t.get("autonomy") != "auto",
        "Proposed (harvested, awaiting triage)": lambda t: t.get("status") == "proposed",
        "Blocked": lambda t: t.get("status") == "blocked"
        or (t.get("status") == "ready" and not deps_met(t, by_id)),
        "Parked (hit the retry limit)": lambda t: t.get("status") == "parked",
        "Running": lambda t: t.get("status") == "running",
    }
    for heading, predicate in groups.items():
        members = [t for t in tasks if predicate(t)]
        if not members:
            continue
        lines += [f"## {heading}", ""]
        for task in sorted(members, key=lambda t: -score_task(t, cfg)[0]):
            score, _ = score_task(task, cfg)
            lines.append(f"- `{task['id']}` ({score:.2f}) {task.get('title','')}")
        lines.append("")

    recent = [t for t in tasks if t.get("status") == "done"]
    recent.sort(key=lambda t: t.get("updated_at", ""), reverse=True)
    if recent:
        lines += ["## Recently done", ""]
        for task in recent[:10]:
            branch = (task.get("result") or {}).get("branch") or ""
            suffix = f" -> `{branch}`" if branch else ""
            lines.append(f"- `{task['id']}` {task.get('title','')}{suffix}")
        lines.append("")

    text = "\n".join(lines)
    ensure_dirs()
    BOARD_PATH.write_text(text)
    if not args.quiet:
        print(text)
    else:
        print(f"wrote {BOARD_PATH}")


def cmd_stats(args) -> None:
    cfg = load_config()
    tasks = all_tasks()
    counts: dict[str, int] = {}
    for task in tasks:
        counts[task.get("status", "?")] = counts.get(task.get("status", "?"), 0) + 1
    auto = ranked(tasks, cfg, for_auto=True)
    backlog_hours = sum(float(t.get("effort", 1)) for _, t in auto)
    print("status counts:")
    for status in ALL_STATUSES:
        if counts.get(status):
            print(f"  {status:<10} {counts[status]}")
    print(f"\nautonomous backlog: {len(auto)} task(s), ~{backlog_hours:.1f}h of estimated work")
    done = [t for t in tasks if t.get("status") == "done"]
    if done:
        attempts = sum(int(t.get("attempts", 0)) + 1 for t in done)
        print(f"completed: {len(done)} task(s) over {attempts} attempt(s)")
    parked = [t for t in tasks if t.get("status") == "parked"]
    if parked:
        print(f"parked (need your eyes): {', '.join(t['id'] for t in parked)}")


# --------------------------------------------------------------------------- #
# harvest
# --------------------------------------------------------------------------- #
TODO_RE = re.compile(r"(?:#|//|<!--|/\*)\s*(TODO|FIXME|XXX|HACK)\b[:\s]*(.{0,160})")
SKIP_RE = re.compile(r"@pytest\.mark\.(skipif|skip|xfail)\s*\(?(.{0,120})")
IGNORE_RE = re.compile(r"#\s*type:\s*ignore(\[[^\]]*\])?")


def tracked_files(cfg: dict) -> list[Path]:
    root = repo_root()
    try:
        out = subprocess.run(
            ["git", "ls-files"], capture_output=True, text=True, check=True, cwd=root
        ).stdout.splitlines()
    except Exception:
        return []
    excludes = [re.compile(pattern) for pattern in cfg["harvest"]["exclude"]]
    paths = []
    for rel in out:
        if any(pattern.search(rel) for pattern in excludes):
            continue
        path = root / rel
        if path.is_file() and path.suffix in {".py", ".md", ".yaml", ".yml", ".toml", ".cfg", ".sh"}:
            paths.append(path)
    return paths


def fingerprint(scanner: str, rel: str, text: str) -> str:
    normalized = re.sub(r"\s+", " ", text).strip().lower()[:160]
    return hashlib.sha1(f"{scanner}|{rel}|{normalized}".encode()).hexdigest()[:16]


def scan(cfg: dict) -> list[dict]:
    root = repo_root()
    scanners = set(cfg["harvest"]["scanners"])
    found: list[dict] = []
    for path in tracked_files(cfg):
        rel = str(path.relative_to(root))
        try:
            lines = path.read_text(errors="replace").splitlines()
        except OSError:
            continue
        for number, line in enumerate(lines, 1):
            if "todo" in scanners:
                match = TODO_RE.search(line)
                if match:
                    note = match.group(2).strip().rstrip("-->").strip()
                    found.append({
                        "scanner": "todo",
                        "title": f"Resolve {match.group(1)} in {rel}: {note[:70] or '(no note)'}",
                        "body": f"`{rel}:{number}` carries a {match.group(1)} comment:\n\n> {line.strip()}\n\n"
                                "Decide whether the note is still valid. If it is, do the work it asks for and "
                                "remove the comment; if it is stale, remove the comment and say why in the commit "
                                "message. If it needs a product decision, mark the task blocked instead of guessing.",
                        "file": rel, "value": 3, "effort": 0.5, "confidence": 0.5,
                        "risk": "low", "tags": ["cleanup", "todo"],
                        "fingerprint": fingerprint("todo", rel, line),
                    })
            if "skipped_test" in scanners and path.suffix == ".py":
                match = SKIP_RE.search(line)
                if match:
                    found.append({
                        "scanner": "skipped_test",
                        # Identical skip lines in one file collapse to a single task by
                        # fingerprint, so keep the line number out of the title.
                        "title": f"Re-enable {match.group(1)} test(s) in {rel}"
                                 + (f" ({match.group(2).strip()[:40]})" if match.group(2).strip() else ""),
                        "body": f"`{rel}:{number}` skips one or more tests:\n\n> {line.strip()}\n\n"
                                "Work out whether the reason still holds. If the test can pass now, unskip it and "
                                "prove it green; if it genuinely cannot, leave it skipped and improve the reason "
                                "string so the next reader knows why. Never delete the test.",
                        "file": rel, "value": 5, "effort": 1.0, "confidence": 0.5,
                        "risk": "medium", "tags": ["tests"],
                        "fingerprint": fingerprint("skipped_test", rel, line),
                    })
            if "type_ignore" in scanners and path.suffix == ".py":
                if IGNORE_RE.search(line):
                    found.append({
                        "scanner": "type_ignore",
                        "title": f"Remove type: ignore in {rel}:{number}",
                        "body": f"`{rel}:{number}` suppresses a type error:\n\n> {line.strip()}\n\n"
                                "Fix the underlying typing problem and drop the suppression, or narrow it to the "
                                "specific error code with a comment explaining why it must stay.",
                        "file": rel, "value": 2, "effort": 0.5, "confidence": 0.4,
                        "risk": "low", "tags": ["typing", "cleanup"],
                        "fingerprint": fingerprint("type_ignore", rel, line),
                    })
    return found


def cmd_harvest(args) -> None:
    cfg = load_config()
    existing = all_tasks()
    seen = {t.get("fingerprint") for t in existing if t.get("fingerprint")}
    candidates = []
    for candidate in scan(cfg):
        if candidate["fingerprint"] in seen:   # already queued, or a duplicate within this scan
            continue
        seen.add(candidate["fingerprint"])
        candidates.append(candidate)
    limit = args.max_new if args.max_new is not None else cfg["harvest"]["max_new"]
    candidates = candidates[:limit]

    if not candidates:
        print("harvest: nothing new")
        return
    if not args.apply:
        print(f"harvest: {len(candidates)} new candidate(s) (re-run with --apply to queue them)\n")
        for candidate in candidates:
            print(f"  [{candidate['scanner']}] {candidate['title']}")
        return

    created = []
    tasks = existing
    for candidate in candidates:
        task = {
            "id": next_id(tasks),
            "title": candidate["title"],
            "body": candidate["body"],
            "status": "proposed",
            "value": candidate["value"],
            "effort": candidate["effort"],
            "confidence": candidate["confidence"],
            "risk": candidate["risk"],
            "autonomy": "supervised",
            "tags": candidate["tags"],
            "depends_on": [],
            "due": None,
            "verify": cfg["run"]["default_verify"],
            "files": [candidate["file"]],
            "source": f"harvest:{candidate['scanner']}",
            "fingerprint": candidate["fingerprint"],
            "attempts": 0,
            "max_attempts": 2,
            "created_at": now_iso(),
            "history": [],
            "result": None,
        }
        log_event(task, "created", f"harvest:{candidate['scanner']}")
        save_task(task)
        tasks = tasks + [task]
        created.append(task)
    print(f"harvest: queued {len(created)} proposal(s) as `proposed`:")
    for task in created:
        print(f"  {task['id']}  {task['title']}")
    print("\nTriage with: taskq.py promote <id> [--autonomy auto]")


def cmd_gc(args) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    archive = HOME / "archive"
    archive.mkdir(parents=True, exist_ok=True)
    moved = 0
    for task in all_tasks():
        if task.get("status") not in ("done", "cancelled"):
            continue
        updated = parse_ts(task.get("updated_at"))
        if updated and updated < cutoff:
            task_path(task["id"]).rename(archive / f"{task['id']}.json")
            moved += 1
    print(f"gc: archived {moved} task(s) older than {args.days} day(s)")


def cmd_path(args) -> None:
    print(HOME)


# --------------------------------------------------------------------------- #
# cli
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="taskq", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    def add_task_fields(p, required_title: bool) -> None:
        if required_title:
            p.add_argument("title")
        else:
            p.add_argument("--title")
        p.add_argument("--body", help="detail / acceptance criteria ('-' reads stdin)")
        p.add_argument("--value", type=float, help="business value 1-10")
        p.add_argument("--effort", type=float, help="estimated hours")
        p.add_argument("--confidence", type=float, help="0-1: how well specified is this?")
        p.add_argument("--risk", choices=["low", "medium", "high"])
        p.add_argument("--autonomy", choices=["auto", "supervised", "manual"])
        p.add_argument("--due", help="YYYY-MM-DD")
        p.add_argument("--tag", action="append")
        p.add_argument("--depends-on", action="append")
        p.add_argument("--verify", action="append", help="command that proves the task is done")
        p.add_argument("--file", action="append", help="file the task is likely to touch")
        p.add_argument("--max-attempts", type=int)

    p_add = sub.add_parser("add", help="add a task")
    add_task_fields(p_add, required_title=True)
    p_add.add_argument("--status", default="ready", choices=list(ALL_STATUSES))
    p_add.add_argument("--source", default="manual")
    p_add.add_argument("--fingerprint", default=None)
    p_add.add_argument("--quiet", action="store_true", help="print only the new id")
    p_add.set_defaults(func=cmd_add, value=5.0, effort=1.0, confidence=0.7,
                       risk="low", autonomy="supervised", max_attempts=2)

    p_edit = sub.add_parser("edit", help="change fields on a task")
    p_edit.add_argument("id")
    add_task_fields(p_edit, required_title=False)
    p_edit.add_argument("--status", choices=list(ALL_STATUSES))
    p_edit.set_defaults(func=cmd_edit)

    p_list = sub.add_parser("list", help="list tasks by score")
    p_list.add_argument("--status", action="append", choices=list(ALL_STATUSES))
    p_list.add_argument("--tag", action="append")
    p_list.add_argument("--all", action="store_true", help="include closed tasks")
    p_list.add_argument("--format", choices=["text", "json"], default="text")
    p_list.set_defaults(func=cmd_list)

    p_next = sub.add_parser("next", help="show the highest-scoring eligible task")
    p_next.add_argument("--for-auto", action="store_true", help="only tasks cleared for unattended runs")
    p_next.add_argument("--limit", type=int, default=1)
    p_next.add_argument("--format", choices=["text", "json", "id"], default="text")
    p_next.set_defaults(func=cmd_next)

    p_show = sub.add_parser("show", help="show one task with its score breakdown")
    p_show.add_argument("id")
    p_show.add_argument("--format", choices=["text", "json"], default="text")
    p_show.set_defaults(func=cmd_show)

    p_prompt = sub.add_parser("prompt", help="render the executor prompt for a task")
    p_prompt.add_argument("id")
    p_prompt.add_argument("--branch")
    p_prompt.set_defaults(func=cmd_prompt)

    p_promote = sub.add_parser("promote", help="move proposed/blocked/parked tasks to ready")
    p_promote.add_argument("ids", nargs="+")
    p_promote.add_argument("--note")
    p_promote.set_defaults(func=cmd_promote)

    p_start = sub.add_parser("start", help="mark a task running")
    p_start.add_argument("id")
    p_start.add_argument("--note")
    p_start.set_defaults(func=cmd_start)

    p_done = sub.add_parser("done", help="mark a task done")
    p_done.add_argument("id")
    p_done.add_argument("--branch")
    p_done.add_argument("--commit")
    p_done.add_argument("--report", help="path to the run report")
    p_done.add_argument("--note")
    p_done.set_defaults(func=cmd_done)

    p_fail = sub.add_parser("fail", help="record a failed attempt (requeues or parks)")
    p_fail.add_argument("id")
    p_fail.add_argument("--report")
    p_fail.add_argument("--note")
    p_fail.set_defaults(func=cmd_fail)

    p_requeue = sub.add_parser("requeue", help="return a task to ready without counting an attempt")
    p_requeue.add_argument("id")
    p_requeue.add_argument("--note")
    p_requeue.set_defaults(func=cmd_requeue)

    p_block = sub.add_parser("block", help="mark a task blocked")
    p_block.add_argument("id")
    p_block.add_argument("--note")
    p_block.set_defaults(func=cmd_block)

    p_cancel = sub.add_parser("cancel", help="cancel a task")
    p_cancel.add_argument("id")
    p_cancel.add_argument("--note")
    p_cancel.set_defaults(func=cmd_cancel)

    p_board = sub.add_parser("board", help="regenerate QUEUE.md")
    p_board.add_argument("--quiet", action="store_true")
    p_board.set_defaults(func=cmd_board)

    p_harvest = sub.add_parser("harvest", help="scan the repo for candidate work")
    p_harvest.add_argument("--apply", action="store_true", help="queue candidates as `proposed`")
    p_harvest.add_argument("--max-new", type=int, default=None)
    p_harvest.set_defaults(func=cmd_harvest)

    p_stats = sub.add_parser("stats", help="queue health summary")
    p_stats.set_defaults(func=cmd_stats)

    p_gc = sub.add_parser("gc", help="archive old closed tasks")
    p_gc.add_argument("--days", type=int, default=30)
    p_gc.set_defaults(func=cmd_gc)

    p_path = sub.add_parser("path", help="print the queue state directory")
    p_path.set_defaults(func=cmd_path)

    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
