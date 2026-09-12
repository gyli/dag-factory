#!/usr/bin/env bash
#
# nightly.sh - drain the task queue with headless Claude Code while you sleep.
#
#   .claude/queue/nightly.sh              # work the queue until the budget runs out
#   .claude/queue/nightly.sh --dry-run    # show what it would do, touch nothing
#   .claude/queue/nightly.sh --max-tasks 2
#   .claude/queue/nightly.sh --print-cron # scheduling snippets for cron / launchd
#
# Each task is worked in its own git worktree on its own branch, so your main
# checkout is never touched. Branches are pushed; no PRs are opened.
set -uo pipefail

QUEUE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$QUEUE_DIR" rev-parse --show-toplevel)"
TASKQ="python3 $QUEUE_DIR/taskq.py"
STATE_DIR="$($TASKQ path)"
WORKTREE_ROOT="${TASKQ_WORKTREES:-${TMPDIR:-/tmp}/taskq-worktrees}"
LOCK_DIR="$STATE_DIR/.lock"
RUN_DATE="$(date +%Y-%m-%d)"
RUN_DIR="$STATE_DIR/runs/$RUN_DATE"
SUMMARY="$RUN_DIR/SUMMARY.md"

DRY_RUN=0
MAX_TASKS=""
STOP_AFTER_MIN=""

cfg() { python3 -c "
import json,sys
cfg=json.load(open('$QUEUE_DIR/config.json'))
print(cfg.get('run',{}).get('$1', '$2'))
" 2>/dev/null || echo "$2"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --max-tasks) MAX_TASKS="$2"; shift 2 ;;
    --stop-after-min) STOP_AFTER_MIN="$2"; shift 2 ;;
    --print-cron) PRINT_CRON=1; shift ;;
    -h|--help) sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "nightly: unknown option $1" >&2; exit 64 ;;
  esac
done

if [ "${PRINT_CRON:-0}" = "1" ]; then
  cat <<CRON
# --- cron (Linux, or macOS with the machine awake) -------------------------
# Run at 00:30 every night. Edit with: crontab -e
30 0 * * * cd $REPO_ROOT && $QUEUE_DIR/nightly.sh >> $STATE_DIR/nightly.log 2>&1

# --- launchd (macOS, survives sleep better) --------------------------------
# Save as ~/Library/LaunchAgents/com.taskq.nightly.plist then:
#   launchctl load ~/Library/LaunchAgents/com.taskq.nightly.plist
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>com.taskq.nightly</string>
  <key>ProgramArguments</key>
  <array><string>$QUEUE_DIR/nightly.sh</string></array>
  <key>WorkingDirectory</key><string>$REPO_ROOT</string>
  <key>StartCalendarInterval</key><dict><key>Hour</key><integer>0</integer><key>Minute</key><integer>30</integer></dict>
  <key>StandardOutPath</key><string>$STATE_DIR/nightly.log</string>
  <key>StandardErrorPath</key><string>$STATE_DIR/nightly.log</string>
</dict></plist>
CRON
  exit 0
fi

[ -n "$MAX_TASKS" ] || MAX_TASKS="$(cfg max_tasks 6)"
[ -n "$STOP_AFTER_MIN" ] || STOP_AFTER_MIN="$(cfg stop_after_min 300)"
TASK_TIMEOUT_MIN="$(cfg task_timeout_min 45)"
BRANCH_PREFIX="$(cfg branch_prefix claude/auto/)"

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*"; }

# --- single-instance lock (portable: mkdir is atomic everywhere) ------------
mkdir -p "$STATE_DIR" "$RUN_DIR"
if [ "$DRY_RUN" = "0" ]; then
  if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    if [ -f "$LOCK_DIR/pid" ] && ! kill -0 "$(cat "$LOCK_DIR/pid")" 2>/dev/null; then
      log "clearing stale lock from pid $(cat "$LOCK_DIR/pid")"
      rm -rf "$LOCK_DIR"; mkdir "$LOCK_DIR" 2>/dev/null || { log "another run holds the lock; exiting"; exit 0; }
    else
      log "another run is in progress; exiting"; exit 0
    fi
  fi
  echo $$ > "$LOCK_DIR/pid"
  trap 'rm -rf "$LOCK_DIR"' EXIT INT TERM
fi

# --- timeout helper (GNU timeout, gtimeout, or a bash fallback) -------------
run_limited() {
  local secs="$1"; shift
  if command -v timeout >/dev/null 2>&1; then timeout "$secs" "$@"; return $?; fi
  if command -v gtimeout >/dev/null 2>&1; then gtimeout "$secs" "$@"; return $?; fi
  "$@" & local pid=$! waited=0
  while kill -0 "$pid" 2>/dev/null; do
    [ "$waited" -ge "$secs" ] && { kill -TERM "$pid" 2>/dev/null; sleep 2; kill -KILL "$pid" 2>/dev/null; return 124; }
    sleep 5; waited=$((waited + 5))
  done
  wait "$pid"
}

if [ "$DRY_RUN" = "0" ] && ! command -v claude >/dev/null 2>&1; then
  log "the \`claude\` CLI is not on PATH"; exit 127
fi

BASE_BRANCH="$(git -C "$REPO_ROOT" symbolic-ref --quiet --short refs/remotes/origin/HEAD 2>/dev/null | sed 's@^origin/@@')"
[ -n "$BASE_BRANCH" ] || BASE_BRANCH="$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD)"

log "queue state: $STATE_DIR"
log "base branch: $BASE_BRANCH | budget: ${MAX_TASKS} task(s) / ${STOP_AFTER_MIN} min"

if [ "$DRY_RUN" = "0" ]; then
  git -C "$REPO_ROOT" fetch --quiet origin "$BASE_BRANCH" 2>/dev/null || log "warning: could not fetch origin/$BASE_BRANCH; using local ref"
fi

# Surface new candidates as `proposed` (never auto-run: triage is yours).
[ "$DRY_RUN" = "0" ] && { $TASKQ harvest --apply >/dev/null 2>&1 || true; }

# A dry run cannot advance the queue (nothing is executed, so nothing completes).
# Show the planned order instead, then exit without touching state.
if [ "$DRY_RUN" = "1" ]; then
  log "[dry-run] planned order for tonight (top $MAX_TASKS):"
  $TASKQ next --for-auto --limit "$MAX_TASKS" --format text | sed 's/^/    /'
  FIRST="$($TASKQ next --for-auto --format id | head -1)"
  if [ -n "$FIRST" ]; then
    log "[dry-run] prompt that would be sent for $FIRST:"
    $TASKQ prompt "$FIRST" | sed 's/^/    | /'
  fi
  log "dry run complete; no state was changed"
  exit 0
fi

START_EPOCH=$(date +%s)
DONE_COUNT=0 FAIL_COUNT=0 BLOCKED_COUNT=0 WORKED=0
if [ -s "$SUMMARY" ]; then
  { echo; echo "---"; echo; echo "## Run started $(date +%H:%M)"; echo; } >> "$SUMMARY"
else
  { echo "# Overnight run $RUN_DATE"; echo; } > "$SUMMARY"
fi

while [ "$WORKED" -lt "$MAX_TASKS" ]; do
  ELAPSED_MIN=$(( ( $(date +%s) - START_EPOCH ) / 60 ))
  if [ "$ELAPSED_MIN" -ge "$STOP_AFTER_MIN" ]; then
    log "time budget spent (${ELAPSED_MIN}m); stopping"; break
  fi

  TASK_ID="$($TASKQ next --for-auto --format id | head -1)"
  [ -n "$TASK_ID" ] || { log "no eligible tasks left"; break; }

  TITLE="$(python3 -c "
import json
print(json.load(open('$STATE_DIR/tasks/$TASK_ID.json'))['title'])")"
  ATTEMPTS="$(python3 -c "
import json
print(json.load(open('$STATE_DIR/tasks/$TASK_ID.json')).get('attempts', 0))")"
  SLUG="$(printf '%s' "$TITLE" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9' '-' | cut -c1-40 | sed 's/-*$//')"
  BRANCH="${BRANCH_PREFIX}${TASK_ID}-${SLUG}"
  # A retry gets its own branch: force-pushing over the previous attempt could
  # throw away work you have already looked at or committed on top of.
  [ "$ATTEMPTS" -gt 0 ] && BRANCH="${BRANCH}-a$((ATTEMPTS + 1))"
  WORKTREE="$WORKTREE_ROOT/$TASK_ID"

  log "=== $TASK_ID  $TITLE"
  $TASKQ start "$TASK_ID" >/dev/null

  rm -rf "$WORKTREE"; mkdir -p "$WORKTREE_ROOT"
  if ! git -C "$REPO_ROOT" worktree add --quiet -B "$BRANCH" "$WORKTREE" "$BASE_BRANCH" 2>&1; then
    log "could not create worktree for $TASK_ID"
    $TASKQ fail "$TASK_ID" --note "worktree creation failed" >/dev/null
    FAIL_COUNT=$((FAIL_COUNT + 1)); WORKED=$((WORKED + 1)); continue
  fi

  REPORT="$RUN_DIR/$TASK_ID.md"
  PROMPT="$($TASKQ prompt "$TASK_ID" --branch "$BRANCH")"
  CLAUDE_ARGS=(${TASKQ_CLAUDE_ARGS:---permission-mode acceptEdits --allowedTools Bash Edit Write Read Glob Grep})
  [ "${TASKQ_YOLO:-0}" = "1" ] && CLAUDE_ARGS=(--dangerously-skip-permissions)

  log "running claude (timeout ${TASK_TIMEOUT_MIN}m)"
  RAW="$RUN_DIR/$TASK_ID.json"
  ( cd "$WORKTREE" && run_limited "$((TASK_TIMEOUT_MIN * 60))" \
      claude -p "$PROMPT" --output-format json "${CLAUDE_ARGS[@]}" ) > "$RAW" 2>"$RUN_DIR/$TASK_ID.err"
  RC=$?

  RESULT_TEXT="$(python3 - "$RAW" <<'PYEOF'
import json, sys
raw = open(sys.argv[1], errors="replace").read()
try:
    data = json.loads(raw)
except Exception:
    # Not JSON (older CLI, a crash, a partial write): show what we got anyway.
    print(raw.strip()[-4000:] or "(no output from claude)"); raise SystemExit
if isinstance(data, list):
    data = data[-1] if data else {}
print(str(data.get("result") or data.get("error") or "(empty result)").strip())
PYEOF
)"
  STATUS_LINE="$(printf '%s' "$RESULT_TEXT" | grep -iEm1 '^STATUS:' | sed 's/^[Ss][Tt][Aa][Tt][Uu][Ss]: *//' | tr '[:upper:]' '[:lower:]')"

  {
    echo "# $TASK_ID - $TITLE"; echo
    echo "- branch: \`$BRANCH\`"; echo "- exit code: $RC"; echo "- finished: $(date)"; echo
    echo "## Claude's report"; echo; echo "$RESULT_TEXT"
  } > "$REPORT"

  # Commit anything the run left uncommitted, then push.
  COMMIT=""
  if [ -n "$(git -C "$WORKTREE" status --porcelain)" ]; then
    git -C "$WORKTREE" add -A
    git -C "$WORKTREE" -c user.name="taskq nightly" -c user.email="noreply@anthropic.com" \
      commit --quiet -m "wip($TASK_ID): uncommitted changes from overnight run" || true
  fi
  PUSHED=0
  if [ -n "$(git -C "$WORKTREE" log --oneline "$BASE_BRANCH..$BRANCH" 2>/dev/null)" ]; then
    COMMIT="$(git -C "$WORKTREE" rev-parse --short HEAD)"
    PUSH_LOG="$RUN_DIR/$TASK_ID.push.log"
    for attempt in 1 2 3 4; do
      if git -C "$WORKTREE" push --quiet -u origin "$BRANCH" >"$PUSH_LOG" 2>&1; then PUSHED=1; break; fi
      if grep -qiE 'rejected|non-fast-forward|denied|authentication|does not appear to be a git repo' "$PUSH_LOG"; then
        log "push rejected (not a network failure); the branch stays local - see $PUSH_LOG"
        break
      fi
      log "push failed (attempt $attempt); retrying"; sleep $((2 ** attempt))
    done
    [ "$PUSHED" = "1" ] || echo "- push: FAILED, branch exists locally only" >> "$REPORT"
  fi

  if printf '%s' "$RESULT_TEXT" | grep -qiE 'usage limit|rate limit|quota (exceeded|reached)|out of (tokens|credits)'; then
    log "hit a usage limit; stopping the run"
    $TASKQ requeue "$TASK_ID" --note "run stopped: usage limit reached" >/dev/null
    echo "- \`$TASK_ID\` **not started** - usage limit reached, requeued unpenalised" >> "$SUMMARY"
    git -C "$REPO_ROOT" worktree remove --force "$WORKTREE" 2>/dev/null
    break
  fi

  case "$RC:$STATUS_LINE" in
    0:done)
      $TASKQ done "$TASK_ID" --branch "$BRANCH" --commit "$COMMIT" --report "$REPORT" >/dev/null
      DONE_COUNT=$((DONE_COUNT + 1))
      [ "$PUSHED" = "1" ] && PUSH_NOTE="" || PUSH_NOTE=" _(local only - push failed)_"
      echo "- \`$TASK_ID\` **done** - $TITLE -> \`$BRANCH\` ($COMMIT)$PUSH_NOTE" >> "$SUMMARY" ;;
    0:blocked|0:no-change)
      $TASKQ block "$TASK_ID" --note "run reported: ${STATUS_LINE:-blocked}" >/dev/null
      BLOCKED_COUNT=$((BLOCKED_COUNT + 1))
      echo "- \`$TASK_ID\` **${STATUS_LINE}** - $TITLE (see $REPORT)" >> "$SUMMARY" ;;
    124:*)
      $TASKQ fail "$TASK_ID" --report "$REPORT" --note "timed out after ${TASK_TIMEOUT_MIN}m" >/dev/null
      FAIL_COUNT=$((FAIL_COUNT + 1))
      echo "- \`$TASK_ID\` **timed out** after ${TASK_TIMEOUT_MIN}m - $TITLE" >> "$SUMMARY" ;;
    *)
      $TASKQ fail "$TASK_ID" --report "$REPORT" --note "exit $RC, status '${STATUS_LINE:-unknown}'" >/dev/null
      FAIL_COUNT=$((FAIL_COUNT + 1))
      echo "- \`$TASK_ID\` **failed** (exit $RC) - $TITLE (see $REPORT)" >> "$SUMMARY" ;;
  esac

  git -C "$REPO_ROOT" worktree remove --force "$WORKTREE" 2>/dev/null || rm -rf "$WORKTREE"
  WORKED=$((WORKED + 1))
  log "--- $TASK_ID finished (rc=$RC, status=${STATUS_LINE:-unknown})"
done

git -C "$REPO_ROOT" worktree prune
$TASKQ board --quiet >/dev/null
{
  echo
  echo "**$DONE_COUNT done, $BLOCKED_COUNT blocked, $FAIL_COUNT failed** in $(( ( $(date +%s) - START_EPOCH ) / 60 )) minutes."
  echo
  echo "Review with: \`git branch --list '${BRANCH_PREFIX}*'\` - nothing was merged or opened as a PR."
} >> "$SUMMARY"

log "done: $DONE_COUNT ok, $BLOCKED_COUNT blocked, $FAIL_COUNT failed"
log "morning report: $SUMMARY"
