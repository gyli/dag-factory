---
description: Show the task queue - what runs tonight, what needs triage, what came back
argument-hint: [optional: a status filter like proposed/blocked/parked]
allowed-tools: Bash(python3 .claude/queue/taskq.py:*), Bash(cat:*), Bash(ls:*), Read
---

Show me the state of the work queue.

1. Run `python3 .claude/queue/taskq.py board --quiet` to refresh the board, then
   `python3 .claude/queue/taskq.py stats`.
2. If `$ARGUMENTS` names a status, also run
   `python3 .claude/queue/taskq.py list --status $ARGUMENTS`.
3. If there is a report from last night under `.claude/queue/state/runs/`, read the
   most recent `SUMMARY.md` and tell me what happened while I was asleep.

Then give me a short read of the queue in prose - not a dump of the table. I want
to know: what will run tonight and roughly how long it will take, anything that came
back blocked or parked and why, and whether anything is starving (high value, low
score, sitting for days). If something needs a decision from me, ask for it.
