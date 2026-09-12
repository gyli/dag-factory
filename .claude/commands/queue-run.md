---
description: Work the top queued task right now, in the foreground, so I can watch it
argument-hint: [optional: a task id to run instead of the top one]
allowed-tools: Bash(.claude/queue/nightly.sh:*), Bash(python3 .claude/queue/taskq.py:*)
---

Run the queue once, now, while I watch - same machinery as the overnight job.

If `$ARGUMENTS` names a task id, first confirm it is eligible
(`python3 .claude/queue/taskq.py show $ARGUMENTS`); if something else outranks it,
tell me and let me choose rather than reordering the queue behind my back.

Then run `.claude/queue/nightly.sh --max-tasks 1` and report what happened: the
branch it produced, what the run said, and whether the verification commands passed.
If it failed, read the report under `.claude/queue/state/runs/` and tell me whether
the task description was the problem or the code was.
