---
description: Harvest new candidate work, triage the proposals, and rebalance priorities
allowed-tools: Bash(python3 .claude/queue/taskq.py:*), Read, Grep, Glob
---

Groom the queue. Work through this in order:

1. **Harvest.** Run `python3 .claude/queue/taskq.py harvest --apply`. New candidates
   land as `proposed` and never run on their own.

2. **Triage the proposals.** Run `list --status proposed`. For each one, look at the
   code it points at and decide:
   - Real work, well specified, verifiable → `promote <id>`, and `edit` it to
     sensible value/effort/confidence. Set `--autonomy auto` only if you would be
     comfortable with the diff landing on a branch unsupervised.
   - Real but needs my judgement → `promote` and leave it `supervised`.
   - Stale, already done, or not worth doing → `cancel <id> --note "<why>"`.
   Batch your reasoning; do not ask me about each one.

3. **Rebalance what is already queued.** Run `list` and look for:
   - Tasks whose `confidence` no longer matches reality (the code moved on).
   - Anything `parked` after two failed attempts - read its run report under
     `.claude/queue/state/runs/` and either fix the task description so the next
     attempt can succeed, or cancel it.
   - `blocked` tasks whose blocker is now `done` - promote them.
   - High-value work sitting at a low score because the effort estimate is inflated.

4. **Regenerate the board** with `board --quiet` and give me a summary: what you
   promoted, what you cancelled and why, and what tonight's run will pick up. Flag
   anything you were unsure about rather than quietly deciding it.
