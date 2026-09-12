---
description: Add work to the queue, with the priority inputs estimated for me
argument-hint: <what you want done>
allowed-tools: Bash(python3 .claude/queue/taskq.py:*), Read, Grep, Glob
---

Add this to the queue: **$ARGUMENTS**

First spend a moment in the codebase - enough to know which files are involved and
whether the request is actually well-specified. Do not start the work.

Then queue it with `python3 .claude/queue/taskq.py add`, filling in every field you
can justify:

- `--value` 1-10: what it is worth to me if it lands. Bug in shipped behaviour or a
  release blocker is high; a cosmetic cleanup is low.
- `--effort` estimated hours. Be honest; the score divides by this, so a lowball
  estimate jumps the queue and then times out at 2am.
- `--confidence` 0-1: how well specified it is *right now*. Under 0.5 means you had
  to guess at the intent - say so in the body.
- `--risk` low/medium/high: blast radius if it goes wrong unattended.
- `--autonomy`: `auto` only if the goal is unambiguous, the change is contained, and
  a command can prove it worked. Anything needing taste, an API decision, or a
  judgement call about product behaviour is `supervised`. Use `manual` for work that
  should never run unattended.
- `--verify` one or more commands that prove it is done (repeat the flag). This is
  what the overnight run trusts instead of my eyes - a task with no verification
  should rarely be `auto`.
- `--file` for each file likely to change, `--tag` for grouping, `--depends-on` if it
  needs another task first, `--due` if there is a real date.
- `--body -` and pipe in the detail: the acceptance criteria, the constraints, and
  anything you learned in the codebase that the 2am run would otherwise rediscover.
  Write it for a session with no memory of this conversation.

Finish by showing me the new task with `show`, and tell me in one line why you set
autonomy the way you did. If the request is too vague to verify, queue it as
`--autonomy supervised --confidence 0.3` and tell me what you would need to know to
make it autonomous.
