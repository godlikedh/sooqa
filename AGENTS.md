# AGENTS.md

## Mission

Build sooqa, the self-hosted Telegram media pipeline described in
the current repository documentation, through small, reviewable increments.
The former product specification is retained as docs/reference/PROJECT_SPEC.md
for historical roadmap context.

## Before coding

1. Read README.md, the relevant active document in docs/, and relevant ADRs.
   Consult docs/reference/PROJECT_SPEC.md only for historical product and
   roadmap context.
2. Inspect the current branch and working tree.
3. Restate the exact scope and acceptance criteria in the working plan.
4. Do not implement future roadmap scope speculatively.

## Architecture rules

- Keep Inbox, Library, Publisher, Jobs, Media, Telegram, and Persistence boundaries explicit.
- PostgreSQL is the source of truth once persistence is introduced.
- Durable jobs and schedules must not live only in memory.
- Never hold database transactions across network or subprocess calls.
- External commands use argument arrays, never a shell.
- Externally retryable commands require idempotency semantics.
- Keep the single-admin MVP simple without making future boundaries impossible.
- Treat code and tests as the final authority when active documentation and
  historical roadmap text differ.

## Commands

- `just fmt`
- `just lint`
- `just test`
- `just check`

## Quality gate

Before declaring work complete:

- run the relevant checks;
- add or update tests for behavior;
- update documentation when behavior changes;
- inspect `git diff --check`;
- report any skipped check and why.

## PR rules

- One primary concern per PR.
- Keep diffs small and reviewable.
- Include why, scope, out-of-scope, testing, risks, and stack position.
- Do not merge or force-push without explicit owner instruction.
