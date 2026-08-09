# ADR 0009: Reset persistence to the five-table MVP model

## Status

Accepted by the owner in GitHub issue #43 on 2026-08-08.

## Context

The pre-reset schema grew a separate relational layer for nearly every
intermediate concept: content, assets, sources, tags, storage objects,
duplicate evidence, draft/schedule/attempt/history publication records,
generic idempotency, update receipts, and database-backed API credentials.
Those abstractions are individually defensible, but they make the single-admin
MVP difficult to reason about and force domain state across too many tables.

There is no production database and no compatibility requirement. The owner
explicitly allows existing local databases to be discarded. A fresh baseline
is therefore safer than a compatibility layer or a dual-write migration.

The active product requirements are documented in
[`docs/product.md`](../product.md). The former `PROJECT_SPEC.md` remains a
historical roadmap reference and is not authority for this reset.

## Decision

Replace the legacy application schema with one clean initial migration
containing only:

```text
queue.jobs
ingests
media
channels
posts
```

`_sqlx_migrations` remains SQLx infrastructure metadata. The new migration
must work from an empty database and be safe to rerun. It must not contain data
copy SQL, compatibility views, old-name aliases, fallback readers, or dual
writes. Existing Docker volumes are never reset automatically; operators must
run the documented destructive reset explicitly.

### Aggregate boundaries

- `ingests` is the durable import process and owns the user-visible workflow
  state, input dedupe key, bounded input data, resulting/matched media ID, and
  current error.
- `media` is the single normalized-media aggregate. It owns exact SHA-256
  deduplication, compact versioned fingerprint data, searchable fields,
  common technical metadata, source metadata, and Telegram storage state.
- `channels` owns the editable target destination and its minimal cadence
  configuration: enablement, time zone, posting window, and interval.
- `posts` owns the intended post and its actual Telegram result. It replaces
  drafts, schedules, attempts, and publication history with one fenced current
  state row.
- `queue.jobs` is the technical queue. It owns typed job payloads, `run_at`,
  bounded retries, leases, fencing tokens, dedupe keys, and current errors.

The ingest workflow advances through durable, stage-specific jobs that
reference the ingest row and are fenced by their queue lease. Each stage
transition enqueues the next stage idempotently. Independently scheduled work,
especially `publish_post`, may remain separate when its job identity
represents a real durable unit of work.

For every successful ingest stage, the ingest transition, successor enqueue,
and success of the current queue job are one database transaction. Final-attempt
lease recovery may mark the owning ingest failed only when that transaction did
not commit; it must not overwrite a committed transition with a successor.

### Idempotency and external effects

Delete the generic `idempotency_records` table and the Telegram update-receipt
table. Put uniqueness at the effect boundary: input keys on `ingests`, dedupe
keys on jobs, canonical hashes on media, request keys on posts, and generation
or fencing tokens on Telegram storage/send state.

Database transactions remain short and never span Telegram, HTTP, ffmpeg,
ffprobe, or another subprocess. A stale worker must not commit with an old
lease token. An uncertain external result becomes an explicit durable unknown
state and is reconciled intentionally rather than blindly retried. Ingest
storage is ordered after media finalization and the video identity gate.
Storage outcomes are consumed by the media identity and reconcile
linked ingests; `attach` completes storage-related waiting/failure states,
`reset` reopens them in a new generation, and `mark-unknown` makes linked
active ingests fail explicitly. Recovery of an expired final job likewise
records an explicit failure on its owning ingest.

### Configuration and authentication

The single-admin API secret, administrator Telegram user ID, bot token, and
storage chat ID are configuration/environment secrets. They are not database
entities. Target publication channels remain database rows because they are
product configuration editable by the administrator.

## Alternatives rejected

- **Compatibility migration:** rejected because there is no production data or
  client compatibility requirement, and old names would keep the discarded
  model alive.
- **Dual writes and transitional views:** rejected because they increase the
  number of state authorities and make failure recovery ambiguous.
- **Keep the generalized content/asset/publisher model:** rejected because it
  over-models the single-admin MVP before a product requirement needs those
  independent lifecycles.
- **Keep relational attempt/history/receipt tables:** rejected for current
  scope; current state belongs on the owning aggregate and historical detail
  belongs in structured logs/metrics unless a product query later requires a
  durable relation.
- **Store the queue only in memory:** rejected because ingest and publication
  must survive process restarts and be claimed safely by concurrent workers.

## Consequences

The implementation will delete substantial code and historical tests. Exact
deduplication, job claiming, lease recovery/fencing, storage ambiguity, post
ambiguity, search, and cadence correctness must be re-established against the
new rows rather than preserved through compatibility shims. Operators must
recreate local databases and volumes explicitly when adopting the reset.

The simplified model makes ownership and state transitions easier to inspect,
at the cost of deferring richer media variants, duplicate evidence history,
publication attempt history, multi-user administration, and generalized policy
features.

## Implementation status

This ADR is the authority layer for the reset. The following implementation
stack must replace the legacy migrations, Rust aggregates/repositories,
applications, API contracts, tests, and active documentation in one coherent
implementation PR. No old-schema compatibility path should be added while
doing so.
