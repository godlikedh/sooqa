# sooqa product authority

This document is the active product authority for the architecture reset
recorded in [ADR 0009](adr/0009-five-table-persistence-reset.md) and GitHub
issue #43. It supersedes the old persistence model and the historical roadmap
when they conflict. Until the implementation stack lands, the checked-out
code and `docs/architecture.md` describe the pre-reset baseline; they are not
permission to preserve the discarded model during the reset.

## Product

sooqa is a self-hosted, single-admin Telegram media pipeline. The backend
accepts media through the private HTTP API or the administrator's Telegram
interaction, processes it durably, exact-deduplicates normalized media, stores
new media in one private Telegram storage channel, and lets the administrator
publish stored media to configured target channels.

The first release is intentionally narrow:

- one administrator and one self-hosted installation;
- PostgreSQL as the source of truth;
- direct HTTP media plus the already-supported Telegram ingest paths;
- durable download, probe, normalization, fingerprint, duplicate-check, and
  storage workflow;
- searchable stored media with captions/descriptions and normalized tags;
- immediate publication or a simple per-channel cadence queue.

## Target persistence model

The durable application model has four product tables and one technical queue
table:

```text
queue.jobs

ingests --------> media <-------- posts --------> channels
```

The SQLx migration table remains infrastructure-owned. The target application
tables are:

- `queue.jobs`: one durable job queue with `run_at`, retry count, lease/fencing
  token, lifecycle state, error fields, and an optional unique dedupe key;
- `ingests`: one durable import process with a unique `input_key`, bounded
  input data, workflow state, optional resulting/matched `media_id`, and
  current error/timestamps;
- `media`: one normalized stored media item with canonical SHA-256, compact
  versioned fingerprint data and video search tokens, searchable text/tags,
  common media properties, source metadata, Telegram storage identifiers, and
  storage ambiguity state;
- `channels`: one target Telegram channel with enablement, IANA time zone,
  posting window, and cadence interval;
- `posts`: one intended Telegram post and its eventual result, including
  caption snapshot, scheduled time, state, Telegram message ID, and current
  send fencing/ambiguity fields.

One normalized media item is one row. The target model does not retain separate
content, asset, source, tag-join, storage-object, draft, schedule, attempt,
publication-history, or duplicate-candidate aggregates.

## Durable workflow rules

The ingest process is the product state machine. It advances through durable,
stage-specific jobs that reference the ingest row and are fenced by their
queue lease; each stage transition enqueues the next stage idempotently. Media
storage is downstream of media finalization: video fingerprinting and
similarity checking finish before the upload job is created. A storage result
is consumed by `media_id`, so success and failure cannot be lost because an
ingest has not reached `storing` yet. `storage_unknown` is an explicit
reconciliation state: attach completes linked storage-waiting or storage-failed
ingests, while reset opens them in a new storage generation. Marking an upload
unknown also fails linked active ingests explicitly instead of leaving them
waiting forever. Independently scheduled work such as `publish_post` and
maintenance remains separate.

For ingest stages, the product transition, successor enqueue, and current-job
success are committed in one database transaction. Final-attempt lease recovery
fails the owning ingest only when that atomic success transaction did not
commit.

Lease heartbeats and terminal job mutations require an unexpired lease. When a
final attempt expires, recovery fails the owning ingest explicitly (or marks
storage unknown for an upload job) so a crashed worker cannot strand the
workflow.

Post cadence slots are assigned when a post is queued. A `publish_post` job
references the `posts` row, and one post row becomes the durable publication
record after success. Telegram calls, HTTP downloads, ffmpeg, and ffprobe run
outside database transactions. External effects use state plus generation or
fencing tokens, and ambiguous effects are retained for explicit reconciliation
instead of being blindly retried.

## Idempotency ownership

Idempotency remains required, but it belongs to the row or effect it protects:

| Effect | Durable protection |
| --- | --- |
| Receive the same input | `ingests.input_key UNIQUE` |
| Enqueue the same work | `queue.jobs.dedupe_key UNIQUE` |
| Store identical normalized bytes | `media.canonical_sha256 UNIQUE` |
| Upload to Telegram storage | media storage state plus generation/token |
| Create the same requested post | `posts.request_key UNIQUE` |
| Send a post | post state plus generation/token; ambiguous sends do not auto-retry |

There is no generic idempotency table and no permanent Telegram update-receipt
table. Repeated creates may return the existing resource; updates should be
naturally idempotent setters where possible.

Issue #44 adds the versioned `video_sequence_v1` media foundation in a stacked
implementation. Its first slice is storage-safe fingerprint encoding, token
shortlisting, and bounded alignment; the active worker identity gate,
`duplicate_pending` decision, and force-save API are the dependent slice.

## Single-admin security

The bot token, API secret, administrator Telegram user ID, and storage chat ID
come from configuration/environment secrets and never from PostgreSQL or Git.
Target publication channels remain database rows because they are editable
product destinations. The API remains private and authenticated; the reset
does not broaden exposure or add multi-user behavior.

## Explicit non-goals

This reset does not add:

- compatibility with old databases, API snapshots, repository interfaces, or
  local data;
- data-copy SQL, compatibility views, old-name aliases, or dual writes;
- multiple administrators, users, tenants, storage providers, albums, media
  variants, derivative assets, or a generalized content taxonomy;
- richer duplicate-interaction UX or a production-active perceptual duplicate
  decision; issue #44's dependent workflow slice owns that gate;
- Grafana/Prometheus deployment;
- Telegram publication functionality beyond behavior already present at the
  selected implementation base.

Existing local databases may be discarded explicitly by the owner. No tool or
test may reset a Docker volume automatically. The implementation must provide
documented, explicit reset instructions and must verify the new model from an
empty database.
