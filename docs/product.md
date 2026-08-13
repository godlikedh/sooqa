# sooqa product authority

This document is the active product authority for the architecture reset
recorded in [ADR 0009](adr/0009-five-table-persistence-reset.md) and GitHub
issue #43. It supersedes the old persistence model and the historical roadmap
when they conflict. The checked-out code and `docs/architecture.md` describe
the shipped consolidated model; this document remains the product authority
for behavior and scope.

## Product

sooqa is a self-hosted, single-admin Telegram media pipeline. The backend
accepts media through the private HTTP API or the administrator's Telegram
interaction, processes it durably, exact-deduplicates normalized media, stores
new media in one private Telegram storage channel, and lets the administrator
publish stored media to configured target channels.

The first release is intentionally narrow:

- one administrator and one self-hosted installation;
- PostgreSQL as the source of truth;
- direct HTTP media, explicitly allowlisted public YouTube/Shorts pages through
  yt-dlp, plus the already-supported Telegram ingest paths;
- durable download, probe, normalization, fingerprint, identity-gate, and
  storage workflow;
- searchable stored media with captions/descriptions and normalized tags;
- immediate publication or a simple per-channel cadence queue.

Publisher queue commands are durable PostgreSQL mutations: enqueue assigns the
next valid channel-local cadence slot, adjacent and explicit slot moves swap
only the affected posts, captions can be edited or explicitly cleared, and
publish-now makes the existing post job due immediately without consuming its
future cadence slot. Each queued post has one fixed-dedupe job and a revision
fence; stale admin views and claimed jobs cannot overwrite newer queue state.

The private administrator bot exposes `/queue` as a bounded control surface.
It first offers count choices on the `1, 2, 5 x 10^n` scale, then renders
text-only cards with localized cadence slots, catalogue metadata, separate
public post text, and links to the existing storage messages. Card actions
call the Publisher commands for moves, slot changes, caption edits, immediate
publication, and removal; callback payloads carry the post revision, and old
views are harmlessly rejected or cleaned up from bounded process-local state.
Caption and slot prompts are accepted only as replies to their own ForceReply
message; ordinary messages and commands do not accidentally become captions.
Draft and failed rows remain visible for editable actions, while cadence moves
and slot assignment are rendered only for queued rows. Queue cards are paced
per chat and retry bounded Telegram flood-control responses without replaying a
completed update.

HTTP ingest requests may carry one versioned follow-up intent on the same
`ingests` row: `save`, normal-cadence `queue`, exact-time `queue`, or
`post_now`. The intent records an optional future publication instant and
public post text separately from media description, tags, page context, and
selected text. Omitting the action retains save-only behavior. This capture
contract survives the asynchronous ingest pipeline and idempotent replays; it
does not create a post until the later materialization and repetition-decision
slice.

Large-media capture uses Telegram's official local Bot API server when the home
deployment is cut over manually. URL/link source downloads, Telegram-source
downloads, and canonical normalized storage output have separate budgets. The
source budgets may exceed the cloud Bot API's download limit; only the
canonical normalized object is uploaded, and its configurable ceiling remains
below the local server's documented 2000 MB upload maximum. Original inputs
remain transient workspace artifacts.
Telegram storage uploads also use a separate bounded deadline suitable for
2 GB-class transfers; it is independent from polling and download stall
timeouts.

Telegram file acceptance is metadata-only: the polling server validates and
queues the file ID before acknowledging the update. The worker performs the
bounded source download asynchronously from that durable file ID, and replayed
updates reuse the same ingest key.

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
storage is downstream of media identity finalization: video fingerprinting and
exact/sequence identity checking finish before the upload job is created. A storage result
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
final attempt expires, recovery fails the owning ingest explicitly (marks
storage unknown for an upload job, or fences an interrupted publication as
unknown) so a crashed worker cannot strand the workflow.

Post cadence slots are assigned when a post is queued. A `publish_post` job
references the `posts` row, and one post row becomes the durable publication
record after success. Telegram calls, HTTP downloads, ffmpeg, and ffprobe run
outside database transactions. External effects use state plus generation or
fencing tokens, and ambiguous effects are retained for explicit reconciliation
instead of being blindly retried. A retryable no-effect publication updates
the running job payload and post revision atomically; on its final attempt the
post becomes failed before the job becomes terminal. Publication copies the
ready Telegram storage
message into the target channel and falls back to the stored media-kind-specific
file ID only for an explicitly safe copy-unavailable response; it never reads
the canonical local file. Missing public captions are sent as an explicit empty
caption so storage metadata does not leak. Caption/entity rejection becomes a
failed, editable post, while flood-control responses known to have had no
effect requeue through the bounded job retry policy.

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

Issue #44 adds the versioned `video_sequence_v1` media foundation and its
pre-storage identity gate. Video exact SHA reuse, bounded token shortlisting,
aligned duplicate evidence, durable `duplicate_pending`, and the authorized
force-save route are shipped together. Issue #52 adds the first durable
duplicate decision: `/api/v1/ingests/{id}/accept-duplicate` and the private
admin bot's `/duplicates` cards for accepting an evidenced media item or
choosing `Save anyway`. Images, animations, and audio retain exact-SHA-only
behavior.

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
- Grafana/Prometheus deployment;
- Telegram publication functionality beyond behavior already present at the
  selected implementation base.

Existing local databases may be discarded explicitly by the owner. No tool or
test may reset a Docker volume automatically. The implementation must provide
documented, explicit reset instructions and must verify the new model from an
empty database.
