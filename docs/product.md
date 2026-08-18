# sooqa product authority

This document is the active product authority for the consolidated architecture
established by [ADR 0009](adr/0009-five-table-persistence-reset.md) and GitHub
issue #43. It supersedes the discarded persistence model and the historical
roadmap when they conflict. The checked-out code and `docs/architecture.md`
describe the shipped model; this document remains the product authority for
behavior and scope.

## Product

sooqa is a self-hosted, single-admin Telegram media pipeline. The backend
accepts media through the private HTTP API or the administrator's Telegram
interaction, processes it durably, exact-deduplicates normalized media, stores
new media in one private Telegram storage channel, and lets the administrator
publish stored media to configured target channels.

The first release is intentionally narrow:

- one administrator and one self-hosted installation;
- PostgreSQL as the source of truth;
- direct HTTP media, plus explicitly allowlisted anonymous public single-video
  YouTube, TikTok, Instagram, and X/Twitter pages through yt-dlp, with the
  YouTube-only bounded high-quality recovery and progressive fallback described
  in the [operations guide](operations.md), plus the already-supported Telegram
  ingest paths;
- durable download, probe, normalization, fingerprint, identity-gate, and
  storage workflow;
- a bounded stored-media catalogue with exact UUID/source/storage-link lookup
  plus editable internal descriptions and normalized tags;
- immediate publication or a simple per-channel cadence queue.

Publisher mutations are durable PostgreSQL operations. A publication intent
requires a decision when the same media is already queued/sending or was
published during the 14 days before the intended send instant; older history
does not block or prompt. Normal cadence enqueue
assigns the next valid channel-local slot, exact/manual scheduling sets only
the selected post to one future instant, and exact posts may share an instant
without moving unrelated cadence slots. Each queued post has one fixed-dedupe
job and a revision fence; stale admin views and claimed jobs cannot overwrite
newer publication state. A repeated media intent becomes a bounded-evidence
draft until the administrator chooses the documented one-time outcome. The
superseded Telegram queue presentation and its earlier/later and occupied-slot
swap commands are removed; web-admin editing is introduced in the bounded
admin slices below.

HTTP ingest requests may carry one versioned follow-up intent on the same
`ingests` row: `save`, normal-cadence `queue`, exact-time `queue`, or
`post_now`. The intent records an optional future publication instant and
public post text separately from media description, tags, page context, and
selected text. Omitting the action retains save-only behavior. This capture
contract survives the asynchronous ingest pipeline and idempotent replays; it
does not create a post during capture. Once storage is ready, one typed
materialization job creates at most one post linked by `origin_ingest_id`; save
intents create neither a post nor a materialization job. Queue/post-now
materialization evaluates repeat conflicts at the intended send instant and
persists bounded evidence on a draft when a decision is required. Decisions
are revision-fenced and replay-safe.

## Bounded admin API

The private API exposes bounded operational slices for the single administrator:
`GET /api/v1/dashboard` returns aggregate counts plus capped duplicate,
publication-repeat, and caption-sync attention cards; `GET /api/v1/ingests`
returns newest-first ingest cards with a timestamp-and-UUID cursor;
`GET /api/v1/media?q=...` performs one exact lookup by media/ingest UUID,
normalized source URL, supported 2ch mirror identity, or private Telegram
storage link; and `GET /api/v1/posts` returns scheduled posts with the same
bounded cursor discipline. The schedule view excludes published and cancelled
posts by default and can opt into history. Post responses distinguish explicit
times from cadence slots.

Channel settings are read and patched through `/api/v1/channels/{id}` with
validated time windows, IANA time zones, an optimistic `updated_at` fence, and
a conflict when enabling a second target would make publication ambiguous.
These routes never return raw job payloads, lease tokens, secrets, or complete
error logs. Caption-sync failure cards read the bounded failure state stored on
the media row.

Media responses expose optional bounded preview metadata and an authenticated
`GET /api/v1/media/{id}/preview` endpoint. Previews are at most 320 by 320 and
128 KiB, are validated before persistence, and are kept on the existing media
row. Video previews use the highest-information frame already decoded by the
`video_sequence_v1` extraction; static images use the existing image thumbnail
boundary; animations make one safe representative-frame attempt; audio has no
bitmap preview. Existing media rows are not downloaded or backfilled.

Telegram storage captions contain only the bounded internal description,
normalized tags, and source URL. One revision-fenced metadata command replaces
the complete tag set and description atomically, advances a generation, marks
sync pending, and enqueues durable fenced work; there are no separate
one-tag mutation routes. The worker edits the existing storage message after
commit and records only a matching generation/claim's result. Public post
text, IDs, hashes, workflow state, and schedule data never enter storage
captions. The authenticated
`POST /api/v1/media/{id}/caption-sync/retry` command creates a new generation
for a failed sync.

## Web admin shell

The existing server serves the dark desktop-oriented `/admin` site from static
HTML, CSS, and vanilla JavaScript embedded in the Rust binary. It has no
frontend runtime, build service, CDN, external fonts, or separate container.
The owner enters the configured bearer token once per browser session; the UI
keeps it only in `sessionStorage`, sends same-origin `Authorization` headers,
and provides a lock action that forgets it. The UI is an operational client,
not a second workflow authority: PostgreSQL-backed API commands remain the
source of truth, and stale revision conflicts refresh the affected view.

The Dashboard renders bounded totals and attention cards for technical
duplicate decisions, publication-repeat decisions, and storage-caption sync
failures. Duplicate and repeat cards invoke their existing revision/idempotent
commands; cancellation leaves media intact. Ingests is a read-only, newest-first
50-row cursor list and refreshes only its first page. Settings edits or creates
the one target channel through the optimistic `updated_at` API fence, including
the IANA time zone and cadence window. Media is a bounded 50-row catalogue: its
exact lookup accepts one media/ingest UUID, normalized source URL including any
supported 2ch mirror, or private Telegram storage link. Each card keeps internal
description/tags edits separate from public publication text, shows the durable
caption-sync state and retry, and fetches a bounded preview through the
authenticated API into a revocable browser object URL. Ready media offers plain
Post now/Queue actions and ellipsis dialogs for optional public text and an
optional future browser-local time; a blank Queue time uses normal cadence,
while a populated time schedules exactly. Repeat drafts go back to Dashboard
decisions.
Schedule is a bounded chronological editor for unpublished posts. It hides
published and cancelled history, distinguishes cadence from exact-time work,
and shows optional bounded still previews from the existing media preview
route, falling back to kind placeholders without becoming a media playback
surface. Draft, queued, and failed rows can edit or clear only public post
text, set any future browser-local exact instant (including collisions), request
Post now, or remove the post while keeping media. Sending and unknown rows
remain visible and read-only because an ambiguous send is not safe to retry
automatically.
Schedule forms use the post revision fence, and a first-page refresh preserves
each actively edited form instead of replacing it; leaving the page discards
those unsaved forms.

Compact UUID references in the Dashboard attention cards, Ingests, Media, and
Schedule views are keyboard-operable copy controls. They keep the shortened
label on screen while copying the exact backend UUID, and keep source,
Telegram, and in-app navigation links separate from the copy action. The
browser Clipboard API is preferred; the admin also tries the selection-based
copy path for trusted-LAN HTTP sessions. If both paths fail, the full UUID is
shown in a selectable field and the owner receives an explicit error message.

Large-media capture uses Telegram's official local Bot API server when the home
deployment is cut over manually. The home worker mounts the local server's data
volume read-only and confines absolute `getFile` paths to that mount; relative
paths and cloud downloads remain bounded HTTP transfers. URL/link source
downloads, Telegram-source downloads, and canonical normalized storage output
have separate budgets. The source budgets may exceed the cloud Bot API's
download limit; only the canonical normalized object is uploaded, and its
configurable ceiling remains below the local server's documented 2000 MB upload
maximum. Original inputs remain transient workspace artifacts.
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
  common media properties, one optional bounded preview, generation-fenced
  storage-caption sync state, source metadata, Telegram storage identifiers,
  and storage ambiguity state;
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
waiting forever. Independently scheduled work such as
`materialize_publication`, `publish_post`, and maintenance remains separate.
Materialization is database-only: it locks the completed ingest, ready media,
and captured target snapshot, then commits the intended post and its
fixed-dedupe publication job without network I/O.

For ingest stages, the product transition, successor enqueue, and current-job
success are committed in one database transaction. Final-attempt lease recovery
fails the owning ingest only when that atomic success transaction did not
commit.

Lease heartbeats and terminal job mutations require an unexpired lease. When a
final attempt expires, recovery fails the owning ingest explicitly (marks
storage unknown for an upload job, or fences an interrupted publication as
unknown) so a crashed worker cannot strand the workflow.

Post cadence slots are assigned when a normal post is queued. Exact/manual
posts store their requested future `scheduled_at` with `cadence_slot_at = NULL`,
so exact collisions do not move other cadence posts. A `publish_post` job
references the `posts` row, and one post row becomes the durable publication
record after success. Telegram calls, HTTP downloads, ffmpeg, and ffprobe run
outside database transactions. External effects use state plus generation or
fencing tokens, and ambiguous effects are retained for explicit reconciliation
instead of being blindly retried. Storage-caption edits commit their generation
and durable job before calling Telegram; stale completions are fenced and
schedule a current-generation reapply. A retryable no-effect publication updates
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
| Create the same requested post | `posts.request_key UNIQUE`; `origin_ingest_id UNIQUE` for materialization |
| Send a post | post state plus generation/token; ambiguous sends do not auto-retry |

There is no generic idempotency table and no permanent Telegram update-receipt
table. Repeated creates may return the existing resource; updates should be
naturally idempotent setters where possible.

The administrator media list is a bounded cursor-ordered view. A media metadata
edit replaces the complete normalized tag set and description under the media
row's `updated_at` revision fence.

Issue #44 adds the versioned `video_sequence_v1` media foundation and its
pre-storage identity gate. Video exact SHA reuse, bounded token shortlisting,
aligned duplicate evidence, durable `duplicate_pending`, and the authorized
force-save route are shipped together. Issue #52 adds the first durable
duplicate decision: the bounded Dashboard technical-duplicate cards and the
existing `/api/v1/ingests/{id}/accept-duplicate` and
`/api/v1/ingests/{id}/force-save` commands accept an evidenced media item or
choose `Save anyway`. Telegram remains a capture and publication transport;
it no longer presents duplicate-review cards. Images, animations, and audio
retain exact-SHA-only behavior.

## Single-admin security

The bot token, API secret, administrator Telegram user ID, and storage chat ID
come from configuration/environment secrets and never from PostgreSQL or Git.
Target publication channels remain database rows because they are editable
product destinations. The API remains private and authenticated; the reset
does not broaden exposure or add multi-user behavior.

The web admin is intended for Windows Chrome on the owner's trusted home LAN.
Its restrictive same-origin CSP, session-only token storage, and text-only
rendering of backend values reduce accidental exposure, but the MVP does not
claim to be safe for internet exposure.

## Explicit non-goals

The current single-admin MVP does not add:

- compatibility with old databases, API snapshots, repository interfaces, or
  local data;
- data-copy SQL, compatibility views, old-name aliases, or dual writes;
- multiple administrators, users, tenants, storage providers, albums, media
  variants, derivative assets, or a generalized content taxonomy;
- Grafana/Prometheus deployment;
- semantic or full-text media search.

Only databases from the discarded pre-reset model were allowed to be recreated
when adopting ADR 0009. A current five-table installation is durable operator
data and must be preserved through forward-only migrations and backups. No
tool or test may reset a Docker volume automatically; the explicit legacy-reset
procedure remains owner-run and documented in the
[operations guide](operations.md).
