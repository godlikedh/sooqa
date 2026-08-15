# Current architecture

sooqa is a modular monolith. `apps/server` owns HTTP and Telegram composition,
`apps/worker` owns durable-job execution, and `apps/companion` is an optional
local capture process. The companion exposes one authenticated loopback
submission route and has no database, media, Telegram, or job dependencies.
The crates are compile-time boundaries inside those processes; they are not
separate network services.

## Durable model

PostgreSQL is the source of truth. The initial migration creates exactly five
application tables:

```mermaid
erDiagram
    INGESTS ||--o| MEDIA : produces
    MEDIA ||--o{ POSTS : published_as
    CHANNELS ||--o{ POSTS : receives
    QUEUE_JOBS {
        uuid id PK
        text kind
        jsonb payload
        text state
        timestamptz run_at
        uuid lease_token
    }
    INGESTS {
        uuid id PK
        uuid workspace_id
        text input_key UK
        text state
        jsonb input_json
        boolean force_save
        jsonb duplicate_evidence
        uuid media_id FK
    }
    MEDIA {
        uuid id PK
        text kind
        text storage_state
        bytea canonical_sha256 UK
        bytea fingerprint_data
        bigint[] fingerprint_search_tokens
        text[] tags
    }
    CHANNELS {
        uuid id PK
        bigint telegram_chat_id UK
        boolean is_enabled
        int interval_minutes
    }
    POSTS {
        uuid id PK
        text request_key UK
        uuid media_id FK
        uuid channel_id FK
        text state
        timestamptz scheduled_at
        uuid send_token
    }
```

`_sqlx_migrations` is migration bookkeeping; it is not application state.
There are no compatibility tables, copy migrations, generic idempotency table,
Telegram receipt table, or automatic volume reset. A local database created by
the previous model must be explicitly recreated by the owner before applying
this baseline.

## Boundaries

- `sooqa-inbox` validates source submissions and defines typed ingest metadata.
- `sooqa-library` defines media, source, normalized tag values, and storage
  domain values; tags remain embedded in the media aggregate.
- `sooqa-publisher` defines channels, posts, and publication transitions.
- `sooqa-jobs` defines typed job kinds and payloads. Persistence decodes the
  JSON envelope once; handlers receive a typed `JobCommand`.
- `sooqa-media` owns direct HTTP, the exact-host 2ch mirror adapter, the
  allowlisted yt-dlp adapter, ffprobe, ffmpeg, image normalization, hashing,
  fingerprints, workspaces, and subprocess safety.
- `sooqa-telegram` owns Telegram protocol mapping, storage upload effects, and
  publication copy/send effects. Polling, worker-side source downloads,
  storage uploads, and publication use separate bounded calls. Telegram file
  acceptance is metadata-only; source bytes are reconstructed by the worker
  from the durable file ID. Publication receives only a ready storage receipt
  and never receives a local media path. The Telegram adapter remains focused
  on capture, duplicate decisions, and sending; bounded web-admin slices own
  publication editing.
- `sooqa-persistence` owns migrations and short database transactions.
- `sooqa-api` owns HTTP routing, one configured bearer secret, limits, and
  stable request-ID errors.

Workspace lifecycle is shared across the ingest, jobs, persistence, media, and
worker boundaries. Persistence owns the durable generation ID and cleanup-job
enqueue/state fence. A valid cleanup attempt clears the current media
local-work-path marker before committing its `Ready` decision, so storage reset
remains reconstruction-required after cleanup success or lease recovery.
Storage reset either commits first and makes cleanup defer, or observes the
durable reclaimed marker; a stale attempt is rejected before it can touch the
filesystem. `sooqa-media` validates and removes a whole workspace without
following symlinks. Reconciliation protects every workspace ID still current
on an ingest, so only old force-save generations are orphan candidates.

## Ingest and worker flow

```mermaid
sequenceDiagram
    participant Client
    participant API
    participant Ingests
    participant Queue as queue.jobs
    participant Worker
    participant Media as media

    Client->>API: POST ingest + Idempotency-Key
    API->>Ingests: insert input_key + request_hash
    Ingests->>Queue: enqueue inspect_source
    API-->>Client: 202 ingest id
    Worker->>Queue: claim queued row
    Queue-->>Worker: running row + lease token
    Worker->>Media: inspect/download/probe/normalize
    Worker->>Ingests: fenced state transition + next job
    alt video
        Worker->>Media: extract video_sequence_v1 outside transaction
        Worker->>Media: advisory-locked SHA/shortlist/alignment decision
        alt exact or new identity
            Media-->>Worker: existing or new media id
        else strong perceptual match
            Worker->>Ingests: duplicate_pending + bounded evidence
        end
    else image, animation, or audio
        Worker->>Media: exact SHA resolution only
        Media-->>Worker: existing or new media id
    end
    Ingests->>Queue: enqueue upload only for a new reservation
    Worker->>Media: upload only after media processing
    Media-->>Worker: ready, failed, or storage_unknown
    Worker->>Ingests: consume storage outcome by media_id
    Ingests->>Queue: enqueue cleanup_workspace with workspace generation
    Worker->>Ingests: check durable ownership and storage fence
    Worker->>Media: remove whole UUID workspace
```

When the video identity gate records `duplicate_pending`, the administrator
can inspect it with `/duplicates` in the configured private bot chat. The bot
renders at most three persisted candidates per ingest: ready candidates have
an `Open media` link and `Use this` action, pending candidates have a `Use
this` action with a storing label, and every card has `Save anyway`. Each
callback is checked against the configured administrator and private chat
before the durable command runs. Telegram callback queries are acknowledged
before repository work so a slow database operation does not leave the client
spinner active.

The HTTP equivalent is
`POST /api/v1/ingests/{id}/accept-duplicate` with `{ "media_id": "..." }`.
The repository locks the ingest row, verifies that the media ID is in the
persisted bounded evidence, locks the candidate media row, and accepts only
`ready` or `pending_storage`. A ready candidate completes immediately. A
pending candidate moves the ingest to `storing` and joins the candidate's
existing storage lifecycle without inserting another media row or upload job.
Incoming supplied tags are unioned into the existing media row and a nonempty
supplied description replaces its description. The same row lock fences
force-save, so a concurrent decision has one winner and the loser receives a
stable conflict; repeating the winning decision is idempotent.

The worker keeps source inspection, download, probe, normalization,
fingerprinting, and exact finalization as separate typed jobs. Each stage
updates `ingests` and enqueues its successor in a short transaction. Video
identity finalization takes one transaction-scoped advisory lock, rechecks the
canonical SHA, asks PostgreSQL for at most twenty plausible fingerprint
candidates, and runs bounded Rust alignment before inserting media. The lock
does not cover download, ffmpeg, filesystem, HTTP, or Telegram work. Images,
animations, and audio skip the video path and use exact SHA resolution. No
storage job is enqueued for `duplicate_pending`; force-save sets the durable
override, reconstructs URL/Telegram source artifacts when necessary, and then
resumes normalization/fingerprinting. Network and subprocess work never runs
while an identity transaction is open. Stage metadata is
bounded JSON input metadata; it is decoded into typed Rust structs at the
handler boundary. Storage completion/failure is applied by `media_id`, and
attach/reset/mark-unknown reconcile the linked ingest rows.

Telegram file messages follow the same durable boundary: the polling server
validates the administrator and advertised size, persists the Telegram file
metadata, and acknowledges only after the ingest transaction commits. It does
not create a workspace or call Telegram file download. The worker creates the
workspace and downloads from the persisted file ID during the probe job, so a
slow or replayed Telegram file cannot block later polling acceptance.

URL ingest rows also persist a typed capture intent: `save`, `queue`, or
`post_now`, with an optional future `requested_publish_at` and separate public
`requested_post_caption`. An omitted action is `save`; a queue without a time
uses the eventual channel cadence, while a future queue time is an exact
instant that the later materializer will keep outside cadence/window rules.
The Inbox validates action/time combinations before insertion and includes all
source, metadata, and intent fields in the existing `input_key` request hash.
The intent is carried through duplicate review, force-save, retries, and
terminal ingest states. When storage becomes ready, Inbox atomically enqueues
one typed `materialize_publication` job for every non-save ingest. The worker's
database-only materializer locks the completed ingest, ready media, and
captured channel snapshot, then creates at most one post through the same
Publisher intent path. Save-only ingests create neither a post nor this job.
Repeat conflicts become bounded evidence on a draft; a revision-fenced decision
then approves once, keeps the exact requested instant, queues normally, or
cancels while retaining the media.

When storage is durably ready, the storage transition, linked-ingest
completion, `local_work_path = NULL`, and cleanup enqueue commit together. A
duplicate or terminal failure uses the one-day cleanup retention window. A
cleanup replay for an old generation is safe after force-save because the
payload ID no longer matches the current ingest generation; a replay against a
missing directory is also successful. Startup and periodic reconciliation scan
only UUID-named workspace directories in bounded batches and protect every ID
still current on an ingest. Completed rows remain protected after their
cleanup job succeeds; the explicit cleanup path owns their deletion, while
reconciliation handles only true old generations.

Each successful ingest stage commits its ingest transition, successor enqueue,
and current queue-job success atomically. Final-attempt recovery therefore
fails an owning ingest only when that success transaction never committed.

A queue claim creates a fresh owner, expiry, and fencing token. Heartbeats and
completion/retry/failure updates require all three. The video identity
finalizer also revalidates the live lease inside its media/evidence/storage
transaction, so a stale worker can neither insert media nor leave duplicate
evidence or an upload job behind. Expired leases return to `queued` (or become
`failed` after the attempt limit). A final expired attempt
also marks its owning ingest terminal with an explicit lease-expired error;
an expired storage upload becomes `storage_unknown` unless the media row is
already `ready`. `run_at` is the retry and scheduling clock; there is no
separate retry-wait state.

## Media and storage

`media` is the normalized business item, not a parent with child assets. Its
canonical SHA is unique, its fingerprint is versioned, and `tags` is a bounded
PostgreSQL array with a GIN index. Source and adapter metadata lives in the
bounded `source_metadata` JSON column. Telegram storage is an effect-local
state machine on the same row: `pending_storage`, `ready`,
`storage_unknown`, or `missing`, with generation/token fields for retries and
ambiguous results. Exact-SHA deduplication preserves the first source identity
and only fills missing non-identity metadata from later observations.

The administrator library list is cursor-bounded. Its single metadata update
command replaces the complete normalized tag set and description while holding
the media row revision fence; the transaction commits before any later
integration work such as caption synchronization.

The issue #44 implementation stores `video_sequence_v1` as bounded binary
`fingerprint_data` plus a partial-GIN-searchable token array. PostgreSQL only
shortlists compatible `pending_storage` and `ready` video rows; bounded Rust
alignment produces at most three scalar evidence matches, capped at 16 KiB.
Exact duplicates reuse the existing media row, strong perceptual matches stop
at durable `duplicate_pending`, and no-match videos insert one
`pending_storage` reservation before upload.

Duplicate acceptance does not create a UI/session/history table. The bounded
`duplicate_evidence` JSON on `ingests` is consumed into a versioned namespaced
accepted-decision marker in `input_json`; once accepted, the evidence is cleared
and the chosen `media_id` becomes the durable decision. Storage completion and
failure continue to fan out by that media ID, and a retry of the winning accept
command remains an idempotent replay even after a retryable or terminal storage
failure.

The home deployment adds the official `tdlib/telegram-bot-api` service in
`--local` mode. It is built at a pinned upstream commit, has a dedicated
persistent working volume, and is reachable only as
`http://telegram-bot-api:8081` on the Compose network. The application server
and worker share the sooqa work-root volume with each other; the Bot API data
volume is separate. Telegram downloads and uploads remain path-based or
streamed, so large files do not enter JSON or a `Vec<u8>`. Cloud/local bot
cutover is an owner-authorized operational procedure, never an application
state transition.

## Publisher

Channels hold only target identity, enablement, timezone, window, and interval.
Posts hold the intended message and the latest send result. A scheduled post
is a query over `posts.state = 'queued'`; normal scheduling assigns
`cadence_slot_at`, while exact/manual scheduling stores the requested future
`scheduled_at` with `cadence_slot_at = NULL` and permits collisions. Both paths
enqueue one fixed-dedupe `publish_post` job referencing the post ID.
Publication mutations lock the channel first and then the affected post/job
rows in a stable order. `posts.revision` is copied into the job payload, so a
stale claim cannot send after an edit or publish-now operation. All post/job
changes commit without Telegram I/O; exact scheduling changes only the
selected post and never restores the removed Telegram reorder/swap surface.
Send generation and token fence retries, while `unknown` preserves an
ambiguous Telegram outcome for explicit reconciliation. `publish now` changes
only the job due time and leaves the cadence slot intact.

Publication claims the queued post in a short transaction, increments its send
generation, records a fresh token, and commits before calling Telegram. The
adapter first calls `copyMessage` from the persisted private storage
chat/message and supplies the public caption (an empty caption when absent),
parse mode, and notification setting. Only an explicit copy-unavailable
response permits the media-kind-specific stored `file_id` fallback. The
canonical local file is never opened or uploaded by this path.

Success and failure completion are conditional on the exact generation and
token, so a stale worker cannot overwrite a newer attempt. Caption syntax
errors become editable `failed` posts and their job is terminal; explicit
no-effect errors such as Telegram flood-control responses requeue the post and
update the running job payload before bounded retry. The final no-effect retry
settles the post as `failed` before the worker settles the job, so a terminal
job cannot leave a queued post behind. Database or malformed-receipt failures
before the Telegram call are classified as known no-effect failures and follow
the same bounded retry/final-failure path; missing or invalid receipts that
cannot succeed are actionable `failed` outcomes. Network, invalid-response,
and unknown Telegram outcomes become `unknown` and are never automatically
sent again. If a worker lease expires while a post is `sending`, recovery
fences that generation as `unknown` before the recovered job completes, and
stale completion from the old attempt is rejected. The job is terminal after
an ambiguous result; operator reconciliation is intentionally a later slice.

```mermaid
sequenceDiagram
    participant Queue as queue.jobs
    participant Worker
    participant DB as posts + media
    participant Telegram

    Queue->>Worker: claim publish_post + expected revision
    Worker->>DB: claim queued post -> sending + generation/token
    DB-->>Worker: commit claim + storage receipt/channel
    Worker->>Telegram: copyMessage(storage -> target)
    alt copy unavailable and safe
        Worker->>Telegram: send media by stored file_id
    end
    alt definite success
        Worker->>DB: complete only matching generation/token -> published
    else caption/entity rejection
        Worker->>DB: matching failure -> failed
    else ambiguous network/API outcome
        Worker->>DB: matching failure -> unknown
    else explicit no-effect retry
        Worker->>DB: matching requeue + new job revision
        Worker->>Queue: bounded retry policy
    end
```

The Telegram adapter does not expose the superseded queue presentation or
its reorder/swap callbacks. It remains responsible for capture, duplicate
decisions, and publication transport; bounded web-admin slices own durable
publication inspection and editing through the API.

The bounded admin API is owned by `sooqa-api` and reads through the Inbox,
Library, Jobs, and Publisher repositories. Repository queries return small
read models with stable timestamp-plus-UUID cursors and a maximum page size of
50; the API does not expose queue payloads, lease tokens, secrets, or complete
error logs. Dashboard counts are aggregate SQL reads, while attention cards
carry only bounded duplicate, repeat-decision, and caption-sync evidence.
Media `q` lookups are exact identifiers/URLs rather than semantic search. The
media row may also carry one validated preview (at most 320 by 320 and 128 KiB)
and a generation-fenced storage-caption sync state. The authenticated preview
route serves private-cache JPEG/PNG bytes without exposing workspace paths;
audio and unsupported/older rows simply have no preview. The channel settings
patch locks and validates the complete candidate, uses `updated_at` for
stale-write rejection, and serializes enablement so at most one active
publication target can be configured. Media metadata editing uses one
revision-fenced command for the complete tags plus internal description; the
caption-sync claim token prevents a recovered old worker from overwriting a
newer same-generation attempt.

The server process also merges an `admin_router` that serves the static
`/admin` shell and its local assets with `include_str!`; no frontend runtime or
separate service is part of the deployment. The page stores the configured
bearer token only in browser `sessionStorage`, calls same-origin API routes,
and uses a restrictive asset CSP. It renders backend strings through DOM text
nodes and uses the existing API/repository fences for duplicate, publication,
and channel decisions. Media and Schedule navigation are intentionally left
to their later slices.

## Security and filesystem rules

The API compares the configured bearer secret; no token administration data is
stored in PostgreSQL. Telegram admin IDs remain configuration. Direct HTTP
validates and pins destinations, and external commands receive argument arrays,
bounded output, timeouts, and no shell. Workspaces are derived from UUIDs and
are cleaned only in known paths.
