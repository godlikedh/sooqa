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
- `sooqa-library` defines media, source, tag, and storage domain values.
- `sooqa-publisher` defines channels, posts, and publication transitions.
- `sooqa-jobs` defines typed job kinds and payloads. Persistence decodes the
  JSON envelope once; handlers receive a typed `JobCommand`.
- `sooqa-media` owns direct HTTP, the exact-host 2ch mirror adapter, ffprobe, ffmpeg, image normalization,
  hashing, fingerprints, workspaces, and subprocess safety.
- `sooqa-telegram` owns Telegram protocol mapping and storage upload effects.
  Polling, worker-side source downloads, and storage uploads use separate HTTP
  clients and timeout policies. Telegram file acceptance is metadata-only;
  source bytes are reconstructed by the worker from the durable file ID.
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

The issue #44 implementation stores `video_sequence_v1` as bounded binary
`fingerprint_data` plus a partial-GIN-searchable token array. PostgreSQL only
shortlists compatible `pending_storage` and `ready` video rows; bounded Rust
alignment produces at most three scalar evidence matches, capped at 16 KiB.
Exact duplicates reuse the existing media row, strong perceptual matches stop
at durable `duplicate_pending`, and no-match videos insert one
`pending_storage` reservation before upload.

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
is a query over `posts.state = 'queued'`; scheduling assigns `cadence_slot_at`
and enqueues a `publish_post` job referencing the post ID. Send generation and
token fence retries, while `unknown` preserves an ambiguous Telegram outcome
for explicit reconciliation.

## Security and filesystem rules

The API compares the configured bearer secret; no token administration data is
stored in PostgreSQL. Telegram admin IDs remain configuration. Direct HTTP
validates and pins destinations, and external commands receive argument arrays,
bounded output, timeouts, and no shell. Workspaces are derived from UUIDs and
are cleaned only in known paths.
