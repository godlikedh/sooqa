# sooqa current architecture

This document describes the system as it exists after PR #3. It is an
operational map for understanding the repository and the jobs foundation.
The product specification remains the source of truth for the complete MVP
and future behavior: [PROJECT_SPEC.md](PROJECT_SPEC.md).

## Current implementation level

Implemented:

- Rust workspace and three application binaries;
- typed TOML/environment configuration and secret redaction;
- structured tracing and graceful process shutdown primitives;
- server liveness endpoint and explicit database migration command;
- PostgreSQL migrations and connection pooling;
- durable job values and PostgreSQL job repository;
- worker identity, bounded polling, handler registration, job outcomes, logs,
  in-process metrics, and graceful shutdown requeue.

Not implemented yet:

- ingest requests and authenticated API submission;
- Telegram bot or Telegram storage integration;
- media download, `ffprobe`, `ffmpeg`, `yt-dlp`, and fingerprinting;
- library records and duplicate detection;
- real worker handlers;
- scheduling and publication.

The production worker registry is currently empty. The worker integration test
registers a small test handler to prove the loop works end to end.

## System context

The intended product flow is broader than the currently running code:

```mermaid
flowchart LR
    Browser[Browser / userscript] --> Companion[sooqa-companion\nplanned capture bridge]
    Companion -->|future authenticated submit| Server[sooqa-server]
    TelegramAdmin[Telegram admin chat\nplanned] --> Server

    Server -->|future commands and job enqueueing| PostgreSQL[(PostgreSQL\nsource of truth)]
    Worker[sooqa-worker] -->|claim and update jobs| PostgreSQL
    Worker --> Registry[Handler registry]
    Registry -. future .-> Media[Media handlers\ndownload / probe / normalize]
    Registry -. future .-> Telegram[Telegram handlers\nstorage / publish]
    Media -. future .-> Tools[ffmpeg / ffprobe / yt-dlp]
    Telegram -. future .-> TelegramAPI[Telegram Bot API]
```

The important distinction is that the diagram contains both current and
planned edges. Today, the server exposes health and migration behavior, while
the worker can execute registered functions against durable jobs. The future
server and companion integrations are not silently implied to exist.

## Processes

| Process | Current responsibility | Future responsibility |
| --- | --- | --- |
| `sooqa-server` | Axum liveness endpoint; explicit `migrate` command | HTTP API, Telegram updates, lightweight commands, job enqueueing, scheduler |
| `sooqa-worker` | Connects to PostgreSQL, polls jobs, dispatches registered handlers, records outcomes | Media processing, Telegram uploads, publication, cleanup |
| `sooqa-companion` | Loads config, initializes tracing, waits for shutdown | Loopback capture API for browser/userscript submissions |
| PostgreSQL | Stores migrations, jobs, attempts, admin rows, and idempotency records | Source of truth for the complete catalogue, schedules, and history |

Only the three `apps/` packages produce executables. The libraries under
`crates/` are compiled into those applications; they are not separate
services or containers.

## Rust workspace dependency shape

```mermaid
flowchart TB
    Server[apps/server] --> API[crates/api]
    Server --> Config[crates/config]
    Server --> Persistence[crates/persistence]
    Server --> Runtime[crates/runtime]

    Worker[apps/worker] --> Jobs[crates/jobs]
    Worker --> Config
    Worker --> Persistence
    Worker --> Runtime

    Companion[apps/companion] --> Config
    Companion --> Runtime

    Persistence --> Jobs
    Persistence --> SQLx[SQLx]
    SQLx --> PostgreSQL[(PostgreSQL)]

    Inbox[crates/inbox]
    Library[crates/library]
    Publisher[crates/publisher]
    Media[crates/media]
    Telegram[crates/telegram]
    Kernel[crates/kernel]

    Server -. future .-> Inbox
    Worker -. future .-> Media
    Worker -. future .-> Telegram
    Inbox -. future .-> Kernel
    Library -. future .-> Kernel
    Publisher -. future .-> Kernel
```

The empty domain crates are architectural boundaries, not completed features.
They should gain code only when the corresponding roadmap slice is being
implemented.

## Jobs architecture

### Responsibilities by layer

```text
crates/jobs
  JobType, JobStatus, NewJob, Job
  domain values with no SQL or worker process behavior

crates/persistence
  JobRepository
  SQL queries, transactions, SKIP LOCKED, leases, attempts, retries

apps/worker
  Worker
  polling loop, handler registry, shutdown, logging, metrics

future application modules
  concrete handlers such as download, normalize, upload, and publish
```

The current design is intentionally small. There is no message broker, no
generic plugin framework, and no separate job service. PostgreSQL is the queue
storage and the worker is a single bounded consumer loop.

### Database model

```mermaid
erDiagram
    JOBS ||--o{ JOB_ATTEMPTS : records

    JOBS {
        uuid id PK
        text job_type
        jsonb payload_json
        text status
        integer priority
        timestamptz available_at
        integer attempt_count
        integer max_attempts
        text lease_owner
        timestamptz lease_expires_at
        timestamptz last_heartbeat_at
        text last_error_class
        text last_error_message
        text idempotency_key UK
        timestamptz created_at
        timestamptz updated_at
        timestamptz completed_at
    }

    JOB_ATTEMPTS {
        uuid id PK
        uuid job_id FK
        integer attempt_number
        text status
        timestamptz started_at
        timestamptz finished_at
        text error_class
        text error_message
    }

    ADMINS {
        uuid id PK
        bigint telegram_user_id UK
        text display_name
        boolean is_enabled
    }

    IDEMPOTENCY_RECORDS {
        uuid id PK
        text scope
        text idempotency_key
        bytea request_hash
        text resource_type
        uuid resource_id
        integer response_status
        jsonb response_body
        timestamptz expires_at
    }
```

`admins` and `idempotency_records` exist in the initial schema but are not yet
used by the HTTP or Telegram application flows. The job repository currently
uses `jobs` and `job_attempts`.

### Job fields

| Field | Meaning |
| --- | --- |
| `job_type` | Typed operation name, stored as snake-case text at the database boundary |
| `payload_json` | Handler input; currently opaque to the repository |
| `status` | Queue state: `queued`, `running`, `succeeded`, `retry_wait`, `failed`, or `cancelled` |
| `priority` | Higher values are claimed first |
| `available_at` | Earliest time at which a queued/retryable job can be claimed |
| `attempt_count` | Incremented atomically when a worker claims the job |
| `max_attempts` | Retry ceiling |
| `lease_owner` | Worker identity currently responsible for the job |
| `lease_expires_at` | Deadline after which the lease can be recovered |
| `last_heartbeat_at` | Last lease renewal timestamp |
| `last_error_*` | Latest failure classification and message |
| `idempotency_key` | Unique key for preventing duplicate job rows |
| `completed_at` | Timestamp for terminal completion/failure |

`jobs.idempotency_key` is job-level uniqueness. It is different from the
future command-level replay behavior represented by `idempotency_records`.

### Current job types

The domain enum contains the initial names from the specification:

```text
inspect_source
download_source
probe_asset
check_exact_duplicate
normalize_asset
compute_fingerprint
check_similarity
upload_storage_asset
finalize_ingest
publish_post
verify_storage_object
cleanup_workspace
recover_stale_jobs
```

These names do not mean handlers already exist. They define the vocabulary for
future persisted jobs.

## Job lifecycle

```mermaid
stateDiagram-v2
    [*] --> queued : enqueue
    queued --> running : claim with active available_at
    retry_wait --> running : claim after available_at

    running --> succeeded : handler succeeds
    running --> retry_wait : retryable failure
    running --> failed : permanent failure
    running --> failed : retry limit reached
    running --> retry_wait : shutdown requeue
    running --> retry_wait : stale lease recovery
    running --> failed : stale lease at retry limit

    queued --> failed : no registered handler
    succeeded --> [*]
    failed --> [*]

    note right of running
      lease_owner must match the worker
      for heartbeat, complete, retry, or fail
    end note

    note right of retry_wait
      available_at controls when it is eligible again
    end note
```

`cancelled` exists in the database status vocabulary, but the current
repository has no cancellation method yet.

## Enqueue and claim sequence

The producer side is future work in the server. The repository operations are
already available.

```mermaid
sequenceDiagram
    participant Producer as Future server/application service
    participant DB as PostgreSQL
    participant Worker as sooqa-worker
    participant Handler as Registered handler

    Producer->>DB: INSERT jobs
    Note over Producer,DB: Future C1 flow will create domain record + first job transactionally

    Worker->>DB: BEGIN
    Worker->>DB: Select eligible row FOR UPDATE SKIP LOCKED
    Worker->>DB: Mark running, set lease, increment attempt_count
    Worker->>DB: INSERT job_attempts(running)
    Worker->>DB: COMMIT

    Worker->>Handler: Execute Job(payload_json)
    Handler-->>Worker: success / retryable failure / permanent failure

    alt success
        Worker->>DB: Mark job succeeded
        Worker->>DB: Finish attempt succeeded
    else retryable failure
        Worker->>DB: retry_wait or failed at max_attempts
        Worker->>DB: Finish attempt with error
    else permanent failure
        Worker->>DB: Mark job failed
        Worker->>DB: Finish attempt with error
    end
```

The claim transaction covers only database work. The handler runs after the
transaction commits, so network calls and subprocesses are never performed
while a database transaction is held open.

## Atomic concurrency behavior

```mermaid
sequenceDiagram
    participant W1 as Worker A
    participant DB as PostgreSQL
    participant W2 as Worker B

    par concurrent claim
        W1->>DB: claim_next(worker-a)
    and
        W2->>DB: claim_next(worker-b)
    end

    DB-->>W1: one eligible job with lease
    DB-->>W2: different job, or no row
    Note over DB: FOR UPDATE SKIP LOCKED prevents both workers claiming the same row
```

The integration test exercises this behavior with two concurrent claim calls.

## Worker loop

```mermaid
flowchart TD
    Start([Process starts]) --> Identity[Generate worker UUID]
    Identity --> Connect[Connect to PostgreSQL]
    Connect --> Poll[claim_next with lease]
    Poll -->|no job| Wait[Sleep poll interval]
    Wait --> ShutdownCheck{Shutdown?}
    ShutdownCheck -->|no| Poll
    ShutdownCheck -->|yes| Stop([Stop])

    Poll -->|job claimed| Count[Increment claimed metric and log]
    Count --> Lookup{Handler registered?}
    Lookup -->|no| FailMissing[Fail handler_not_registered]
    FailMissing --> Poll
    Lookup -->|yes| Execute[Run handler outside DB transaction]

    Execute --> Outcome{Handler result}
    Outcome -->|success| Complete[complete job and attempt]
    Outcome -->|retryable error| Retry[retry with 1-second delay]
    Outcome -->|permanent error| Fail[fail job and attempt]
    Complete --> Poll
    Retry --> Poll
    Fail --> Poll

    Execute -->|shutdown signal| Requeue[retry with worker_shutdown]
    Requeue --> Stop
```

The loop is bounded in two ways:

1. it claims at most one job at a time;
2. when no job exists, it waits for the configured poll interval instead of
   spinning.

The default values are a five-second poll interval and a sixty-second lease.
They can be configured with TOML or:

```text
SOOQA_WORKER_POLL_INTERVAL_SECONDS
SOOQA_WORKER_LEASE_DURATION_SECONDS
```

The process identity is generated once at startup as `worker-<UUID>` and is
stored in `lease_owner` when claiming a job.

## Shutdown behavior

```mermaid
sequenceDiagram
    participant Signal as SIGTERM / Ctrl-C
    participant Worker as Worker loop
    participant DB as PostgreSQL
    participant Handler as Active handler

    Signal->>Worker: shutdown future resolves
    Worker->>Handler: cancel active handler future
    Worker->>DB: retry active job with worker_shutdown
    Worker->>Worker: increment shutdown_requeued metric
    Worker-->>Signal: stop loop
```

If the process crashes before it can requeue the job, the lease remains until
`lease_expires_at`. The repository exposes `recover_stale_leases()` for
recovery, but a periodic recovery tick is not wired into the current worker
loop yet.

Likewise, `heartbeat()` exists in the repository, but the current worker does
not yet run a separate heartbeat task while a handler is active. This is safe
for the current short test handler and must be added before long-running media
handlers are introduced.

## Retry and failure behavior

The repository distinguishes two paths:

```text
handler succeeds
  → succeeded

handler returns retryable failure
  → retry_wait with available_at in the future
  → failed when attempt_count reaches max_attempts

handler returns permanent failure
  → failed immediately

worker shuts down during handler
  → retry_wait with worker_shutdown
```

The current worker uses a one-second retry delay. The specification calls for
exponential backoff, bounded jitter, and Telegram `retry_after` handling; those
belong with concrete handlers and later retry-policy work.

## Lease ownership rules

Only the worker that owns a running lease may mutate the active job through:

- `heartbeat(job_id, worker_id, duration)`;
- `complete(job_id, worker_id)`;
- `retry(job_id, worker_id, ...)`;
- `fail(job_id, worker_id, ...)`.

If the owner does not match, the repository returns `LeaseLost`. This prevents
an old worker from completing a job after another worker has recovered it.

## Transactions and external work

Current transaction boundaries are:

```text
claim transaction:
  select eligible job with SKIP LOCKED
  mark running and set lease
  insert job_attempts row
  commit

outside transaction:
  execute handler

completion transaction:
  update job state
  update matching job_attempts row
  commit
```

No database transaction is held while a future handler calls Telegram,
downloads a URL, or runs a media subprocess.

## Current limitations and next work

The next product stack is the inbox vertical slice:

1. add `ingest_requests` and related domain state;
2. validate submitted URLs;
3. create an ingest record and first job in one transaction;
4. add authenticated HTTP submission;
5. register the first real source-inspection handler.

Before long-running media jobs are enabled, the worker should also gain:

- periodic stale-lease recovery;
- active-job heartbeats;
- concrete retry/backoff policy;
- bounded handler cancellation for subprocesses;
- exported metrics rather than only in-process counters.

## Local development

With Colima as the Docker context:

```bash
colima start --runtime docker
docker context use colima
just db-up
just db-migrate
DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa \
  cargo run -p sooqa-worker
```

Run the full database-backed test path with:

```bash
just test-integration
```

The worker does not automatically run migrations. Apply them explicitly
before starting it.
