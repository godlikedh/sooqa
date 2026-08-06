# sooqa — product and engineering specification

> Working repository name: `sooqa`  
> Document status: **implementation-ready draft**  
> Intended audience: project owner, Codex CLI coding agent, reviewers, future contributors  
> Baseline date: **2026-08-06**  
> Primary language of the codebase and public documentation: **English**  
> Discussion language may be Russian.

---

## 0. How to use this document

This file is the source specification for building an open-source, self-hosted media ingestion, cataloguing, deduplication, and publishing system for Telegram channels.

The intended workflow is:

1. The owner creates an empty GitHub repository.
2. This file is committed as `docs/PROJECT_SPEC.md`.
3. Codex CLI reads this file together with the repository-level `AGENTS.md`.
4. Work proceeds through small, reviewable pull requests.
5. Each pull request must leave the repository buildable, testable, and internally consistent.
6. The owner reviews every pull request before it is merged.
7. Later pull requests may be stacked on earlier ones, but the active stack should normally be no deeper than 2–5 pull requests.
8. No implementation decision in this document is sacred when evidence proves it wrong. Material deviations require an Architecture Decision Record in `docs/adr/`.

This specification deliberately distinguishes:

- **product requirements** — what the system must do;
- **architecture constraints** — how major responsibilities are separated;
- **recommended implementation** — a strong default for the first version;
- **future extensions** — explicitly not required for MVP.

When a section contains `MUST`, `SHOULD`, or `MAY`, interpret them as follows:

- **MUST**: required for acceptance of the relevant milestone;
- **SHOULD**: expected unless a documented reason exists not to do it;
- **MAY**: optional or deferred.

---

## 1. Executive summary

The product is a self-hosted personal media CMS for Telegram channel operators.

It provides a pipeline:

```text
capture -> download -> inspect -> normalize -> fingerprint -> catalogue
        -> review -> queue -> schedule -> publish -> record history
```

A typical user flow:

1. The administrator sees an interesting YouTube video, direct MP4/WebM, image, Telegram post, or media page.
2. A Tampermonkey userscript sends the page URL and optional context to a small Windows localhost companion.
3. The companion forwards the ingest request to an always-on backend.
4. The backend downloads the source, probes it, converts it into a canonical Telegram-friendly format, creates fingerprints, and checks for duplicates or near-duplicates.
5. The canonical media is uploaded to a private Telegram storage channel.
6. PostgreSQL stores searchable metadata and mappings to Telegram `chat_id`, `message_id`, `file_id`, and `file_unique_id` where available.
7. The administrator can tag, describe, search, and create publication drafts.
8. The publisher schedules posts independently of whether the Windows machine is running.
9. Before publishing, the system checks configurable repost/cooldown policies and warns when similar content was posted recently.
10. Publication history is retained so the system can answer when, where, and with which caption a content item was posted.

The first release is single-admin and self-hosted. The internal design must avoid needless multi-tenant complexity, while keeping boundaries clean enough to support multiple admins later.

---

## 2. Product positioning

### 2.1 Product statement

> A self-hosted media inbox, catalogue, deduplication engine, and publishing queue for Telegram channels.

### 2.2 Core value proposition

The product reduces the friction between discovering content and publishing it responsibly:

- one-click capture from a desktop browser;
- normalized, Telegram-compatible media;
- searchable personal media library;
- exact and perceptual duplicate detection;
- reusable Telegram-hosted media;
- scheduled posting that runs on an always-on server;
- configurable repost/cooldown warnings;
- full publication history.

### 2.3 Primary persona

**Single channel administrator** who:

- browses content on Windows;
- operates one or more Telegram channels;
- wants to collect material continuously;
- wants publication to continue while the PC is off;
- wants control over captions and posting cadence;
- accepts occasional reposting but wants policy-driven warnings;
- is willing to self-host Docker services.

### 2.4 Secondary future personas

Not part of MVP:

- editor with limited permissions;
- reviewer/approver;
- multiple channel owners on one installation;
- hosted SaaS operator;
- non-Telegram publication targets.

---

## 3. Guiding principles

1. **Modular monolith before microservices.** Inbox, Library, Publisher, Jobs, Media, and Telegram are explicit modules, but initially share one repository and one PostgreSQL database.
2. **Always-on server owns durable workflows.** The Windows companion is a capture bridge, not a scheduler or source of truth.
3. **PostgreSQL is the source of truth.** Telegram stores media objects and messages; it is not the authoritative catalogue.
4. **Every asynchronous operation is durable.** No business-critical schedule or media-processing workflow may exist only in memory.
5. **Idempotency by design.** Retries must not create duplicate content items, duplicate storage messages, or duplicate channel posts.
6. **Evidence over magic.** Duplicate detection returns explainable signals and confidence, not an unexplained boolean.
7. **Safe defaults.** Localhost is loopback-only, server APIs require authentication, remote downloads are protected against SSRF, and subprocesses are constrained.
8. **Vertical increments.** Each milestone should deliver a small usable flow rather than a broad collection of unfinished abstractions.
9. **External binaries are acceptable.** `ffmpeg`, `ffprobe`, and `yt-dlp` should initially be invoked as subprocesses instead of wrapped in complex native bindings.
10. **Open-source operability matters.** Docker Compose, health checks, migrations, backup instructions, and diagnostics are product features.
11. **Do not overfit to Telegram forever.** Introduce provider interfaces only at real integration seams, not for every domain type.
12. **Human remains in control.** Near-duplicates, captions, and cooldown violations should be reviewable and overridable.

---

## 4. Scope and milestones

### 4.1 MVP / v0.1

The MVP MUST support:

- one administrator identified by Telegram user ID;
- ingesting a URL through the backend HTTP API;
- ingesting a URL through a private chat with the bot;
- direct image/video upload to the bot when Bot API limits permit;
- durable PostgreSQL-backed jobs;
- downloading direct media URLs;
- downloading supported pages through `yt-dlp`;
- `ffprobe` inspection;
- canonical normalization through `ffmpeg`;
- SHA-256 exact file hashing;
- a basic perceptual video fingerprint based on sampled frames;
- exact duplicate detection;
- near-duplicate candidate detection with a score and evidence;
- private Telegram storage channel upload;
- a searchable Library with text and tag filters;
- creation of a post draft from a library item;
- immediate and scheduled publication to one or more configured Telegram channels;
- durable publication history;
- simple configurable cooldown warning;
- Docker Compose deployment;
- CI with formatting, linting, tests, and migration validation;
- structured logs and health endpoints;
- a CLI-only Windows companion that accepts localhost requests and forwards them to the backend;
- a sample Tampermonkey userscript.

### 4.2 v0.2 candidates

- albums/media groups;
- browser extension instead of or in addition to Tampermonkey;
- richer Telegram inline keyboard UI;
- web admin UI;
- audio fingerprinting;
- scene-change-aware sampling;
- OCR and speech-to-text;
- semantic embeddings;
- S3-compatible storage provider;
- local filesystem storage provider;
- configurable caption templates;
- multiple publication windows per channel;
- recurring/repost rules by tag or content family;
- automatic source attribution formatting.

### 4.3 Explicit non-goals for MVP

- Kubernetes;
- Kafka, RabbitMQ, or a mandatory Redis dependency;
- microservice deployment;
- multi-tenant SaaS;
- billing;
- automatic copyright determination;
- automatic publication without configured policy;
- arbitrary user-supplied shell commands;
- fully general webpage scraping;
- perfect detection of transformed videos;
- storing only in Telegram with no database backup;
- a polished desktop GUI;
- an LLM dependency for core workflows.

---

## 5. Legal and responsible-use boundary

The project handles third-party media. The software MUST make no claim that a downloadable URL grants permission to download or republish its content.

Documentation MUST state:

- users are responsible for copyright, platform terms, privacy, and local law;
- source URLs and attribution metadata should be preserved;
- downloader adapters may stop working when upstream sites change;
- the project should not ship site-specific credential theft, DRM bypass, or access-control circumvention;
- authenticated cookies, if ever supported, must be opt-in and stored securely;
- removal of watermarks or attribution is not a product goal.

The codebase SHOULD keep download adapters isolated so maintainers can disable or remove problematic integrations.

---

## 6. System context

```text
+-------------------------+
| Browser on Windows      |
| - YouTube / web pages   |
| - Tampermonkey script   |
+------------+------------+
             |
             | POST http://127.0.0.1:<port>/v1/submit
             v
+-------------------------+
| Local Companion         |
| - loopback HTTP API     |
| - local token           |
| - forwards to backend   |
+------------+------------+
             |
             | HTTPS + device API token
             v
+-------------------------------------------------------+
| Always-on Backend                                      |
|                                                       |
|  Server process                                       |
|  - public/private HTTP API                            |
|  - Telegram updates                                   |
|  - Inbox application service                         |
|  - Library queries/commands                           |
|  - Publisher scheduler                               |
|                                                       |
|  Worker process                                       |
|  - download                                           |
|  - ffprobe / ffmpeg                                   |
|  - hashing / fingerprinting                           |
|  - Telegram upload                                    |
|  - publication jobs                                   |
|                                                       |
|  PostgreSQL                                           |
|  - catalogue                                          |
|  - jobs                                               |
|  - schedules and history                              |
+-------------------------+-----------------------------+
                          |
                          v
              +-------------------------+
              | Telegram                |
              | - admin bot chat        |
              | - private storage       |
              | - target channels       |
              +-------------------------+
```

---

## 7. Deployment topology

### 7.1 Recommended production topology

Docker Compose services:

```text
postgres
app-server
app-worker
telegram-bot-api   # optional profile, recommended for large media
reverse-proxy      # optional; Caddy/Traefik/Nginx chosen by deployer
```

Shared volumes:

```text
postgres-data/
work-data/
telegram-bot-api-data/   # when enabled
```

`work-data` contains only:

- in-progress downloads;
- intermediate normalized files;
- generated thumbnails and sampled frames;
- retryable upload material;
- bounded cache entries.

It MUST NOT be treated as the only durable copy of catalogue data.

### 7.2 Development topology

On Windows, either:

- run Rust natively and PostgreSQL/Telegram Bot API through Docker Desktop; or
- use WSL2 for the whole development environment.

The project SHOULD support both, but CI on Linux is the reference environment.

A developer must be able to run:

```bash
cargo run -p sooqa-server
cargo run -p sooqa-worker
cargo run -p sooqa-companion
```

and alternatively:

```bash
docker compose up --build
```

### 7.3 Process responsibilities

#### `sooqa-server`

- runs database migrations only when explicitly configured or via a dedicated command;
- serves API routes;
- consumes Telegram updates by long polling in MVP;
- executes lightweight application commands;
- runs the durable scheduler tick;
- enqueues heavy jobs;
- exposes `/health/live`, `/health/ready`, and `/metrics` when metrics are enabled.

#### `sooqa-worker`

- claims durable jobs;
- downloads media;
- executes `ffprobe`, `ffmpeg`, and `yt-dlp`;
- computes hashes and fingerprints;
- uploads canonical media to the storage channel;
- publishes scheduled posts;
- applies retries and records errors;
- performs periodic cleanup jobs.

#### `sooqa-companion`

- binds to `127.0.0.1` only;
- accepts authenticated localhost submissions;
- forwards normalized requests to the backend;
- keeps no Telegram bot token;
- stores only a device token and non-sensitive local settings;
- has no responsibility after the backend accepts the request.

---

## 8. Technology choices

### 8.1 Language and toolchain

- Rust stable toolchain;
- Rust 2024 edition;
- repository-pinned toolchain via `rust-toolchain.toml`;
- `cargo fmt` and `cargo clippy` enforced in CI;
- committed `Cargo.lock` because this repository builds applications.

Do not hardcode a stale compiler version in this specification. At repository bootstrap, pin the current stable version, document it in the first PR, and update deliberately.

### 8.2 Recommended Rust stack

Use current mutually compatible releases at implementation time and centralize versions in `[workspace.dependencies]`.

- async runtime: `tokio`;
- HTTP server: `axum`;
- middleware: `tower`, `tower-http`;
- Telegram bot framework: `teloxide`, while keeping Bot API calls behind a project adapter;
- HTTP client: `reqwest` with rustls;
- database: PostgreSQL + `sqlx`;
- serialization: `serde`, `serde_json`;
- IDs: `uuid` with UUIDv7 where available;
- time: `time` or `chrono`, choose one and use it consistently;
- errors: `thiserror` for typed library errors, `anyhow` only at binary/application boundaries;
- logging/tracing: `tracing`, `tracing-subscriber`;
- configuration: `config` or a small explicit loader combining environment and TOML;
- hashing: `sha2`;
- image decoding/resizing: `image`;
- CLI: `clap`;
- secrets in memory: `secrecy` where practical;
- tests: standard Rust tests, `tokio::test`, `testcontainers` or Docker-backed integration tests;
- HTTP contract tests: direct router tests and real-process smoke tests;
- temporary files: `tempfile`.

### 8.3 External runtime dependencies

- PostgreSQL;
- `ffmpeg` and `ffprobe`;
- `yt-dlp`;
- optionally Telegram Local Bot API Server.

At application startup, emit a clear diagnostic showing detected versions and missing required binaries. The server should not require media binaries; the worker does.

### 8.4 Why subprocesses first

Invoke external media tools through `tokio::process::Command`.

Benefits:

- faster implementation;
- behavior matches upstream tools;
- easier upgrades;
- fewer unsafe native bindings;
- easier reproduction from command lines in logs.

Requirements:

- never invoke through a shell;
- pass every argument as a separate argument;
- impose timeouts;
- capture bounded stderr/stdout;
- use process groups or platform-equivalent termination where possible;
- redact credentials and secrets from logs;
- record executable version in diagnostics.

---

## 9. Repository layout

Recommended initial layout:

```text
.
├── AGENTS.md
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── rustfmt.toml
├── clippy.toml
├── deny.toml
├── .editorconfig
├── .env.example
├── .gitignore
├── LICENSE
├── README.md
├── CHANGELOG.md
├── docker-compose.yml
├── Dockerfile
├── Justfile
├── apps/
│   ├── server/
│   ├── worker/
│   └── companion/
├── crates/
│   ├── kernel/
│   ├── inbox/
│   ├── library/
│   ├── publisher/
│   ├── jobs/
│   ├── media/
│   ├── telegram/
│   ├── persistence/
│   ├── api/
│   └── test-support/
├── migrations/
├── fixtures/
│   ├── metadata/
│   └── README.md
├── scripts/
│   ├── dev/
│   └── ci/
├── deploy/
│   ├── compose/
│   └── examples/
├── docs/
│   ├── PROJECT_SPEC.md
│   ├── architecture.md
│   ├── operations.md
│   ├── security.md
│   ├── development.md
│   ├── review-checklist.md
│   └── adr/
├── userscripts/
│   └── tampermonkey.user.js
└── .github/
    ├── workflows/
    ├── ISSUE_TEMPLATE/
    ├── pull_request_template.md
    ├── dependabot.yml
    └── CODEOWNERS
```

### 9.1 Crate responsibilities

#### `kernel`

Shared primitives only:

- typed IDs;
- UTC timestamp wrapper/conventions;
- pagination types;
- common validation primitives;
- redacted secret type helpers;
- domain-neutral error codes.

It MUST NOT depend on Axum, SQLx, Teloxide, or media tools.

#### `inbox`

- ingest request domain model;
- source submission validation;
- application commands for creating/cancelling/retrying ingest;
- source normalization rules;
- ports for enqueueing the first processing job.

#### `library`

- content item and media asset models;
- source records;
- tags and descriptions;
- fingerprints and duplicate candidates;
- storage references;
- search commands/queries;
- merge/variant decisions.

#### `publisher`

- post drafts;
- target channel configuration;
- publication schedules;
- channel policies;
- cooldown evaluation;
- publication history and attempts;
- idempotency semantics.

#### `jobs`

- job model;
- claiming and lease semantics;
- retry policy;
- handler registry;
- heartbeat and stale-job recovery;
- scheduling adapter.

#### `media`

- downloader interfaces and implementations;
- probe model;
- normalization plan;
- ffmpeg/ffprobe/yt-dlp runners;
- exact hashing;
- frame extraction and perceptual fingerprinting;
- workspace lifecycle.

#### `telegram`

- Bot API adapter;
- update translation;
- storage provider implementation;
- publication target implementation;
- command/keyboard rendering;
- Telegram error classification.

#### `persistence`

- SQLx repositories;
- transaction helpers;
- migrations integration;
- PostgreSQL job repository;
- search query implementations.

#### `api`

- Axum routes;
- request/response DTOs;
- authentication middleware;
- idempotency middleware;
- error mapping;
- OpenAPI generation only if it remains low-friction.

### 9.2 Dependency direction

Domain/application crates must not import adapters.

Preferred direction:

```text
kernel
  ^
  |
inbox   library   publisher   jobs
  ^        ^          ^         ^
  |        |          |         |
media   telegram   persistence  api
          ^             ^        ^
          +-------------+--------+
                        |
                 server / worker
```

Exact Rust crate edges may differ, but circular dependencies are forbidden.

---

## 10. Domain glossary

### Ingest Request

A durable record that the administrator submitted a URL, Telegram message, or file for processing.

### Source

A representation of where media came from: page URL, direct media URL, platform content ID, Telegram origin, or uploaded file.

### Content Item

The logical editorial item: “that particular video/image”. Multiple transformed files may belong to the same content item.

### Media Asset

A concrete binary representation of content: original download, normalized MP4, JPEG preview, thumbnail, or Telegram-stored representation.

### Canonical Asset

The preferred normalized asset used for storage and publication.

### Fingerprint

Data used to identify exact or perceptually similar content. Examples: SHA-256, frame dHashes, duration, dimensions.

### Duplicate Candidate

A scored relationship between two content items or assets that may represent the same underlying content.

### Storage Object

A durable reference to media uploaded to a storage provider. The first provider is Telegram private-channel storage.

### Post Draft

Editable publication intent containing content, caption, target, and optional overrides.

### Publication Schedule

A durable request to publish a draft at or within a defined time window.

### Publication Attempt

One execution attempt, successful or failed.

### Published Post

A confirmed Telegram channel message resulting from a publication schedule.

### Channel Policy

Configuration controlling cadence, cooldown, time windows, and duplicate warnings for a target channel.

---

## 11. Core domain model

Use UUIDv7 identifiers and UTC timestamps. Database naming uses `snake_case`; Rust uses normal Rust naming conventions.

### 11.1 Ingest Request

Suggested fields:

```text
id
kind                         # url | telegram_message | upload
status
submitted_via                # api | companion | telegram_bot
submitted_by_admin_id
original_input               # redacted/structured as appropriate
source_url
page_url
page_title
supplied_caption
supplied_tags[]
idempotency_key
error_code
error_message
created_at
updated_at
completed_at
```

State machine:

```text
received
  -> queued
  -> downloading
  -> probing
  -> exact_dedup_check
  -> normalizing
  -> fingerprinting
  -> similarity_check
  -> storing
  -> completed

Any active state -> failed_retryable -> queued
Any active state -> failed_terminal
Any non-terminal state -> cancelled
```

The state should describe user-visible progress, while technical job states remain separate.

### 11.2 Content Item

```text
id
kind                         # video | image | animation
status                       # active | archived | deleted
canonical_asset_id
preferred_title
editorial_description
notes
created_at
updated_at
archived_at
```

### 11.3 Media Asset

```text
id
content_item_id
role                         # original | canonical | preview | thumbnail
media_kind
mime_type
container
video_codec
audio_codec
width
height
duration_ms
bit_rate
file_size_bytes
sha256
local_work_path              # nullable, never exposed publicly
storage_state
created_at
```

A local work path is temporary operational state. After confirmed storage and retention expiry it becomes null.

### 11.4 Source Record

```text
id
content_item_id
ingest_request_id
source_type                  # webpage | direct_url | youtube | telegram | upload
original_url
normalized_url
platform
platform_content_id
author_name
source_title
source_description
source_published_at
retrieved_at
metadata_json
```

Unique constraints should prevent repeated attachment of the same platform ID or normalized URL where safe.

### 11.5 Tag

```text
tag(id, normalized_name, display_name, created_at)
content_item_tag(content_item_id, tag_id, created_at)
```

Rules:

- normalized names are lowercase;
- trim whitespace;
- reject empty tags;
- establish a conservative maximum length;
- do not silently merge visually different Unicode strings without a documented normalization rule.

### 11.6 Fingerprint

```text
id
asset_id
algorithm                    # sha256 | frame_dhash_v1 | frame_histogram_v1
algorithm_version
fingerprint_json_or_bytes
created_at
```

Do not overwrite old versions. Version algorithms so the library can be re-indexed later.

### 11.7 Duplicate Candidate

```text
id
left_content_item_id
right_content_item_id
score                        # 0.0 .. 1.0
classification               # exact | likely | possible | dismissed | confirmed_variant
signals_json
status                       # pending | confirmed | dismissed
created_at
resolved_at
```

Canonical ordering MUST prevent both `(A,B)` and `(B,A)` rows.

### 11.8 Telegram Storage Reference

```text
id
asset_id
provider                     # telegram
storage_chat_id
storage_message_id
telegram_file_id
telegram_file_unique_id
media_kind
stored_at
verified_at
status                       # active | missing | inaccessible | deleted
```

`telegram_file_id` is reusable but bot-specific and should not be treated as a universal content identity. `file_unique_id` is an additional Telegram-side identity signal, not a replacement for local fingerprints.

### 11.9 Target Channel

```text
id
name
telegram_chat_id
is_enabled
default_parse_mode
default_disable_notification
created_at
updated_at
```

### 11.10 Channel Policy

```text
target_channel_id
minimum_post_interval_seconds
same_content_cooldown_seconds
similar_content_cooldown_seconds
similarity_threshold
on_cooldown_violation        # warn | block | allow
allowed_windows_json
max_posts_per_day
jitter_seconds
updated_at
```

For MVP, support one simple daily window or no window. Model the table so multiple windows can be added later.

### 11.11 Post Draft

```text
id
content_item_id
asset_id
target_channel_id
caption
parse_mode
status                       # editing | ready | scheduled | published | cancelled
created_at
updated_at
```

### 11.12 Publication Schedule

```text
id
post_draft_id
status                       # pending | queued | publishing | published | failed | cancelled
publish_at
not_before
not_after
priority
cooldown_override            # nullable
idempotency_key
created_at
updated_at
```

### 11.13 Publication Attempt

```text
id
publication_schedule_id
attempt_number
status
started_at
finished_at
telegram_request_id_or_key
error_class
error_message
response_json_redacted
```

### 11.14 Published Post

```text
id
publication_schedule_id
content_item_id
asset_id
target_channel_id
telegram_chat_id
telegram_message_id
caption_snapshot
published_at
status                       # active | edited | deleted | unknown
```

---

## 12. PostgreSQL schema requirements

### 12.1 General conventions

- all timestamps use `timestamptz` and UTC;
- all primary keys use UUID;
- JSONB is acceptable for external metadata and versioned evidence, not as a substitute for all relational columns;
- foreign keys are required unless a documented performance reason exists;
- destructive cascades must be rare and intentional;
- user-facing deletion should normally archive or tombstone;
- migrations are forward-only for production; a rollback is a new migration;
- every table has a documented owner module.

### 12.2 Required tables for MVP

```text
admins
device_tokens
ingest_requests
content_items
media_assets
source_records
tags
content_item_tags
fingerprints
duplicate_candidates
storage_objects
target_channels
channel_policies
post_drafts
publication_schedules
publication_attempts
published_posts
jobs
job_attempts
idempotency_records
outbox_events                # optional in MVP; recommended if notifications grow
```

### 12.3 Important indexes

At minimum:

- `ingest_requests(status, created_at)`;
- unique partial index on non-null `ingest_requests.idempotency_key`;
- unique `media_assets.sha256` where non-null, with careful variant semantics;
- `source_records(platform, platform_content_id)` where non-null;
- `source_records(normalized_url)`;
- `content_item_tags(tag_id, content_item_id)`;
- `fingerprints(asset_id, algorithm, algorithm_version)`;
- `duplicate_candidates(status, score desc)`;
- unique storage `(provider, storage_chat_id, storage_message_id)`;
- `publication_schedules(status, publish_at, priority)`;
- `published_posts(content_item_id, target_channel_id, published_at desc)`;
- `jobs(status, available_at, priority desc)`;
- GIN full-text index for preferred title, description, source title, and tags, if PostgreSQL FTS is implemented in MVP.

### 12.4 Search document

For simple and fast MVP search, maintain a generated or explicitly updated `tsvector` column on `content_items`, populated from:

- preferred title;
- editorial description;
- notes;
- source title/description;
- tag names.

Alternative: build a SQL view/query joining related tables. Choose the approach with simpler correctness first. Add trigram search only after measuring need.

---

## 13. Durable job queue

PostgreSQL is sufficient for MVP and avoids mandatory Redis/RabbitMQ.

### 13.1 Job fields

```text
id
job_type
payload_json
status                       # queued | running | succeeded | retry_wait | failed | cancelled
priority
available_at
attempt_count
max_attempts
lease_owner
lease_expires_at
last_heartbeat_at
last_error_class
last_error_message
idempotency_key
created_at
updated_at
completed_at
```

### 13.2 Claiming algorithm

Use a transaction and `FOR UPDATE SKIP LOCKED`:

```sql
WITH candidate AS (
    SELECT id
    FROM jobs
    WHERE status IN ('queued', 'retry_wait')
      AND available_at <= now()
    ORDER BY priority DESC, available_at ASC, created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE jobs
SET status = 'running',
    lease_owner = $1,
    lease_expires_at = now() + $2::interval,
    last_heartbeat_at = now(),
    attempt_count = attempt_count + 1,
    updated_at = now()
WHERE id = (SELECT id FROM candidate)
RETURNING *;
```

Implementation details may vary. The behavior MUST remain atomic.

### 13.3 Leases and heartbeats

- long-running jobs renew leases;
- a recovery task requeues expired running jobs;
- the worker ID is random per process start;
- a process must stop handling a job if it loses its lease;
- job handlers must remain idempotent even with leases.

### 13.4 Retry classification

Retryable examples:

- transient HTTP 5xx;
- Telegram rate limit or temporary network failure;
- temporary upstream unavailable;
- worker crash;
- database serialization failure.

Terminal examples:

- unsupported media type;
- invalid URL;
- permanent Telegram permission error;
- source not found after a bounded policy;
- invalid media that ffprobe cannot parse;
- exceeded configured size/duration limit;
- authentication required but unavailable.

Use exponential backoff with bounded jitter. Honor Telegram `retry_after` when provided.

### 13.5 Job types

Initial set:

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

A single orchestration job may call stages directly in early iterations, but persisted stage transitions and idempotency boundaries MUST remain clear.

### 13.6 Idempotency

Every externally triggered command should support an idempotency key.

Examples:

- companion submission;
- Telegram update ID processing;
- storage upload for a canonical asset;
- publication schedule execution.

Publication must use a deterministic execution key such as:

```text
publish:<publication_schedule_id>:v1
```

Before sending, record an attempt in a transaction. After sending, store the returned message ID. On ambiguous network failure, do not blindly send again; mark the attempt `unknown` and require reconciliation or a bounded recovery strategy.

Telegram does not provide a universal client-provided idempotency key for message sending, so this ambiguity must be explicitly modeled.

---

## 14. Ingestion module

### 14.1 Supported submission forms

MVP:

- HTTP URL submission;
- Telegram text message containing one URL;
- Telegram direct video/image/document;
- companion-forwarded URL plus context.

Later:

- multiple URLs per request;
- albums;
- browser extension;
- clipboard image/file;
- forwarded Telegram posts with full origin preservation.

### 14.2 URL normalization

Normalization MUST be conservative.

Safe transformations:

- lowercase scheme and host;
- remove default port;
- remove fragment;
- normalize known tracking parameters only through an allowlisted rule set;
- preserve query parameters by default;
- extract known platform content IDs where reliable.

Do not assume two URLs are identical just because their path is similar.

### 14.3 Ingest API command

Request:

```http
POST /api/v1/ingest-requests
Authorization: Bearer <device-token>
Idempotency-Key: <uuid-or-random-string>
Content-Type: application/json
```

```json
{
  "url": "https://example.com/video-page",
  "page_url": "https://example.com/thread/123",
  "page_title": "Interesting clip",
  "selected_text": "Possible caption",
  "tags": ["reaction", "cats"]
}
```

Response `202 Accepted`:

```json
{
  "id": "019...",
  "status": "queued",
  "links": {
    "self": "/api/v1/ingest-requests/019..."
  }
}
```

Repeated request with the same key and same payload returns the original response. Same key with a different payload returns `409 Conflict`.

### 14.4 Source inspection order

1. Validate URL and policy.
2. Resolve DNS safely and reject forbidden destinations.
3. Perform bounded HEAD/GET inspection when useful.
4. Detect direct media by content type, URL, and magic bytes.
5. Otherwise invoke `yt-dlp` metadata mode.
6. Choose the downloader adapter.
7. Persist retrieved metadata before large download when possible.

### 14.5 Downloader interface

Conceptual interface:

```rust
#[async_trait]
pub trait SourceDownloader: Send + Sync {
    async fn inspect(&self, source: &SourceInput) -> Result<SourceInspection, DownloadError>;
    async fn download(
        &self,
        inspection: &SourceInspection,
        destination: &Path,
        limits: &DownloadLimits,
    ) -> Result<DownloadedSource, DownloadError>;
}
```

First implementations:

- `DirectHttpDownloader`;
- `YtDlpDownloader`;
- `TelegramFileDownloader`.

### 14.6 Download limits

Configuration MUST include:

- maximum response bytes;
- maximum redirects;
- connect timeout;
- total download timeout;
- minimum/maximum accepted media duration;
- maximum source file size;
- allowed protocols (`http`, `https` only);
- allowed media types;
- concurrency limit per worker;
- optional domain denylist and allowlist.

Files must stream to disk; never buffer full videos in memory.

### 14.7 Progress

The job may emit progress to PostgreSQL at a bounded rate, such as every 2 seconds or 5% change. Avoid a write per downloaded chunk.

The bot may update one status message, but Telegram update frequency must be rate-limited.

---

## 15. Media workspace lifecycle

Each ingest request gets an isolated workspace:

```text
work-data/jobs/<job-id>/
  source/
  normalized/
  frames/
  previews/
  logs/
  manifest.json
```

Rules:

- generated path components come from internal IDs, never raw URLs or titles;
- workspace permissions should be restrictive;
- no symlink traversal;
- all outputs must remain under the workspace root;
- subprocess output filenames are explicitly controlled;
- success schedules delayed cleanup;
- retryable failure retains files for configured time;
- terminal failure retains only bounded diagnostic data;
- cleanup never deletes paths outside the configured work root.

`manifest.json` is diagnostic convenience, not the source of truth.

---

## 16. Probe and normalization

### 16.1 Probe

Use `ffprobe` JSON output, for example conceptually:

```bash
ffprobe \
  -v error \
  -show_format \
  -show_streams \
  -of json \
  input
```

Parse into a project-owned `MediaProbe` model. Do not expose ffprobe JSON directly throughout the domain.

Record:

- container format;
- file duration;
- file size;
- streams;
- video codec/profile/pixel format;
- width/height and display aspect;
- frame rate;
- rotation/display matrix;
- audio codec/sample rate/channels;
- bit rate when reliable.

### 16.2 Canonical video profile v1

Default target:

```text
container: MP4
video codec: H.264
pixel format: yuv420p
audio codec: AAC
fast start: enabled
maximum resolution: configurable, default 1080p without upscaling
frame rate: preserve unless invalid/excessive; cap configurable
metadata: remove unnecessary metadata, preserve explicit attribution in DB
```

The exact encoding parameters should balance quality and Telegram size constraints. Suggested baseline:

```bash
ffmpeg -i input \
  -map 0:v:0 -map 0:a:0? \
  -vf "scale='min(iw,1920)':'min(ih,1080)':force_original_aspect_ratio=decrease,format=yuv420p" \
  -c:v libx264 -preset medium -crf 23 \
  -c:a aac -b:a 128k \
  -movflags +faststart \
  -map_metadata -1 \
  output.mp4
```

This is illustrative, not a command to copy blindly. The implementation MUST correctly handle portrait video and avoid distorting aspect ratio. Test the actual filter expression.

### 16.3 Remux fast path

If the input is already compatible:

- MP4 container;
- supported H.264 video;
- acceptable pixel format;
- acceptable AAC or no audio;
- acceptable resolution, frame rate, and size;

then remux without re-encoding:

```bash
ffmpeg -i input -map 0:v:0 -map 0:a:0? -c copy -movflags +faststart output.mp4
```

Validate output with ffprobe. Fall back to transcode when remux fails or violates policy.

### 16.4 Image profile v1

MVP target:

- JPEG for opaque photos;
- PNG only when transparency materially matters;
- strip unnecessary metadata;
- configurable maximum dimensions;
- preserve aspect ratio;
- generate a thumbnail/preview.

Animated images should initially be treated as video/animation and normalized to MP4 where practical.

### 16.5 Size adaptation

The processor MUST know the effective upload limit of the configured Telegram API endpoint.

Strategy:

1. create the standard canonical version;
2. if over limit, calculate a lower target bitrate/resolution;
3. perform a second bounded attempt;
4. if still over limit, fail with an actionable error or split only in a future milestone.

Do not loop indefinitely.

### 16.6 Progress and cancellation

Use ffmpeg progress output (`-progress pipe:1`) when practical. Cancellation should terminate the process and mark the job cancelled. On Windows and Linux, ensure child processes are not orphaned.

---

## 17. Exact and perceptual deduplication

### 17.1 Deduplication goals

Detect:

- repeated submission of the same URL;
- repeated platform content ID;
- identical source bytes;
- identical canonical output;
- same video re-encoded at a different quality;
- likely variants requiring human decision.

MVP is not expected to reliably detect:

- severe cropping;
- picture-in-picture;
- large overlays;
- mirrored content;
- speed changes;
- reordered scenes;
- compilations containing only part of an existing video.

### 17.2 Layered signals

#### Layer A: source identity

Signals:

- normalized URL;
- platform + platform content ID;
- Telegram `file_unique_id`;
- source message identity.

#### Layer B: exact bytes

- SHA-256 of downloaded source;
- SHA-256 of canonical output;
- file size.

#### Layer C: structural metadata

- duration difference;
- aspect ratio;
- width/height;
- stream count;
- audio presence.

#### Layer D: visual fingerprint

Version `frame_dhash_v1`:

1. Determine video duration.
2. Select normalized timestamps excluding extreme intro/outro where possible, e.g. 5%, 15%, 30%, 50%, 70%, 85%, 95%.
3. For very short videos, choose fewer distinct timestamps.
4. Extract one frame per timestamp through ffmpeg.
5. Decode with the `image` crate.
6. Convert to grayscale.
7. Resize to 9×8.
8. Compute 64-bit difference hash from adjacent pixels.
9. Store timestamp ratio and hash.
10. Optionally store a coarse color histogram for tie-breaking.

The implementation should generate fixtures and unit tests for hash stability.

### 17.3 Comparing fingerprint sequences

For two videos:

- reject or heavily penalize large duration mismatch;
- compare each frame hash to nearby relative positions in the other sequence;
- calculate Hamming distance;
- aggregate robustly using median or trimmed mean;
- require multiple matching frames;
- produce evidence containing per-frame distances and duration ratio.

Example conceptual score:

```text
visual_score = 1 - normalized_median_hamming_distance
duration_score = max(0, 1 - abs(d1-d2)/max(d1,d2))
structure_score = aspect/audio compatibility
final_score = 0.75*visual + 0.20*duration + 0.05*structure
```

Weights are initial hypotheses. Put them in configuration or a versioned algorithm constant and cover them with tests.

### 17.4 Thresholds

Initial product defaults, subject to calibration:

```text
1.00              exact SHA-256 match
>= 0.90           likely duplicate
0.75 .. 0.90      possible duplicate, human review
< 0.75            no warning
```

Never silently discard a source solely because a perceptual score is high. Exact duplicates may reuse the existing asset, but should still attach the new source record.

### 17.5 Duplicate behavior

#### Exact source or file duplicate

- reuse existing content item and canonical asset;
- add a new source record if new;
- complete ingest as `completed_existing`;
- do not upload another Telegram storage message;
- notify admin with a link to the existing item.

#### Likely perceptual duplicate

- create a candidate relationship;
- either attach as a provisional variant or create a separate item, based on implementation simplicity;
- present evidence to admin;
- allow `confirm variant`, `keep separate`, or `dismiss`.

### 17.6 Re-indexing

Fingerprint algorithm version is stored. Provide a future CLI/job command:

```bash
sooqa-admin fingerprints reindex --algorithm frame_dhash_v2
```

Not required for v0.1 UI, but the schema must support it.

---

## 18. Telegram integration

### 18.1 Roles of Telegram

Telegram provides:

- bot chat UI;
- private storage channel;
- publication destination channels;
- reusable `file_id` for media already stored by the bot.

PostgreSQL provides:

- catalogue;
- metadata;
- search;
- fingerprints;
- schedules;
- publication history;
- configuration.

### 18.2 Official Bot API constraints

The implementation must not hardcode assumptions without startup diagnostics and documentation.

As verified on 2026-08-06, Telegram documents that a Local Bot API Server allows downloads without a size limit and uploads up to 2000 MB. Official reference:

- https://core.telegram.org/bots/api#using-a-local-bot-api-server

The application MUST support a configurable Bot API base URL:

```text
TELEGRAM_API_BASE_URL=https://api.telegram.org
```

or local:

```text
TELEGRAM_API_BASE_URL=http://telegram-bot-api:8081
```

### 18.3 Storage channel

A private channel is configured by `TELEGRAM_STORAGE_CHAT_ID`. The bot must be an administrator with permission to post.

Storage message caption should be short and diagnostic, for example:

```text
asset: <asset-id>
content: <content-item-id>
source: <hostname/platform>
sha256: <prefix>
```

Do not put sensitive tokens or full internal metadata into Telegram captions.

### 18.4 Storage write protocol

1. Verify canonical asset exists locally and hash matches DB.
2. Check whether an active storage object already exists for the asset.
3. Create an upload intent/idempotency record.
4. Send media to storage channel.
5. Persist returned message/file identifiers in one transaction.
6. Mark asset stored.
7. Schedule local cleanup after retention period.

If sending succeeds but DB persistence fails, reconciliation must be possible. Include internal asset ID in the storage caption so recent storage messages can be matched manually or automatically.

### 18.5 Reuse for publication

Prefer sending by stored `file_id` when possible. If a file ID becomes invalid:

1. mark storage reference degraded;
2. attempt to obtain/re-upload from retained/local/provider data;
3. create a new storage object;
4. retry publication within policy.

### 18.6 Telegram update processing

MVP uses long polling for operational simplicity.

Requirements:

- persist last processed update ID or rely on idempotent update processing;
- deduplicate by Telegram update ID;
- accept commands only from configured admin user IDs;
- reject group/channel commands unless explicitly configured;
- classify unauthorized attempts without leaking configuration;
- rate-limit command responses.

### 18.7 Bot commands for MVP

```text
/start        show status and authorization result
/add          explain how to submit; accept URL following command
/status       system and queue summary
/item <id>    show library item
/search ...   text/tag search
/duplicates   pending duplicate candidates
/drafts       recent drafts
/queue        upcoming publications
/publish      guided immediate publication
/cancel <id>  cancel ingest or publication where valid
/help         concise help
```

Plain messages containing one URL should create an ingest request.

### 18.8 Bot result card

Example:

```text
✅ Media processed
ID: 019...
Video · 00:23 · 720×1280 · 4.8 MB
Tags: reaction, cats

Similarity: 0.91 to item 019...
Last posted to @channel: 12 days ago
```

Buttons:

```text
[Open item] [Add tags]
[Create draft] [Duplicate details]
[Keep separate] [Mark as variant]
```

MVP may implement only a subset, but command behavior and callback data must be versioned and validated.

---

## 19. Library module

### 19.1 Responsibilities

- maintain content and asset records;
- maintain source metadata;
- manage tags and editorial fields;
- expose search and item detail;
- manage duplicate decisions;
- expose storage status;
- provide publisher with publishable assets.

Library MUST NOT decide when to publish.

### 19.2 Search API

```http
GET /api/v1/library/items?q=cat&tags=reaction,vertical&kind=video&status=active&limit=20&cursor=...
```

Search response includes:

- ID;
- title/description;
- kind;
- canonical asset summary;
- tags;
- source summary;
- duplicate warning count;
- last publication timestamp per requested/default channel where practical;
- thumbnail reference if available.

### 19.3 Item operations

```text
GET    /api/v1/library/items/{id}
PATCH  /api/v1/library/items/{id}
POST   /api/v1/library/items/{id}/tags
DELETE /api/v1/library/items/{id}/tags/{tag}
POST   /api/v1/library/items/{id}/archive
POST   /api/v1/duplicate-candidates/{id}/confirm-variant
POST   /api/v1/duplicate-candidates/{id}/keep-separate
```

Use optimistic concurrency for edits, e.g. `updated_at`/ETag or a version field, to avoid accidental overwrites later.

### 19.4 Archive and delete

MVP exposes archive, not hard delete.

Hard deletion is an administrative maintenance operation requiring:

- explicit confirmation;
- publication history handling;
- storage object handling;
- audit log or structured event;
- no orphaned references.

---

## 20. Publisher module

### 20.1 Responsibilities

- drafts and captions;
- target selection;
- scheduling;
- policy evaluation;
- durable publication execution;
- retry and ambiguous outcome handling;
- publication history.

### 20.2 Draft creation

```http
POST /api/v1/post-drafts
```

```json
{
  "content_item_id": "019...",
  "target_channel_id": "019...",
  "caption": "Caption text",
  "parse_mode": "HTML"
}
```

Validate:

- content exists and is active;
- canonical/stated asset is stored and publishable;
- target channel is enabled;
- caption length and parse mode are valid;
- no unsafe/inconsistent Telegram markup.

### 20.3 Scheduling modes

MVP supports:

- publish now;
- publish at exact UTC timestamp;
- publish after timestamp, letting policy choose next allowed slot.

Later:

- recurring daily slots;
- queue balancing;
- randomized windows;
- content categories and campaigns.

### 20.4 Scheduler

The scheduler runs on the always-on backend and never depends on the companion.

Algorithm every configured tick:

1. select pending schedules due for evaluation;
2. lock rows with `SKIP LOCKED`;
3. validate target enabled;
4. evaluate policy and cooldown;
5. if allowed, enqueue deterministic `publish_post` job;
6. if warned, keep state and notify admin or enqueue depending on override;
7. if blocked, calculate next eligible time when possible;
8. persist decision evidence.

Do not hold database locks while calling Telegram.

### 20.5 Cooldown model

Two distinct checks:

1. **same-content cooldown**: same `content_item_id` was recently published;
2. **similar-content cooldown**: a confirmed or high-score similar item was recently published.

MVP policy:

```text
on_cooldown_violation = warn | block | allow
```

For `warn`, scheduling requires an explicit override or remains awaiting review. Immediate publish may return a warning and require confirmation.

Evidence shown:

- prior post date/time;
- target channel;
- content item ID;
- similarity score if applicable;
- configured cooldown duration;
- next recommended eligible time.

### 20.6 Posting cadence

MVP channel policy supports:

- minimum interval between any two posts;
- maximum posts per day;
- optional allowed daily time window;
- jitter optional and default zero.

All policy time zones must be explicit. Store timestamps in UTC; store channel scheduling time zone as an IANA identifier.

### 20.7 Publication execution

1. Claim `publish_post` job.
2. Load schedule, draft, content, asset, storage reference, target, policy snapshot.
3. Re-check state and policy.
4. Create publication attempt.
5. Send by Telegram `file_id` plus caption.
6. Persist `PublishedPost` and complete schedule.
7. Notify admin.

The caption stored in `PublishedPost` is a snapshot, not a live link to the draft.

### 20.8 Ambiguous failures

If the HTTP connection fails after the request may have reached Telegram:

- mark attempt `unknown`;
- do not automatically retry immediately;
- alert administrator;
- provide reconciliation command;
- future enhancement may inspect recent target-channel posts to match asset/caption/time.

This is preferable to silently posting duplicates.

---

## 21. Local Windows companion

### 21.1 MVP form

A console/tray-less Rust executable is sufficient initially.

Commands:

```text
sooqa-companion init
sooqa-companion run
sooqa-companion status
sooqa-companion submit <url>
```

### 21.2 Local configuration

Store in the platform config directory, for example conceptually:

```text
%APPDATA%/Sooqa/config.toml
```

Fields:

```toml
listen_address = "127.0.0.1:47831"
backend_url = "https://media.example.com"
device_token = "..."
local_token = "..."
request_timeout_seconds = 15
```

Restrict file permissions where the OS permits.

### 21.3 Local endpoint

```http
POST /v1/submit
Authorization: Bearer <local-token>
Content-Type: application/json
```

Payload:

```json
{
  "url": "https://example.com/watch/123",
  "page_url": "https://example.com/thread/456",
  "page_title": "Page title",
  "selected_text": "Optional caption idea",
  "tags": ["tag1", "tag2"]
}
```

Response mirrors the backend accepted ID.

### 21.4 Security requirements

- bind only to loopback;
- require a random local token;
- constant-time token comparison where reasonable;
- reject missing/invalid content type;
- cap body size;
- rate-limit requests;
- no generic proxy endpoint;
- no file read endpoint;
- no arbitrary command execution;
- no server token returned to the userscript;
- do not log full tokens;
- optional Origin allowlist;
- browser page JavaScript must not be able to infer secrets from responses.

### 21.5 Device registration

MVP may provision the device token manually through an admin CLI command or environment variable. Later, add one-time pairing through the Telegram bot.

Device tokens stored server-side:

- store a secure hash, not plaintext;
- identify device name;
- record created/last-used/revoked timestamps;
- allow revocation.

---

## 22. Tampermonkey userscript

### 22.1 Required behavior

- add a userscript menu command and optional hotkey;
- collect current page URL and title;
- optionally collect selected text;
- optionally prompt for comma-separated tags;
- call companion using `GM_xmlhttpRequest` or the current recommended Tampermonkey API;
- send local authorization token from Tampermonkey private storage/config;
- show success/failure notification;
- never contain the remote backend device token or Telegram bot token.

### 22.2 Example contract

Pseudo-code:

```javascript
GM_registerMenuCommand("Save to sooqa", async () => {
  const payload = {
    url: location.href,
    page_url: location.href,
    page_title: document.title,
    selected_text: window.getSelection()?.toString() || null,
    tags: []
  };

  // POST to http://127.0.0.1:47831/v1/submit
});
```

The production script must handle token configuration without committing a user secret to Git.

### 22.3 Page-specific extraction

Do not implement arbitrary extraction in the first PR. The backend downloader gets the page URL. Site-specific extractors may later supply direct media URLs as hints, but must not become mandatory for the general flow.

---

## 23. HTTP API design

### 23.1 General conventions

- versioned prefix `/api/v1`;
- JSON requests/responses;
- UTC RFC3339 timestamps;
- request ID on every response;
- stable machine-readable error codes;
- no stack traces in client responses;
- cursor pagination;
- body size limits;
- `Idempotency-Key` for create commands that may be retried;
- admin/device token scopes;
- OpenAPI optional but recommended once endpoints stabilize.

### 23.2 Error format

```json
{
  "error": {
    "code": "source_unsupported",
    "message": "The submitted source is not supported.",
    "request_id": "019...",
    "details": {}
  }
}
```

### 23.3 Endpoint inventory

#### System

```text
GET /health/live
GET /health/ready
GET /api/v1/system/status
```

#### Ingest

```text
POST /api/v1/ingest-requests
GET  /api/v1/ingest-requests/{id}
GET  /api/v1/ingest-requests
POST /api/v1/ingest-requests/{id}/cancel
POST /api/v1/ingest-requests/{id}/retry
```

#### Library

```text
GET   /api/v1/library/items
GET   /api/v1/library/items/{id}
PATCH /api/v1/library/items/{id}
POST  /api/v1/library/items/{id}/archive
POST  /api/v1/library/items/{id}/tags
DELETE /api/v1/library/items/{id}/tags/{tag}
GET   /api/v1/library/items/{id}/duplicates
```

#### Duplicate candidates

```text
GET  /api/v1/duplicate-candidates
POST /api/v1/duplicate-candidates/{id}/confirm-variant
POST /api/v1/duplicate-candidates/{id}/keep-separate
POST /api/v1/duplicate-candidates/{id}/dismiss
```

#### Publisher

```text
POST  /api/v1/post-drafts
GET   /api/v1/post-drafts/{id}
PATCH /api/v1/post-drafts/{id}
POST  /api/v1/post-drafts/{id}/schedule
POST  /api/v1/post-drafts/{id}/publish-now
GET   /api/v1/publication-schedules
POST  /api/v1/publication-schedules/{id}/cancel
GET   /api/v1/published-posts
```

#### Admin/config

Initially CLI or environment-driven. Add writable endpoints only when authentication/audit are mature.

### 23.4 Authentication scopes

Suggested scopes:

```text
ingest:create
library:read
library:write
publisher:read
publisher:write
admin
```

The companion device token only needs `ingest:create` and possibly ingest status read for its own requests.

---

## 24. Configuration

### 24.1 Principles

- secrets through environment or secret files;
- non-secret structured config through TOML/environment;
- fail fast on invalid required configuration;
- print effective non-secret config at startup;
- never print secrets;
- every config key documented;
- environment overrides file config.

### 24.2 Example server config

```toml
[server]
listen_address = "0.0.0.0:8080"
public_base_url = "https://media.example.com"
request_body_limit_bytes = 1048576

[database]
url_env = "DATABASE_URL"
max_connections = 20

[telegram]
bot_token_env = "TELEGRAM_BOT_TOKEN"
api_base_url = "https://api.telegram.org"
admin_user_ids = [123456789]
storage_chat_id = -1001234567890
poll_timeout_seconds = 30

[media]
work_root = "/var/lib/sooqa/work"
ffmpeg_path = "ffmpeg"
ffprobe_path = "ffprobe"
ytdlp_path = "yt-dlp"
max_source_bytes = 2147483648
max_duration_seconds = 3600
max_parallel_jobs = 2
retain_success_hours = 24
retain_failure_hours = 72

[publisher]
scheduler_tick_seconds = 15

[observability]
log_format = "json"
log_level = "info"
metrics_enabled = true
```

### 24.3 Secret list

```text
DATABASE_URL
TELEGRAM_BOT_TOKEN
DEVICE_TOKEN_PEPPER or token-hashing configuration
optional webhook secret in future
optional source credentials in future
```

---

## 25. Security model

### 25.1 Main threats

- unauthorized bot commands;
- leaked Telegram bot token;
- leaked companion device token;
- malicious webpage triggering localhost requests;
- SSRF through submitted URLs;
- DNS rebinding;
- decompression bombs or malicious media;
- command injection into ffmpeg/yt-dlp;
- path traversal;
- oversized downloads and disk exhaustion;
- denial of service through repeated jobs;
- Telegram permission changes;
- accidental duplicate posting;
- secrets in logs;
- compromised dependency or container image;
- unsafe agent-generated code changes.

### 25.2 SSRF controls

The backend MUST:

- allow only `http` and `https`;
- resolve DNS and reject loopback, link-local, multicast, private, carrier-grade NAT, and metadata-service ranges unless explicitly allowed;
- revalidate destination after redirects;
- cap redirects;
- avoid trusting only the hostname string;
- use a resolver/connect strategy resistant to DNS rebinding where practical;
- reject URLs with embedded credentials;
- maintain explicit exceptions only through admin configuration;
- test IPv4 and IPv6 cases.

### 25.3 Subprocess controls

- no shell;
- fixed executable path from config;
- argument arrays;
- generated output paths;
- timeouts;
- process termination;
- resource limits where deploy environment supports them;
- container memory/CPU/PID limits recommended;
- run worker as non-root;
- read-only container filesystem except work volume;
- keep external tool output bounded.

### 25.4 Token handling

- server stores hashes of device tokens;
- bot token is never persisted to DB;
- local token is not sent to backend;
- remote device token is not exposed to browser userscript;
- token prefixes may identify records, but full values are shown only once;
- rotation and revocation are supported.

### 25.5 Authorization

All Telegram admin commands compare sender user ID against configured/admin DB records. Channel IDs are not a substitute for user authorization.

All mutating HTTP endpoints require scopes.

### 25.6 Audit trail

MVP should at least log structured security-relevant events:

- unauthorized command attempt;
- token creation/revocation;
- target channel changes;
- cooldown override;
- publication cancellation;
- duplicate resolution;
- hard-delete operation;
- configuration validation failure.

A dedicated audit table may be introduced before multi-admin support.

### 25.7 Dependency security

CI SHOULD run:

- `cargo deny check`;
- `cargo audit` or equivalent advisory scanning;
- GitHub dependency review for pull requests;
- container image scan if practical;
- secret scanning.

Lock third-party GitHub Actions to immutable commit SHAs for mature releases.

---

## 26. Reliability and consistency

### 26.1 Transaction boundaries

Use database transactions for state changes that must be atomic, such as:

- creating ingest request + first job;
- exact duplicate resolution + source attachment;
- creating publication schedule + publish job intent;
- persisting successful publication + schedule completion.

Never hold a transaction open during network or ffmpeg calls.

### 26.2 Outbox pattern

Not mandatory for the first vertical slice. Introduce a database outbox when domain changes need reliable notifications or multiple downstream consumers.

Example:

```text
transaction:
  update ingest status
  insert outbox event
commit

notifier worker:
  claim event
  send Telegram status update
  mark delivered
```

### 26.3 Graceful shutdown

Server and worker MUST:

- stop accepting new work;
- stop claiming jobs;
- signal active handlers;
- wait for bounded graceful completion;
- release/expire leases;
- flush tracing;
- exit non-zero on fatal startup errors.

### 26.4 Startup checks

Server readiness:

- database reachable;
- migrations compatible;
- required config valid;
- Telegram token validated optionally with bounded startup call or async health state.

Worker readiness:

- database reachable;
- work root writable;
- ffmpeg/ffprobe/yt-dlp executable and versions detectable;
- storage configuration present.

---

## 27. Observability

### 27.1 Structured logging

Use `tracing` spans with fields:

```text
request_id
ingest_request_id
content_item_id
asset_id
job_id
job_type
publication_schedule_id
telegram_chat_id
telegram_message_id
worker_id
attempt
```

Never log:

- tokens;
- authorization headers;
- full cookies;
- sensitive URL query values without redaction;
- arbitrary downloaded metadata at info level.

### 27.2 Log levels

- `ERROR`: terminal failure or invariant violation;
- `WARN`: retry, cooldown warning, unauthorized attempt, degraded storage;
- `INFO`: lifecycle transitions and successful major operations;
- `DEBUG`: command plans and sanitized external metadata;
- `TRACE`: disabled by default.

### 27.3 Metrics

Recommended Prometheus metrics:

```text
sooqa_http_requests_total
sooqa_http_request_duration_seconds
sooqa_jobs_queued
sooqa_jobs_running
sooqa_jobs_completed_total
sooqa_job_duration_seconds
sooqa_job_retries_total
sooqa_ingests_total
sooqa_download_bytes_total
sooqa_ffmpeg_duration_seconds
sooqa_duplicate_candidates_total
sooqa_telegram_requests_total
sooqa_telegram_rate_limits_total
sooqa_publications_total
sooqa_publication_failures_total
sooqa_workspace_bytes
```

Avoid high-cardinality labels such as content IDs.

### 27.4 Health endpoints

`/health/live`: process event loop is alive; no dependency checks.

`/health/ready`: database and essential configuration are usable. Do not make every transient upstream failure mark the server unready indefinitely.

`/api/v1/system/status`: authenticated operational summary:

- build version;
- queue counts;
- oldest queued job age;
- storage channel status;
- worker heartbeats;
- external binary versions;
- next scheduled post.

---

## 28. Testing strategy

### 28.1 Test pyramid

#### Unit tests

- validation;
- URL normalization;
- state transitions;
- cooldown calculations;
- fingerprint Hamming distance and scoring;
- retry policy;
- caption validation;
- token hashing/verification.

#### Repository integration tests

Against real PostgreSQL:

- migrations;
- constraints;
- job claiming concurrency;
- lease recovery;
- idempotency records;
- search;
- schedule locking;
- duplicate candidate uniqueness.

#### Adapter contract tests

- fake Telegram Bot API server;
- fake HTTP media source;
- fake `ffmpeg`/`yt-dlp` executable scripts for error classification;
- real ffmpeg smoke tests in a dedicated CI job.

#### End-to-end tests

Small flow:

```text
submit generated test video URL
-> download
-> normalize
-> fingerprint
-> fake Telegram storage upload
-> create draft
-> schedule
-> fake publish
-> verify history
```

### 28.2 Media fixtures

Do not commit large or copyrighted media.

Generate deterministic fixtures with ffmpeg:

```bash
ffmpeg -f lavfi -i testsrc=size=320x240:rate=25 -t 2 ...
```

Test variants:

- exact copy;
- remux;
- lower bitrate re-encode;
- changed resolution;
- distinct video;
- portrait video;
- no-audio video;
- corrupted input;
- very short input.

### 28.3 Property tests

Useful candidates:

- tag normalization idempotence;
- URL normalization idempotence;
- fingerprint distance symmetry;
- duplicate pair canonical ordering;
- scheduler never chooses a time before `not_before`;
- retry delay remains inside bounds.

### 28.4 Concurrency tests

- two workers cannot successfully claim the same job lease;
- two scheduler instances cannot enqueue duplicate publish jobs;
- repeated idempotency keys return one logical result;
- exact duplicate ingests racing result in one canonical asset.

### 28.5 Test commands

The repository MUST provide simple commands, preferably via `just`:

```bash
just fmt
just lint
just test
just test-integration
just test-media
just check
```

`just check` should be the local pre-PR gate.

---

## 29. CI and GitHub configuration

### 29.1 Required checks

For every pull request:

- formatting;
- `cargo check --workspace --all-targets`;
- Clippy with warnings denied;
- unit tests;
- PostgreSQL integration tests;
- migration validation;
- dependency/license checks;
- documentation link/check where practical.

Separate heavier job:

- real ffmpeg media tests;
- Docker image build;
- end-to-end smoke test.

### 29.2 Branch protection

For `main`:

- pull request required;
- required status checks;
- conversation resolution required;
- no force pushes;
- no direct pushes except repository bootstrap if unavoidable;
- linear history preferred;
- signed commits optional;
- one approving review can be configured even for a solo project by using self-review discipline/Codex review, though GitHub may not allow self-approval to satisfy protection.

### 29.3 Pull request template

Every PR description should contain:

```markdown
## Why

## Scope

## Out of scope

## Architecture / data model impact

## How to test

## Evidence

## Risks and rollback

## Stack
- Base PR: #...
- Next PR: #...

## Checklist
- [ ] focused diff
- [ ] tests added/updated
- [ ] docs updated
- [ ] migrations reviewed
- [ ] no secrets
- [ ] Codex `/review` run against base
```

### 29.4 Commit style

Use concise imperative commits. Conventional Commits are recommended but not mandatory.

Examples:

```text
chore: bootstrap Rust workspace
feat(jobs): claim jobs with PostgreSQL leases
feat(media): add ffprobe metadata parser
fix(publisher): prevent duplicate schedule enqueue
```

One PR may contain multiple commits during review, but squash merge should produce one clear commit per PR unless preserving commits has value.

---

## 30. Stacked pull request strategy

### 30.1 Recommendation

The proposed workflow is good: small logical PRs, each reviewable, with dependent work stacked when necessary.

As of 2026-08-06, GitHub documents native stacked pull requests in public preview and provides a `gh stack` workflow. Official documentation:

- https://docs.github.com/en/pull-requests/reference/stacked-pull-requests

A stack is conceptually:

```text
main <- PR1 <- PR2 <- PR3
```

Each PR targets the branch below it.

### 30.2 Rules for this project

1. Active stack depth SHOULD be 2–5 PRs.
2. A PR SHOULD usually stay below roughly 300–500 meaningful changed lines, excluding generated lockfiles/migrations, but coherence matters more than a numeric limit.
3. Each PR MUST have one primary purpose.
4. Each PR MUST compile and pass its relevant tests against its own base.
5. Avoid stacking schema-heavy PRs with many speculative application PRs above them.
6. Review and stabilize the bottom PR early.
7. Rebase the stack after lower-layer changes.
8. Merge bottom-to-top or use GitHub’s stack merge when available and understood.
9. Never hide unrelated cleanup in a feature PR.
10. If a stack becomes hard to review, stop and land/refactor the lower layer first.

### 30.3 Suggested branch names

```text
stack/01-bootstrap
stack/02-config
stack/03-db-foundation

stack/ingest-01-domain
stack/ingest-02-api
stack/ingest-03-worker
```

### 30.4 Review order

For every PR:

1. Read PR description and acceptance criteria.
2. Review public API and migration first.
3. Review tests before or alongside implementation.
4. Run `just check`.
5. Ask Codex CLI to `/review` against the PR base.
6. Inspect security-sensitive subprocess/network code manually.
7. Merge only when the diff is understandable without reading future PRs.

### 30.5 Why not one long stack

A 15–25 PR stack creates:

- repeated rebases;
- review comments invalidated by lower changes;
- difficult rollback;
- CI duplication;
- pressure to approve foundations because many PRs depend on them.

Use several short stacks aligned to milestones instead.

---

## 31. Codex CLI operating model

### 31.1 Setup

Codex CLI can inspect, edit, run commands, and review changes in the local repository. Official CLI documentation:

- https://developers.openai.com/codex/cli

Official guidance recommends using `AGENTS.md` for durable repository instructions and `/review` for review workflows:

- https://developers.openai.com/codex/learn/best-practices

Codex runs with sandbox and approval controls; keep normal work constrained to the repository and approve network/destructive actions deliberately:

- https://developers.openai.com/codex/agent-approvals-security

### 31.2 Human-agent contract

Codex MUST:

- read `AGENTS.md` and relevant specs before changing code;
- state assumptions in the PR description;
- implement only the requested PR scope;
- run required checks;
- add tests;
- update docs when behavior changes;
- never push secrets;
- never merge its own PR without explicit owner instruction;
- avoid broad dependency upgrades unrelated to the task;
- stop and document a material architecture conflict rather than silently redesigning the project;
- leave the working tree clean.

The owner MUST:

- review migrations, network boundaries, subprocess code, and publication idempotency carefully;
- keep secrets outside prompts and Git;
- approve permission escalations consciously;
- merge stacks in dependency order;
- turn repeated agent mistakes into concrete `AGENTS.md` rules.

### 31.3 Suggested repository `AGENTS.md`

Create a concise file similar to:

```markdown
# AGENTS.md

## Mission
Build the system described in `docs/PROJECT_SPEC.md` through small, reviewable PRs.

## Before coding
1. Read `docs/PROJECT_SPEC.md` and relevant ADRs.
2. Inspect the current branch and its PR base.
3. Restate the exact scope and acceptance criteria in your internal plan.
4. Do not implement future PR scope.

## Architecture rules
- Keep Inbox, Library, Publisher, Jobs, Media, Telegram, and Persistence boundaries explicit.
- PostgreSQL is the source of truth.
- Durable jobs and schedules must not live only in memory.
- Never hold DB transactions across network or subprocess calls.
- External commands use argument arrays, never a shell.
- All externally retryable commands require idempotency semantics.

## Commands
- `just fmt`
- `just lint`
- `just test`
- `just test-integration`
- `just check`

## Quality gate
Before declaring work complete:
- run the relevant checks;
- add/update tests;
- update docs;
- inspect `git diff --check`;
- run a PR-style review against the base branch;
- report any skipped check and why.

## PR rules
- One primary concern per PR.
- Keep diffs small and reviewable.
- Include why, scope, out-of-scope, test evidence, risks, and stack position.
- Do not merge or force-push without explicit instruction.
```

### 31.4 Prompt template for each PR

```text
Implement PR <number/title> from docs/PROJECT_SPEC.md.

Base branch: <branch>
New branch: <branch>

Scope:
- ...

Acceptance criteria:
- ...

Explicitly out of scope:
- ...

Process:
1. Read AGENTS.md and the relevant spec sections.
2. Inspect current code and propose a short implementation plan.
3. Implement only this PR.
4. Add tests and documentation.
5. Run `just check` and any PR-specific tests.
6. Run a PR-style review against the base branch and fix high-confidence findings.
7. Commit with a clear message.
8. Push the branch and open a PR with the repository template.
9. Do not merge.
```

### 31.5 Safe Codex configuration

Start with workspace-only write permissions and approval-on-request. Do not run unrestricted/full-access mode merely for convenience. Network access should be enabled only when needed for dependency resolution or GitHub interaction, and preferably constrained.

Never place production tokens in `.codex/config.toml`, `AGENTS.md`, prompts, shell history, or test fixtures.

---

## 32. Iterative implementation roadmap

The roadmap is intentionally split into short stacks. PR numbers are logical placeholders.

### Stack A — repository foundation

#### PR A1 — Bootstrap Rust workspace

Scope:

- initialize Git repository structure;
- add workspace crates/apps as empty compilable crates;
- pin Rust toolchain;
- add formatting/lint configuration;
- add `Justfile`;
- add baseline README, license placeholder decision, changelog;
- add `AGENTS.md` and this spec;
- add basic CI for fmt/check/test;
- no database or Telegram integration.

Acceptance:

- `cargo check --workspace --all-targets` passes;
- `cargo test --workspace` passes;
- `just check` works on Windows/WSL/Linux or has documented platform notes;
- CI is green.

#### PR A2 — Configuration and application skeleton

Base: A1.

Scope:

- typed config loading;
- environment + TOML precedence;
- secret redaction;
- `sooqa-server --check-config`;
- `sooqa-worker --check-config`;
- structured tracing initialization;
- graceful shutdown skeleton;
- unit tests for config validation.

Out of scope:

- DB connection;
- HTTP routes beyond placeholder liveness;
- Telegram.

#### PR A3 — HTTP health and build metadata

Base: A2.

Scope:

- Axum server;
- request IDs;
- body/time limits;
- `/health/live`;
- build/version metadata;
- graceful shutdown test;
- Dockerfile build skeleton.

Merge Stack A before continuing.

### Stack B — PostgreSQL and jobs foundation

#### PR B1 — PostgreSQL connection and migrations

Scope:

- PostgreSQL Compose service;
- SQLx pool;
- migration command;
- migration compatibility startup check;
- integration-test harness;
- initial `admins`, `jobs`, `job_attempts`, `idempotency_records` tables.

Acceptance:

- clean DB migrates from zero;
- repeated migration is safe;
- integration tests run locally and in CI.

#### PR B2 — Durable job repository

Base: B1.

Scope:

- job domain types;
- enqueue;
- atomic claim with `SKIP LOCKED`;
- complete/fail/retry;
- lease/heartbeat;
- stale lease recovery;
- concurrency tests.

#### PR B3 — Worker loop

Base: B2.

Scope:

- worker identity;
- bounded polling;
- handler registry;
- graceful shutdown;
- test handler;
- job metrics/logging;
- no media jobs yet.

Merge Stack B.

### Stack C — Inbox vertical slice

#### PR C1 — Ingest domain and schema

Scope:

- `ingest_requests` migration;
- state model and transition tests;
- URL submission validation;
- create ingest + enqueue first job in one transaction;
- idempotency key behavior.

#### PR C2 — Authenticated ingest HTTP API

Base: C1.

Scope:

- device token schema and hashing;
- `POST /api/v1/ingest-requests`;
- `GET /api/v1/ingest-requests/{id}`;
- stable errors;
- body limits and scopes;
- API integration tests.

#### PR C3 — Source inspection job with fake adapter

Base: C2.

Scope:

- source downloader port;
- fake/test implementation;
- `inspect_source` handler;
- visible state transitions;
- no real network download.

Merge Stack C. At this point the system accepts and tracks durable submissions.

### Stack D — Real download and media probe

#### PR D1 — SSRF-safe direct HTTP downloader

Scope:

- safe URL resolver/validator;
- streaming download;
- redirects and size/time limits;
- content sniffing;
- fake HTTP server tests including private IP and redirect attacks.

This PR is security-sensitive and should be reviewed independently.

#### PR D2 — Media workspace and SHA-256

Base: D1.

Scope:

- isolated workspace;
- streaming SHA-256;
- manifest diagnostics;
- cleanup primitives;
- path traversal tests.

#### PR D3 — ffprobe adapter

Base: D2.

Scope:

- external binary runner abstraction;
- ffprobe JSON parsing;
- timeout and error classification;
- generated test media fixtures;
- worker startup diagnostics.

#### PR D4 — yt-dlp adapter

Base: D3.

Scope:

- metadata inspect and download;
- sanitized args;
- configured format selection;
- bounded output;
- fake executable tests;
- real smoke test optional and not dependent on a live third-party site in normal CI.

Merge Stack D.

### Stack E — Library and exact deduplication

#### PR E1 — Library schema and repositories

Scope:

- content items;
- media assets;
- source records;
- tags;
- storage object table skeleton;
- repository tests.

#### PR E2 — Exact duplicate flow

Base: E1.

Scope:

- source/platform identity checks;
- source SHA-256 lookup;
- canonical asset uniqueness rules;
- race-safe exact duplicate resolution;
- attach new source to existing content;
- concurrency tests.

#### PR E3 — Library read/search API

Base: E2.

Scope:

- item detail;
- text/tag search;
- cursor pagination;
- edit title/description/tags;
- archive;
- API tests.

Merge Stack E.

### Stack F — Normalization

#### PR F1 — Normalization planner

Scope:

- canonical profile config;
- probe-to-plan decision;
- remux versus transcode logic;
- deterministic command construction;
- unit tests without running ffmpeg.

#### PR F2 — ffmpeg execution

Base: F1.

Scope:

- run plan;
- parse progress;
- cancellation/timeout;
- output validation with ffprobe;
- record canonical asset and SHA-256;
- real generated-media tests.

#### PR F3 — Image normalization

Base: F2.

Scope:

- JPEG/PNG path;
- dimensions and metadata handling;
- thumbnail generation;
- tests with generated images.

Merge Stack F. The backend can now produce canonical assets.

### Stack G — Perceptual fingerprinting

#### PR G1 — Frame extraction and dHash

Scope:

- timestamp selection;
- ffmpeg frame extraction;
- deterministic dHash implementation;
- fingerprint versioning;
- unit and fixture tests.

#### PR G2 — Similarity scoring and candidates

Base: G1.

Scope:

- candidate prefilter by duration/aspect;
- frame sequence comparison;
- score/evidence;
- duplicate candidate schema/repository;
- threshold config;
- calibration fixture tests.

#### PR G3 — Duplicate resolution API

Base: G2.

Scope:

- list candidate details;
- confirm variant;
- keep separate;
- dismiss;
- audit/log events;
- Telegram UI deferred.

Merge Stack G.

### Stack H — Telegram bot and storage

#### PR H1 — Telegram adapter and admin authorization

Scope:

- Teloxide setup behind project adapter;
- configurable API base URL;
- long polling;
- update idempotency;
- `/start`, `/help`, `/status`;
- admin authorization tests using mocked API.

#### PR H2 — URL ingest via bot

Base: H1.

Scope:

- parse one URL from private admin message;
- create ingest request;
- status response;
- unauthorized behavior;
- callback data conventions.

#### PR H3 — Telegram storage provider

Base: H2.

Scope:

- storage channel upload;
- persist message/file references;
- upload idempotency intent;
- reuse existing object;
- mock API contract tests;
- startup permission diagnostic where practical.

#### PR H4 — Direct Telegram file ingest

Base: H3.

Scope:

- accept photo/video/document from admin;
- download through configured Bot API;
- preserve source message metadata;
- detect configured API limitations;
- route through existing pipeline.

Merge Stack H. This is the first broadly useful end-to-end ingest release.

### Stack I — Publisher foundation

#### PR I1 — Target channel, drafts, schedules schema

Scope:

- target channels;
- channel policies;
- post drafts;
- publication schedules/attempts/history;
- repositories and state tests.

#### PR I2 — Draft and schedule API

Base: I1.

Scope:

- create/edit draft;
- publish-now/schedule commands;
- caption validation;
- target validation;
- API tests.

#### PR I3 — Scheduler

Base: I2.

Scope:

- due schedule locking;
- minimum interval and daily limit;
- exact-time behavior;
- deterministic publish job enqueue;
- two-scheduler concurrency tests.

#### PR I4 — Telegram publication handler

Base: I3.

Scope:

- send stored asset by `file_id`;
- persist attempt and published post;
- retries and rate limit classification;
- ambiguous outcome state;
- mock API end-to-end tests.

Merge Stack I. Posting now runs independently of the Windows PC.

### Stack J — Cooldown and Telegram editorial UX

#### PR J1 — Same-content cooldown

Scope:

- policy evaluation;
- warning/block/allow;
- next eligible time;
- override evidence;
- tests across time zones and boundaries.

#### PR J2 — Similar-content cooldown

Base: J1.

Scope:

- use confirmed/high-score candidates;
- show prior posts and scores;
- tests.

#### PR J3 — Bot library and queue commands

Base: J2.

Scope:

- `/search`, `/item`, `/duplicates`, `/drafts`, `/queue`;
- inline buttons for common actions;
- pagination;
- callback authorization and expiry.

#### PR J4 — Guided publish flow

Base: J3.

Scope:

- create draft from item;
- edit caption through bot;
- choose target;
- publish now or schedule;
- cooldown confirmation.

Merge Stack J.

### Stack K — Companion and userscript

#### PR K1 — Companion CLI and secure config

Scope:

- `init`, `run`, `status`, `submit`;
- local config storage;
- token generation;
- backend client;
- no localhost server yet.

#### PR K2 — Loopback API

Base: K1.

Scope:

- bind loopback only;
- local bearer auth;
- body/rate limits;
- forward submission;
- tests proving non-loopback configuration is rejected by default.

#### PR K3 — Device token administration

Base: K2.

Scope:

- server CLI to create/list/revoke device tokens;
- token hashing;
- companion setup docs;
- audit logs.

#### PR K4 — Tampermonkey userscript

Base: K3.

Scope:

- menu command/hotkey;
- page title/URL/selection;
- local token setup;
- notifications;
- installation documentation.

Merge Stack K.

### Stack L — production readiness

#### PR L1 — Docker Compose production profile

- non-root images;
- health checks;
- persistent volumes;
- optional local Bot API profile;
- documented reverse proxy/TLS;
- resource limit examples.

#### PR L2 — Backup and restore tooling

- `pg_dump`-based documented backup;
- config backup guidance;
- restore test;
- storage reconciliation notes;
- no promise that Telegram alone is a backup.

#### PR L3 — Metrics and operational status

- Prometheus metrics;
- worker heartbeat;
- queue age;
- workspace disk metrics;
- authenticated system status.

#### PR L4 — Security and failure-mode review

- SSRF test review;
- subprocess review;
- secrets review;
- dependency policy;
- threat-model documentation;
- chaos tests for worker crash and Telegram failure.

#### PR L5 — v0.1 release

- README quickstart;
- migration/release notes;
- tagged container image;
- changelog;
- sample configuration;
- known limitations;
- demo flow.

---

## 33. Definition of done

A feature PR is done only when:

- implementation matches stated acceptance criteria;
- no unrelated scope is included;
- public/domain behavior has tests;
- integration points have contract tests or justified gaps;
- relevant commands pass;
- logs and errors are actionable and do not leak secrets;
- configuration is documented;
- migrations are reviewed and tested from a clean database;
- API/docs are updated;
- failure and retry behavior are considered;
- idempotency is defined for retryable operations;
- Codex PR-style review has been run and high-confidence findings addressed;
- the PR description explains risks and stack position;
- the owner can understand the diff without reading future PRs.

A milestone is done only when:

- all included PRs are merged;
- `main` is green;
- Docker Compose starts from a clean machine/environment;
- a documented smoke test succeeds;
- known limitations are written down;
- no temporary bypass remains undocumented.

---

## 34. Acceptance scenarios for v0.1

### Scenario 1 — Browser capture while server is remote

Given:

- backend and worker run on an always-on server;
- companion runs on Windows;
- Tampermonkey script is configured.

When:

- admin saves a supported video page.

Then:

- companion returns accepted request ID;
- Windows can be shut down;
- server finishes processing;
- bot reports completion;
- content appears in Library.

### Scenario 2 — Exact duplicate URL

When the same URL is submitted twice:

- only one canonical asset is stored;
- new ingest completes successfully as existing content;
- duplicate source metadata is handled idempotently;
- no duplicate Telegram storage message is created.

### Scenario 3 — Re-encoded duplicate

When a lower-resolution re-encode of an existing test video is submitted:

- exact SHA-256 differs;
- perceptual candidate is created above configured threshold;
- admin can confirm variant or keep separate;
- evidence is visible.

### Scenario 4 — Scheduled posting with PC off

Given a stored item and target channel:

- admin schedules a post;
- companion/PC is stopped;
- server publishes at the correct time;
- history contains Telegram message ID and caption snapshot.

### Scenario 5 — Cooldown warning

Given the same item was posted yesterday and cooldown is 14 days:

- new schedule is warned or blocked according to policy;
- response shows prior publication and next eligible time;
- explicit override is recorded.

### Scenario 6 — Worker crash

When worker dies during normalization:

- job lease expires;
- another/restarted worker reclaims it;
- pipeline does not create duplicate content/storage records;
- abandoned workspace is eventually cleaned.

### Scenario 7 — Telegram transient rate limit

When Telegram returns a retry-after response:

- job is rescheduled using the advised delay;
- attempt is recorded;
- no duplicate post is sent.

### Scenario 8 — SSRF attempt

When a URL resolves or redirects to loopback/private/metadata IP:

- request fails terminally with a safe error;
- no internal resource is fetched;
- security event is logged without leaking sensitive response data.

---

## 35. Backup, restore, and disaster recovery

### 35.1 What must be backed up

- PostgreSQL database;
- deployment configuration excluding or securely including secrets;
- encryption/token peppers if used;
- optional retained local media/cache if the operator wants stronger recovery;
- Telegram storage channel ownership/access.

### 35.2 Minimum backup policy

- daily PostgreSQL dump;
- retain multiple generations;
- encrypt backups at rest;
- copy off-host;
- periodically test restore;
- document recovery point objective chosen by operator.

### 35.3 Telegram loss scenarios

If a storage message is deleted:

- storage object is marked missing;
- publication by old `file_id` may fail;
- if local/S3 copy exists, re-upload;
- otherwise content metadata remains but binary may be unrecoverable.

Therefore the project must explicitly say: Telegram storage is convenient and potentially durable, but not a replacement for operator-controlled backups when the content is irreplaceable.

### 35.4 Reconciliation jobs

Future/admin command:

```bash
sooqa-admin storage verify --recent 1000
sooqa-admin storage reconcile
sooqa-admin publications reconcile --since ...
```

MVP should at least expose per-object verification and clear degraded status.

---

## 36. Performance and resource policy

Initial target: single admin, modest personal channel workload.

Reasonable baseline:

- 1–2 concurrent transcodes;
- 2–4 concurrent downloads;
- hundreds of thousands of catalogue records eventually, but optimize after measurement;
- scheduler tick around 15 seconds;
- search response under one second for normal catalogue size;
- bounded work directory.

Configuration should control concurrency independently:

```text
download_concurrency
transcode_concurrency
fingerprint_concurrency
telegram_upload_concurrency
publication_concurrency
```

Use semaphores in workers in addition to durable job limits.

CPU-bound image hashing may use `spawn_blocking` or a bounded Rayon pool. Do not block async runtime threads with ffmpeg waits or heavy image loops.

---

## 37. Open-source project requirements

### 37.1 License

Choose explicitly in PR A1 through an ADR.

Recommended default: Apache-2.0 for a clear patent grant and broad adoption. MIT is also reasonable. AGPL-3.0 is appropriate only if the owner intentionally wants hosted modifications to remain open.

Do not publish with an ambiguous or missing license.

### 37.2 Documentation

Public repository should include:

- concise README;
- architecture overview;
- quickstart;
- configuration reference;
- operations and backup guide;
- security policy;
- contributing guide;
- code of conduct when community participation begins;
- changelog;
- known limitations;
- responsible-use notice.

### 37.3 Contributions

- require tests for behavioral changes;
- use issue templates;
- label good first issues only after core architecture stabilizes;
- do not accept downloader modules that bypass access controls;
- document supported ffmpeg/yt-dlp/PostgreSQL ranges;
- automate dependency updates but review them in small PRs.

### 37.4 Versioning

Use semantic versioning after first public release.

Before `1.0`, migrations and configuration may evolve, but release notes must call out breaking changes.

---

## 38. Architecture Decision Records

Create ADRs for material choices. Initial ADR candidates:

```text
0001-modular-monolith.md
0002-postgresql-durable-jobs.md
0003-telegram-storage-provider.md
0004-external-media-binaries.md
0005-single-admin-security-model.md
0006-license.md
0007-long-polling-before-webhooks.md
0008-canonical-media-profile-v1.md
0009-perceptual-fingerprint-v1.md
0010-publication-idempotency-and-unknown-outcomes.md
```

ADR template:

```markdown
# ADR NNNN: Title

## Status
Proposed | Accepted | Superseded

## Context

## Decision

## Consequences

## Alternatives considered

## Follow-up
```

---

## 39. Coding conventions

- prefer explicit domain types over raw strings/UUIDs at module boundaries;
- avoid `unwrap()`/`expect()` outside tests and truly impossible startup invariants with explanations;
- errors should preserve source chains internally and map to stable public codes;
- database enums may use text + check constraints to ease migrations, unless native enums provide clear value;
- avoid giant service structs;
- no global mutable state;
- dependency injection through constructors and traits at real boundaries;
- keep traits small and consumer-owned;
- do not mock every internal function;
- use test fakes for external ports;
- timestamps supplied through a clock abstraction where deterministic time tests matter;
- random IDs/tokens supplied through injectable generators in tests;
- sanitize filenames and never trust MIME extension alone;
- comments explain why, not syntax;
- public functions and non-obvious invariants need rustdoc;
- use `#[must_use]` where ignoring a result/value is dangerous;
- enforce clippy but allow justified, local exceptions.

---

## 40. Failure taxonomy

Define stable internal/public error classes:

```text
validation
unauthorized
forbidden
not_found
conflict
rate_limited
timeout
network_transient
source_unsupported
source_unavailable
source_too_large
media_invalid
media_unsupported
media_processing_failed
storage_unavailable
storage_permission_denied
publication_policy_blocked
publication_outcome_unknown
database_transient
invariant_violation
```

Every adapter maps raw errors to this taxonomy. Retry policy depends on class, not string matching.

---

## 41. Initial administrative CLI

A small admin binary may be added when needed, or server subcommands may be used.

Suggested commands:

```text
sooqa-server migrate
sooqa-server check-config
sooqa-server admin add-telegram-user <id>
sooqa-server token create --name windows-pc --scope ingest:create
sooqa-server token list
sooqa-server token revoke <id>
sooqa-server channel add --chat-id ... --name ...
sooqa-server storage verify <asset-id>
sooqa-server jobs retry <job-id>
sooqa-server doctor
```

`doctor` should check:

- config;
- DB;
- migrations;
- Telegram bot identity;
- storage channel access;
- target channel access;
- external tool versions;
- work directory;
- queue health.

---

## 42. Release readiness checklist

Before v0.1 tag:

- [ ] clean install tested from README;
- [ ] Docker images run as non-root;
- [ ] database backup and restore tested;
- [ ] no known secret in repository history;
- [ ] SSRF tests pass;
- [ ] worker crash recovery tested;
- [ ] duplicate publication prevention tested;
- [ ] Telegram permission failure produces actionable message;
- [ ] local Bot API mode documented;
- [ ] cloud Bot API limitations documented;
- [ ] Windows companion installation tested;
- [ ] Tampermonkey setup tested;
- [ ] exact duplicate and re-encode duplicate fixtures pass;
- [ ] scheduled post works with PC off;
- [ ] cooldown warning works;
- [ ] license and third-party notices present;
- [ ] dependency/license scan clean or exceptions documented;
- [ ] changelog and known limitations current.

---

## 43. Known hard problems and deliberate compromises

### Telegram is not a contractual object store

Use it as the MVP media provider, but retain DB backups and expose provider health. Keep `StorageProvider` boundary real and narrow.

### Publication idempotency is imperfect

A network failure after Telegram accepted a message may be ambiguous. Model `unknown`; do not pretend exactly-once delivery is guaranteed.

### Perceptual deduplication requires calibration

Start with deterministic frame hashes and transparent scoring. Collect false-positive/false-negative examples before adding ML.

### Downloaders are unstable

`yt-dlp` and sites change. Keep adapter errors clear, make the binary replaceable, and do not base normal CI on live third-party sites.

### Video processing is resource-heavy

Bound concurrency and disk. A single server process must never spawn unlimited ffmpeg jobs.

### Stacked PRs can become review debt

Keep stacks shallow and merge foundations before building too far above them.

---

## 44. First command to give the coding agent

After creating an empty repository and placing this file at `docs/PROJECT_SPEC.md`, use a prompt like:

```text
You are implementing the open-source project described in docs/PROJECT_SPEC.md.

Start with PR A1 only: Bootstrap Rust workspace.

Requirements:
- Read docs/PROJECT_SPEC.md completely.
- Create a concise repository-level AGENTS.md based on section 31.3.
- Initialize the Rust 2024 workspace and the directory structure from section 9.
- Create compilable placeholder crates/apps with minimal code, not speculative abstractions.
- Pin the current stable Rust toolchain in rust-toolchain.toml.
- Add rustfmt/clippy/editor config, Justfile, README, CHANGELOG, .gitignore, .env.example, and GitHub CI.
- Add an ADR deciding the license, defaulting to Apache-2.0 unless a concrete incompatibility appears.
- CI must run format check, cargo check, Clippy with warnings denied, and tests.
- Keep the diff focused on repository bootstrap. Do not add PostgreSQL, Telegram, Axum, SQLx, media processing, or business entities yet.
- Run all checks locally.
- Run a PR-style Codex review against main and fix high-confidence findings.
- Commit, push branch stack/01-bootstrap, and open a pull request using a clear description with why/scope/out-of-scope/testing/risks.
- Do not merge the pull request.
```

After A1 is approved, issue the PR A2 prompt using the template in section 31.4.

---

## 45. Owner review guide

For every PR, ask:

1. Can I explain the purpose in one sentence?
2. Does the diff implement only that purpose?
3. Is there a smaller API or data model that would still satisfy the requirement?
4. What happens on retry?
5. What happens on process crash?
6. What happens if Telegram succeeds but DB write fails?
7. Does any user-controlled value reach a path, URL, SQL query, shell, or caption unsafely?
8. Are secrets redacted?
9. Are tests exercising behavior rather than implementation trivia?
10. Can I roll this PR back without understanding future PRs?
11. Does the next stacked PR pressure me to approve this one prematurely?
12. Is the operational behavior documented?

Pay extra attention to:

- migrations;
- job locking;
- SSRF;
- subprocess arguments;
- Telegram message duplication;
- token storage;
- cleanup paths;
- time zone and cooldown calculations.

---

## 46. Official references checked for this specification

These links are intentionally primary/official sources. Re-check them when implementing because APIs and tooling evolve.

### Telegram

- Bot API: https://core.telegram.org/bots/api
- Local Bot API Server section: https://core.telegram.org/bots/api#using-a-local-bot-api-server
- Bot API changelog: https://core.telegram.org/bots/api-changelog

### GitHub stacked pull requests

- Reference: https://docs.github.com/en/pull-requests/reference/stacked-pull-requests
- Creating stacks: https://docs.github.com/en/pull-requests/how-tos/create-pull-requests/creating-stacked-pull-requests
- Managing stacks: https://docs.github.com/en/pull-requests/how-tos/create-pull-requests/managing-stacked-pull-requests

### Codex CLI

- CLI overview: https://developers.openai.com/codex/cli
- Best practices and AGENTS.md: https://developers.openai.com/codex/learn/best-practices
- Approvals and sandbox security: https://developers.openai.com/codex/agent-approvals-security
- Configuration: https://developers.openai.com/codex/config-basic
- Open-source repository: https://github.com/openai/codex

### Rust stack

- Tokio tutorial: https://tokio.rs/tokio/tutorial
- Axum documentation: https://docs.rs/axum/latest/axum/
- SQLx documentation: https://docs.rs/sqlx/latest/sqlx/
- Teloxide repository: https://github.com/teloxide/teloxide

---

## 47. Final architectural decision summary

```text
Product model:       self-hosted, open-source, single-admin first
Architecture:        modular monolith
Runtime topology:    always-on server + worker + optional Windows companion
Primary database:    PostgreSQL
Technical queue:     PostgreSQL leases with SKIP LOCKED
Media processing:    ffprobe/ffmpeg subprocesses
Downloader:          direct HTTP + yt-dlp adapter
Exact dedup:         source identity + SHA-256
Perceptual dedup:    sampled-frame dHash sequence v1
Media storage MVP:   private Telegram channel
Bot API:             configurable cloud or Local Bot API Server
Publishing:          durable scheduler and worker
UI MVP:              Telegram bot + HTTP API + Tampermonkey companion
Time storage:        UTC; IANA zone in channel policy
Development:         Rust 2024, CI-gated small PRs
Review workflow:     shallow stacked PRs, merged bottom-up
Agent workflow:      Codex CLI + AGENTS.md + per-PR acceptance criteria
```

The most important sequencing rule is: **first make a reliable durable pipeline, then improve intelligence and UX**. Exact hashes, transparent frame fingerprints, robust jobs, and publication history provide more product value than early ML or a large web interface.
