# Operations

## Fresh database

The five-table baseline is intentionally incompatible with the pre-reset local
database. The owner must recreate a development database explicitly; sooqa
does not drop volumes or schemas automatically. Then apply the clean migration:

```bash
DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/sooqa \
  cargo run -p sooqa-server -- migrate
```

The same `DATABASE_URL` is used by server and worker. PostgreSQL is the durable
source of truth for ingests, media, channels, posts, and jobs.

## Server and worker

Configure the API secret and database URL in the environment:

```bash
SOOQA_API_TOKEN='replace-with-a-long-random-secret'
DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/sooqa cargo run -p sooqa-server
DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/sooqa cargo run -p sooqa-worker
```

The worker claims `queue.jobs`, renews leases, and recovers expired claims at
startup and periodically. A job's `run_at` controls retry and publication
availability. Every heartbeat or terminal mutation must use an unexpired lease.
If the final lease expires, recovery marks the owning ingest terminal instead
of leaving it in an intermediate state. Inspect the row and its lease token
when diagnosing a crash.

## Telegram and media

Telegram polling is enabled only when its bot token and configured admin IDs
are present. The media budgets and processing deadline are intentionally
separate:

- `SOOQA_MEDIA_PROCESSING_TIMEOUT_SECONDS` bounds one ffmpeg normalization or
  complete video-fingerprint extraction command. Fingerprinting starts one
  ffmpeg child per video, samples the canonical input sequentially into a
  fresh extraction-scoped directory, and then decodes one bounded frame at a
  time. It defaults to one hour and is capped at 24 hours so large canonical
  media can finish without an unbounded subprocess;
- `SOOQA_MEDIA_SOURCE_DOWNLOAD_MAX_BYTES` bounds URL/link source staging and
  may be larger than 2 GB because normalization can reduce the source;
- `SOOQA_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES` bounds Telegram-source staging;
- `SOOQA_TELEGRAM_UPLOAD_TIMEOUT_SECONDS` bounds one Telegram storage upload
  independently from polling and download timeouts. It defaults to one hour
  and is capped at 24 hours;
- `SOOQA_MEDIA_NORMALIZED_STORAGE_MAX_BYTES` bounds the canonical normalized
  object uploaded to Telegram storage. It must remain below the documented
  2000 MB local Bot API upload limit.

The server and worker must share `media.work_root`; ffprobe and ffmpeg are
needed for probing and normalization. Source downloads and Telegram uploads
are streamed or path-based; no file-sized byte buffer is used.
Polling, downloads, and storage uploads use separate HTTP timeout policies so a
long upload cannot inherit the long-poll or download-stall deadline.

Video fingerprint extraction uses the `video_sequence_v1` grid without a
per-sample subprocess or permanent frame cache. One FFmpeg process first
normalizes timestamps to zero, pads the final decoded frame for one sample
interval, and uses a `select` expression to choose the first decoded frame at
or after each grid timestamp. The padding preserves the final grid point when
container or audio duration extends just beyond the video stream; variable-
frame-rate PNG output preserves that selection instead of applying a rounding
policy. Output is capped at the calculated sample count (at most 2,048). A
bounded consumer
decodes stable numbered PNGs as they arrive and deletes each one, while the
producer monitors the extraction directory. The configured aggregate
temporary sequence limit is 4 GiB (`DEFAULT_MAX_FRAME_SEQUENCE_BYTES`), in
addition to the 16 MiB per-frame decode limit. The worker retains only compact
features and the previous normalized luma plane, and removes the temporary
sequence on success, failure, timeout, or cancellation.

## Workspace lifecycle

Every ingest generation owns one workspace at
`<SOOQA_MEDIA_WORK_ROOT>/jobs/<workspace-id>`. The workspace ID is persisted on
the ingest row; a force-save receives a new ID before its replacement pipeline
is queued. Cleanup jobs carry both the ingest ID and that generation-scoped ID,
so a delayed cleanup can remove only an orphaned generation and cannot remove
the force-save replacement.

Cleanup is durable and replay-safe:

- ready storage completes linked ingests, clears `media.local_work_path`, and
  queues immediate cleanup;
- duplicate, terminal-failure, and other deferred paths queue cleanup after a
  one-day retention period;
- pending storage, ambiguous storage, active leases, retryable stages, and
  queued/running work protect their workspace;
- every workspace ID still referenced by an ingest is protected from periodic
  reconciliation, including completed ingests whose explicit cleanup job has
  already succeeded; only old generation directories are scavenger orphans;
- cleanup is confined to the configured `jobs` directory and UUID-named roots.

Cleanup jobs are also a database-backed deletion fence. Before a valid cleanup
attempt returns `Ready`, it clears the current media row's local work path in
the same transaction. A storage reset therefore fails with an explicit
“workspace reclaimed; reconstruction is required” result even after cleanup
succeeds or its lease is recovered for retry. If reset wins first, the cleanup
job observes the durable storage job and defers; a stale recovered attempt is
also rejected before it can touch the filesystem. A ready or attached media
item whose local path has already been reclaimed follows the same explicit
reconstruction path; it never queues an upload job that cannot find bytes.

The worker reconciles a bounded batch at startup and every five minutes from
the protected workspace IDs derived from PostgreSQL. It can therefore repair a
crash between a state commit and filesystem deletion without treating queue
job IDs as workspace ownership. The batch limit is 128 workspaces; a failed
filesystem operation remains retryable or is picked up by later reconciliation.

## Telegram file acceptance

For a supported private Telegram file message, the polling server performs
authorization, metadata/size validation, and the PostgreSQL ingest transaction
only. It persists the Telegram `file_id`, `file_unique_id`, message identity,
caption, media kind, MIME type, name, and advertised size, then acknowledges
the update. It does not create a workspace or download media bytes.

The worker creates the generation workspace while probing and reconstructs the
source from the durable `file_id`. The download is streamed into the private
workspace with `SOOQA_TELEGRAM_SOURCE_DOWNLOAD_MAX_BYTES`, then probed and
processed under the normal durable lease/retry flow. A replayed update uses the
same Telegram update idempotency key, so it returns the existing ingest without
another acceptance job or eager download. This keeps the polling loop
responsive while a large worker download is running.

## Local companion and 2ch capture

For Windows, download `sooqa-companion-windows-x86_64.exe` and its
`.sha256` checksum from a GitHub Release. The executable is self-contained;
Windows users do not need Rust or a checkout of this repository. Verify the
checksum in PowerShell before starting it:

```powershell
$exe = ".\sooqa-companion-windows-x86_64.exe"
$expected = (Get-Content "$exe.sha256").Split()[0].ToLowerInvariant()
$actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $exe).Hash.ToLowerInvariant()
if ($actual -ne $expected) { throw "companion checksum mismatch" }
```

Start the companion on the Windows workstation with a loopback listener and
two different secrets:

```powershell
$env:SOOQA_COMPANION_BACKEND_URL = "https://sooqa.example.test"
$env:SOOQA_COMPANION_LOCAL_TOKEN = [guid]::NewGuid().ToString("N")
$env:SOOQA_COMPANION_BACKEND_TOKEN = "replace-with-the-sooqa-api-token"
.\sooqa-companion-windows-x86_64.exe
```

For local development, the equivalent source build is:

```bash
SOOQA_COMPANION_BACKEND_URL=https://sooqa.example.test \
SOOQA_COMPANION_LOCAL_TOKEN='random-local-token' \
SOOQA_COMPANION_BACKEND_TOKEN="$SOOQA_API_TOKEN" \
cargo run -p sooqa-companion
```

The companion exposes only `POST http://127.0.0.1:47831/v1/submit`. Its body is
bounded and contains a direct MP4/WebM URL, page context, an optional internal
description, tags, and a browser action ID. A successful response means only
that the backend accepted the ingest request; the userscript does not poll or
claim that media is already stored. A failed request can be retried with the
same action ID, preserving backend idempotency.

Install `userscripts/sooqa-2ch-save.user.js` in Tampermonkey. It is matched only
to `https://2ch.su/*`, `https://2ch.org/*`, and `https://2ch.life/*`,
discovers direct `.mp4`/`.webm` links and media nodes, and observes
dynamically added posts. The first run asks for the local token and stores it in
Tampermonkey's private storage. It never receives or stores the backend token.
`Save...` opens one metadata dialog for comma-separated tags and an internal
description; those values become media metadata, not a public Telegram post.

For those three exact hosts, the worker's 2ch media adapter inspects official
mirrors in the fixed order `2ch.org`, `2ch.su`, `2ch.life`, preserving the
submitted path and query. It falls through only for DNS/connection/TLS
failures or non-success HTTP responses. Unsupported media and source-size
policy failures remain terminal. The successful mirror URL is retained in the
inspection and library source metadata, while the submitted URL and page
context remain the provenance; the later download reuses that selected URL.
This policy is not applied to other direct HTTP sources or to arbitrary
subdomains.

## Home local Bot API deployment

The development Compose file at the repository root contains only PostgreSQL.
The self-hosted home topology is separate and lives under `deploy/home`; its
named volumes are therefore separate from development and CI volumes.

Prepare the deployment without committing secrets:

```bash
cp deploy/home/.env.example deploy/home/.env
# edit deploy/home/.env
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml build
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml run --rm server migrate
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml up -d
```

The official `tdlib/telegram-bot-api` source is built at the commit pinned in
`deploy/telegram-bot-api/Dockerfile`. The local server runs with `--local`,
stores its working state in the dedicated `home-telegram-bot-api-data` volume,
and exposes port 8081 only to the Compose network. sooqa reaches it as
`http://telegram-bot-api:8081`; the server and worker share only the separate
`home-sooqa-work` volume for sooqa media workspaces.

Do not run this cutover automatically. To move a running bot from the cloud to
the local server, obtain owner authorization, stop every sooqa poller, call
`logOut` against the cloud endpoint, start the local Bot API service, wait for
its health check, verify `getMe`, the configured administrator, and storage
channel permissions, and only then start the sooqa server/worker. For rollback,
stop every poller, call `logOut` against the local endpoint, switch the base URL
back to `https://api.telegram.org`, verify `getMe` and permissions, and start
the poller again. Never use `down --volumes` as part of either operation.

The official local server accepts HTTP and needs TLS termination if it is ever
placed behind a remote endpoint; the Compose service here is intentionally
private. See the [official local Bot API documentation](https://core.telegram.org/bots/api#using-a-local-bot-api-server)
and [upstream server README](https://github.com/tdlib/telegram-bot-api#usage)
for Telegram-side requirements.

## Storage ambiguity

Storage upload state is carried by the media row. `storage_unknown` means the
external Telegram result is unresolved; it must be reconciled before a new
generation is started. Marking an upload unknown explicitly fails linked active
ingests. Attaching the Telegram message completes linked storage-waiting or
storage-failed ingests; resetting opens those storage-related failures in a new
generation and queues a fresh upload. The existing CLI names remain available
for this operator workflow:

```bash
cargo run -p sooqa-server -- storage list
cargo run -p sooqa-server -- storage mark-unknown <media-id>
cargo run -p sooqa-server -- storage reset <media-id> --confirm
cargo run -p sooqa-server -- storage attach <media-id> <generation> <chat-id> <message-id> <file-id> <file-unique-id>
```

## Secrets and logs

Keep database URLs, API tokens, and Telegram tokens in the environment. Config
summaries redact secrets. Do not log request bodies, bearer values, or media
contents.
