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
  video-frame extraction command. It defaults to one hour and is capped at 24
  hours so large canonical media can finish without an unbounded subprocess;
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
