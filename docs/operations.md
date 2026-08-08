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
availability. Inspect the row and its lease token when diagnosing a crash.

## Telegram and media

Telegram polling is enabled only when its bot token and configured admin IDs
are present. `SOOQA_TELEGRAM_MAX_DOWNLOAD_BYTES` bounds staged media. The
server and worker must share `media.work_root`; ffprobe and ffmpeg are needed
for probing and normalization.

## Storage ambiguity

Storage upload state is carried by the media row. `storage_unknown` means the
external Telegram result is unresolved; it must be reconciled before a new
generation is started. The existing CLI names remain available for this
operator workflow:

```bash
cargo run -p sooqa-server -- storage-intents list
cargo run -p sooqa-server -- storage-intents mark-unknown <media-id>
cargo run -p sooqa-server -- storage-intents reset <media-id> --confirm
cargo run -p sooqa-server -- storage-intents attach <media-id> <chat-id> <message-id> <file-id> <file-unique-id>
```

## Secrets and logs

Keep database URLs, API tokens, and Telegram tokens in the environment. Config
summaries redact secrets. Do not log request bodies, bearer values, or media
contents.
