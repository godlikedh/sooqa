# Operations

The development Compose file provides PostgreSQL. Production deployment,
database backup, Telegram configuration, and media tool requirements will be
documented as those runtime components are added.

## Local container runtime

Docker Desktop is optional on macOS. Colima supplies the Docker-compatible
engine without a Docker account:

    brew install colima
    colima start --runtime docker
    docker context use colima

Verify the selected engine with `docker context show`, then use the normal
Compose commands. Stop the VM with `colima stop` when it is not needed.

The B1 migration command is:

    DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/sooqa cargo run -p sooqa-server -- migrate

The migration set now includes the initial Library tables for content items,
media assets, source records, tags, and storage objects. Apply migrations with
the same command after upgrading the application; migrations are forward-only.

After migrations are applied, the worker can be started with:

    DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/sooqa cargo run -p sooqa-worker

The worker's non-secret media executable paths can be configured in TOML:

    [media]
    ffmpeg_path = "ffmpeg"
    ffprobe_path = "ffprobe"
    ytdlp_path = "yt-dlp"
    ytdlp_format = "bestvideo*+bestaudio/best"

The equivalent environment overrides are `SOOQA_MEDIA_FFMPEG_PATH`,
`SOOQA_MEDIA_FFPROBE_PATH`, `SOOQA_MEDIA_YTDLP_PATH`, and
`SOOQA_MEDIA_YTDLP_FORMAT`. On normal startup,
the worker logs the detected version of each required binary and exits before
database connection if a binary is unavailable. The server does not require
these media tools. The current worker still has no production media or
Telegram handlers; it polls durably stored jobs, executes registered handlers,
records outcomes, and stops gracefully on SIGTERM or Ctrl-C.
