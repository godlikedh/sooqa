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

## Admin API

Use the configured bearer token to inspect the bounded operational slices:
`/api/v1/dashboard`, `/api/v1/ingests`, `/api/v1/media?q=...`, and
`/api/v1/posts`. Lists default to at most 50 rows and use opaque cursors that
combine the ordering timestamp with a stable UUID; retain the cursor returned
by one response for the next request. The schedule list omits published and
cancelled posts unless `include_history=true`. Media `q` is an exact UUID,
credential-free source URL, supported 2ch mirror URL, or private Telegram
storage link; it is not full-text search.

Channel settings are editable through the revision-fenced
`PATCH /api/v1/channels/{id}` endpoint. Reload after a `channel_changed` or
`channel_configuration_ambiguous` conflict. Do not place API or Telegram
secrets in channel rows or query strings. Caption-sync failure cards appear
when the later #82 worker writes the bounded media metadata marker.

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

### Allowlisted YouTube pages

The worker is direct-only when `SOOQA_MEDIA_YTDLP_ALLOWED_HOSTS` is empty. To
enable public YouTube video and Shorts pages, set it to a comma-separated list
such as:

```bash
SOOQA_MEDIA_YTDLP_ALLOWED_HOSTS=youtube.com,youtu.be
```

Entries are normalized as DNS hostnames. `youtube.com` also allows its
dot-delimited subdomains; `youtu.be` is an exact host entry. Credentials, IP
literals, wildcards, paths, and non-default ports are rejected. Direct MP4 and
WebM responses continue to use the direct HTTP adapter even when their host is
not in this list. The worker logs whether yt-dlp is disabled or enabled and
fails startup if an enabled yt-dlp or Deno capability is missing or too old.

The home Compose deployment uses `youtube.com,youtu.be` when the variable is
unset; set it to an empty value in `deploy/home/.env` for direct-only operation.
The initial URL host is the allowlist decision point. yt-dlp is run without
configuration files, browser cookies, netrc, plugins, or remote components, but
an accepted provider page can still follow provider redirects and fetch its
CDN media URLs. The supported home path is public regular videos and Shorts;
private, members-only, age-restricted, geo-bypassed, and cookie-authenticated
media are intentionally outside this setup.

The home image downloads the official standalone `yt-dlp` distribution, which
contains the bundled `yt-dlp-ejs` component, and a pinned Deno runtime. The
current Dockerfile pins yt-dlp `2026.06.09` and Deno `2.8.1` with architecture-
specific SHA-256 checksums. When the allowlist is enabled, worker startup runs
an offline local-info fixture through yt-dlp with the configured EJS/Deno
flags, checks that the bundled EJS component is discoverable, and executes a
small Deno probe. Each yt-dlp download runs inside a unique attempt directory:
its relative final output, temporary fragments, split streams, merge
intermediates, and disabled-cache state are confined there. The worker monitors
the aggregate attempt directory with a three-times-final-size budget to allow
video/audio merging, while the final published file remains bounded by
`SOOQA_MEDIA_SOURCE_DOWNLOAD_MAX_BYTES`; a successful attempt must contain
exactly one regular media file. To update either dependency, change its version, asset names
if needed, and every matching checksum together; build the home image and
verify the startup diagnostics before doing an owner smoke test. CI uses fake
executables and does not contact YouTube.

The server and worker must share `media.work_root`; ffprobe and ffmpeg are
needed for probing and normalization. Source downloads and Telegram uploads
are streamed or path-based; no file-sized byte buffer is used.
Polling, downloads, and storage uploads use separate HTTP timeout policies so a
long upload cannot inherit the long-poll or download-stall deadline.

Publisher scheduling is controlled only by PostgreSQL `posts.scheduled_at`,
`posts.cadence_slot_at`, and `queue.jobs.run_at`. Normal queueing assigns the
next channel cadence slot; exact/manual scheduling writes only the selected
post's future instant, leaves `cadence_slot_at` null, permits same-instant
collisions, and does not move unrelated cadence posts. API publication
mutations use `posts.revision` as an optimistic fence and keep one
fixed-dedupe `publish_post` job per queued post. If a job is already claimed,
its post cannot be edited or cancelled. A stale HTTP caller receives a
conflict and must reload the post; Telegram is never contacted while these
mutations are committed.

When an ingest reaches completed storage, Inbox commits its state transition,
cleanup enqueue, and one `materialize_publication` job together. The worker
materializer performs only database work: it replays the captured action and
target channel through Publisher, stores `origin_ingest_id`, evaluates repeat
conflicts at the intended send instant, and creates the fixed-dedupe publish
job only when no decision is pending. A save-only ingest has no materializer
job or post. Inspect `queue.jobs` by the ingest payload and then inspect the
linked post's `repeat_evidence` when a publication is waiting in `draft`.
Decision commands are one-time, revision-fenced, and idempotent by their
per-post request key; cancellation leaves the media available.

Publication claims the post before calling Telegram and records a fresh
generation/token. It copies the ready storage message into the target channel;
only a known copy-unavailable response may use the stored file ID fallback.
Caption/entity rejection leaves an editable `failed` post. Explicit no-effect
errors such as flood control requeue the post and follow bounded job retries.
Network, invalid-response, and unknown API outcomes leave the post `unknown`
and are not automatically resent. Inspect `posts.error_class` and
`posts.error_message` before manual reconciliation; a `sending` post with a
lost worker lease must be investigated before any new send is authorized.

The superseded Telegram queue presentation, earlier/later controls, and
occupied-slot swaps are removed. Publication inspection and editing belong to
the bounded web-admin slices and remain revision-fenced API operations.

In the administrator's private bot chat, `/duplicates` lists up to three
pending duplicate ingests at a time. Ready candidates link to their Telegram
storage message; `Use this` reuses the existing media item, while `Save anyway`
starts the normal force-save pipeline. A pending-storage candidate remains on
the existing media upload lifecycle. The HTTP equivalent is documented in
`docs/openapi.yaml`.

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

The Windows workflow publishes these two files together only from a pushed
semantic release tag such as `v0.2.0`; the tag is the companion protocol
version boundary and the download is available from that release's Assets
section. Do not use an unversioned or legacy prerelease named `release`, and do
not replace an existing asset in place. A companion release that forwards
queue or post-now intent requires a backend with the #76 ingest contract;
save-only requests remain compatible because omitted intent fields are omitted
from the forwarded JSON.

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
description, tags, and a browser action ID. The backend ingest contract also
accepts an optional `requested_action` (`save`, `queue`, or `post_now`), a
future RFC3339 `requested_publish_at` for exact queueing, and separate public
`requested_post_caption`; deployed save-only companions may omit these fields.
Optional description and public caption values are trimmed and omitted when
blank; nonblank multiline text permits newline, carriage return, and tab, while
public captions remain limited to the Publisher's 1,024-character contract.
A successful response means only that the backend accepted the ingest request;
the userscript does not poll or claim that media is already stored. A failed
request can be retried with the same action ID, preserving backend idempotency.

Install `userscripts/sooqa-2ch-save.user.js` in Tampermonkey. Its stable
`@updateURL` and `@downloadURL` point to the script path on the repository's
`main` branch, while the script's incremented `@version` lets Tampermonkey
detect later releases.
It is matched only to `https://2ch.su/*`, `https://2ch.org/*`, and
`https://2ch.life/*`, discovers direct `.mp4`/`.webm` links and media nodes
inside real `.post` attachment areas, and observes dynamically added posts.
Arbitrary page-level media and 2ch's generated fullscreen viewer are ignored.
The first run asks for the local token and
stores it in Tampermonkey's private storage. It never receives or stores the
backend token. Each media preview gets `Post now`, `Post now…`, `Queue`,
`Queue…`, `Save`, and `Save…` actions. Detailed actions collect only their
documented metadata: internal tags/description, optional public post text, and
the required browser-local exact time for `Queue…`; exact time is converted to
an RFC3339 instant before submission. Their modal layout is userscript-owned so
board CSS cannot make the form unusable; if native modal opening or focus fails,
the existing prompt fallback is used. Cancel, Escape, and submission are
one-shot exits that restore the action buttons.
Native `figure.post__image` filename/preview pairs are treated as one
attachment, so galleries retain their existing preview structure while rows
are added around each item.

Accepted-action history is informational and local to the canonical thread and
media identity across the three mirrors. It survives reloads, is bounded and
clearable, never disables a button, and does not poll for ingest or publication
state. A timeout/no-response retry keeps the same action ID for backend
idempotency; a new deliberate action gets a new ID.

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

Canonical videos are uploaded to the private storage channel with Telegram's
`supports_streaming=true` `sendVideo` flag. The canonical video profile already
produces MP4/H.264 video with optional AAC audio and fast-start metadata, so
newly stored videos can begin playback before the full file is downloaded by a
client. Images, animations, and audio use their existing upload methods and do
not receive this video-only flag. This does not change existing storage
messages; it applies to new uploads after deployment.

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
