# Operations

## Database initialization and preservation

For a brand-new installation, create an empty database and apply all migrations:

```bash
DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/sooqa \
  cargo run -p sooqa-server -- migrate
```

The same `DATABASE_URL` is used by server and worker. PostgreSQL is the durable
source of truth for ingests, media, channels, posts, and jobs.

The five-table baseline is intentionally incompatible only with the discarded
pre-reset schema. Once a database has the current five-table baseline, preserve
it: take a backup before upgrades and apply forward migrations in place. sooqa
does not drop schemas or volumes automatically. The explicit legacy-reset
procedure later in this document is destructive and owner-run.

The supported populated upgrade boundary is migration
`0006_remove_unused_ingest_status`. The PostgreSQL integration suite seeds that
five-table schema with active and terminal ingests, all media storage states,
channels, posts, and queue lifecycle states, then runs the real repository
migrator through the current HEAD (including `0007` and `0008`) and exercises
current repository reads and transitions. Future forward migrations are
included by the same HEAD migrator automatically. The test uses only a
uniquely named temporary database; it does not reset or inspect a home or
production database.

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

The HTTP server owns the server process lifetime. Telegram polling runs as a
supervised task with bounded exponential backoff, so a Telegram outage or a
temporary failure during webhook cleanup does not remove `/health/live` or the
admin API. Polling resumes automatically after the upstream recovers. A
Telegram token, storage-channel permission, or other static remote permission
failure is reported as `status=terminally_misconfigured` and requires operator
action; it does not make the HTTP server exit. On SIGINT/SIGTERM the server
signals the polling task, then drains HTTP requests before stopping.

The worker performs its Telegram storage-chat preflight in the background and
starts claiming non-storage jobs immediately. A temporary preflight outage is
reported as `status=degraded`; upload and caption-sync jobs retain their normal
durable retry/terminal policy. Watch structured logs with
`target=sooqa.telegram` and the `status` values `degraded`, `retrying`,
`recovered`, `ready`, and `terminally_misconfigured` when diagnosing Telegram
availability. Successful liveness only means the HTTP process is serving; it
is not a Telegram-readiness signal.

Storage upload shutdown is phase-aware. A stop before the Telegram request can
escape releases the media reservation and leaves the upload job retryable. A
stop after the request may have escaped, including a lost upload heartbeat,
clears the reservation token, records `storage_unknown`, fails linked active
ingests, and settles the upload job as failed. Reconcile that media row with
the storage commands below before starting a new generation; do not manually
retry the queued job or delete its media row. Lease recovery gives a recent
active reservation a short cancellation-drain grace and restores a settled
pre-dispatch claim to the retryable queue without consuming an attempt.

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
secrets in channel rows or query strings. Media responses include optional
bounded preview metadata; fetch bytes through the authenticated
`GET /api/v1/media/{id}/preview` route. A missing preview is expected for
audio, older rows, and animations whose decoder could not safely produce one.
The route returns a private-cache ETag and never exposes a workspace path.
Caption-sync failures appear from the media row's bounded sync marker and can
be requeued with the authenticated
`POST /api/v1/media/{id}/caption-sync/retry` command.

Open `/admin` in the owner's Chrome session for the embedded web client. The
tracked home Compose configuration binds it to Mac loopback by default; a
Windows machine must first use the trusted-LAN procedure below. Enter the
configured API token when prompted; it is kept only in that browser session
and can be removed with `Forget token`. The Dashboard loads
only the bounded dashboard response and invokes the existing duplicate and
publication-decision commands. Ingests loads 50 rows at a time and its refresh
button returns to the first page; it has no retry or job controls. Settings
loads disabled channels too, edits one selected target with its `updated_at`
fence, and reloads after a stale or ambiguous configuration conflict.

Media browsing is also bounded to 50 rows. Use the exact lookup field for one
media UUID, ingest UUID, supported mirror-equivalent 2ch URL, or private
Telegram storage link; it is not a semantic search box. Catalogue edits send
the complete tag set and internal description with the media `updated_at` fence.
The response changes the caption marker to `Syncing`; `Retry sync` calls the
durable retry endpoint after a failure. Preview bytes are fetched through the
authenticated API and held only in revocable browser object URLs. Ready cards'
Post now/Queue actions create publication intents; ellipsis variants collect
only public text and an optional future local browser time. Leaving the
`Queue…` time blank uses normal cadence; a populated time queues exactly. A
repeat draft is resolved from Dashboard rather than in the Media page.

Schedule loads `GET /api/v1/posts?limit=50` in scheduled-time/UUID order and
does not request publication history. Rows with optional preview metadata fetch
the same authenticated bounded still bytes as Media; audio, older, and failed
previews retain the kind placeholder, and Schedule never becomes a playback
surface. Cadence rows are marked separately from explicit-time rows. Draft,
queued, and failed cards may patch only the public caption, cancel the post,
request immediate publication, or call
`/schedule-exact` with a future browser-local instant. Exact scheduling writes
only the selected post, allows collisions, bypasses cadence/window rules, and
leaves other posts untouched. Sending and unknown rows remain visible but
read-only. Each mutation includes the current post revision; a conflict clears
the stale item and reloads it while preserving other dirty cards, and a
first-page refresh leaves each actively edited form in place. Leaving Schedule
discards unsaved forms.

Compact UUID labels in Dashboard attention cards, Ingests, Media, and Schedule
are copy controls rather than navigation links. Click one, or focus it and
press Enter or Space, to copy the exact backend UUID while keeping the short
label. The control uses `navigator.clipboard` when available and otherwise
tries a temporary selected field with `document.execCommand("copy")`, which
also supports the trusted-LAN HTTP setup. If copying still fails, the full
UUID is revealed and selected for manual copying, with a visible error
message. Source, Telegram, and in-app navigation links remain separate.

The server emits the admin HTML, CSS, and JavaScript from the binary, so no
Node process, frontend service, CDN, or asset volume is required. Keep `/admin`
behind the same trusted-LAN boundary as the bearer API; the page's CSP and
session-only token handling are not an internet-exposure claim.

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
- `SOOQA_TELEGRAM_LOCAL_FILE_ROOT` optionally enables worker-side copying of
  absolute paths returned by a local Bot API server. The root must be absolute;
  every configured local path is canonicalized and confined beneath it. The
  home Compose deployment sets this to `/var/lib/telegram-bot-api` only for the
  worker and mounts that volume read-only;
- `SOOQA_TELEGRAM_UPLOAD_TIMEOUT_SECONDS` bounds one Telegram storage upload
  independently from polling and download timeouts. It defaults to one hour
  and is capped at 24 hours;
- `SOOQA_MEDIA_NORMALIZED_STORAGE_MAX_BYTES` bounds the canonical normalized
  object uploaded to Telegram storage. It must remain below the documented
  2000 MB local Bot API upload limit.

Canonical video adaptation uses the versioned `canonical_video_v2` profile. Its
validated defaults are configured under `[media.inline_video]`:

```toml
target_max_bytes = 14680064 # 14 MiB
preferred_crf = 23
maximum_crf = 27
minimum_short_edge = 480
```

The corresponding environment overrides are
`SOOQA_MEDIA_INLINE_VIDEO_TARGET_MAX_BYTES`,
`SOOQA_MEDIA_INLINE_VIDEO_PREFERRED_CRF`,
`SOOQA_MEDIA_INLINE_VIDEO_MAXIMUM_CRF`, and
`SOOQA_MEDIA_INLINE_VIDEO_MINIMUM_SHORT_EDGE`. A compatible MP4 at or below
the target is remuxed. Oversized inputs try preferred CRF, then CRF values up
to the maximum, then an aspect-preserving even-dimension ladder down to the
minimum short edge. Every candidate is checked by actual output bytes, and
candidates above `normalized_storage_max_bytes` are discarded. If the target
cannot be met without crossing a quality floor, the earliest highest-quality
candidate within that storage ceiling is retained and Telegram remains
click-to-play. If no candidate is within the storage ceiling, normalization
fails instead of producing an unstorable artifact.
Native inputs smaller than the floor are never upscaled.

### Allowlisted social-video pages

The worker is direct-only when `SOOQA_MEDIA_YTDLP_ALLOWED_HOSTS` is empty. To
enable the supported public single-video families, set it to a comma-separated
list such as:

```bash
SOOQA_MEDIA_YTDLP_ALLOWED_HOSTS=youtube.com,youtu.be,tiktok.com,instagram.com,x.com,twitter.com,t.co
```

Entries are normalized as DNS hostnames and must belong to the closed provider
policy. YouTube accepts `youtube.com` and its dot-delimited subdomains plus
`youtu.be`. TikTok accepts `tiktok.com`, `www.tiktok.com`, `vm.tiktok.com`,
`vt.tiktok.com`, and `m.tiktok.com`; Instagram accepts `instagram.com` and
`www.instagram.com`; X/Twitter accepts `x.com`, `www.x.com`, `twitter.com`,
and `www.twitter.com`. `t.co` is a short-link candidate only when inspection
resolves it to an X/Twitter status. Credentials, IP literals, wildcards, paths,
unsupported hosts, and non-default ports are rejected. Direct MP4 and WebM
responses continue to use the direct HTTP adapter regardless of this list.
The worker fails startup if enabled yt-dlp runtime dependencies are missing;
the PO-token provider is required only when a YouTube family host is enabled.

The home Compose deployment enables all supported families when the variable is
unset; set it to an empty value in `deploy/home/.env` for direct-only operation.
The initial URL host is the allowlist decision point. yt-dlp is run with
`--ignore-config`, explicit no-cookie and no-browser-cookie flags, a cleared
environment, and no remote components. It discovers only the pinned bgutil
plugin from `/usr/local/share/sooqa/yt-dlp-plugins`; no browser state or netrc
credentials are available to the child. An accepted provider page can still
follow provider redirects and fetch its CDN media URLs. The supported URL
shapes are YouTube watch/Shorts pages, TikTok `/@user/video/<id>` pages or
`vm`/`vt`/`m` share links, Instagram `/reel/<id>` and `/p/<id>` pages, and
X/Twitter `/<user>/status/<id>` posts. Profiles, feeds, stories, live pages,
Spaces, galleries, playlists, image-only results, private/member-only,
age-restricted, account-required, and cookie-authenticated media are
intentionally outside this setup.

The home Compose deployment starts `brainicism/bgutil-ytdlp-pot-provider` as a
private, health-checked service with no published host port and configures the
worker with `http://pot-provider:4416`. The image is pinned by its multi-
architecture digest. The server and worker do not use a Compose health
dependency on this service: direct-only or social-only deployments remain
usable without a YouTube PO-token provider, while the worker's restart policy
retries its fail-closed preflight when YouTube page support is enabled. The
worker checks `/ping` for provider version `1.3.1` before enabling YouTube page
jobs; a missing provider or version mismatch fails startup with a
provider-specific diagnostic. Standalone deployments can set
`SOOQA_MEDIA_YTDLP_POT_PROVIDER_URL` to another validated provider origin.
yt-dlp retains its normal supported-client selection; the pinned provider is
made available through its extractor argument only for YouTube rather than
forcing one client for every page.

The submitted URL remains the ingest provenance. After inspection, the worker
uses the validated canonical `resolved_url` for yt-dlp execution and checks its
scheme, credentials, port, family, and host against the same policy before
starting the child. `t.co` inspection resolves each bounded hop with the shared
DNS/private-address guard and pinned connection, and rejects non-status X pages
before starting the child. The configured high-quality format is attempted first. For
YouTube only, if a fresh
attempt reports the specific media-byte `HTTP Error 403`, the worker starts one
more high-quality attempt with fresh extractor state; if that also receives the
same error, it makes one clean attempt with the bounded combined progressive
selection `best[ext=mp4][vcodec!=none][acodec!=none]/best[vcodec!=none][acodec!=none]`.
The winning selection is recorded as `input_json.download.selected_format`.
Private, removed, account-required, unsupported-surface, and other extractor
failures stay terminal, while a failed progressive attempt returns to the existing bounded
job retry policy. Every attempt has its own directory and failed or partial
files are removed before another attempt.

The home image downloads the official standalone `yt-dlp` distribution, the
`bgutil-ytdlp-pot-provider` plugin `1.3.1`, and a pinned Deno runtime. The
current Dockerfile pins yt-dlp `2026.07.04`, Deno `2.8.1`, and the plugin ZIP
with architecture-specific or release SHA-256 checksums. When the allowlist is
enabled, worker startup runs an offline local-info fixture through yt-dlp with
the configured plugin/EJS/Deno flags, checks that the bundled EJS and pinned
plugin are discoverable, executes a small Deno probe, and then performs the
provider `/ping` preflight. Each yt-dlp download runs inside a unique attempt
directory:
its relative final output, temporary fragments, split streams, merge
intermediates, and disabled-cache state are confined there. The worker monitors
the aggregate attempt directory with a three-times-final-size budget to allow
video/audio merging, while the final published file remains bounded by
`SOOQA_MEDIA_SOURCE_DOWNLOAD_MAX_BYTES`; a successful attempt must contain
exactly one regular media file. To update any pinned dependency, change its
version, asset names if needed, and every matching checksum together; the
Dockerfile also rejects a bgutil version/checksum override unless the Docker
and Rust runtime pins are updated together. Build the home image and
verify the startup diagnostics before doing an owner smoke test. CI uses fake
executables and does not contact YouTube.

The server and worker must share `media.work_root`; ffprobe and ffmpeg are
needed for probing and normalization. Source downloads and Telegram uploads
are streamed or path-based; no file-sized byte buffer is used.
Polling, downloads, and storage uploads use separate HTTP timeout policies so a
long upload cannot inherit the long-poll or download-stall deadline.

Preview bytes are separate from canonical storage output. The worker validates
preview MIME, dimensions, encoded size, and SHA-256 before the media row is
committed. Video preview production reuses the best frame already decoded by
the sequence extractor; animation preview production makes only one bounded
first-frame decode attempt. No preview path triggers a second full video
decode, an ffmpeg process per sample, an original-media retention rule, or an
automatic backfill.

After a storage message exists, the revision-fenced media metadata command
replaces the complete description and tag set in one transaction and enqueues
a durable `sync_storage_caption` job after the database commit. The job edits
the existing storage message and uses both the media caption generation and a
claim token as fences. Retries are bounded and restore a pending claim before
re-running; an expired final lease records a failure for the current
generation. A late old-generation completion schedules a current-generation
reapply so the final Telegram caption cannot remain stale.

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

The web Dashboard is the sole owner-facing duplicate-decision surface. Its
technical-duplicate cards show the bounded persisted candidate evidence and
storage links; `Same — use this` reuses the existing media item, while
`Different — save as new` starts the normal force-save pipeline. A
pending-storage candidate remains on the existing media upload lifecycle.
The durable HTTP commands are documented in `docs/openapi.yaml`. Telegram
duplicate cards and callbacks are retired; stale callback data is acknowledged
and ignored.

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
source from the durable `file_id`. Relative file paths and cloud Bot API files
are streamed over the bounded HTTP route. In local Bot API mode, an absolute
`getFile` path is copied only when the worker has the configured read-only file
root; canonicalization rejects traversal, outside-root paths, symlink escapes,
and non-regular files. Both paths use the same size-limited temporary file and
atomic publication, and the reported size must match the copied bytes. Internal
Bot API paths are not persisted or included in normal API/log output. A
terminal local-path/configuration error is not retried five times as an HTTP
download. A replayed update uses the same Telegram update idempotency key, so it
returns the existing ingest without another acceptance job or eager download.
This keeps the polling loop responsive while a large worker download is
running.

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
an optional browser-local exact time for `Queue…`. A blank time uses normal
cadence; a populated future time is converted to an RFC3339 instant before
submission. Their modal layout is userscript-owned so
board CSS cannot make the form unusable. Native dialog support is preferred;
otherwise a labelled in-page overlay provides the same structured fields,
traps keyboard focus, makes the underlying page inert while open, and restores
focus when it closes. An initial focus failure leaves the form open and usable,
and metadata never falls back to a pipe-delimited prompt. Cancel, Escape, and
submission are one-shot exits that restore the action buttons.
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

### Trusted-LAN admin access

The tracked Compose file publishes port 8080 as `127.0.0.1:8080`, so it is not
reachable from Windows by default. On a trusted home LAN, replace only that
host address in the `server.ports` entry with the Mac's fixed LAN address, for
example:

```yaml
ports:
  - "192.168.1.132:8080:8080"
```

Recreate only the server container after the edit:

```bash
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml up -d --no-deps --force-recreate server
```

Then open `http://192.168.1.132:8080/admin` from Windows and use the configured
`SOOQA_API_TOKEN`. Use the Mac's actual LAN address, allow TCP 8080 only on the
trusted/private firewall profile, and do not use `0.0.0.0` on an untrusted
network. This connection is plain HTTP: use a TLS reverse proxy before crossing
an untrusted network or exposing the service remotely.

### Backup, upgrade, restore, and rollback

Before every home upgrade, create a private PostgreSQL custom-format backup.
Use an explicit owner-only path and keep the file outside the repository:

```bash
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml exec -T postgres \
  pg_dump --username=sooqa --dbname=sooqa --format=custom \
  > /Users/OWNER/Backups/sooqa-before-upgrade.dump
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml exec -T postgres \
  pg_restore --list \
  < /Users/OWNER/Backups/sooqa-before-upgrade.dump > /dev/null
```

An upgrade keeps all named volumes and applies forward migrations before the
new server and worker start:

```bash
docker image tag sooqa:home sooqa:home-before-upgrade
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml build
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml stop server worker
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml run --rm server migrate
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml up -d
curl --fail http://127.0.0.1:8080/health/live
```

Update the Git checkout to the intended reviewed commit before `build`; source
control selection is deliberately separate from the deployment commands. Do
not use `down --volumes` during an upgrade.

To restore a backup, first confirm the exact archive path. The following block
irreversibly replaces the current `sooqa` database, so stop application writers
and take another backup if the current state might be needed:

```bash
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml stop server worker
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml exec -T postgres \
  dropdb --username=sooqa --force sooqa
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml exec -T postgres \
  createdb --username=sooqa --owner=sooqa sooqa
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml exec -T postgres \
  pg_restore --username=sooqa --dbname=sooqa --no-owner --no-privileges --exit-on-error \
  < /Users/OWNER/Backups/sooqa-before-upgrade.dump
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml run --rm server migrate
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml up -d
```

For binary-only rollback when no migration was applied, run:

```bash
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml stop server worker
docker image tag sooqa:home-before-upgrade sooqa:home
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml up -d --no-build --force-recreate server worker
```

If the upgrade applied any migration, an old binary is not assumed compatible
with the new schema. Stop server and worker, retag the old image, restore the
matching pre-upgrade database backup without applying newer migrations, and
only then recreate the old server and worker with `--no-build`.

### Destructive legacy-schema reset

Use this only to discard a database from before ADR 0009, never as a normal
upgrade or test command. It deletes the home PostgreSQL and transient sooqa
workspace volumes but deliberately retains the local Bot API volume:

```bash
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml down
docker volume rm sooqa-home_home-postgres-data sooqa-home_home-sooqa-work
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml up -d postgres telegram-bot-api
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml run --rm server migrate
docker compose --env-file deploy/home/.env -f deploy/home/docker-compose.yml up -d
```

This reset is irreversible unless a PostgreSQL backup exists. Never add it to
application startup, CI, tests, or an unattended deployment script.

The official `tdlib/telegram-bot-api` source is built at the commit pinned in
`deploy/telegram-bot-api/Dockerfile`. The local server runs with `--local`,
stores its working state in the dedicated `home-telegram-bot-api-data` volume,
and exposes port 8081 only to the Compose network. sooqa reaches it as
`http://telegram-bot-api:8081`; the server and worker share the separate
`home-sooqa-work` volume for sooqa media workspaces, while the worker alone
mounts `home-telegram-bot-api-data` read-only at
`/var/lib/telegram-bot-api`. This second mount lets the worker copy the
absolute paths returned by `getFile` without putting those paths into an HTTP
file URL. Relative paths continue to use the bounded HTTP route.

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
`supports_streaming=true` `sendVideo` flag plus explicit probed duration, width,
and height. The persisted bounded JPEG preview is validated against Telegram's
thumbnail limits, staged only for the multipart request, and removed after the
request. The canonical video profile produces MP4/H.264 video with optional AAC
audio and fast-start metadata, so newly stored videos can begin playback before
the full file is downloaded by a client. Images, animations, and audio use their
existing upload methods and do not receive this video-only metadata. This does
not change existing storage messages; it applies to new uploads after
deployment.

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
