# Security

This is a single-admin self-hosted service. Keep the server, database, media
work root, and Telegram bot credentials on a trusted host. The historical
roadmap security section is in [PROJECT_SPEC.md](reference/PROJECT_SPEC.md);
the controls below describe the current implementation.

HTTP requests require a bearer token with the route-specific scope. The
database stores only a SHA-256 token hash and a short prefix for administration;
tokens are not recoverable from PostgreSQL. Request bodies and processing
timeouts are bounded, and errors return a request ID without secrets.

The Telegram adapter accepts private messages only from configured positive
administrator IDs. It records update claims durably, avoids logging message
contents, and stages media in a per-update workspace. The configured application
download limit is enforced before and during streaming; partial files are
removed on download or flush errors.

Direct HTTP source handling validates HTTP(S) URLs, rejects credentials and
private/special IP ranges, resolves and pins destinations, and revalidates
manual redirects. yt-dlp, ffmpeg, and ffprobe are invoked with argument arrays,
bounded output, timeouts, and no shell. yt-dlp and ffmpeg write unique
same-directory temporary files and publish only after validation.

Workspaces use job/update IDs rather than user-controlled path components.
File names cannot contain separators or parent-directory components, workspace
paths reject symlinked parents, and cleanup removes only the expected direct
child of the configured jobs directory. A shared hostile work root would need
descriptor-relative no-follow filesystem operations beyond this MVP.

Database constraints enforce SHA-256 length, canonical ownership/role, job
lease state, and job-attempt states in addition to repository validation.
External network or subprocess calls never run while a PostgreSQL transaction
is held. Storage upload ambiguity is retained as an explicit durable intent
instead of being silently retried.
