# Development

Use the pinned Rust toolchain (1.97.1). The normal gate is:

```bash
just check
```

It runs formatting, future-incompatibility reporting, Clippy with warnings
denied, and non-ignored workspace tests. Before a PR, also run:

```bash
git diff --check
just openapi-validate
```

## PostgreSQL tests

Integration tests are marked `#[ignore]` because they need a real PostgreSQL
server. Every PostgreSQL integration test uses SQLx's `#[sqlx::test]` support
with the repository migrations and receives its own `PgPool`. SQLx creates a
fresh database, applies the migrations, and drops that database when the test
finishes, so Rust's default parallel test execution is safe and intentional.

The PostgreSQL role in `DATABASE_URL` must be allowed to create and drop test
databases (`CREATEDB`, or a superuser role in local development). Point this
URL at a dedicated development/CI PostgreSQL account; never use a runtime or
production database account for the test suite. One legacy-migration test
creates an additional uniquely named database and therefore also requires the
account to create databases directly.

Cargo's separator is intentional:

```bash
DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa \
  cargo test -p sooqa-persistence --tests -- --ignored
```

The first `--` belongs to Cargo; `--ignored` is passed to the test harness and
causes ignored tests to run. `just test-integration` runs the focused
persistence, API, and worker integration commands. Do not add
`--test-threads=1`; isolation is provided by SQLx and serialization would hide
missing isolation while slowing the suite.

The reset is intentionally clean. SQLx test databases start from the five-table
migration; they do not copy rows from a previous schema, and no command resets
a Docker volume automatically. If a local runtime database still has the old
model, recreate it explicitly before migrating.

## OpenAPI and binaries

`docs/openapi.yaml` is the versioned HTTP contract. Validate it with
`just openapi-validate`. The media crate owns direct HTTP, ffprobe, ffmpeg,
yt-dlp, workspaces, hashing, and fingerprints. Tests that need local binaries
are separate from the normal unit suite:

```bash
just test-media
```

External commands use argument arrays and never a shell. Database transactions
must be closed before network or subprocess work starts.
