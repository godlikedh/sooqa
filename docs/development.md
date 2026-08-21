# Development

Use the pinned Rust toolchain (1.97.1). The authoritative local quality
command is `just check`:

```bash
just check
```

It runs both Node.js suites through `tools/test-javascript.sh` as part of the
test phase. The CI `check` job is the pull-request quality gate and invokes
that same script with Node.js 22, so userscript and admin asset failures are
required checks in both environments.

It runs formatting, future-incompatibility reporting, Clippy with warnings
denied, and non-ignored workspace tests. Before a PR, also run:

```bash
git diff --check
just openapi-validate
```

## PostgreSQL tests

Integration tests are marked `#[ignore]` because they need a real PostgreSQL
server. Every PostgreSQL integration test uses SQLx's `#[sqlx::test]` support
with the repository migrations and receives its own `PgPool`. SQLx uses the
database named by `DATABASE_URL` as a test-control database: before creating
per-test databases, it writes bookkeeping state to the `_sqlx_test`
schema/table there. Each test then gets a fresh database with the repository
migrations. A successful test result causes SQLx to drop that database
automatically. A failed or panicking test intentionally leaves its database
behind for diagnosis; a later run of the same test path can reclaim it, or it
must be dropped explicitly. This keeps Rust's default parallel test execution
safe and intentional.

The PostgreSQL role in `DATABASE_URL` must own or be allowed to write to the
test-control database and must be allowed to create databases (`CREATEDB`, or a
superuser role in local development). The `sooqa` database in the command below
is intentionally the disposable local/CI test-control database. Do not point
this URL at the runtime or home database; use a separate test-control database
name if those environments also use `sooqa`. Use a dedicated development/CI
PostgreSQL account, never a runtime or production database account, for the
test suite. One legacy-migration test creates an additional uniquely named
database and therefore also requires the account to create databases directly.

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
