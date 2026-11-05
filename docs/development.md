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

Use the shared PostgreSQL gate for all integration suites:

```bash
DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa \
  sh tools/test-postgres.sh
```

The script runs persistence and API tests plus the named worker targets for
worker, inspection, identity, caption synchronization, publication, disk admission, and storage
shutdown. The caller's `DATABASE_URL` is preserved; `just test-integration`
uses the disposable development URL above only when the variable is unset.
Each Cargo command passes `--ignored` to the test harness. Do not add
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

## Code health reviews

[Desloppify](https://github.com/peteromallet/desloppify) supplements the required
quality gate with structural, duplication, and subjective review findings. It
does not replace `just check` or the PostgreSQL and media acceptance tests.
Install the review tools locally with Python 3.11+ and npm:

```bash
python3 -m venv .desloppify/venv
.desloppify/venv/bin/python -m pip install --cache-dir .desloppify/pip-cache 'desloppify[full]==1.0'
npm install --prefix .desloppify/npm --cache .desloppify/npm-cache --no-audit --no-fund jscpd@5.4.2
export PATH="$PWD/.desloppify/venv/bin:$PWD/.desloppify/npm/node_modules/.bin:$PATH"
DESLOPPIFY_ROOT="$PWD/.desloppify" desloppify update-skill codex
desloppify config set generate_scorecard false
desloppify exclude target
desloppify exclude work-data
desloppify exclude .desloppify
desloppify --lang rust scan --path . --no-badge
desloppify --lang rust next
```

The Rust workspace is one coherent scan target. Build output, runtime media,
and the review environment are excluded; source and tests remain visible.
The environment, workflow guide, review packets, and scan state stay under
`.desloppify/`, which is ignored by Git and Docker builds. Re-export the `PATH`
above in each shell before continuing with `desloppify --lang rust next`.

Follow the generated review and triage queue, verify findings against code and
the product authority, and resolve only changes actually made. Blind reviews
must use the generated packet without score history. Unassessed subjective
dimensions count as zero, so report the objective score separately until the
review is complete. In particular, public Rust items within this unpublished
workspace do not automatically need published-library compatibility machinery,
and inline test doubles need to be distinguished from production async code.
