set shell := ["sh", "-cu"]

default: check

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

lint:
    cargo clippy --workspace --all-targets -- -D warnings

future-incompat:
    cargo check --workspace --all-targets --future-incompat-report

test: test-userscript
    cargo test --workspace --all-targets

test-userscript:
    node --test userscripts/test/*.test.cjs

db-up:
    docker compose up -d postgres

db-down:
    docker compose down

db-migrate:
    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa cargo run -p sooqa-server -- migrate

test-integration:
    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa cargo test -p sooqa-persistence --tests -- --ignored
    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa cargo test -p sooqa-api --tests -- --ignored
    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa cargo test -p sooqa-worker --test worker -- --ignored
    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa cargo test -p sooqa-worker --test inspection -- --ignored
    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa cargo test -p sooqa-worker --test identity -- --ignored

test-media:
    cargo test -p sooqa-media --lib -- --ignored

openapi-validate:
    sh tools/openapi-validate.sh

openapi-generate:
    sh tools/openapi-generate.sh

check: fmt-check future-incompat lint test
