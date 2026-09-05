#!/bin/sh

set -eu

: "${DATABASE_URL:?set DATABASE_URL to a disposable PostgreSQL test-control database}"

cargo test -p sooqa-persistence --tests -- --ignored
cargo test -p sooqa-api --tests -- --ignored
# Keep ignored worker coverage on named integration targets; worker --lib also
# contains an external-media fixture test that is not part of this gate.
cargo test -p sooqa-worker --test worker -- --ignored
cargo test -p sooqa-worker --test inspection -- --ignored
cargo test -p sooqa-worker --test identity -- --ignored
cargo test -p sooqa-worker --test publication -- --ignored
cargo test -p sooqa-worker --test disk_admission -- --ignored
cargo test -p sooqa-worker --test storage_shutdown -- --ignored
