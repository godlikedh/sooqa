use std::env;

use sooqa_inbox::{IngestSubmission, IngestSubmissionInput, SubmittedVia};
use sooqa_persistence::{Database, InboxRepositoryError};
use uuid::Uuid;

async fn database() -> Database {
    let url = env::var("DATABASE_URL").expect("DATABASE_URL must point to PostgreSQL");
    let database = Database::connect(&url, 10).await.expect("database should connect");
    database.migrate().await.expect("migration should apply");
    database
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn input_key_replays_identical_ingests_and_rejects_conflicts() {
    let database = database().await;
    let key = format!("test-ingest-{}", Uuid::new_v4());
    let mut input = IngestSubmissionInput::new("https://example.test/video", SubmittedVia::Api);
    input.idempotency_key = Some(key.clone());
    let first = database
        .inbox()
        .create_ingest(IngestSubmission::try_new(input.clone()).unwrap())
        .await
        .unwrap();
    let replay =
        database.inbox().create_ingest(IngestSubmission::try_new(input).unwrap()).await.unwrap();
    assert_eq!(first.request.id, replay.request.id);
    assert!(!replay.created);

    let mut conflicting =
        IngestSubmissionInput::new("https://example.test/other", SubmittedVia::Api);
    conflicting.idempotency_key = Some(key);
    let error = database
        .inbox()
        .create_ingest(IngestSubmission::try_new(conflicting).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(error, InboxRepositoryError::IdempotencyConflict { .. }));
    sqlx::query("DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1")
        .bind(first.request.id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM ingests WHERE id = $1")
        .bind(first.request.id)
        .execute(database.pool())
        .await
        .unwrap();
}
