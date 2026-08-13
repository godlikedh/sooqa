use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use sooqa_api::{ApiSettings, ApiState, router};
use sooqa_inbox::{IngestSubmission, IngestSubmissionInput, SubmittedVia};
use sooqa_persistence::Database;
use tower::ServiceExt;
use uuid::Uuid;

fn app(pool: sqlx::PgPool) -> (Database, axum::Router) {
    let database = Database::from_pool(pool);
    let app = router(
        ApiSettings::default(),
        ApiState::new(database.inbox(), "test-api-token", database.library(), database.publisher()),
    );
    (database, app)
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn api_authenticates_with_the_single_configured_bearer_secret(pool: sqlx::PgPool) {
    let (_database, app) = app(pool);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/ingests")
                .header("content-type", "application/json")
                .body(Body::from(json!({"url": "https://example.test"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/ingests")
                .header("content-type", "application/json")
                .header("authorization", "Bearer test-api-token")
                .header("idempotency-key", "api-test-key")
                .body(Body::from(json!({"url": "https://example.test"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn force_save_is_authenticated_idempotent_and_durable(pool: sqlx::PgPool) {
    let (database, app) = app(pool);
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/api-force-save-{}", Uuid::new_v4()),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE ingests SET state = 'duplicate_pending', duplicate_evidence = $2 WHERE id = $1",
    )
    .bind(ingest.ingest.id)
    .bind(json!({"algorithm_version": "video_sequence_v1", "matches": []}))
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE queue.jobs SET state = 'succeeded', completed_at = now() WHERE kind = 'inspect_source' AND payload->>'ingest_id' = $1",
    )
        .bind(ingest.ingest.id.to_string())
        .execute(database.pool())
        .await
        .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/ingests/{}/force-save", ingest.ingest.id))
                .header("content-type", "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/ingests/{}/force-save", ingest.ingest.id))
                .header("authorization", "Bearer test-api-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["status"], "queued");
    assert_eq!(body["force_save"], true);
    assert!(body["duplicate_evidence"].is_null());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'inspect_source' AND state = 'queued' AND payload->>'ingest_id' = $1",
        )
        .bind(ingest.ingest.id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap(),
        1
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/ingests/{}/force-save", ingest.ingest.id))
                .header("authorization", "Bearer test-api-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["id"], ingest.ingest.id.to_string());
    assert_eq!(body["status"], "queued");
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn accept_duplicate_reuses_ready_media_and_replays_without_uploading(pool: sqlx::PgPool) {
    let (database, app) = app(pool);
    let media_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO media (id, kind, storage_state, tags, description, telegram_storage_chat_id, telegram_storage_message_id, telegram_file_id) VALUES ($1, 'video', 'ready', $2, $3, $4, $5, $6)",
    )
    .bind(media_id)
    .bind(vec!["existing".to_owned()])
    .bind("existing description")
    .bind(-100123_i64)
    .bind(77_i64)
    .bind("ready-file")
    .execute(database.pool())
    .await
    .unwrap();

    let mut input = IngestSubmissionInput::new(
        format!("https://example.test/api-accept-{}", Uuid::new_v4()),
        SubmittedVia::Api,
    );
    input.supplied_description = Some("incoming description".to_owned());
    input.supplied_tags = vec!["incoming".to_owned(), "existing".to_owned()];
    let ingest =
        database.inbox().create_ingest(IngestSubmission::try_new(input).unwrap()).await.unwrap();
    sqlx::query(
        "UPDATE ingests SET state = 'duplicate_pending', duplicate_evidence = $2 WHERE id = $1",
    )
    .bind(ingest.ingest.id)
    .bind(duplicate_evidence(media_id))
    .execute(database.pool())
    .await
    .unwrap();

    let invalid_media_id = Uuid::now_v7();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/ingests/{}/accept-duplicate", ingest.ingest.id))
                .header("content-type", "application/json")
                .header("authorization", "Bearer test-api-token")
                .body(Body::from(json!({"media_id": invalid_media_id}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "duplicate_candidate_not_evidenced");
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM ingests WHERE id = $1")
            .bind(ingest.ingest.id)
            .fetch_one(database.pool())
            .await
            .unwrap(),
        "duplicate_pending"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/ingests/{}/accept-duplicate", ingest.ingest.id))
                .header("content-type", "application/json")
                .header("authorization", "Bearer test-api-token")
                .body(Body::from(json!({"media_id": media_id}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["status"], "completed");
    assert_eq!(body["media_id"], media_id.to_string());
    assert!(body["duplicate_evidence"].is_null());

    let (description, tags) = sqlx::query_as::<_, (Option<String>, Vec<String>)>(
        "SELECT description, tags FROM media WHERE id = $1",
    )
    .bind(media_id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(description.as_deref(), Some("incoming description"));
    assert_eq!(tags, ["existing", "incoming"]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM queue.jobs WHERE kind = 'upload_storage_asset' AND payload->>'media_id' = $1",
        )
        .bind(media_id.to_string())
        .fetch_one(database.pool())
        .await
        .unwrap(),
        0
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/ingests/{}/accept-duplicate", ingest.ingest.id))
                .header("content-type", "application/json")
                .header("authorization", "Bearer test-api-token")
                .body(Body::from(json!({"media_id": media_id}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/ingests/{}/accept-duplicate", ingest.ingest.id))
                .header("content-type", "application/json")
                .header("authorization", "Bearer test-api-token")
                .body(Body::from(json!({"media_id": Uuid::now_v7()}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    sqlx::query("UPDATE ingests SET input_json = '{}'::jsonb WHERE id = $1")
        .bind(ingest.ingest.id)
        .execute(database.pool())
        .await
        .unwrap();
    for state in ["completed", "storing"] {
        sqlx::query("UPDATE ingests SET state = $2 WHERE id = $1")
            .bind(ingest.ingest.id)
            .bind(state)
            .execute(database.pool())
            .await
            .unwrap();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/ingests/{}/accept-duplicate", ingest.ingest.id))
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer test-api-token")
                    .body(Body::from(json!({"media_id": media_id}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    sqlx::query("DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1")
        .bind(ingest.ingest.id.to_string())
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM ingests WHERE id = $1")
        .bind(ingest.ingest.id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(media_id)
        .execute(database.pool())
        .await
        .unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn accept_duplicate_replays_after_pending_storage_failure(pool: sqlx::PgPool) {
    let (database, app) = app(pool);
    let media_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO media (id, kind, storage_state) VALUES ($1, 'video', 'pending_storage')",
    )
    .bind(media_id)
    .execute(database.pool())
    .await
    .unwrap();
    let ingest = database
        .inbox()
        .create_ingest(
            IngestSubmission::try_new(IngestSubmissionInput::new(
                format!("https://example.test/api-failure-replay-{}", Uuid::new_v4()),
                SubmittedVia::Api,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE ingests SET state = 'duplicate_pending', duplicate_evidence = $2 WHERE id = $1",
    )
    .bind(ingest.ingest.id)
    .bind(duplicate_evidence(media_id))
    .execute(database.pool())
    .await
    .unwrap();

    let accept_request = |requested_media_id| {
        app.clone().oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/ingests/{}/accept-duplicate", ingest.ingest.id))
                .header("content-type", "application/json")
                .header("authorization", "Bearer test-api-token")
                .body(Body::from(json!({"media_id": requested_media_id}).to_string()))
                .unwrap(),
        )
    };
    assert_eq!(accept_request(media_id).await.unwrap().status(), StatusCode::ACCEPTED);
    assert_eq!(
        database
            .inbox()
            .fail_storage_for_media(
                media_id,
                sooqa_inbox::IngestStatus::FailedRetryable,
                "storage_upload",
                "temporary storage failure",
            )
            .await
            .unwrap(),
        1
    );
    let response = accept_request(media_id).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["status"], "failed_retryable");

    assert_eq!(
        database
            .inbox()
            .fail_storage_for_media(
                media_id,
                sooqa_inbox::IngestStatus::FailedTerminal,
                "storage_upload",
                "permanent storage failure",
            )
            .await
            .unwrap(),
        1
    );
    let response = accept_request(media_id).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["status"], "failed_terminal");

    let response = accept_request(Uuid::now_v7()).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    sqlx::query(
        "DELETE FROM queue.jobs WHERE payload->>'ingest_id' = $1 OR payload->>'media_id' = $2",
    )
    .bind(ingest.ingest.id.to_string())
    .bind(media_id.to_string())
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query("DELETE FROM ingests WHERE id = $1")
        .bind(ingest.ingest.id)
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM media WHERE id = $1")
        .bind(media_id)
        .execute(database.pool())
        .await
        .unwrap();
}

fn duplicate_evidence(media_id: Uuid) -> Value {
    json!({
        "algorithm_version": "video_sequence_v1",
        "matches": [{
            "media_id": media_id,
            "fingerprint_version": "video_sequence_v1",
            "classification": "strong_duplicate",
            "aligned_offset_ms": 0,
            "informative_matched_samples": 8,
            "incoming_coverage_bps": 9000,
            "candidate_coverage_bps": 9000,
            "median_distance_bps": 100,
            "high_percentile_distance_bps": 200,
            "longest_temporally_consistent_run": 8,
            "unmatched_incoming_prefix": 0,
            "unmatched_incoming_suffix": 0,
            "unmatched_candidate_prefix": 0,
            "unmatched_candidate_suffix": 0,
            "gap_count": 0,
            "score_bps": 9500,
            "shared_token_count": 12,
            "token_overlap_bps": 8000
        }]
    })
}
