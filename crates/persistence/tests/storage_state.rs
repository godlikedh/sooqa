use std::time::Duration;

use sooqa_jobs::{JobType, NewJob};
use sooqa_library::{
    MediaStorageState, StorageUploadReservation, StorageUploadReservationRequest,
    StorageUploadStore,
};
use sooqa_persistence::{Database, LibraryRepositoryError};
use uuid::Uuid;

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn storage_rows_decode_before_reservation_decisions(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    for state in [
        MediaStorageState::Pending,
        MediaStorageState::Ready,
        MediaStorageState::Unknown,
        MediaStorageState::Missing,
    ] {
        let media_id: Uuid = sqlx::query_scalar(
            "INSERT INTO media (kind, storage_state, telegram_storage_chat_id, telegram_storage_message_id, telegram_file_id) VALUES ('image', $1, -100123, 42, 'file') RETURNING id",
        )
        .bind(state.as_str())
        .fetch_one(database.pool())
        .await
        .unwrap();
        let media = database.library().find_media(media_id).await.unwrap().unwrap();
        assert_eq!(media.storage_state, state);
        let reservation = database
            .library()
            .reserve_storage_upload(StorageUploadReservationRequest { media_id, generation: 0 })
            .await
            .unwrap();
        match state {
            MediaStorageState::Pending => {
                assert!(matches!(reservation, StorageUploadReservation::Reserved { .. }));
            }
            MediaStorageState::Ready => {
                assert!(matches!(reservation, StorageUploadReservation::Reused(_)));
            }
            MediaStorageState::Unknown | MediaStorageState::Missing => {
                assert!(matches!(reservation, StorageUploadReservation::ReconciliationRequired));
            }
        }
    }
}

#[sqlx::test(migrations = "../../migrations")]
#[ignore = "requires PostgreSQL"]
async fn malformed_storage_state_blocks_reads_and_recovers_conservatively(pool: sqlx::PgPool) {
    let database = Database::from_pool(pool);
    // Only this SQLx-isolated test database permits a malformed state.
    sqlx::query("ALTER TABLE media DROP CONSTRAINT media_storage_state_check")
        .execute(database.pool())
        .await
        .unwrap();
    let media_id: Uuid = sqlx::query_scalar(
        "INSERT INTO media (kind, storage_state) VALUES ('image', 'unrecognized') RETURNING id",
    )
    .fetch_one(database.pool())
    .await
    .unwrap();
    let error = database.library().find_media(media_id).await.unwrap_err();
    assert!(
        matches!(error, LibraryRepositoryError::Database(sqlx::Error::ColumnDecode { index, .. }) if index == "storage_state")
    );
    assert!(
        database
            .library()
            .reserve_storage_upload(StorageUploadReservationRequest { media_id, generation: 0 })
            .await
            .is_err()
    );
    let token: Option<Uuid> = sqlx::query_scalar("SELECT storage_token FROM media WHERE id = $1")
        .bind(media_id)
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert!(token.is_none());

    database.jobs().enqueue(NewJob::upload_storage_asset(media_id)).await.unwrap();
    let job = database
        .jobs()
        .claim_next("state-recovery-test", Duration::from_secs(30), &[JobType::UploadStorageAsset])
        .await
        .unwrap()
        .unwrap();
    sqlx::query(
        "UPDATE queue.jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
    )
    .bind(job.id)
    .execute(database.pool())
    .await
    .unwrap();
    assert_eq!(database.jobs().recover_stale_leases().await.unwrap(), 1);
    let (state, error_class): (String, String) =
        sqlx::query_as("SELECT state, error_class FROM queue.jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(state, "failed");
    assert_eq!(error_class, "storage_unknown");
    assert_eq!(
        database.library().find_media(media_id).await.unwrap().unwrap().storage_state,
        MediaStorageState::Unknown
    );
}
