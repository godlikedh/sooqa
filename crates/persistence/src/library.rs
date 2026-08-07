use serde_json::Value;
use sooqa_library::{
    AssetRole, ContentItem, ExactDuplicateRequest, ExactDuplicateResolution, MediaAsset,
    NewContentItem, NewMediaAsset, NewSourceRecord, NewSourceRecordDraft, NewStorageObject, NewTag,
    SourceRecord, StorageObject, Tag,
};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone)]
pub struct LibraryRepository {
    pool: PgPool,
}

impl LibraryRepository {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn resolve_exact_duplicate(
        &self,
        request: ExactDuplicateRequest,
    ) -> Result<ExactDuplicateResolution, LibraryRepositoryError> {
        validate_exact_duplicate_request(&request)?;
        let mut transaction = self.pool.begin().await?;

        if let Some(source_record) =
            find_source_by_identity(&mut transaction, &request.source).await?
        {
            let (content_item, canonical_asset) =
                load_resolution_for_content(&mut transaction, source_record.content_item_id, None)
                    .await?;
            transaction.commit().await?;
            return Ok(ExactDuplicateResolution {
                content_item,
                canonical_asset,
                source_record,
                content_created: false,
                source_created: false,
            });
        }

        if let Some(asset) = find_media_asset_by_sha256(
            &mut transaction,
            request.asset.sha256.as_deref().expect("validated SHA-256 must exist"),
        )
        .await?
        {
            let (content_item, canonical_asset) =
                load_resolution_for_content(&mut transaction, asset.content_item_id, Some(asset))
                    .await?;
            let (source_record, source_created) =
                insert_source_or_find_existing(&mut transaction, content_item.id, &request.source)
                    .await?;

            if source_record.content_item_id == content_item.id {
                transaction.commit().await?;
                return Ok(ExactDuplicateResolution {
                    content_item,
                    canonical_asset,
                    source_record,
                    content_created: false,
                    source_created,
                });
            }

            let (content_item, canonical_asset) =
                load_resolution_for_content(&mut transaction, source_record.content_item_id, None)
                    .await?;
            transaction.commit().await?;
            return Ok(ExactDuplicateResolution {
                content_item,
                canonical_asset,
                source_record,
                content_created: false,
                source_created: false,
            });
        }

        let content_row =
            insert_content_item_in_transaction(&mut transaction, &request.content_item).await?;
        let content_item = content_row.into_content_item()?;
        let asset_row = insert_canonical_asset_in_transaction(
            &mut transaction,
            content_item.id,
            &request.asset,
        )
        .await?;

        let Some(asset_row) = asset_row else {
            let asset = find_media_asset_by_sha256(
                &mut transaction,
                request.asset.sha256.as_deref().expect("validated SHA-256 must exist"),
            )
            .await?
            .ok_or(LibraryRepositoryError::Invariant(
                "canonical asset conflict had no matching asset",
            ))?;
            let (existing_content, canonical_asset) =
                load_resolution_for_content(&mut transaction, asset.content_item_id, Some(asset))
                    .await?;
            let (source_record, source_created) = insert_source_or_find_existing(
                &mut transaction,
                existing_content.id,
                &request.source,
            )
            .await?;
            delete_uncommitted_content(&mut transaction, content_item.id).await?;

            if source_record.content_item_id == existing_content.id {
                transaction.commit().await?;
                return Ok(ExactDuplicateResolution {
                    content_item: existing_content,
                    canonical_asset,
                    source_record,
                    content_created: false,
                    source_created,
                });
            }

            let (content_item, canonical_asset) =
                load_resolution_for_content(&mut transaction, source_record.content_item_id, None)
                    .await?;
            transaction.commit().await?;
            return Ok(ExactDuplicateResolution {
                content_item,
                canonical_asset,
                source_record,
                content_created: false,
                source_created: false,
            });
        };

        let asset = asset_row.into_media_asset()?;
        let content_item =
            set_canonical_asset_in_transaction(&mut transaction, content_item.id, asset.id)
                .await?
                .into_content_item()?;
        let Some(source_record) =
            insert_source_in_transaction(&mut transaction, content_item.id, &request.source)
                .await?
        else {
            let source_record = find_source_by_identity(&mut transaction, &request.source)
                .await?
                .ok_or(LibraryRepositoryError::Invariant(
                "source conflict had no matching source record",
            ))?;
            let (existing_content, canonical_asset) =
                load_resolution_for_content(&mut transaction, source_record.content_item_id, None)
                    .await?;
            delete_uncommitted_content(&mut transaction, content_item.id).await?;
            transaction.commit().await?;
            return Ok(ExactDuplicateResolution {
                content_item: existing_content,
                canonical_asset,
                source_record,
                content_created: false,
                source_created: false,
            });
        };

        transaction.commit().await?;
        Ok(ExactDuplicateResolution {
            content_item,
            canonical_asset: asset,
            source_record,
            content_created: true,
            source_created: true,
        })
    }

    pub async fn create_content_item(
        &self,
        new_item: NewContentItem,
    ) -> Result<ContentItem, LibraryRepositoryError> {
        let row = sqlx::query_as::<_, ContentItemRow>(
            r#"
            INSERT INTO content_items (
                kind, preferred_title, editorial_description, notes
            )
            VALUES ($1, $2, $3, $4)
            RETURNING
                id, kind, status, canonical_asset_id, preferred_title,
                editorial_description, notes, created_at, updated_at, archived_at
            "#,
        )
        .bind(new_item.kind.as_str())
        .bind(new_item.preferred_title)
        .bind(new_item.editorial_description)
        .bind(new_item.notes)
        .fetch_one(&self.pool)
        .await?;

        row.into_content_item()
    }

    pub async fn find_content_item(
        &self,
        id: Uuid,
    ) -> Result<Option<ContentItem>, LibraryRepositoryError> {
        let row = sqlx::query_as::<_, ContentItemRow>(
            r#"
            SELECT
                id, kind, status, canonical_asset_id, preferred_title,
                editorial_description, notes, created_at, updated_at, archived_at
            FROM content_items
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(ContentItemRow::into_content_item).transpose()
    }

    pub async fn create_media_asset(
        &self,
        new_asset: NewMediaAsset,
    ) -> Result<MediaAsset, LibraryRepositoryError> {
        let duration_ms = to_database_u64(new_asset.duration_ms, "duration_ms")?;
        let bit_rate = to_database_u64(new_asset.bit_rate, "bit_rate")?;
        let file_size_bytes = to_database_u64(new_asset.file_size_bytes, "file_size_bytes")?;
        let row = sqlx::query_as::<_, MediaAssetRow>(
            r#"
            INSERT INTO media_assets (
                content_item_id, role, media_kind, mime_type, container,
                video_codec, audio_codec, width, height, duration_ms, bit_rate,
                file_size_bytes, sha256, local_work_path, storage_state
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            RETURNING
                id, content_item_id, role, media_kind, mime_type, container,
                video_codec, audio_codec, width, height, duration_ms, bit_rate,
                file_size_bytes, sha256, local_work_path, storage_state, created_at
            "#,
        )
        .bind(new_asset.content_item_id)
        .bind(new_asset.role.as_str())
        .bind(new_asset.media_kind.as_str())
        .bind(new_asset.mime_type)
        .bind(new_asset.container)
        .bind(new_asset.video_codec)
        .bind(new_asset.audio_codec)
        .bind(new_asset.width)
        .bind(new_asset.height)
        .bind(duration_ms)
        .bind(bit_rate)
        .bind(file_size_bytes)
        .bind(new_asset.sha256)
        .bind(new_asset.local_work_path)
        .bind(new_asset.storage_state.as_str())
        .fetch_one(&self.pool)
        .await?;

        row.into_media_asset()
    }

    pub async fn find_media_asset(
        &self,
        id: Uuid,
    ) -> Result<Option<MediaAsset>, LibraryRepositoryError> {
        let row = sqlx::query_as::<_, MediaAssetRow>(
            r#"
            SELECT
                id, content_item_id, role, media_kind, mime_type, container,
                video_codec, audio_codec, width, height, duration_ms, bit_rate,
                file_size_bytes, sha256, local_work_path, storage_state, created_at
            FROM media_assets
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(MediaAssetRow::into_media_asset).transpose()
    }

    pub async fn create_source_record(
        &self,
        new_source: NewSourceRecord,
    ) -> Result<SourceRecord, LibraryRepositoryError> {
        let row = sqlx::query_as::<_, SourceRecordRow>(
            r#"
            INSERT INTO source_records (
                content_item_id, ingest_request_id, source_type, original_url,
                normalized_url, platform, platform_content_id, author_name,
                source_title, source_description, source_published_at, metadata_json
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING
                id, content_item_id, ingest_request_id, source_type, original_url,
                normalized_url, platform, platform_content_id, author_name,
                source_title, source_description, source_published_at,
                retrieved_at, metadata_json
            "#,
        )
        .bind(new_source.content_item_id)
        .bind(new_source.ingest_request_id)
        .bind(new_source.source_type.as_str())
        .bind(new_source.original_url)
        .bind(new_source.normalized_url)
        .bind(new_source.platform)
        .bind(new_source.platform_content_id)
        .bind(new_source.author_name)
        .bind(new_source.source_title)
        .bind(new_source.source_description)
        .bind(new_source.source_published_at)
        .bind(new_source.metadata_json)
        .fetch_one(&self.pool)
        .await?;

        row.into_source_record()
    }

    pub async fn list_source_records(
        &self,
        content_item_id: Uuid,
    ) -> Result<Vec<SourceRecord>, LibraryRepositoryError> {
        let rows = sqlx::query_as::<_, SourceRecordRow>(
            r#"
            SELECT
                id, content_item_id, ingest_request_id, source_type, original_url,
                normalized_url, platform, platform_content_id, author_name,
                source_title, source_description, source_published_at,
                retrieved_at, metadata_json
            FROM source_records
            WHERE content_item_id = $1
            ORDER BY retrieved_at ASC, id ASC
            "#,
        )
        .bind(content_item_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(SourceRecordRow::into_source_record).collect()
    }

    pub async fn upsert_tag(&self, new_tag: NewTag) -> Result<Tag, LibraryRepositoryError> {
        let row = sqlx::query_as::<_, TagRow>(
            r#"
            INSERT INTO tags (normalized_name, display_name)
            VALUES ($1, $2)
            ON CONFLICT (normalized_name) DO UPDATE
            SET display_name = EXCLUDED.display_name
            RETURNING id, normalized_name, display_name, created_at
            "#,
        )
        .bind(new_tag.normalized_name)
        .bind(new_tag.display_name)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.into_tag())
    }

    pub async fn attach_tag(
        &self,
        content_item_id: Uuid,
        tag_id: Uuid,
    ) -> Result<(), LibraryRepositoryError> {
        sqlx::query(
            r#"
            INSERT INTO content_item_tags (content_item_id, tag_id)
            VALUES ($1, $2)
            ON CONFLICT (content_item_id, tag_id) DO NOTHING
            "#,
        )
        .bind(content_item_id)
        .bind(tag_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_tags(
        &self,
        content_item_id: Uuid,
    ) -> Result<Vec<Tag>, LibraryRepositoryError> {
        let rows = sqlx::query_as::<_, TagRow>(
            r#"
            SELECT t.id, t.normalized_name, t.display_name, t.created_at
            FROM tags t
            INNER JOIN content_item_tags cit ON cit.tag_id = t.id
            WHERE cit.content_item_id = $1
            ORDER BY t.normalized_name ASC
            "#,
        )
        .bind(content_item_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(TagRow::into_tag).collect())
    }

    pub async fn create_storage_object(
        &self,
        new_object: NewStorageObject,
    ) -> Result<StorageObject, LibraryRepositoryError> {
        let row = sqlx::query_as::<_, StorageObjectRow>(
            r#"
            INSERT INTO storage_objects (
                asset_id, provider, storage_chat_id, storage_message_id,
                telegram_file_id, telegram_file_unique_id, media_kind
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING
                id, asset_id, provider, storage_chat_id, storage_message_id,
                telegram_file_id, telegram_file_unique_id, media_kind,
                stored_at, verified_at, status
            "#,
        )
        .bind(new_object.asset_id)
        .bind(new_object.provider)
        .bind(new_object.storage_chat_id)
        .bind(new_object.storage_message_id)
        .bind(new_object.telegram_file_id)
        .bind(new_object.telegram_file_unique_id)
        .bind(new_object.media_kind.as_str())
        .fetch_one(&self.pool)
        .await?;

        row.into_storage_object()
    }
}

const SHA256_LENGTH: usize = 32;

fn validate_exact_duplicate_request(
    request: &ExactDuplicateRequest,
) -> Result<(), LibraryRepositoryError> {
    if request.asset.role != AssetRole::Canonical {
        return Err(LibraryRepositoryError::InvalidCanonicalAssetRole);
    }
    let sha256 = request.asset.sha256.as_ref().ok_or(LibraryRepositoryError::MissingSha256)?;
    if sha256.len() != SHA256_LENGTH {
        return Err(LibraryRepositoryError::InvalidSha256Length { actual: sha256.len() });
    }
    if request.source.platform.is_some() != request.source.platform_content_id.is_some() {
        return Err(LibraryRepositoryError::InvalidSourceIdentity);
    }
    Ok(())
}

async fn insert_content_item_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    new_item: &NewContentItem,
) -> Result<ContentItemRow, LibraryRepositoryError> {
    Ok(sqlx::query_as::<_, ContentItemRow>(
        r#"
        INSERT INTO content_items (kind, preferred_title, editorial_description, notes)
        VALUES ($1, $2, $3, $4)
        RETURNING
            id, kind, status, canonical_asset_id, preferred_title,
            editorial_description, notes, created_at, updated_at, archived_at
        "#,
    )
    .bind(new_item.kind.as_str())
    .bind(new_item.preferred_title.as_deref())
    .bind(new_item.editorial_description.as_deref())
    .bind(new_item.notes.as_deref())
    .fetch_one(&mut **transaction)
    .await?)
}

async fn insert_canonical_asset_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    content_item_id: Uuid,
    asset: &sooqa_library::NewMediaAssetDraft,
) -> Result<Option<MediaAssetRow>, LibraryRepositoryError> {
    let duration_ms = to_database_u64(asset.duration_ms, "duration_ms")?;
    let bit_rate = to_database_u64(asset.bit_rate, "bit_rate")?;
    let file_size_bytes = to_database_u64(asset.file_size_bytes, "file_size_bytes")?;

    Ok(sqlx::query_as::<_, MediaAssetRow>(
        r#"
        INSERT INTO media_assets (
            content_item_id, role, media_kind, mime_type, container,
            video_codec, audio_codec, width, height, duration_ms, bit_rate,
            file_size_bytes, sha256, local_work_path, storage_state
        )
        VALUES ($1, 'canonical', $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT DO NOTHING
        RETURNING
            id, content_item_id, role, media_kind, mime_type, container,
            video_codec, audio_codec, width, height, duration_ms, bit_rate,
            file_size_bytes, sha256, local_work_path, storage_state, created_at
        "#,
    )
    .bind(content_item_id)
    .bind(asset.media_kind.as_str())
    .bind(asset.mime_type.as_deref())
    .bind(asset.container.as_deref())
    .bind(asset.video_codec.as_deref())
    .bind(asset.audio_codec.as_deref())
    .bind(asset.width)
    .bind(asset.height)
    .bind(duration_ms)
    .bind(bit_rate)
    .bind(file_size_bytes)
    .bind(asset.sha256.clone())
    .bind(asset.local_work_path.as_deref())
    .bind(asset.storage_state.as_str())
    .fetch_optional(&mut **transaction)
    .await?)
}

async fn set_canonical_asset_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    content_item_id: Uuid,
    asset_id: Uuid,
) -> Result<ContentItemRow, LibraryRepositoryError> {
    Ok(sqlx::query_as::<_, ContentItemRow>(
        r#"
        UPDATE content_items
        SET canonical_asset_id = $2, updated_at = now()
        WHERE id = $1
        RETURNING
            id, kind, status, canonical_asset_id, preferred_title,
            editorial_description, notes, created_at, updated_at, archived_at
        "#,
    )
    .bind(content_item_id)
    .bind(asset_id)
    .fetch_one(&mut **transaction)
    .await?)
}

async fn find_source_by_identity(
    transaction: &mut Transaction<'_, Postgres>,
    source: &NewSourceRecordDraft,
) -> Result<Option<SourceRecord>, LibraryRepositoryError> {
    let row = sqlx::query_as::<_, SourceRecordRow>(
        r#"
        SELECT
            id, content_item_id, ingest_request_id, source_type, original_url,
            normalized_url, platform, platform_content_id, author_name,
            source_title, source_description, source_published_at,
            retrieved_at, metadata_json
        FROM source_records
        WHERE ($1::text IS NOT NULL AND normalized_url = $1)
           OR ($2::text IS NOT NULL AND platform = $2 AND platform_content_id = $3)
        ORDER BY retrieved_at ASC, id ASC
        LIMIT 1
        "#,
    )
    .bind(source.normalized_url.as_deref())
    .bind(source.platform.as_deref())
    .bind(source.platform_content_id.as_deref())
    .fetch_optional(&mut **transaction)
    .await?;

    row.map(SourceRecordRow::into_source_record).transpose()
}

async fn find_media_asset_by_sha256(
    transaction: &mut Transaction<'_, Postgres>,
    sha256: &[u8],
) -> Result<Option<MediaAssetRow>, LibraryRepositoryError> {
    Ok(sqlx::query_as::<_, MediaAssetRow>(
        r#"
        SELECT
            id, content_item_id, role, media_kind, mime_type, container,
            video_codec, audio_codec, width, height, duration_ms, bit_rate,
            file_size_bytes, sha256, local_work_path, storage_state, created_at
        FROM media_assets
        WHERE sha256 = $1
        ORDER BY (role = 'canonical') DESC, created_at ASC, id ASC
        LIMIT 1
        "#,
    )
    .bind(sha256)
    .fetch_optional(&mut **transaction)
    .await?)
}

async fn find_media_asset_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    asset_id: Uuid,
) -> Result<Option<MediaAssetRow>, LibraryRepositoryError> {
    Ok(sqlx::query_as::<_, MediaAssetRow>(
        r#"
        SELECT
            id, content_item_id, role, media_kind, mime_type, container,
            video_codec, audio_codec, width, height, duration_ms, bit_rate,
            file_size_bytes, sha256, local_work_path, storage_state, created_at
        FROM media_assets
        WHERE id = $1
        "#,
    )
    .bind(asset_id)
    .fetch_optional(&mut **transaction)
    .await?)
}

async fn find_canonical_asset_for_content(
    transaction: &mut Transaction<'_, Postgres>,
    content_item_id: Uuid,
) -> Result<Option<MediaAssetRow>, LibraryRepositoryError> {
    Ok(sqlx::query_as::<_, MediaAssetRow>(
        r#"
        SELECT
            id, content_item_id, role, media_kind, mime_type, container,
            video_codec, audio_codec, width, height, duration_ms, bit_rate,
            file_size_bytes, sha256, local_work_path, storage_state, created_at
        FROM media_assets
        WHERE content_item_id = $1 AND role = 'canonical'
        ORDER BY created_at ASC, id ASC
        LIMIT 1
        "#,
    )
    .bind(content_item_id)
    .fetch_optional(&mut **transaction)
    .await?)
}

async fn find_any_asset_for_content(
    transaction: &mut Transaction<'_, Postgres>,
    content_item_id: Uuid,
) -> Result<Option<MediaAssetRow>, LibraryRepositoryError> {
    Ok(sqlx::query_as::<_, MediaAssetRow>(
        r#"
        SELECT
            id, content_item_id, role, media_kind, mime_type, container,
            video_codec, audio_codec, width, height, duration_ms, bit_rate,
            file_size_bytes, sha256, local_work_path, storage_state, created_at
        FROM media_assets
        WHERE content_item_id = $1
        ORDER BY created_at ASC, id ASC
        LIMIT 1
        "#,
    )
    .bind(content_item_id)
    .fetch_optional(&mut **transaction)
    .await?)
}

async fn insert_source_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    content_item_id: Uuid,
    source: &NewSourceRecordDraft,
) -> Result<Option<SourceRecord>, LibraryRepositoryError> {
    let source = source.clone().for_content_item(content_item_id);
    let row = sqlx::query_as::<_, SourceRecordRow>(
        r#"
        INSERT INTO source_records (
            content_item_id, ingest_request_id, source_type, original_url,
            normalized_url, platform, platform_content_id, author_name,
            source_title, source_description, source_published_at, metadata_json
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT DO NOTHING
        RETURNING
            id, content_item_id, ingest_request_id, source_type, original_url,
            normalized_url, platform, platform_content_id, author_name,
            source_title, source_description, source_published_at,
            retrieved_at, metadata_json
        "#,
    )
    .bind(source.content_item_id)
    .bind(source.ingest_request_id)
    .bind(source.source_type.as_str())
    .bind(source.original_url)
    .bind(source.normalized_url)
    .bind(source.platform)
    .bind(source.platform_content_id)
    .bind(source.author_name)
    .bind(source.source_title)
    .bind(source.source_description)
    .bind(source.source_published_at)
    .bind(source.metadata_json)
    .fetch_optional(&mut **transaction)
    .await?;

    row.map(SourceRecordRow::into_source_record).transpose()
}

async fn insert_source_or_find_existing(
    transaction: &mut Transaction<'_, Postgres>,
    content_item_id: Uuid,
    source: &NewSourceRecordDraft,
) -> Result<(SourceRecord, bool), LibraryRepositoryError> {
    if let Some(source_record) =
        insert_source_in_transaction(transaction, content_item_id, source).await?
    {
        return Ok((source_record, true));
    }

    let source_record = find_source_by_identity(transaction, source).await?.ok_or(
        LibraryRepositoryError::Invariant("source conflict had no matching source record"),
    )?;
    Ok((source_record, false))
}

async fn load_resolution_for_content(
    transaction: &mut Transaction<'_, Postgres>,
    content_item_id: Uuid,
    fallback_asset: Option<MediaAssetRow>,
) -> Result<(ContentItem, MediaAsset), LibraryRepositoryError> {
    let content_row = sqlx::query_as::<_, ContentItemRow>(
        r#"
        SELECT
            id, kind, status, canonical_asset_id, preferred_title,
            editorial_description, notes, created_at, updated_at, archived_at
        FROM content_items
        WHERE id = $1
        "#,
    )
    .bind(content_item_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(LibraryRepositoryError::Invariant(
        "source or asset referred to a missing content item",
    ))?;
    let content_item = content_row.into_content_item()?;

    let asset_row = if let Some(canonical_asset_id) = content_item.canonical_asset_id {
        find_media_asset_in_transaction(transaction, canonical_asset_id).await?.ok_or(
            LibraryRepositoryError::Invariant("content item referred to a missing canonical asset"),
        )?
    } else if let Some(asset_row) =
        find_canonical_asset_for_content(transaction, content_item.id).await?
    {
        asset_row
    } else if let Some(asset_row) = fallback_asset {
        asset_row
    } else {
        find_any_asset_for_content(transaction, content_item.id)
            .await?
            .ok_or(LibraryRepositoryError::Invariant("duplicate content item had no asset"))?
    };

    Ok((content_item, asset_row.into_media_asset()?))
}

async fn delete_uncommitted_content(
    transaction: &mut Transaction<'_, Postgres>,
    content_item_id: Uuid,
) -> Result<(), LibraryRepositoryError> {
    sqlx::query("UPDATE content_items SET canonical_asset_id = NULL WHERE id = $1")
        .bind(content_item_id)
        .execute(&mut **transaction)
        .await?;
    sqlx::query("DELETE FROM media_assets WHERE content_item_id = $1")
        .bind(content_item_id)
        .execute(&mut **transaction)
        .await?;
    sqlx::query("DELETE FROM content_items WHERE id = $1")
        .bind(content_item_id)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum LibraryRepositoryError {
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("database returned invalid {field} value {value:?}")]
    InvalidEnum { field: &'static str, value: String },
    #[error("database returned an invalid non-negative {field} value")]
    InvalidNumber { field: &'static str },
    #[error("exact duplicate resolution requires a canonical asset")]
    InvalidCanonicalAssetRole,
    #[error("exact duplicate resolution requires a SHA-256 digest")]
    MissingSha256,
    #[error("SHA-256 digest must contain exactly 32 bytes, got {actual}")]
    InvalidSha256Length { actual: usize },
    #[error("platform and platform content ID must be supplied together")]
    InvalidSourceIdentity,
    #[error("database invariant violated: {0}")]
    Invariant(&'static str),
}

#[derive(Debug, FromRow)]
struct ContentItemRow {
    id: Uuid,
    kind: String,
    status: String,
    canonical_asset_id: Option<Uuid>,
    preferred_title: Option<String>,
    editorial_description: Option<String>,
    notes: Option<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    archived_at: Option<OffsetDateTime>,
}

impl ContentItemRow {
    fn into_content_item(self) -> Result<ContentItem, LibraryRepositoryError> {
        Ok(ContentItem {
            id: self.id,
            kind: parse_enum("content_items.kind", &self.kind)?,
            status: parse_enum("content_items.status", &self.status)?,
            canonical_asset_id: self.canonical_asset_id,
            preferred_title: self.preferred_title,
            editorial_description: self.editorial_description,
            notes: self.notes,
            created_at: self.created_at,
            updated_at: self.updated_at,
            archived_at: self.archived_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct MediaAssetRow {
    id: Uuid,
    content_item_id: Uuid,
    role: String,
    media_kind: String,
    mime_type: Option<String>,
    container: Option<String>,
    video_codec: Option<String>,
    audio_codec: Option<String>,
    width: Option<i32>,
    height: Option<i32>,
    duration_ms: Option<i64>,
    bit_rate: Option<i64>,
    file_size_bytes: Option<i64>,
    sha256: Option<Vec<u8>>,
    local_work_path: Option<String>,
    storage_state: String,
    created_at: OffsetDateTime,
}

impl MediaAssetRow {
    fn into_media_asset(self) -> Result<MediaAsset, LibraryRepositoryError> {
        Ok(MediaAsset {
            id: self.id,
            content_item_id: self.content_item_id,
            role: parse_enum("media_assets.role", &self.role)?,
            media_kind: parse_enum("media_assets.media_kind", &self.media_kind)?,
            mime_type: self.mime_type,
            container: self.container,
            video_codec: self.video_codec,
            audio_codec: self.audio_codec,
            width: self.width,
            height: self.height,
            duration_ms: from_database_i64(self.duration_ms, "duration_ms")?,
            bit_rate: from_database_i64(self.bit_rate, "bit_rate")?,
            file_size_bytes: from_database_i64(self.file_size_bytes, "file_size_bytes")?,
            sha256: self.sha256,
            local_work_path: self.local_work_path,
            storage_state: parse_enum("media_assets.storage_state", &self.storage_state)?,
            created_at: self.created_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct SourceRecordRow {
    id: Uuid,
    content_item_id: Uuid,
    ingest_request_id: Option<Uuid>,
    source_type: String,
    original_url: Option<String>,
    normalized_url: Option<String>,
    platform: Option<String>,
    platform_content_id: Option<String>,
    author_name: Option<String>,
    source_title: Option<String>,
    source_description: Option<String>,
    source_published_at: Option<OffsetDateTime>,
    retrieved_at: OffsetDateTime,
    metadata_json: Value,
}

impl SourceRecordRow {
    fn into_source_record(self) -> Result<SourceRecord, LibraryRepositoryError> {
        Ok(SourceRecord {
            id: self.id,
            content_item_id: self.content_item_id,
            ingest_request_id: self.ingest_request_id,
            source_type: parse_enum("source_records.source_type", &self.source_type)?,
            original_url: self.original_url,
            normalized_url: self.normalized_url,
            platform: self.platform,
            platform_content_id: self.platform_content_id,
            author_name: self.author_name,
            source_title: self.source_title,
            source_description: self.source_description,
            source_published_at: self.source_published_at,
            retrieved_at: self.retrieved_at,
            metadata_json: self.metadata_json,
        })
    }
}

#[derive(Debug, FromRow)]
struct TagRow {
    id: Uuid,
    normalized_name: String,
    display_name: String,
    created_at: OffsetDateTime,
}

impl TagRow {
    fn into_tag(self) -> Tag {
        Tag {
            id: self.id,
            normalized_name: self.normalized_name,
            display_name: self.display_name,
            created_at: self.created_at,
        }
    }
}

#[derive(Debug, FromRow)]
struct StorageObjectRow {
    id: Uuid,
    asset_id: Uuid,
    provider: String,
    storage_chat_id: i64,
    storage_message_id: i64,
    telegram_file_id: Option<String>,
    telegram_file_unique_id: Option<String>,
    media_kind: String,
    stored_at: OffsetDateTime,
    verified_at: Option<OffsetDateTime>,
    status: String,
}

impl StorageObjectRow {
    fn into_storage_object(self) -> Result<StorageObject, LibraryRepositoryError> {
        Ok(StorageObject {
            id: self.id,
            asset_id: self.asset_id,
            provider: self.provider,
            storage_chat_id: self.storage_chat_id,
            storage_message_id: self.storage_message_id,
            telegram_file_id: self.telegram_file_id,
            telegram_file_unique_id: self.telegram_file_unique_id,
            media_kind: parse_enum("storage_objects.media_kind", &self.media_kind)?,
            stored_at: self.stored_at,
            verified_at: self.verified_at,
            status: parse_enum("storage_objects.status", &self.status)?,
        })
    }
}

fn parse_enum<T>(field: &'static str, value: &str) -> Result<T, LibraryRepositoryError>
where
    T: for<'value> TryFrom<&'value str, Error = String>,
{
    T::try_from(value).map_err(|value| LibraryRepositoryError::InvalidEnum { field, value })
}

fn to_database_u64(
    value: Option<u64>,
    field: &'static str,
) -> Result<Option<i64>, LibraryRepositoryError> {
    value
        .map(|value| {
            i64::try_from(value).map_err(|_| LibraryRepositoryError::InvalidNumber { field })
        })
        .transpose()
}

fn from_database_i64(
    value: Option<i64>,
    field: &'static str,
) -> Result<Option<u64>, LibraryRepositoryError> {
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| LibraryRepositoryError::InvalidNumber { field })
        })
        .transpose()
}
