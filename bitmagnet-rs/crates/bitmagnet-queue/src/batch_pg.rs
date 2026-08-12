//! Read-only PostgreSQL adapter for `process_torrent_batch` page selection.

use sqlx::Row;

use crate::{BatchSelection, ProtocolId, QueuePgError, QueueStore};

impl QueueStore {
    /// Select one page from the live Go schema without locks or writes.
    pub async fn select_process_torrent_batch_page(
        &self,
        selection: &BatchSelection,
    ) -> Result<Vec<ProtocolId>, QueuePgError> {
        if selection.order_by != "info_hash_asc" {
            return Err(QueuePgError::InvalidBatchSelection(
                "order_by must be info_hash_asc",
            ));
        }
        if selection.limit == 0 {
            return Err(QueuePgError::InvalidBatchSelection(
                "limit must be positive",
            ));
        }
        let limit = i64::try_from(selection.limit).map_err(|_| {
            QueuePgError::InvalidBatchSelection("limit does not fit PostgreSQL bigint")
        })?;
        let updated_before = selection
            .updated_before
            .parsed()
            .map_err(|_| QueuePgError::InvalidBatchSelection("updated_before is not RFC3339"))?;
        let apply_content_filter = !selection.content_types.is_empty();
        let include_null = selection.content_types.iter().any(Option::is_none);
        let content_types = selection
            .content_types
            .iter()
            .filter_map(Clone::clone)
            .collect::<Vec<_>>();

        let rows = sqlx::query(
            "SELECT torrent.info_hash \
             FROM torrents AS torrent \
             WHERE torrent.info_hash > $1 \
               AND torrent.updated_at < $2 \
               AND (NOT $3::boolean OR EXISTS ( \
                 SELECT 1 FROM torrent_contents AS content \
                 WHERE content.info_hash = torrent.info_hash \
                   AND (content.content_type = ANY($4::text[]) \
                        OR ($5::boolean AND content.content_type IS NULL)) \
               )) \
               AND (NOT $6::boolean OR NOT EXISTS ( \
                 SELECT 1 FROM torrent_contents AS orphan_content \
                 WHERE orphan_content.info_hash = torrent.info_hash \
               )) \
             ORDER BY torrent.info_hash ASC \
             LIMIT $7",
        )
        .bind(selection.after_exclusive.as_bytes().as_slice())
        .bind(updated_before)
        .bind(apply_content_filter)
        .bind(content_types)
        .bind(include_null)
        .bind(selection.orphans)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;

        rows.into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row.try_get("info_hash")?;
                ProtocolId::try_from(bytes.as_slice())
                    .map_err(|_| QueuePgError::InvalidInfoHashLength(bytes.len()))
            })
            .collect()
    }
}
