//! A content row's `content_attributes` and collections, in Go's order.
//!
//! Go hydrates every local-search candidate with `ContentDefaultPreload` (the
//! attributes) and `ContentDefaultHydrate` (the collections). Both orders are
//! observable: `Content.UpdateTsv` appends genre collection names and `id`
//! attribute values in slice order, so the order sets the lexeme positions of
//! the `tsv` the processor writes. The order is attributes by `(source, key)`
//! and collections by the collection's `(type, source, id)`, compared under the
//! database collation, so the statements below sort and Rust keeps the row
//! order. Never re-sort these in Rust: the order is the database collation's.
//! Prod and CI run musl `postgres:16-alpine`, which sorts bytewise whatever
//! `datcollate` says; a glibc or ICU database puts `a7` before `B8`.
//!
//! The statements are shared with the processor's writer loader, which hydrates
//! a reused content row the same way.

use std::collections::BTreeMap;

use bitmagnet_classifier::ResolveError;
use bitmagnet_model::{Content, ContentAttribute, ContentCollection};
use sqlx::{PgConnection, Row};

// The association scans use the sentinel pattern: one row past each
// per-content and statement-wide ceiling proves that an apparently hydrated
// content image was not silently truncated.

/// Attributes of `UNNEST($1, $2, $3)`'s content keys, at most `$4` rows each and
/// `$5` in total, ordered by content key, then `(source, key)`.
pub const CONTENT_ATTRIBUTES_SQL: &str = "\
SELECT r.content_type, r.content_source, r.content_id, \
       a.source AS attribute_source, a.key AS attribute_key, a.value \
FROM UNNEST($1::text[], $2::text[], $3::text[]) \
     AS r(content_type, content_source, content_id) \
CROSS JOIN LATERAL ( \
    SELECT source, key, value \
    FROM content_attributes \
    WHERE content_type = r.content_type \
      AND content_source = r.content_source \
      AND content_id = r.content_id \
    LIMIT $4 \
) AS a \
ORDER BY r.content_type, r.content_source, r.content_id, a.source, a.key \
LIMIT $5";

/// Collection links of `UNNEST($1, $2, $3)`'s content keys, at most `$4` rows
/// each and `$5` in total, ordered by content key, then collection key.
pub const CONTENT_COLLECTION_LINKS_SQL: &str = "\
SELECT r.content_type, r.content_source, r.content_id, \
       l.content_collection_type, l.content_collection_source, \
       l.content_collection_id \
FROM UNNEST($1::text[], $2::text[], $3::text[]) \
     AS r(content_type, content_source, content_id) \
CROSS JOIN LATERAL ( \
    SELECT content_collection_type, content_collection_source, \
           content_collection_id \
    FROM content_collections_content \
    WHERE content_type = r.content_type \
      AND content_source = r.content_source \
      AND content_id = r.content_id \
    LIMIT $4 \
) AS l \
ORDER BY r.content_type, r.content_source, r.content_id, \
         l.content_collection_type, l.content_collection_source, \
         l.content_collection_id \
LIMIT $5";

/// The `content_collections` rows of `UNNEST($1, $2, $3)`'s collection keys.
pub const CONTENT_COLLECTIONS_SQL: &str = "\
SELECT type AS collection_type, source, id, name \
FROM content_collections \
WHERE (type, source, id) \
      IN (SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[])) \
ORDER BY type, source, id";

/// Per-content ceilings, the writer loader's. A candidate past either is a
/// lookup failure, never a truncated image.
const MAX_ATTRIBUTES_PER_CONTENT: usize = 1_024;
const MAX_COLLECTION_LINKS_PER_CONTENT: usize = 1_024;

type Key = (String, String, String);

/// Fill each content's `attributes` and `collections` from `connection`.
///
/// `contents` must hold distinct rows with empty associations, as a
/// `content_from_row` decode leaves them.
pub(crate) async fn hydrate_associations(
    connection: &mut PgConnection,
    contents: &mut [Content],
) -> Result<(), ResolveError> {
    if contents.is_empty() {
        return Ok(());
    }
    let mut index = BTreeMap::new();
    for (position, content) in contents.iter().enumerate() {
        if index.insert(content_key(content), position).is_some() {
            return Err(hydration_error(format!(
                "duplicate candidate {}",
                render(&content_key(content))
            )));
        }
    }
    let keys: Vec<&Key> = index.keys().collect();
    let types: Vec<&str> = keys.iter().map(|key| key.0.as_str()).collect();
    let sources: Vec<&str> = keys.iter().map(|key| key.1.as_str()).collect();
    let ids: Vec<&str> = keys.iter().map(|key| key.2.as_str()).collect();

    let rows = sqlx::query(CONTENT_ATTRIBUTES_SQL)
        .bind(&types)
        .bind(&sources)
        .bind(&ids)
        .bind(limit(MAX_ATTRIBUTES_PER_CONTENT + 1))
        .bind(limit(keys.len() * MAX_ATTRIBUTES_PER_CONTENT + 1))
        .fetch_all(&mut *connection)
        .await
        .map_err(database_error)?;
    let mut attributes = vec![Vec::new(); contents.len()];
    for row in &rows {
        let position = position(&index, row)?;
        let content = &contents[position];
        let into = &mut attributes[position];
        into.push(ContentAttribute {
            content_type: content.content_type,
            content_source: content.source.clone(),
            content_id: content.id.clone(),
            source: row.try_get("attribute_source").map_err(database_error)?,
            key: row.try_get("attribute_key").map_err(database_error)?,
            value: row.try_get("value").map_err(database_error)?,
        });
        bound(
            content,
            "attributes",
            into.len(),
            MAX_ATTRIBUTES_PER_CONTENT,
        )?;
    }

    let rows = sqlx::query(CONTENT_COLLECTION_LINKS_SQL)
        .bind(&types)
        .bind(&sources)
        .bind(&ids)
        .bind(limit(MAX_COLLECTION_LINKS_PER_CONTENT + 1))
        .bind(limit(keys.len() * MAX_COLLECTION_LINKS_PER_CONTENT + 1))
        .fetch_all(&mut *connection)
        .await
        .map_err(database_error)?;
    let mut links: Vec<Vec<Key>> = vec![Vec::new(); contents.len()];
    for row in &rows {
        let position = position(&index, row)?;
        let into = &mut links[position];
        into.push((
            row.try_get("content_collection_type")
                .map_err(database_error)?,
            row.try_get("content_collection_source")
                .map_err(database_error)?,
            row.try_get("content_collection_id")
                .map_err(database_error)?,
        ));
        bound(
            &contents[position],
            "collection links",
            into.len(),
            MAX_COLLECTION_LINKS_PER_CONTENT,
        )?;
    }

    let collection_keys: Vec<&Key> = links
        .iter()
        .flatten()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut collections = BTreeMap::new();
    if !collection_keys.is_empty() {
        let types: Vec<&str> = collection_keys.iter().map(|key| key.0.as_str()).collect();
        let sources: Vec<&str> = collection_keys.iter().map(|key| key.1.as_str()).collect();
        let ids: Vec<&str> = collection_keys.iter().map(|key| key.2.as_str()).collect();
        let rows = sqlx::query(CONTENT_COLLECTIONS_SQL)
            .bind(&types)
            .bind(&sources)
            .bind(&ids)
            .fetch_all(&mut *connection)
            .await
            .map_err(database_error)?;
        for row in &rows {
            let collection = ContentCollection {
                collection_type: row.try_get("collection_type").map_err(database_error)?,
                source: row.try_get("source").map_err(database_error)?,
                id: row.try_get("id").map_err(database_error)?,
                name: row.try_get("name").map_err(database_error)?,
            };
            let key = (
                collection.collection_type.clone(),
                collection.source.clone(),
                collection.id.clone(),
            );
            if collections.insert(key, collection).is_some() {
                return Err(hydration_error("duplicate content collection row".into()));
            }
        }
    }

    for ((content, attributes), links) in contents.iter_mut().zip(attributes).zip(links) {
        content.attributes = attributes;
        content.collections = links
            .iter()
            .map(|key| {
                collections.get(key).cloned().ok_or_else(|| {
                    hydration_error(format!(
                        "{} links missing collection {}",
                        render(&content_key(content)),
                        render(key)
                    ))
                })
            })
            .collect::<Result<_, _>>()?;
    }
    Ok(())
}

fn content_key(content: &Content) -> Key {
    (
        content.content_type.as_str().to_owned(),
        content.source.clone(),
        content.id.clone(),
    )
}

fn position(
    index: &BTreeMap<Key, usize>,
    row: &sqlx::postgres::PgRow,
) -> Result<usize, ResolveError> {
    let key: Key = (
        row.try_get("content_type").map_err(database_error)?,
        row.try_get("content_source").map_err(database_error)?,
        row.try_get("content_id").map_err(database_error)?,
    );
    index
        .get(&key)
        .copied()
        .ok_or_else(|| hydration_error(format!("unrequested content {}", render(&key))))
}

/// The statement-wide sentinel needs no check of its own: it can only be
/// reached through a content past its per-content ceiling.
fn bound(
    content: &Content,
    association: &str,
    count: usize,
    ceiling: usize,
) -> Result<(), ResolveError> {
    if count > ceiling {
        return Err(hydration_error(format!(
            "{} has more than {ceiling} {association}",
            render(&content_key(content))
        )));
    }
    Ok(())
}

fn limit(rows: usize) -> i64 {
    i64::try_from(rows).expect("bounded association limit fits i64")
}

fn render(key: &Key) -> String {
    format!("{}:{}:{}", key.0, key.1, key.2)
}

fn hydration_error(detail: String) -> ResolveError {
    ResolveError::LocalSearch(format!("content association hydration: {detail}"))
}

fn database_error(error: sqlx::Error) -> ResolveError {
    ResolveError::LocalSearch(error.to_string())
}
