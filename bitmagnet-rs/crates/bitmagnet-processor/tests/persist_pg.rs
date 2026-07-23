//! Live-PostgreSQL gate for the Lane P transaction kernel.
//!
//! Set `BITMAGNET_PROCESSOR_TEST_DATABASE_URL` to a disposable goose-26
//! database. The test truncates processor-owned rows and must never target a
//! production database.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bitmagnet_processor::{
    persist_write_set, BlockingManager, BoxError, TorrentContentPersistence, TorrentContentWrite,
    WriteSet,
};
use futures::future::BoxFuture;
use sqlx::{PgPool, Row};

const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const HASH_C: &str = "cccccccccccccccccccccccccccccccccccccccc";

struct RecordingBlocker {
    pool: PgPool,
    calls: Arc<Mutex<Vec<Vec<String>>>>,
    observed_before_delete: Arc<Mutex<bool>>,
}

impl BlockingManager for RecordingBlocker {
    fn block<'a>(&'a self, info_hashes: &'a [String]) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("blocker calls mutex")
                .push(info_hashes.to_vec());
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM torrents WHERE info_hash = decode($1, 'hex')",
            )
            .bind(HASH_C)
            .fetch_one(&self.pool)
            .await?;
            *self
                .observed_before_delete
                .lock()
                .expect("blocker observation mutex") = count == 1;
            Ok(())
        })
    }
}

fn torrent_content(info_hash: &str, content_type: &str, languages: &[&str]) -> TorrentContentWrite {
    TorrentContentWrite {
        id: format!("{info_hash}:{content_type}:?:?"),
        info_hash: info_hash.to_owned(),
        content_type: Some(content_type.to_owned()),
        content_source: None,
        content_id: None,
        languages: languages.iter().map(|value| (*value).to_owned()).collect(),
        episodes: "S01E01-02".to_owned(),
        video_resolution: Some("V1080p".to_owned()),
        video_source: Some("BluRay".to_owned()),
        video_codec: Some("x265".to_owned()),
        video_3d: None,
        video_modifier: None,
        release_group: Some("group".to_owned()),
        size: 42,
        files_count: Some(2),
    }
}

fn metadata(rows: &[TorrentContentWrite]) -> BTreeMap<String, TorrentContentPersistence> {
    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            (
                row.id.clone(),
                TorrentContentPersistence {
                    seeders: Some(10 + index as u64),
                    leechers: Some(2 + index as u64),
                    published_at_micros: 1_700_000_000_123_456 + index as i64,
                    tsv: "'fixture':1A".to_owned(),
                },
            )
        })
        .collect()
}

async fn seed_torrent(pool: &PgPool, info_hash: &str, name: &str) {
    sqlx::query(
        "INSERT INTO torrents \
         (info_hash, name, size, private, created_at, updated_at) \
         VALUES (decode($1, 'hex'), $2, 42, false, NOW(), NOW())",
    )
    .bind(info_hash)
    .bind(name)
    .execute(pool)
    .await
    .expect("seed torrent");
}

#[tokio::test]
async fn transaction_order_upserts_tags_deletes_and_rolls_back() {
    let Ok(database_url) = std::env::var("BITMAGNET_PROCESSOR_TEST_DATABASE_URL") else {
        eprintln!("skipping: BITMAGNET_PROCESSOR_TEST_DATABASE_URL is not set");
        return;
    };
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect disposable PostgreSQL");

    sqlx::query("TRUNCATE torrent_tags, torrent_contents, torrents CASCADE")
        .execute(&pool)
        .await
        .expect("reset processor tables");
    seed_torrent(&pool, HASH_A, "A").await;
    seed_torrent(&pool, HASH_B, "B").await;
    seed_torrent(&pool, HASH_C, "C").await;

    sqlx::query(
        "INSERT INTO torrent_contents \
         (info_hash, content_type, languages, episodes, created_at, updated_at, tsv, \
          published_at, size) \
         VALUES \
         (decode($1, 'hex'), NULL, '[]'::jsonb, '{}'::jsonb, NOW(), NOW(), \
          ''::tsvector, NOW(), 1), \
         (decode($2, 'hex'), 'music', '[]'::jsonb, '{}'::jsonb, NOW(), NOW(), \
          ''::tsvector, NOW(), 1)",
    )
    .bind(HASH_A)
    .bind(HASH_B)
    .execute(&pool)
    .await
    .expect("seed existing contents");

    let rows = vec![
        torrent_content(HASH_A, "movie", &["en"]),
        torrent_content(HASH_B, "music", &["fr", "en"]),
    ];
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed_before_delete = Arc::new(Mutex::new(false));
    let blocker = RecordingBlocker {
        pool: pool.clone(),
        calls: Arc::clone(&calls),
        observed_before_delete: Arc::clone(&observed_before_delete),
    };
    let write_set = WriteSet {
        torrent_contents: rows.clone(),
        delete_ids: vec![format!("{HASH_A}:?:?:?")],
        delete_info_hashes: vec![HASH_C.to_owned()],
        add_tags: BTreeMap::from([(
            HASH_A.to_owned(),
            vec!["trusted".to_owned(), "trusted".to_owned()],
        )]),
        ..WriteSet::default()
    };

    persist_write_set(&pool, &write_set, &metadata(&rows), &blocker)
        .await
        .expect("persist supported write set");

    assert_eq!(
        calls.lock().expect("calls mutex").as_slice(),
        &[vec![HASH_C.to_owned()]]
    );
    assert!(*observed_before_delete.lock().expect("observation mutex"));

    let live = sqlx::query(
        "SELECT id, languages, episodes, seeders, leechers, \
         (EXTRACT(EPOCH FROM published_at) * 1000000)::bigint AS published_at_micros, \
         size, files_count, tsv::text AS tsv \
         FROM torrent_contents ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("read persisted contents");
    assert_eq!(live.len(), 2);
    assert_eq!(live[0].try_get::<String, _>("id").unwrap(), rows[0].id);
    assert_eq!(
        live[0].try_get::<serde_json::Value, _>("episodes").unwrap(),
        serde_json::json!({"1": {"1": {}, "2": {}}})
    );
    assert_eq!(live[0].try_get::<i32, _>("seeders").unwrap(), 10);
    assert_eq!(
        live[0].try_get::<i64, _>("published_at_micros").unwrap(),
        1_700_000_000_123_456
    );
    assert_eq!(live[0].try_get::<String, _>("tsv").unwrap(), "'fixture':1A");
    assert_eq!(live[1].try_get::<i64, _>("size").unwrap(), 42);
    assert_eq!(live[1].try_get::<i32, _>("files_count").unwrap(), 2);

    let tag_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM torrent_tags WHERE name = 'trusted'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(tag_count, 1);
    let deleted_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM torrents WHERE info_hash = decode($1, 'hex')")
            .bind(HASH_C)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(deleted_count, 0);

    sqlx::query(
        "CREATE OR REPLACE FUNCTION reject_processor_test_tag() RETURNS trigger \
         LANGUAGE plpgsql AS $$ BEGIN \
         IF NEW.name = 'force-rollback' THEN RAISE EXCEPTION 'forced rollback'; END IF; \
         RETURN NEW; END $$",
    )
    .execute(&pool)
    .await
    .expect("install rollback-test trigger function");
    sqlx::query("DROP TRIGGER IF EXISTS reject_processor_test_tag ON torrent_tags")
        .execute(&pool)
        .await
        .expect("drop prior rollback-test trigger");
    sqlx::query(
        "CREATE TRIGGER reject_processor_test_tag BEFORE INSERT ON torrent_tags \
         FOR EACH ROW EXECUTE FUNCTION reject_processor_test_tag()",
    )
    .execute(&pool)
    .await
    .expect("install rollback-test trigger");

    let rollback_row = torrent_content(HASH_A, "ebook", &[]);
    let rollback = WriteSet {
        torrent_contents: vec![rollback_row.clone()],
        delete_ids: vec![rows[0].id.clone()],
        add_tags: BTreeMap::from([(HASH_A.to_owned(), vec!["force-rollback".to_owned()])]),
        ..WriteSet::default()
    };
    let error = persist_write_set(
        &pool,
        &rollback,
        &metadata(std::slice::from_ref(&rollback_row)),
        &blocker,
    )
    .await
    .expect_err("invalid tag must roll back the transaction");
    assert!(error.to_string().contains("database"));

    let surviving_ids: Vec<String> =
        sqlx::query_scalar("SELECT id FROM torrent_contents ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        surviving_ids,
        vec![rows[0].id.clone(), rows[1].id.clone()],
        "the stale delete and preceding upsert must roll back with the tag failure"
    );
}
