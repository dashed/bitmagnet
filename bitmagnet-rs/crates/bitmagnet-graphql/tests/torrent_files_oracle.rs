//! Replays the committed Go `torrent.files` oracle through the Rust schema.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_graphql::{EmptySubscription, Request, Variables};
use async_trait::async_trait;
use bitmagnet_graphql::schema::{Mutation, Query};
use bitmagnet_graphql::{
    TorrentFilesBlob, TorrentFilesError, TorrentFilesLimits, TorrentFilesRuntime,
    TorrentFilesRuntimeData,
};
use bitmagnet_model::{serialize_files, BlobFile, InfoHash};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixture {
    info_hash: InfoHash,
    files: Vec<FixtureFile>,
}

#[derive(Debug, Deserialize)]
struct FixtureFile {
    index: u32,
    path: String,
    extension: String,
    size: u64,
}

#[derive(Debug, Deserialize)]
struct OracleCase {
    id: String,
    input: Value,
    expected: Value,
}

#[derive(Clone)]
struct FixtureRuntime {
    blobs: Vec<TorrentFilesBlob>,
}

#[async_trait]
impl TorrentFilesRuntime for FixtureRuntime {
    async fn load(
        &self,
        info_hashes: &[InfoHash],
        _limits: TorrentFilesLimits,
    ) -> Result<Vec<TorrentFilesBlob>, TorrentFilesError> {
        let mut blobs = self
            .blobs
            .iter()
            .filter(|blob| info_hashes.contains(&blob.info_hash))
            .cloned()
            .collect::<Vec<_>>();
        // Production SQL explicitly orders blobs by hash. Go's DAO does not
        // contract a cross-hash tie order; the oracle fixture uses this same
        // ascending seed/hash order for equal-key stable sorts.
        blobs.sort_by_key(|blob| blob.info_hash);
        Ok(blobs)
    }
}

#[tokio::test]
async fn rust_schema_matches_go_torrent_files_oracle() {
    let fixtures: Vec<Fixture> = serde_json::from_slice(
        &fs::read(parity_dir().join("fixtures.json")).expect("read torrent.files fixtures"),
    )
    .expect("decode torrent.files fixtures");
    let blobs = fixtures
        .into_iter()
        .map(|fixture| {
            let files = fixture
                .files
                .into_iter()
                .map(|file| BlobFile {
                    index: file.index,
                    path: file.path,
                    extension: file.extension,
                    size: file.size,
                })
                .collect::<Vec<_>>();
            TorrentFilesBlob {
                info_hash: fixture.info_hash,
                file_count: files.len(),
                files_data: Some(serialize_files(&files).expect("serialize fixture blob")),
            }
        })
        .collect();
    let runtime: Arc<dyn TorrentFilesRuntime> = Arc::new(FixtureRuntime { blobs });
    let schema = async_graphql::Schema::build(Query, Mutation, EmptySubscription)
        .data(TorrentFilesRuntimeData::new(runtime))
        .finish();

    for case in load_cases(&parity_dir().join("corpus.jsonl")) {
        let response = schema
            .execute(
                Request::new(
                    "query TorrentFilesParity($input: TorrentFilesQueryInput!) {\
                     torrent { files(input: $input) {\
                     totalCount hasNextPage items {\
                     infoHash index path extension fileType size createdAt updatedAt\
                     } } } }",
                )
                .variables(Variables::from_json(
                    serde_json::json!({ "input": case.input }),
                )),
            )
            .await;
        assert!(
            response.errors.is_empty(),
            "oracle case {:?} returned errors: {:?}",
            case.id,
            response.errors
        );
        let actual = serde_json::to_value(response.data).expect("encode GraphQL response data");
        assert_eq!(
            actual,
            serde_json::json!({ "torrent": { "files": case.expected } }),
            "oracle case {:?}",
            case.id
        );
    }
}

fn load_cases(path: &Path) -> Vec<OracleCase> {
    fs::read_to_string(path)
        .expect("read torrent.files corpus")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("decode torrent.files oracle line"))
        .collect()
}

fn parity_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join("testdata/parity/graphql-torrent-files")
}
