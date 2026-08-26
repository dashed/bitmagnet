use std::collections::BTreeMap;

use bitmagnet_metainfo::parse_info_bytes;
use serde::Deserialize;
use sha2::{Digest, Sha256};

const FIXTURE_TEXT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/dht/dht_crawler_persist_torrents.jsonl"
));
const FIXTURE_BYTES: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../testdata/parity/dht/dht_crawler_persist_torrents.jsonl"
));
const FIXTURE_SHA256: &str = "40adced4a96a860354d8ba74c412566e2a72979261bd674994c4ef18d6680bc5";

const FIXTURE_IDS: [&str; 6] = [
    "production_source_factory_batcher_lifecycle_lookup_dedup_transaction_and_fanout_contract",
    "v1_single_default_projection_from_verified_raw_info",
    "v1_threshold_n_and_n_plus_one_save_pieces_blob_summary_matrix",
    "pure_v2_single_and_pinned_hybrid_dual_identity_file_order_matrix",
    "v2_duplicate_filter_existing_batch_same_pk_v1_and_stable_order_matrix",
    "exact_primary_key_first_wins_and_classifier_100_101_grouping_harness",
];

const ROW_CLASSIFICATIONS: [&str; 6] = [
    "SOURCE_ONLY",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT",
    "RUNTIME_EXACT_TEST_HARNESS",
];

const ROW_EXECUTIONS: [&str; 6] = [
    "SOURCE_ONLY_NO_RUNTIME_OR_DATABASE_EXECUTION",
    "GO_ACTUAL_PARSE_META_INFO_BYTES_AND_CREATE_TORRENT_MODEL",
    "GO_ACTUAL_PARSE_MODEL_BLOB_DECODE_SUMMARY_AND_PIECES_MATRIX",
    "GO_ACTUAL_PARSE_AND_MODEL_PURE_V2_AND_HYBRID_MATRIX",
    "GO_ACTUAL_FILTER_V2_DUPLICATES_MATRIX",
    "SOURCE_PINNED_PRIMARY_DEDUP_AND_CLASSIFIER_GROUPING_HARNESS_ONLY",
];

const RUST_NONCLAIMS: [&str; 5] = [
    "this_strict_consumer_does_not_execute_the_Rust_torrent_persistence_planner_or_worker",
    "Rust_torrent_repository_or_PostgreSQL_execution",
    "Rust_queue_writer_classifier_or_scrape_fanout_execution",
    "Go_runPersistTorrents_or_batcher_runtime_execution",
    "application_supervisor_deployment_or_production_readiness",
];

const RUST_EXECUTION_PARTITION: [(&str, &str); 6] = [
    (
        FIXTURE_IDS[0],
        "SOURCE_CONTRACT_ASSERTIONS_ONLY_NO_RUST_OR_GO_RUNTIME_EXECUTION",
    ),
    (
        FIXTURE_IDS[1],
        "RUST_METAINFO_PARSER_AND_HASH_REPLAY_ONLY_GO_MODEL_PROJECTION_IS_EVIDENCE",
    ),
    (
        FIXTURE_IDS[2],
        "RUST_METAINFO_PARSER_AND_HASH_REPLAY_ONLY_GO_MODEL_BLOB_SUMMARY_AND_PIECES_ARE_EVIDENCE",
    ),
    (
        FIXTURE_IDS[3],
        "RUST_METAINFO_PARSER_AND_HASH_REPLAY_ONLY_GO_MODEL_FILE_ORDER_AND_BLOB_ARE_EVIDENCE",
    ),
    (
        FIXTURE_IDS[4],
        "STRICT_FIXTURE_ASSERTIONS_ONLY_NO_RUST_DEDUP_EXECUTION",
    ),
    (
        FIXTURE_IDS[5],
        "RUST_EMBEDDED_PAYLOAD_PARSE_AND_RAW_FINGERPRINT_VERIFICATION_ONLY_NO_CLASSIFIER_OR_QUEUE_WRITER",
    ),
];

const GO_DEPENDENCIES: [&str; 5] = [
    "github.com/anacrolix/torrent v1.58.0",
    "github.com/klauspost/compress v1.17.11",
    "github.com/vmihailenco/msgpack/v5 v5.4.1",
    "gorm.io/gen v0.3.26",
    "gorm.io/gorm v1.25.12",
];

const GO_NONCLAIMS: [&str; 15] = [
    "runPersistTorrents_batch_receive_loop_or_any_database_transaction_execution",
    "batching_goroutine_ticker_timing_ready_select_winner_or_closed_channel_runtime_behavior",
    "exact_GORM_generated_SQL_bind_order_rows_affected_commit_or_rollback_behavior",
    "live_PostgreSQL_schema_permissions_constraints_triggers_or_transaction_atomicity",
    "lookupExistingV2_live_query_chunk_bind_order_partial_error_or_database_contents",
    "scrape_fanout_Go_map_iteration_order_delivery_or_downstream_execution",
    "time_Now_wall_clock_values_queue_run_after_or_timestamp_equality_across_processes",
    "cross_language_outer_ZSTD_byte_or_length_equality_and_cross_version_stability",
    "live_queue_job_unique_index_conflict_or_classifier_execution",
    "Prometheus_log_delivery_or_actual_inserted_updated_committed_row_counts",
    "context_cancellation_shutdown_drain_join_retry_requeue_or_exactly_once_delivery",
    "createTorrentModel_error_path_from_current_valid_runtime_rows",
    "production_rejection_of_negative_or_uint_overflow_file_lengths_beyond_the_fixture_harness_checks",
    "synthetic_pure_v2_pieces_root_content_merkle_correctness_beyond_32_byte_structural_shape",
    "Rust_worker_repository_application_supervisor_deployment_or_readiness",
];

const NORMALIZED_AST: [(&str, &str); 37] = [
    (
        "batching.In",
        "f5ef939724dc08bc0fa39e9fa2e0863e45acd1c965609ad91fa7082fd6632b21",
    ),
    (
        "batching.NewBatchingChannel",
        "2c9a3fa894f82680a8cb8437d8dbad6d3bc2da9a7594c83553ef7650dd472dc6",
    ),
    (
        "batching.Out",
        "f677733fd65c621331747365d30bc29503cda90a21e5aba68ece706afd5d2e3c",
    ),
    (
        "batching.batch",
        "ebedd32544fc4a53c3cb016fd883da2e76267dd492a7c5f88ba2ebcf8232858c",
    ),
    (
        "batching.flush",
        "3c72fb1d8c6d52bfed5b60a796d5bfee0e13da3b745c220ac01467a88de1f274",
    ),
    (
        "blob.BuildFileSummary",
        "be962f342758d0e7f03a831d49827c8af57a4d8a3560b3c80cc716196610854c",
    ),
    (
        "blob.DeserializeFiles",
        "d150898e585295c4eb231b6308e0477ac64da15062b468fe6f4c1cb1a5e53bd3",
    ),
    (
        "blob.ExtractUniqueExtensions",
        "adb6aaebde6329309ae9bf10485a0a2085f73a7f27ce7d2ff44389c7755f5db7",
    ),
    (
        "blob.SerializeFiles",
        "62127451ffc61b855c9c5a2a0ffc4977f7d1ecc5b546ca9f670c13296842c1b2",
    ),
    (
        "config.Config",
        "3883ac0fbf4869de1caa10bfe01100147e8b5e9681a65ad44c2910a79e531a73",
    ),
    (
        "config.NewDefaultConfig",
        "d044a4710817daf9a87dfab03ce22f138da3c6e1bf94d40bbbfd0fea70673f32",
    ),
    (
        "crawler.infoHashWithMetaInfo",
        "7de701e7f26b3dbbe7f82adc220ec88ffc362afd476bf5899fe20401afa0ce6d",
    ),
    (
        "crawler.start",
        "d61a318ce626352ee4f5cd5dd48191d767bbfe45b6a9def673cd185eada4f67b",
    ),
    (
        "factory.New",
        "0204a00fd63b275339d63d622865858571c153bc81fc738784a78e1c150fec80",
    ),
    (
        "factory.Params",
        "265ba054222c6a3e228fb2b11e822ab994c6295a36a536531bc1c1bb4401c00a",
    ),
    (
        "factory.Result",
        "ba6e8c3112414947f599febf6d342c19a06b91eb386e6a932a04e888523cea65",
    ),
    (
        "metainfo.Info",
        "08928b81ea00a8adcee59959f876fcdd623a7059f580e7452f41eded1789954e",
    ),
    (
        "metainfo.ParseMetaInfoBytes",
        "4de434e83335941b1db217f8cade3a09c7c01df133555f085baf70ad616f9b8b",
    ),
    (
        "metainfo.ParsedInfo",
        "51664f615ffbaff8382bef86eadfc7d0b1c722acfd76b2ca86705a921d3065d0",
    ),
    (
        "model.NewQueueJob",
        "a1a890551e6feb59b062a2dd48be25758013050fa62ebf21e4b84f5772f8e25a",
    ),
    (
        "model.QueueJob",
        "7183f138723be0f7dab2841209f85a97bfb18a6c95735b84130c5cb6f0db9285",
    ),
    (
        "model.QueueJobDelayBy",
        "e219f5e4d4fd1382964aec2c37dcdff2528ddd962daf6fc014f73d56087c52a1",
    ),
    (
        "model.Torrent",
        "42657deef97d08eea3bdbe92724885e367be6b2967b0cd81fef21f88e88d358a",
    ),
    (
        "model.TorrentFile",
        "c5631b4d8156c03dfb99459791f062045f9e191a47bb23041ba5783c5cdba109",
    ),
    (
        "model.TorrentFileSummary",
        "250b98900b722c9d57a7abcc23d395fae07865e474ac10f9be7a84261ccb2620",
    ),
    (
        "model.TorrentPieces",
        "db025620774cb761a9a58267163583454c8f1ef039ead228e0aaf7e1c0c4097c",
    ),
    (
        "model.TorrentsTorrentSource",
        "f71036cb64dfaa18994e0caa7fe63e394a93e3f29cf00312ce7f7d2e2cf358e5",
    ),
    (
        "persist.buildTorrentFileSummary",
        "8762a76dd0e409fb062b141ef9cbbf252f192e974b4dbaece52d57ef41b8b139",
    ),
    (
        "persist.createTorrentModel",
        "ec8602b3a04c724a6941c2012a1b7c4891a53828dc4f34c5cbc7f7978f646852",
    ),
    (
        "persist.dropV2Duplicate",
        "8f644dca197a59dd99b92c6f3648e2e4a31148be0db3b0aefa573c4e232a1b04",
    ),
    (
        "persist.filterV2Duplicates",
        "dde18c5742a58b6290a578c1e397bb94587a40854e0a4b0250860e922df526f5",
    ),
    (
        "persist.lookupExistingV2",
        "8e22d9abdb957e5f12a55187e834b0438ba907820160dda8d33a5542138d4b04",
    ),
    (
        "persist.runPersistTorrents",
        "fb761e3ec7c805218cc826f978352c8ebf831ab35b329ef5d868b9d8d12be199",
    ),
    (
        "persist.torrentFileSummaryPersistQuery",
        "dac2d8fa8853858404ddb89cd926f70ab04a014472a815af195361b5e0f0254f",
    ),
    (
        "processor.MessageParams",
        "d440147bc9e96dac2e745fedbe4f8c1a64f5192a76bb9122d4614cccf1e78990",
    ),
    (
        "processor.NewQueueJob",
        "5ce97c9b684c7a1f1e18afcd873a7e162cc50c8a093c0b2276a6c94dc80fa0da",
    ),
    (
        "protocol.InfoHashV2.ToShort",
        "3bc66809740dd16c9e4dfd8813d4a19667a1d9a1a353197de3f03144e68d457b",
    ),
];

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Fixture {
    id: String,
    subsystem: String,
    classification: String,
    execution: String,
    oracle: Oracle,
    input: Input,
    expected: Expected,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Oracle {
    composition: String,
    determinism: String,
    harness: String,
    database: String,
    clock: String,
    run_persist_torrents_executed: bool,
    batcher_executed: bool,
    database_executed: bool,
    actual_functions_executed: Vec<String>,
    source_pinned_harness_steps: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Input {
    kind: String,
    cases: Vec<ModelCase>,
    dedup_cases: Vec<DedupCase>,
    classifier: Option<ClassifierInput>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModelCase {
    label: String,
    raw_info_hex: String,
    raw_info_sha256: String,
    requested_info_hash: String,
    save_pieces: bool,
    save_files_threshold: u64,
    source_fixture: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Expected {
    models: Vec<ModelResult>,
    dedup_cases: Vec<DedupResult>,
    classifier: Option<ClassifierResult>,
    source: Option<Source>,
    run_persist_torrents_executed: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModelResult {
    label: String,
    parse_error: String,
    create_error: String,
    info_hash: String,
    info_hash_v1: String,
    info_hash_v2: String,
    meta_version: u16,
    meta_version_valid: bool,
    name: String,
    size: u64,
    private: bool,
    files_status: String,
    files_count: u64,
    files_count_valid: bool,
    files: Option<Vec<FileResult>>,
    files_nil: bool,
    files_data_present: bool,
    files_data_nil: bool,
    files_data_byte_length: u64,
    files_data_sha256: String,
    decoded_files: Option<Vec<FileResult>>,
    decoded_files_nil: bool,
    decoded_files_match_retained_core_fields: bool,
    file_extensions: Option<Vec<String>>,
    file_extensions_nil: bool,
    sources: Option<Vec<SourceResult>>,
    sources_nil: bool,
    pieces: PiecesResult,
    summary: Option<SummaryResult>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FileResult {
    index: u64,
    path: String,
    size: u64,
    extension: String,
    extension_valid: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SourceResult {
    source: String,
    info_hash: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PiecesResult {
    present: bool,
    info_hash: String,
    piece_length: i64,
    pieces_hex: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SummaryResult {
    info_hash: String,
    file_count: u64,
    total_size: i64,
    largest_file_size: i64,
    extensions: Vec<String>,
    has_video: bool,
    has_subtitle: bool,
    has_audio: bool,
    compressed_bytes_valid: bool,
    compressed_bytes: u64,
    compressed_bytes_matches_files_data: bool,
    created_at: String,
    updated_at: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DedupCase {
    label: String,
    items: Vec<DedupItem>,
    existing: Vec<ExistingV2>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DedupItem {
    primary_info_hash: String,
    info_hash_v2: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExistingV2 {
    info_hash_v2: String,
    primary_info_hash: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DedupResult {
    label: String,
    kept_primary_info_hashes: Vec<String>,
    dropped: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClassifierInput {
    unique_count: u64,
    classify_batch_size: u64,
    duplicate_info_hash: String,
    first_marker: String,
    later_marker: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClassifierResult {
    input_count: u64,
    unique_count: u64,
    duplicate_info_hashes: Vec<String>,
    duplicate_winner_marker: String,
    classifier_groups: Vec<Vec<String>>,
    queue_jobs: Vec<QueueJob>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QueueJob {
    queue: String,
    payload: String,
    fingerprint: String,
    status: String,
    retries: u64,
    max_retries: u64,
    priority: i64,
    archival_duration_nanoseconds: u64,
    delay_millis: u64,
    absolute_run_after_excluded: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Source {
    factory: Factory,
    batcher: Batcher,
    lifecycle: Lifecycle,
    worker: Worker,
    lookup_dedup: LookupDedup,
    transactions: Vec<Transaction>,
    schema_constraints: Vec<SchemaConstraint>,
    seeded_torrent_sources: Vec<SeededTorrentSource>,
    fanout: Fanout,
    dependencies: Vec<String>,
    normalized_ast_sha256: BTreeMap<String, String>,
    source_sha256: BTreeMap<String, String>,
    prerequisite_sha256: BTreeMap<String, String>,
    nonclaims: Vec<String>,
    evidence: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Factory {
    input_capacity: u64,
    maximum_batch_size: u64,
    batch_interval_millis: u64,
    output_capacity: u64,
    configuration_fields: Vec<String>,
    default_save_files_threshold: u64,
    default_save_pieces: bool,
    hardcoded: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Batcher {
    flush_at_maximum_size: bool,
    flush_on_nonempty_ticker: bool,
    ticker_starts_at_construction: bool,
    ticker_resets_after_flush: bool,
    flush_blocks_on_output: bool,
    context_aware: bool,
    input_close_exits_loop: bool,
    closed_input_source_outcome: String,
    output_receive_checks_open_boolean: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Lifecycle {
    factory_starts_crawler_detached: bool,
    crawler_starts_persist_worker_detached: bool,
    crawler_waits_only_for_stopped: bool,
    shared_context_cancelled_after_stopped: bool,
    stop_closes_batcher_input: bool,
    stop_drains_persist_torrents: bool,
    stop_joins_worker_or_batcher: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Worker {
    v2_lookup_precedes_primary_dedup: bool,
    exact_primary_key_first_occurrence_wins: bool,
    primary_key_inserted_before_conversion: bool,
    conversion_error_logged_and_skipped: bool,
    classifier_batch_size: u64,
    classifier_order: String,
    classifier_includes_converted_unique_only: bool,
    transaction_called_once_per_received_batch: bool,
    metric_entity: String,
    metric_counts_prepared_torrent_models: bool,
    metric_incremented_only_on_transaction_ok: bool,
    runtime_uint_bits: u8,
    fixture_lengths_checked_before_conversion: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LookupDedup {
    lookup_full_v2_only: bool,
    unique_lookup_set: bool,
    lookup_chunk_size: u64,
    lookup_order: String,
    lookup_error_outcome: String,
    existing_different_primary_drops: bool,
    batch_different_primary_drops: bool,
    same_primary_kept: bool,
    v1_without_v2_kept: bool,
    first_v2_primary_wins_within_batch: bool,
    database_v2_index_unique: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Transaction {
    order: u64,
    table: String,
    conditional: String,
    chunk_size: u64,
    conflict_target: Vec<String>,
    conflict_action: String,
    updated_columns: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SchemaConstraint {
    table: String,
    kind: String,
    columns: Vec<String>,
    predicate: String,
    references: String,
    expression: String,
    unique: bool,
    source_migration: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SeededTorrentSource {
    key: String,
    name: String,
    source_migration: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Fanout {
    one_explicit_transaction: bool,
    one_wall_clock_sample_before_model_loop: bool,
    file_summary_uses_same_wall_clock_sample: bool,
    scrape_only_after_transaction_success: bool,
    scrape_values_from_primary_hash_map: bool,
    scrape_order: String,
    scrape_send_context_aware: bool,
    v2_duplicate_metric_before_transaction: bool,
    queue_job_delay_millis: u64,
    queue_groups_contain_unique_converted_hashes: bool,
    queue_conflict_rolls_back_transaction: bool,
    transaction_retry: bool,
    metrics_or_scrape_on_transaction_error: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct QueuePayload {
    info_hashes: Vec<String>,
}

const GO_SOURCES: [(&str, &[u8], &str); 42] = [
    (
        "go.mod",
        include_bytes!("../../../../go.mod"),
        "bdeb8dff1aa2ee347af6d84614f8c4f79c15c75edb76d991c97150706385d71c",
    ),
    (
        "go.sum",
        include_bytes!("../../../../go.sum"),
        "4b1675395f71a90d7e2c761481165bb55cdba5e8d985269778fc60c2030b8096",
    ),
    (
        "internal/blobmigration/serializer.go",
        include_bytes!("../../../../internal/blobmigration/serializer.go"),
        "2f59d059187b8f0078e0b8bbd81ac78b8e9745909f762c0f403db12a6c1e8082",
    ),
    (
        "internal/concurrency/batching_channel.go",
        include_bytes!("../../../../internal/concurrency/batching_channel.go"),
        "72b3c9fd5fbc8ecbfb0ba2bc2ed5e6c1d45de01f03d3e015b2467f114ec70975",
    ),
    (
        "internal/dhtcrawler/config.go",
        include_bytes!("../../../../internal/dhtcrawler/config.go"),
        "b3cac15378cdca0f21c5f21f37aeb0679815d5bacd16bfa0c3bac2af56db87ef",
    ),
    (
        "internal/dhtcrawler/crawler.go",
        include_bytes!("../../../../internal/dhtcrawler/crawler.go"),
        "ae6ca2484a57231a08351629c21fdc0a875f2272bfd4ad42a4e5386be86500b6",
    ),
    (
        "internal/dhtcrawler/factory.go",
        include_bytes!("../../../../internal/dhtcrawler/factory.go"),
        "ed34129835773817736d70e74c7c884e5b9197e35741dee922ee9a5d691288a6",
    ),
    (
        "internal/dhtcrawler/persist.go",
        include_bytes!("../../../../internal/dhtcrawler/persist.go"),
        "8a76c32f1aeaad074ce22c53193a012ae447a34a3c07e9c2123e9e7e27ac2022",
    ),
    (
        "internal/dhtcrawler/request_meta_info.go",
        include_bytes!("../../../../internal/dhtcrawler/request_meta_info.go"),
        "d20fc943aee947055dd5521235a55fd19c5fdd41a4203b8c09523b443c6ea0a6",
    ),
    (
        "internal/model/duration.go",
        include_bytes!("../../../../internal/model/duration.go"),
        "cf25513113e8c1f73be5432d35c22e7770cbeb6cf298be442c4623e4fba75259",
    ),
    (
        "internal/model/file_type.go",
        include_bytes!("../../../../internal/model/file_type.go"),
        "6084eb911f6ed67b4cbb28b66f994ab3aec281748956e663523837b3195d4c98",
    ),
    (
        "internal/model/file_type_enum.go",
        include_bytes!("../../../../internal/model/file_type_enum.go"),
        "95b2875703b6fa9aca945e5fb8c7b2b038f80d969bc70b5dc3483d9370d5e2bb",
    ),
    (
        "internal/model/files_status.go",
        include_bytes!("../../../../internal/model/files_status.go"),
        "32019b822ff2ab4c98552584f7737e2f41f466d30664ddcb91b542fc523e9440",
    ),
    (
        "internal/model/files_status_enum.go",
        include_bytes!("../../../../internal/model/files_status_enum.go"),
        "5f723e62282dcc82e2037c96d1423f81075cddca24b14e29a544340f5650e9a0",
    ),
    (
        "internal/model/null.go",
        include_bytes!("../../../../internal/model/null.go"),
        "b9c3762d286201140c51cd3ca2630361fb35fb76464c297a37d85037d1be782d",
    ),
    (
        "internal/model/queue_job_status.go",
        include_bytes!("../../../../internal/model/queue_job_status.go"),
        "d72f631194bdfa5715eb562a347640c55be247b7eeb9db91a253013432f9ce3b",
    ),
    (
        "internal/model/queue_job_status_enum.go",
        include_bytes!("../../../../internal/model/queue_job_status_enum.go"),
        "88ffd86967597e560975cfea5c6ae882d6c5360d9b2c076eda461113a3780b40",
    ),
    (
        "internal/model/queue_jobs.gen.go",
        include_bytes!("../../../../internal/model/queue_jobs.gen.go"),
        "4526e4ed3d3dd2da6da378edd4cbb8449122b83665937432531bd08ada9f7f40",
    ),
    (
        "internal/model/queue_jobs.go",
        include_bytes!("../../../../internal/model/queue_jobs.go"),
        "577ba9fc7b4fd85b49068ef34c69c37ea7994ad6a78b334f638f8849334700c7",
    ),
    (
        "internal/model/torrent_file_summary.go",
        include_bytes!("../../../../internal/model/torrent_file_summary.go"),
        "16cebd26035e0f3e1b92777187a1932216e3d38bd61dee96cb4a5b21ee4eacae",
    ),
    (
        "internal/model/torrent_files.gen.go",
        include_bytes!("../../../../internal/model/torrent_files.gen.go"),
        "8d95da8c9989cf7374babf625585988320f864fdb39bb0e371f0c0d466b39d68",
    ),
    (
        "internal/model/torrent_files.go",
        include_bytes!("../../../../internal/model/torrent_files.go"),
        "9947be961649540422cfd9c63834ed1994e310bfb590e292a43a31e652a1199e",
    ),
    (
        "internal/model/torrent_pieces.gen.go",
        include_bytes!("../../../../internal/model/torrent_pieces.gen.go"),
        "9d6dc7d8960801cd4f7a611462f98e19a0b11674b574f856bca11ad4842caf2d",
    ),
    (
        "internal/model/torrents.gen.go",
        include_bytes!("../../../../internal/model/torrents.gen.go"),
        "3c3fb6debefdca25530b9f3cecd818e8b98817528f36ff87a76dfee79cad84e0",
    ),
    (
        "internal/model/torrents_torrent_sources.gen.go",
        include_bytes!("../../../../internal/model/torrents_torrent_sources.gen.go"),
        "a5431060dd68f51ac77aced27f4a3c1481124054bef43365d368bded4a405b41",
    ),
    (
        "internal/processor/message.go",
        include_bytes!("../../../../internal/processor/message.go"),
        "f4efcfbbab0d8768c3fcbc3ac61fc01c9271ffcb25ea58b08c0c4779223c6e34",
    ),
    (
        "internal/protocol/id.go",
        include_bytes!("../../../../internal/protocol/id.go"),
        "e1947e2b4af4cc008f5bb8cf5000ebfe784a82e119cb0418c2a74c3ed5f8c26f",
    ),
    (
        "internal/protocol/infohash_v2.go",
        include_bytes!("../../../../internal/protocol/infohash_v2.go"),
        "397ff948d1b494c8982a5ffee1bbbd61d2acf9a07b616ecc5386d5da7d75c668",
    ),
    (
        "internal/protocol/metainfo/metainfo.go",
        include_bytes!("../../../../internal/protocol/metainfo/metainfo.go"),
        "b75c5f74d42431ad76fe2889f5ce6573cce89f5faedf66f27996dc458e3a7816",
    ),
    (
        "internal/protocol/metainfo/parse.go",
        include_bytes!("../../../../internal/protocol/metainfo/parse.go"),
        "edda62d0c67ae79ded3a03b77b5d6108ac42eeb3df9d83f5fb73cf16cabaea0c",
    ),
    (
        "migrations/00001_init.sql",
        include_bytes!("../../../../migrations/00001_init.sql"),
        "32e729ca54f5140446cf313dc2207eee03c0eaae5ebe1dcbc2ffbcf1f4340d17",
    ),
    (
        "migrations/00002_files_status.sql",
        include_bytes!("../../../../migrations/00002_files_status.sql"),
        "4978391375a22fbf44b062a5c00748bd069091f35a861f631decefb241166761",
    ),
    (
        "migrations/00012_queue.sql",
        include_bytes!("../../../../migrations/00012_queue.sql"),
        "d30cbca84b9aa88448228840b9290cc5db104e28ff91044f7a455afd541146f2",
    ),
    (
        "migrations/00013_torrent_pieces.sql",
        include_bytes!("../../../../migrations/00013_torrent_pieces.sql"),
        "e5955eff777e4128fc6d1bf533b9998a5f7c2fcb7b6c0188d8a7716ef374e4cf",
    ),
    (
        "migrations/00015_queue_priority.sql",
        include_bytes!("../../../../migrations/00015_queue_priority.sql"),
        "19c561e809290534997982af1d64da44939c24fb2ce202f6c335eeef01891231",
    ),
    (
        "migrations/00016_files.sql",
        include_bytes!("../../../../migrations/00016_files.sql"),
        "08a12c384bdd2cf6b90e71a0b8dffe377db62de94fb7268594a98410f21ded2a",
    ),
    (
        "migrations/00017_ordering_fields.sql",
        include_bytes!("../../../../migrations/00017_ordering_fields.sql"),
        "63cedb8a21c89613aeab1cdd42b7d29378a08d47fad7a78462951b08d9b14955",
    ),
    (
        "migrations/00019_queue_fix_duplicate_key.sql",
        include_bytes!("../../../../migrations/00019_queue_fix_duplicate_key.sql"),
        "ab80e291ea11a02c1b5fc55603a97859c129ad60cbb29d047c419a454d5a673c",
    ),
    (
        "migrations/00021_blob_storage.sql",
        include_bytes!("../../../../migrations/00021_blob_storage.sql"),
        "143c65bd92a24d97b71ca1e4d31cb548d04c90e22e81bdd22c4aaa201275d6df",
    ),
    (
        "migrations/00023_v2_infohash.sql",
        include_bytes!("../../../../migrations/00023_v2_infohash.sql"),
        "a3c050a14f1faab09c2eda3131a2e4ba9b5082eb199f9613750190321d51a4e3",
    ),
    (
        "migrations/00025_dht_seen_count.sql",
        include_bytes!("../../../../migrations/00025_dht_seen_count.sql"),
        "92c7571e4f1f1044c10c3854f551e28a2ed04b0f654c57404c71ed9777704f3f",
    ),
    (
        "migrations/00026_summary_compressed_bytes.sql",
        include_bytes!("../../../../migrations/00026_summary_compressed_bytes.sql"),
        "8bf8344a69cab4d5ec21f2ece153d94ad6710c92fafe4e933370d3577e50e98b",
    ),
];

const PREREQUISITES: [(&str, &[u8], &str); 3] = [
    (
        "internal/dhtcrawler/testdata/bittorrent-v2-hybrid-test.torrent",
        include_bytes!(
            "../../../../internal/dhtcrawler/testdata/bittorrent-v2-hybrid-test.torrent"
        ),
        "8ba7575e64e9046cac74ca6523bff6445ff5c3e369d5d132607a793a1834e93f",
    ),
    (
        "testdata/parity/dht/dht_crawler_request_meta_info.jsonl",
        include_bytes!("../../../../testdata/parity/dht/dht_crawler_request_meta_info.jsonl"),
        "03ce2ab0da2b0f9ba1173b8ba52481a903265ca6862f957b40490cf67a9e4ec5",
    ),
    (
        "testdata/parity/queue/fingerprints.jsonl",
        include_bytes!("../../../../testdata/parity/queue/fingerprints.jsonl"),
        "5636896337cf3c27cda78eae4d4315f48bc4c447300beecfef55b35a5f831a8b",
    ),
];

fn fixtures() -> Vec<Fixture> {
    FIXTURE_TEXT
        .lines()
        .enumerate()
        .map(|(index, line)| {
            serde_json::from_str(line)
                .unwrap_or_else(|error| panic!("parse persist-torrents row {}: {error}", index + 1))
        })
        .collect()
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn expected_map<const N: usize>(entries: [(&str, &str); N]) -> BTreeMap<String, String> {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

fn string_vec(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn file_result(index: u64, path: &str, size: u64, extension: &str, valid: bool) -> FileResult {
    FileResult {
        index,
        path: path.to_owned(),
        size,
        extension: extension.to_owned(),
        extension_valid: valid,
    }
}

fn dedup_item(primary_info_hash: &str, info_hash_v2: &str) -> DedupItem {
    DedupItem {
        primary_info_hash: primary_info_hash.to_owned(),
        info_hash_v2: info_hash_v2.to_owned(),
    }
}

fn existing_v2(info_hash_v2: &str, primary_info_hash: &str) -> ExistingV2 {
    ExistingV2 {
        info_hash_v2: info_hash_v2.to_owned(),
        primary_info_hash: primary_info_hash.to_owned(),
    }
}

fn assert_lower_hex(value: &str, bytes: usize) {
    assert_eq!(value.len(), bytes * 2, "wrong hex width: {value}");
    assert!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "hex is not lowercase: {value}"
    );
}

fn decode_lower_hex<const N: usize>(value: &str) -> [u8; N] {
    assert_lower_hex(value, N);
    let mut output = [0; N];
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let digit = |byte| match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => unreachable!(),
        };
        output[index] = digit(pair[0]) * 16 + digit(pair[1]);
    }
    output
}

fn encode_hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn assert_source_contract(source: &Source) {
    assert_eq!(
        source.factory,
        Factory {
            input_capacity: 1_000,
            maximum_batch_size: 1_000,
            batch_interval_millis: 60_000,
            output_capacity: 1,
            configuration_fields: string_vec(&["saveFilesThreshold", "savePieces"]),
            default_save_files_threshold: 100,
            default_save_pieces: false,
            hardcoded: true,
        }
    );
    assert_eq!(
        source.batcher,
        Batcher {
            flush_at_maximum_size: true,
            flush_on_nonempty_ticker: true,
            ticker_starts_at_construction: true,
            ticker_resets_after_flush: true,
            flush_blocks_on_output: true,
            context_aware: false,
            input_close_exits_loop: false,
            closed_input_source_outcome: "closed_input_busy_loop_without_output_close".to_owned(),
            output_receive_checks_open_boolean: false,
        }
    );
    assert_eq!(
        source.lifecycle,
        Lifecycle {
            factory_starts_crawler_detached: true,
            crawler_starts_persist_worker_detached: true,
            crawler_waits_only_for_stopped: true,
            shared_context_cancelled_after_stopped: true,
            stop_closes_batcher_input: false,
            stop_drains_persist_torrents: false,
            stop_joins_worker_or_batcher: false,
        }
    );
    assert_eq!(
        source.worker,
        Worker {
            v2_lookup_precedes_primary_dedup: true,
            exact_primary_key_first_occurrence_wins: true,
            primary_key_inserted_before_conversion: true,
            conversion_error_logged_and_skipped: true,
            classifier_batch_size: 100,
            classifier_order: "kept_input_first_occurrence_order".to_owned(),
            classifier_includes_converted_unique_only: true,
            transaction_called_once_per_received_batch: true,
            metric_entity: "Torrent".to_owned(),
            metric_counts_prepared_torrent_models: true,
            metric_incremented_only_on_transaction_ok: true,
            runtime_uint_bits: 64,
            fixture_lengths_checked_before_conversion: true,
        }
    );
    assert_eq!(
        source.lookup_dedup,
        LookupDedup {
            lookup_full_v2_only: true,
            unique_lookup_set: true,
            lookup_chunk_size: 1_000,
            lookup_order: "unspecified_Go_map_iteration_order".to_owned(),
            lookup_error_outcome: "log_and_fail_open_with_partial_results".to_owned(),
            existing_different_primary_drops: true,
            batch_different_primary_drops: true,
            same_primary_kept: true,
            v1_without_v2_kept: true,
            first_v2_primary_wins_within_batch: true,
            database_v2_index_unique: false,
        }
    );

    let transactions = source
        .transactions
        .iter()
        .map(|item| {
            format!(
                "{}|{}|{}|{}|{}|{}|{}",
                item.order,
                item.table,
                item.conditional,
                item.chunk_size,
                item.conflict_target.join(","),
                item.conflict_action,
                item.updated_columns.join(",")
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(transactions, string_vec(&[
        "1|torrents|always_called|100|info_hash|update|name,files_status,files_count,updated_at,files_data,file_extensions",
        "2|torrent_files|only_when_nonempty|100||do_nothing|",
        "3|torrent_file_summary|only_when_nonempty|100|info_hash|update|file_count,total_size,largest_file_size,extensions,has_video,has_subtitle,has_audio,compressed_bytes,updated_at",
        "4|torrents_torrent_sources|always_called|100||do_nothing|",
        "5|torrent_pieces|only_when_savePieces|10||do_nothing|",
        "6|queue_jobs|always_called|10||gorm_default_insert|",
    ]));

    let schemas = source
        .schema_constraints
        .iter()
        .map(|item| {
            format!(
                "{}|{}|{}|{}|{}|{}|{}|{}",
                item.table,
                item.kind,
                item.columns.join(","),
                item.predicate,
                item.references,
                item.expression,
                item.unique,
                item.source_migration
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(schemas, string_vec(&[
        "torrents|primary_key|info_hash||||true|migrations/00001_init.sql",
        "torrents|plain_index|info_hash_v2||||false|migrations/00023_v2_infohash.sql",
        "torrents|nullable_columns|info_hash_v1,info_hash_v2,meta_version||||false|migrations/00023_v2_infohash.sql",
        "torrent_files|primary_key|info_hash,path||||true|migrations/00001_init.sql",
        "torrent_files|unique_constraint|info_hash,index||||true|migrations/00001_init.sql",
        "torrent_files|foreign_key|info_hash||torrents(info_hash) ON DELETE CASCADE||false|migrations/00001_init.sql",
        "torrent_files|generated_column|extension|||substring(lower(path) from '[^/.]\\.([a-z0-9]+)$')|false|migrations/00001_init.sql",
        "torrent_file_summary|primary_key|info_hash||||true|migrations/00021_blob_storage.sql",
        "torrent_file_summary|foreign_key|info_hash||torrents(info_hash) ON DELETE CASCADE||false|migrations/00021_blob_storage.sql",
        "torrent_file_summary|nullable_columns|compressed_bytes||||false|migrations/00026_summary_compressed_bytes.sql",
        "torrents_torrent_sources|primary_key|source,info_hash||||true|migrations/00001_init.sql",
        "torrents_torrent_sources|foreign_key|info_hash||torrents(info_hash) ON DELETE CASCADE||false|migrations/00001_init.sql",
        "torrents_torrent_sources|foreign_key|source||torrent_sources(key) ON DELETE CASCADE||false|migrations/00001_init.sql",
        "torrent_pieces|primary_key|info_hash||||true|migrations/00013_torrent_pieces.sql",
        "torrent_pieces|foreign_key|info_hash||torrents(info_hash) ON DELETE CASCADE||false|migrations/00013_torrent_pieces.sql",
        "queue_jobs|primary_key|id||||true|migrations/00012_queue.sql",
        "queue_jobs|not_null_columns|fingerprint,status||||false|migrations/00012_queue.sql",
        "queue_jobs|partial_unique_index|fingerprint|status IN ('pending', 'retry')|||true|migrations/00019_queue_fix_duplicate_key.sql",
    ]));
    assert_eq!(
        source.seeded_torrent_sources,
        vec![SeededTorrentSource {
            key: "dht".to_owned(),
            name: "DHT".to_owned(),
            source_migration: "migrations/00001_init.sql".to_owned(),
        }]
    );
    assert_eq!(
        source.fanout,
        Fanout {
            one_explicit_transaction: true,
            one_wall_clock_sample_before_model_loop: true,
            file_summary_uses_same_wall_clock_sample: true,
            scrape_only_after_transaction_success: true,
            scrape_values_from_primary_hash_map: true,
            scrape_order: "unspecified_Go_map_iteration_order".to_owned(),
            scrape_send_context_aware: true,
            v2_duplicate_metric_before_transaction: true,
            queue_job_delay_millis: 60_000,
            queue_groups_contain_unique_converted_hashes: true,
            queue_conflict_rolls_back_transaction: true,
            transaction_retry: false,
            metrics_or_scrape_on_transaction_error: false,
        }
    );
    assert_eq!(source.dependencies, string_vec(&GO_DEPENDENCIES));
    assert_eq!(source.normalized_ast_sha256, expected_map(NORMALIZED_AST));
    assert_eq!(
        source.source_sha256,
        expected_map(GO_SOURCES.map(|(path, _, digest)| (path, digest)))
    );
    assert_eq!(
        source.prerequisite_sha256,
        expected_map(PREREQUISITES.map(|(path, _, digest)| (path, digest)))
    );
    assert_eq!(source.nonclaims, string_vec(&GO_NONCLAIMS));
    assert_eq!(source.normalized_ast_sha256.len(), NORMALIZED_AST.len());
    assert_eq!(source.source_sha256.len(), GO_SOURCES.len());
    assert_eq!(source.prerequisite_sha256.len(), PREREQUISITES.len());
    assert_eq!(source.evidence, "runtime rows execute only their ordered named parser/model/blob/summary/dedup/queue-constructor functions; the classifier row combines a source-pinned loop harness with actual queue constructors; runPersistTorrents, batching goroutines, lookupExistingV2, GORM, PostgreSQL, metrics, logs, and scrape fanout are not executed");
}

#[test]
fn source_hashes_layout_partition_and_contract_are_exact() {
    assert_eq!(sha256(FIXTURE_BYTES), FIXTURE_SHA256);
    assert!(FIXTURE_BYTES.ends_with(b"\n"));
    assert!(!FIXTURE_BYTES.contains(&b'\r'));
    assert_eq!(
        FIXTURE_BYTES.iter().filter(|byte| **byte == b'\n').count(),
        6
    );

    let rows = fixtures();
    assert_eq!(rows.len(), 6);
    assert_eq!(
        rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        FIXTURE_IDS
    );
    assert_eq!(
        rows.iter()
            .map(|row| row.classification.as_str())
            .collect::<Vec<_>>(),
        ROW_CLASSIFICATIONS
    );
    assert_eq!(
        rows.iter()
            .map(|row| row.execution.as_str())
            .collect::<Vec<_>>(),
        ROW_EXECUTIONS
    );
    assert!(rows
        .iter()
        .all(|row| row.subsystem == "dht_crawler_persist_torrents"));
    assert_eq!(RUST_EXECUTION_PARTITION.len(), rows.len());
    assert_eq!(RUST_EXECUTION_PARTITION.map(|entry| entry.0), FIXTURE_IDS);
    assert_eq!(
        RUST_NONCLAIMS,
        [
            "this_strict_consumer_does_not_execute_the_Rust_torrent_persistence_planner_or_worker",
            "Rust_torrent_repository_or_PostgreSQL_execution",
            "Rust_queue_writer_classifier_or_scrape_fanout_execution",
            "Go_runPersistTorrents_or_batcher_runtime_execution",
            "application_supervisor_deployment_or_production_readiness",
        ]
    );

    for (path, bytes, digest) in GO_SOURCES.into_iter().chain(PREREQUISITES) {
        assert_eq!(sha256(bytes), digest, "source drift: {path}");
    }
    assert_eq!(GO_SOURCES.len(), 42);
    assert_eq!(NORMALIZED_AST.len(), 37);
    assert_eq!(PREREQUISITES.len(), 3);
    assert_eq!(expected_map(NORMALIZED_AST).len(), NORMALIZED_AST.len());
    assert_eq!(
        expected_map(GO_SOURCES.map(|(path, _, digest)| (path, digest))).len(),
        GO_SOURCES.len()
    );
    assert_eq!(
        expected_map(PREREQUISITES.map(|(path, _, digest)| (path, digest))).len(),
        PREREQUISITES.len()
    );
    assert_eq!(GO_DEPENDENCIES.len(), 5);
    assert_eq!(
        rows[0].oracle.actual_functions_executed,
        Vec::<String>::new()
    );
    assert_source_contract(rows[0].expected.source.as_ref().unwrap());
    assert_eq!(
        rows[0]
            .expected
            .source
            .as_ref()
            .unwrap()
            .schema_constraints
            .len(),
        18
    );
}

#[test]
fn row_metadata_execution_flags_and_optional_key_presence_are_exact() {
    let rows = fixtures();
    let lane_shapes = [
        (0, 0, false, 0, 0, false, true),
        (1, 0, false, 1, 0, false, false),
        (2, 0, false, 2, 0, false, false),
        (2, 0, false, 2, 0, false, false),
        (0, 5, false, 0, 5, false, false),
        (0, 0, true, 0, 0, true, false),
    ];
    for (row, shape) in rows.iter().zip(lane_shapes) {
        assert_eq!(
            (
                row.input.cases.len(),
                row.input.dedup_cases.len(),
                row.input.classifier.is_some(),
                row.expected.models.len(),
                row.expected.dedup_cases.len(),
                row.expected.classifier.is_some(),
                row.expected.source.is_some(),
            ),
            shape,
            "irrelevant lane was populated for {}",
            row.id
        );
    }
    let compositions = [
        "production_source_factory_batcher_lifecycle_lookup_dedup_transaction_and_fanout_contract",
        "actual_ParseMetaInfoBytes_then_actual_createTorrentModel_v1_single_default",
        "actual_ParseMetaInfoBytes_createTorrentModel_DeserializeFiles_and_buildTorrentFileSummary_threshold_matrix",
        "actual_ParseMetaInfoBytes_then_actual_createTorrentModel_pure_v2_single_and_pinned_hybrid",
        "actual_filterV2Duplicates_with_fixed_in_memory_items_and_existing_map",
        "source_pinned_exact_primary_hashMap_first_wins_and_flushHashesToClassify_grouping_harness",
    ];
    let determinism = [
        "exact_normalized_AST_full_source_prerequisite_and_dependency_freshness",
        "single_synthetic_bencoded_info_with_derived_v1_hash",
        "fixed_v1_exactly_N_and_N_plus_one_inputs_fixed_summary_clock",
        "synthetic_pure_v2_plus_SHA_pinned_repository_hybrid_info_bytes",
        "ordered_slice_iteration_and_fixed_map_lookups_without_database_or_clock",
        "fixed_ordered_101_unique_hashes_one_exact_primary_duplicate_and_constant_batch_size",
    ];
    let harness = [
        "source_inspection_only",
        "none",
        "none",
        "none",
        "none",
        "source_pinned_loop_harness_not_runPersistTorrents",
    ];
    let clocks = [
        "not_read",
        "not_read",
        "fixed_test_clock_for_summary_only",
        "not_read",
        "not_read",
        "read_but_absolute_run_after_excluded",
    ];
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.oracle.composition, compositions[index]);
        assert_eq!(row.oracle.determinism, determinism[index]);
        assert_eq!(row.oracle.harness, harness[index]);
        assert_eq!(row.oracle.database, "not_executed");
        assert_eq!(row.oracle.clock, clocks[index]);
        assert!(!row.oracle.run_persist_torrents_executed);
        assert!(!row.oracle.batcher_executed);
        assert!(!row.oracle.database_executed);
        assert!(!row.expected.run_persist_torrents_executed);
    }
    assert_eq!(
        rows[1].oracle.actual_functions_executed,
        string_vec(&["metainfo.ParseMetaInfoBytes", "persist.createTorrentModel"])
    );
    assert_eq!(
        rows[2].oracle.actual_functions_executed,
        string_vec(&[
            "metainfo.ParseMetaInfoBytes",
            "persist.createTorrentModel",
            "blob.SerializeFiles",
            "blob.ExtractUniqueExtensions",
            "blob.DeserializeFiles",
            "persist.buildTorrentFileSummary",
            "blob.BuildFileSummary",
            "blob.ExtractUniqueExtensions",
            "metainfo.ParseMetaInfoBytes",
            "persist.createTorrentModel",
            "blob.SerializeFiles",
            "blob.ExtractUniqueExtensions",
            "blob.DeserializeFiles",
            "persist.buildTorrentFileSummary",
            "blob.BuildFileSummary",
            "blob.ExtractUniqueExtensions",
        ])
    );
    assert_eq!(
        rows[3].oracle.actual_functions_executed,
        string_vec(&[
            "metainfo.ParseMetaInfoBytes",
            "persist.createTorrentModel",
            "metainfo.ParseMetaInfoBytes",
            "persist.createTorrentModel",
            "blob.SerializeFiles",
            "blob.ExtractUniqueExtensions",
            "blob.DeserializeFiles",
        ])
    );
    assert_eq!(
        rows[4].oracle.actual_functions_executed,
        string_vec(&[
            "persist.filterV2Duplicates",
            "persist.dropV2Duplicate",
            "persist.filterV2Duplicates",
            "persist.dropV2Duplicate",
            "persist.filterV2Duplicates",
            "persist.dropV2Duplicate",
            "persist.dropV2Duplicate",
            "persist.dropV2Duplicate",
            "persist.filterV2Duplicates",
            "persist.dropV2Duplicate",
            "persist.dropV2Duplicate",
            "persist.filterV2Duplicates",
        ])
    );
    assert_eq!(
        rows[5].oracle.actual_functions_executed,
        string_vec(&[
            "model.QueueJobDelayBy",
            "processor.NewQueueJob",
            "model.NewQueueJob",
            "model.QueueJobDelayBy",
            "processor.NewQueueJob",
            "model.NewQueueJob",
        ])
    );
    assert_eq!(
        rows[0].oracle.source_pinned_harness_steps,
        string_vec(&[
            "parse_and_format_named_production_AST_nodes",
            "hash_full_source_and_prerequisite_bytes",
            "extract_exact_go_mod_dependency_lines",
        ])
    );
    assert_eq!(
        rows[4].oracle.source_pinned_harness_steps,
        string_vec(&[
            "construct_fixed_order_infoHashWithMetaInfo_slices",
            "construct_fixed_existing_full_v2_to_primary_maps",
            "project_kept_primary_hashes_in_returned_slice_order",
        ])
    );
    assert_eq!(
        rows[5].oracle.source_pinned_harness_steps,
        string_vec(&[
            "iterate_fixed_input_in_order",
            "skip_later_exact_primary_key_occurrences",
            "append_each_first_unique_hash_to_classifier_slice",
            "flush_exactly_at_100_hashes",
            "flush_final_nonempty_suffix",
        ])
    );

    let values = FIXTURE_TEXT
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let row_optional_keys = [
        (false, false, true),
        (false, false, false),
        (false, false, false),
        (false, false, false),
        (false, false, false),
        (true, true, false),
    ];
    for (index, value) in values.iter().enumerate() {
        assert_eq!(
            value["input"]
                .as_object()
                .unwrap()
                .contains_key("classifier"),
            row_optional_keys[index].0
        );
        assert_eq!(
            value["expected"]
                .as_object()
                .unwrap()
                .contains_key("classifier"),
            row_optional_keys[index].1
        );
        assert_eq!(
            value["expected"]
                .as_object()
                .unwrap()
                .contains_key("source"),
            row_optional_keys[index].2
        );
        for model in value["expected"]["models"].as_array().unwrap() {
            for key in ["files", "decodedFiles", "fileExtensions", "sources"] {
                assert!(
                    model.as_object().unwrap().contains_key(key),
                    "missing {key}"
                );
            }
            assert_eq!(
                model.as_object().unwrap().contains_key("summary"),
                model["summary"].is_object()
            );
        }
        for case in value["input"]["cases"].as_array().unwrap() {
            assert_eq!(
                case.as_object().unwrap().contains_key("sourceFixture"),
                case["sourceFixture"].is_string()
            );
        }
    }
    let model_optional_keys = [
        ("v1_single_default", false, false),
        ("exactly_n_files", false, true),
        ("n_plus_one_files", false, true),
        ("pure_v2_top_level_single", true, false),
        ("pinned_hybrid_discovered_by_v1", true, false),
    ];
    let raw_model_cases = values[1..4]
        .iter()
        .flat_map(|value| {
            value["input"]["cases"]
                .as_array()
                .unwrap()
                .iter()
                .zip(value["expected"]["models"].as_array().unwrap())
        })
        .collect::<Vec<_>>();
    assert_eq!(raw_model_cases.len(), model_optional_keys.len());
    for ((case, model), (label, source_fixture, summary)) in
        raw_model_cases.into_iter().zip(model_optional_keys)
    {
        assert_eq!(case["label"], label);
        assert_eq!(model["label"], label);
        assert_eq!(
            case.as_object().unwrap().contains_key("sourceFixture"),
            source_fixture
        );
        assert_eq!(model.as_object().unwrap().contains_key("summary"), summary);
    }
}

#[test]
fn recursive_schema_rejects_unknown_fields() {
    let values = FIXTURE_TEXT
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let mutations: &[(usize, &[&str])] = &[
        (0, &[]),
        (0, &["oracle"]),
        (0, &["input"]),
        (0, &["expected"]),
        (0, &["expected", "source"]),
        (0, &["expected", "source", "factory"]),
        (0, &["expected", "source", "batcher"]),
        (0, &["expected", "source", "lifecycle"]),
        (0, &["expected", "source", "worker"]),
        (0, &["expected", "source", "lookupDedup"]),
        (0, &["expected", "source", "transactions", "0"]),
        (0, &["expected", "source", "schemaConstraints", "0"]),
        (0, &["expected", "source", "seededTorrentSources", "0"]),
        (0, &["expected", "source", "fanout"]),
        (1, &["input", "cases", "0"]),
        (1, &["expected", "models", "0"]),
        (1, &["expected", "models", "0", "sources", "0"]),
        (1, &["expected", "models", "0", "pieces"]),
        (2, &["expected", "models", "0", "files", "0"]),
        (2, &["expected", "models", "0", "decodedFiles", "0"]),
        (2, &["expected", "models", "0", "summary"]),
        (4, &["input", "dedupCases", "0"]),
        (4, &["input", "dedupCases", "0", "items", "0"]),
        (4, &["input", "dedupCases", "0", "existing", "0"]),
        (4, &["expected", "dedupCases", "0"]),
        (5, &["input", "classifier"]),
        (5, &["expected", "classifier"]),
        (5, &["expected", "classifier", "queueJobs", "0"]),
    ];
    for (row, path) in mutations {
        let mut value = values[*row].clone();
        let mut target = &mut value;
        for component in *path {
            target = if let Ok(index) = component.parse::<usize>() {
                &mut target.as_array_mut().unwrap()[index]
            } else {
                target.as_object_mut().unwrap().get_mut(*component).unwrap()
            };
        }
        target
            .as_object_mut()
            .unwrap()
            .insert("unknownField".to_owned(), serde_json::Value::Bool(true));
        assert!(
            serde_json::from_value::<Fixture>(value).is_err(),
            "unknown field accepted at path {path:?}"
        );
    }
}

#[test]
fn v1_threshold_v2_and_hybrid_rows_pin_hashes_and_go_model_evidence() {
    let rows = fixtures();
    assert_eq!(
        rows.iter()
            .map(|row| row.input.kind.as_str())
            .collect::<Vec<_>>(),
        [
            "production_source_contract",
            "verified_raw_info_model_matrix",
            "verified_raw_info_model_matrix",
            "verified_raw_info_model_matrix",
            "filter_v2_duplicates_matrix",
            "primary_dedup_and_classifier_grouping_harness",
        ]
    );
    assert_eq!(
        rows[1..4]
            .iter()
            .flat_map(|row| &row.input.cases)
            .map(|case| (
                case.label.as_str(),
                case.save_pieces,
                case.save_files_threshold,
                case.source_fixture.as_deref(),
            ))
            .collect::<Vec<_>>(),
        [
            ("v1_single_default", false, 100, None),
            ("exactly_n_files", true, 3, None),
            ("n_plus_one_files", true, 3, None),
            (
                "pure_v2_top_level_single",
                false,
                1_000,
                Some("synthetic_structurally_valid_BEP52_bencode"),
            ),
            (
                "pinned_hybrid_discovered_by_v1",
                false,
                1_000,
                Some("internal/dhtcrawler/testdata/bittorrent-v2-hybrid-test.torrent"),
            ),
        ]
    );

    for row in &rows[1..4] {
        assert_eq!(row.input.cases.len(), row.expected.models.len());
        for (case, model) in row.input.cases.iter().zip(&row.expected.models) {
            assert_eq!(case.label, model.label);
            assert!(model.parse_error.is_empty());
            assert!(model.create_error.is_empty());
            assert_lower_hex(&case.raw_info_sha256, 32);
            assert_lower_hex(&case.requested_info_hash, 20);
            assert!(case.raw_info_hex.len() % 2 == 0);
            assert!(case
                .raw_info_hex
                .bytes()
                .all(|byte| { byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte) }));
            let raw = (0..case.raw_info_hex.len())
                .step_by(2)
                .map(|index| u8::from_str_radix(&case.raw_info_hex[index..index + 2], 16).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(sha256(&raw), case.raw_info_sha256);
            let parsed = parse_info_bytes(decode_lower_hex(&case.requested_info_hash), &raw)
                .unwrap_or_else(|error| panic!("Rust parser rejected {}: {error}", case.label));
            let v1 = parsed.info_hash_v1().map(encode_hex).unwrap_or_default();
            let v2 = parsed.info_hash_v2().map(encode_hex).unwrap_or_default();
            assert_eq!(v1, model.info_hash_v1);
            assert_eq!(v2, model.info_hash_v2);
            assert_eq!(u16::from(parsed.meta_version().as_u8()), model.meta_version);
            if case.label == "pure_v2_top_level_single" {
                let files = parsed.info().upverted_files().unwrap();
                assert_eq!(files.len(), 1);
                assert_eq!(files[0].length(), 1_500_000_000);
                assert_eq!(files[0].pieces_root(), Some([0x11; 32]));
            }
            let primary = if v1.is_empty() {
                v2[..40].to_owned()
            } else {
                v1
            };
            assert_eq!(primary, model.info_hash);
            assert_eq!(case.requested_info_hash, model.info_hash);
            assert_eq!(model.meta_version_valid, model.meta_version != 0);
            assert_eq!(model.files.is_none(), model.files_nil);
            assert_eq!(model.decoded_files.is_none(), model.decoded_files_nil);
            assert_eq!(model.file_extensions.is_none(), model.file_extensions_nil);
            assert_eq!(model.sources.is_none(), model.sources_nil);
            assert_eq!(model.files_data_present, !model.files_data_nil);
            assert!(model.files_data_byte_length <= i64::MAX as u64);
            assert!(model.size <= i64::MAX as u64);
            for file in model.files.iter().flatten() {
                assert!(file.size <= i64::MAX as u64);
            }
            for file in model.decoded_files.iter().flatten() {
                assert!(file.size <= i64::MAX as u64);
            }
            if model.files_data_present {
                assert_lower_hex(&model.files_data_sha256, 32);
            } else {
                assert_eq!(model.files_data_byte_length, 0);
                assert!(model.files_data_sha256.is_empty());
            }
            let sources = model.sources.as_ref().unwrap();
            assert_eq!(
                sources,
                &vec![SourceResult {
                    source: "dht".to_owned(),
                    info_hash: model.info_hash.clone(),
                }]
            );
            if model.info_hash_v1.is_empty() {
                assert_lower_hex(&model.info_hash_v2, 32);
            } else {
                assert_lower_hex(&model.info_hash_v1, 20);
            }
            if !model.info_hash_v2.is_empty() {
                assert_lower_hex(&model.info_hash_v2, 32);
            }
            if model.pieces.present {
                assert_eq!(model.pieces.info_hash, model.info_hash);
                assert!(model.pieces.piece_length > 0);
                assert!(model.pieces.pieces_hex.len() % 40 == 0);
                assert_lower_hex(&model.pieces.pieces_hex, model.pieces.pieces_hex.len() / 2);
            } else {
                assert!(model.pieces.info_hash.is_empty());
                assert_eq!(model.pieces.piece_length, 0);
                assert!(model.pieces.pieces_hex.is_empty());
            }
        }
    }

    let single = &rows[1].expected.models[0];
    assert_eq!(single.label, "v1_single_default");
    assert_eq!(single.name, "synthetic-single.bin");
    assert_eq!(single.size, 4_096);
    assert!(!single.private);
    assert_eq!(single.files_status, "single");
    assert_eq!(single.files_count, 0);
    assert!(!single.files_count_valid);
    assert!(single.files.is_none());
    assert!(single.decoded_files.is_none());
    assert!(single.file_extensions.is_none());
    assert!(!single.files_data_present);
    assert!(single.files_data_nil);
    assert_eq!(single.files_data_byte_length, 0);
    assert!(single.files_data_sha256.is_empty());
    assert!(single.decoded_files_match_retained_core_fields);
    assert!(!single.pieces.present);
    assert!(single.summary.is_none());

    let exact = &rows[2].expected.models[0];
    let over = &rows[2].expected.models[1];
    assert_eq!(
        (exact.label.as_str(), over.label.as_str()),
        ("exactly_n_files", "n_plus_one_files")
    );
    assert_eq!(
        (exact.files_status.as_str(), over.files_status.as_str()),
        ("multi", "over_threshold")
    );
    assert_eq!((exact.files_count, over.files_count), (3, 4));
    assert!(!exact.private && !over.private);
    assert_eq!(
        (exact.name.as_str(), exact.size),
        ("threshold-exact", 1_500)
    );
    assert_eq!((over.name.as_str(), over.size), ("threshold-over", 1_550));
    assert!(exact.files_count_valid && over.files_count_valid);
    assert_eq!(
        rows[2]
            .input
            .cases
            .iter()
            .map(|case| case.save_files_threshold)
            .collect::<Vec<_>>(),
        [3, 3]
    );
    assert!(rows[2].input.cases.iter().all(|case| case.save_pieces));
    let retained = vec![
        file_result(0, "media/video.mkv", 1_000, "", false),
        file_result(1, "media/subs.srt", 200, "", false),
        file_result(2, "media/audio.mp3", 300, "", false),
    ];
    let decoded = vec![
        file_result(0, "media/video.mkv", 1_000, "mkv", true),
        file_result(1, "media/subs.srt", 200, "srt", true),
        file_result(2, "media/audio.mp3", 300, "mp3", true),
    ];
    assert_eq!(exact.files.as_ref().unwrap(), &retained);
    assert_eq!(over.files.as_ref().unwrap(), &retained);
    assert_eq!(exact.decoded_files.as_ref().unwrap(), &decoded);
    assert_eq!(over.decoded_files.as_ref().unwrap(), &decoded);
    assert_eq!(
        (exact.files_data_byte_length, over.files_data_byte_length),
        (111, 111)
    );
    assert_eq!(
        exact.files_data_sha256,
        "96acd62521b68897a5280d07a76710d94309d4b26848af16281c8fc6ae5fd74a"
    );
    assert_eq!(over.files_data_sha256, exact.files_data_sha256);
    assert!(exact.decoded_files_match_retained_core_fields);
    assert!(over.decoded_files_match_retained_core_fields);
    assert_eq!(
        exact.file_extensions.as_ref().unwrap(),
        &string_vec(&["mkv", "mp3", "srt"])
    );
    assert_eq!(over.file_extensions, exact.file_extensions);
    assert_eq!(
        (exact.pieces.piece_length, over.pieces.piece_length),
        (32_768, 32_768)
    );
    assert_eq!(
        exact.pieces.pieces_hex,
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f2021222324252627"
    );
    assert_eq!(over.pieces.pieces_hex, exact.pieces.pieces_hex);
    for (model, summary) in [
        (exact, exact.summary.as_ref().unwrap()),
        (over, over.summary.as_ref().unwrap()),
    ] {
        assert_eq!(summary.info_hash, model.info_hash);
        assert_eq!(
            (
                summary.file_count,
                summary.total_size,
                summary.largest_file_size
            ),
            (3, 1_500, 1_000)
        );
        assert_eq!(summary.extensions, string_vec(&["mkv", "mp3", "srt"]));
        assert!(summary.has_video && summary.has_subtitle && summary.has_audio);
        assert!(summary.compressed_bytes_valid && summary.compressed_bytes_matches_files_data);
        assert_eq!(summary.compressed_bytes, 111);
        assert_eq!(summary.created_at, "2027-01-15T08:00:00.123456789Z");
        assert_eq!(summary.updated_at, summary.created_at);
    }

    let pure = &rows[3].expected.models[0];
    assert_eq!(
        (pure.info_hash.as_str(), pure.info_hash_v2.as_str()),
        (
            "8ed5899fc3eab8658426eacfc8e099435545c63c",
            "8ed5899fc3eab8658426eacfc8e099435545c63c2364d08b9d889b4a563f5adb",
        )
    );
    assert!(pure.info_hash_v1.is_empty());
    assert!(!pure.private);
    assert_eq!(
        (pure.name.as_str(), pure.size, pure.files_status.as_str()),
        ("movie.mkv", 1_500_000_000, "single")
    );
    assert_eq!(pure.files_count, 0);
    assert!(!pure.files_count_valid);
    assert!(pure.files.is_none());
    assert!(pure.decoded_files.is_none());
    assert!(pure.file_extensions.is_none());
    assert!(!pure.files_data_present);
    assert!(pure.files_data_nil);
    assert_eq!(pure.files_data_byte_length, 0);
    assert!(pure.files_data_sha256.is_empty());
    assert!(pure.decoded_files_match_retained_core_fields);
    assert!(!pure.pieces.present);
    assert!(pure.summary.is_none());
    let hybrid = &rows[3].expected.models[1];
    assert_eq!(
        (hybrid.info_hash_v1.as_str(), hybrid.info_hash_v2.as_str()),
        (
            "631a31dd0a46257d5078c0dee4e66e26f73e42ac",
            "d8dd32ac93357c368556af3ac1d95c9d76bd0dff6fa9833ecdac3d53134efabb",
        )
    );
    assert_eq!(
        (hybrid.files_count, hybrid.files.as_ref().unwrap().len()),
        (9, 9)
    );
    assert!(!hybrid.private);
    assert_eq!(hybrid.name, "bittorrent-v1-v2-hybrid-test");
    assert_eq!(hybrid.size, 895_544_883);
    assert_eq!(hybrid.files_status, "multi");
    assert!(hybrid.files_count_valid);
    let hybrid_core = [
        (
            "Darkroom (Stellar, 1994, Amiga ECS) HQ.mp4",
            6_535_405,
            "mp4",
        ),
        ("Spaceballs-StateOfTheArt.avi", 20_506_624, "avi"),
        (
            "cncd_fairlight-ceasefire_(all_falls_down)-1080p.mp4",
            342_230_630,
            "mp4",
        ),
        ("eld-dust.mkv", 61_638_604, "mkv"),
        (
            "fairlight_cncd-agenda_circling_forth-1080p30lq.mp4",
            277_889_766,
            "mp4",
        ),
        (
            "meet the deadline - Still _ Evoke 2014.mp4",
            44_577_773,
            "mp4",
        ),
        ("readme.txt", 61, "txt"),
        ("tbl-goa.avi", 26_296_320, "avi"),
        ("tbl-tint.mpg", 115_869_700, "mpg"),
    ];
    let hybrid_retained = hybrid_core
        .iter()
        .enumerate()
        .map(|(index, (path, size, _))| file_result(index as u64, path, *size, "", false))
        .collect::<Vec<_>>();
    let hybrid_decoded = hybrid_core
        .iter()
        .enumerate()
        .map(|(index, (path, size, extension))| {
            file_result(index as u64, path, *size, extension, true)
        })
        .collect::<Vec<_>>();
    assert_eq!(hybrid.files.as_ref().unwrap(), &hybrid_retained);
    assert_eq!(hybrid.decoded_files.as_ref().unwrap(), &hybrid_decoded);
    assert_eq!(
        hybrid
            .files
            .as_ref()
            .unwrap()
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        [
            "Darkroom (Stellar, 1994, Amiga ECS) HQ.mp4",
            "Spaceballs-StateOfTheArt.avi",
            "cncd_fairlight-ceasefire_(all_falls_down)-1080p.mp4",
            "eld-dust.mkv",
            "fairlight_cncd-agenda_circling_forth-1080p30lq.mp4",
            "meet the deadline - Still _ Evoke 2014.mp4",
            "readme.txt",
            "tbl-goa.avi",
            "tbl-tint.mpg",
        ]
    );
    assert_eq!(hybrid.files_data_byte_length, 357);
    assert_eq!(
        hybrid.files_data_sha256,
        "fec0e4033891cd5b09685b9f4edc69645de97870bf103035f76bd92296011747"
    );
    assert_eq!(
        hybrid.file_extensions.as_ref().unwrap(),
        &string_vec(&["avi", "mkv", "mp4", "mpg", "txt"])
    );
    assert!(hybrid.decoded_files_match_retained_core_fields);
    assert!(!hybrid.pieces.present);
    assert!(hybrid.summary.is_none());
}

#[test]
fn dedup_and_queue_rows_pin_go_evidence_without_claiming_rust_worker_execution() {
    let rows = fixtures();
    assert_eq!(
        rows[1].oracle.source_pinned_harness_steps,
        string_vec(&[
            "construct_fixed_v1_single_Info",
            "bencode_Info",
            "derive_requested_v1_hash_from_raw_info",
        ])
    );
    assert_eq!(
        rows[2].oracle.source_pinned_harness_steps,
        string_vec(&[
            "construct_fixed_exactly_N_and_N_plus_one_v1_Info_values",
            "bencode_each_Info_and_derive_requested_v1_hash",
            "supply_fixed_summary_clock_after_model_projection",
        ])
    );
    assert_eq!(rows[3].oracle.source_pinned_harness_steps, string_vec(&[
        "encode_fixed_structurally_valid_BEP52_top_level_single_info_dictionary_with_synthetic_32_byte_pieces_root",
        "derive_requested_truncated_v2_hash_from_raw_info",
        "load_SHA_pinned_hybrid_torrent_and_extract_raw_info_dictionary",
        "derive_requested_v1_hash_from_hybrid_raw_info",
    ]));
    assert_eq!(
        rows[3].input.cases[1].source_fixture.as_deref(),
        Some("internal/dhtcrawler/testdata/bittorrent-v2-hybrid-test.torrent")
    );
    assert!(rows[1].input.cases[0].source_fixture.is_none());

    let dedup = &rows[4];
    assert_eq!(
        dedup
            .input
            .dedup_cases
            .iter()
            .map(|case| case.label.as_str())
            .collect::<Vec<_>>(),
        [
            "existing_cross_primary_drops",
            "existing_same_primary_kept",
            "batch_first_v2_primary_wins_stable_order",
            "same_primary_rediscovery_kept",
            "v1_without_v2_unaffected",
        ]
    );
    let p1 = "0101010101010101010101010101010101010101";
    let p2 = "0202020202020202020202020202020202020202";
    let p3 = "0303030303030303030303030303030303030303";
    let p4 = "0404040404040404040404040404040404040404";
    let v2a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let v2b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    assert_eq!(
        dedup.input.dedup_cases,
        vec![
            DedupCase {
                label: "existing_cross_primary_drops".to_owned(),
                items: vec![dedup_item(p2, v2a)],
                existing: vec![existing_v2(v2a, p1)],
            },
            DedupCase {
                label: "existing_same_primary_kept".to_owned(),
                items: vec![dedup_item(p1, v2a)],
                existing: vec![existing_v2(v2a, p1)],
            },
            DedupCase {
                label: "batch_first_v2_primary_wins_stable_order".to_owned(),
                items: vec![
                    dedup_item(p1, v2a),
                    dedup_item(p2, v2a),
                    dedup_item(p3, v2b),
                    dedup_item(p4, ""),
                ],
                existing: Vec::new(),
            },
            DedupCase {
                label: "same_primary_rediscovery_kept".to_owned(),
                items: vec![dedup_item(p1, v2a), dedup_item(p1, v2a)],
                existing: Vec::new(),
            },
            DedupCase {
                label: "v1_without_v2_unaffected".to_owned(),
                items: vec![dedup_item(p1, ""), dedup_item(p2, "")],
                existing: Vec::new(),
            },
        ]
    );
    assert_eq!(
        dedup
            .expected
            .dedup_cases
            .iter()
            .map(|case| case.label.as_str())
            .collect::<Vec<_>>(),
        [
            "existing_cross_primary_drops",
            "existing_same_primary_kept",
            "batch_first_v2_primary_wins_stable_order",
            "same_primary_rediscovery_kept",
            "v1_without_v2_unaffected",
        ]
    );
    for case in &dedup.input.dedup_cases {
        for item in &case.items {
            assert_lower_hex(&item.primary_info_hash, 20);
            if !item.info_hash_v2.is_empty() {
                assert_lower_hex(&item.info_hash_v2, 32);
            }
        }
        for item in &case.existing {
            assert_lower_hex(&item.primary_info_hash, 20);
            assert_lower_hex(&item.info_hash_v2, 32);
        }
    }
    assert_eq!(
        dedup.expected.dedup_cases[0].kept_primary_info_hashes,
        Vec::<String>::new()
    );
    assert_eq!(dedup.expected.dedup_cases[0].dropped, 1);
    assert_eq!(
        dedup.expected.dedup_cases[1].kept_primary_info_hashes,
        string_vec(&["0101010101010101010101010101010101010101"])
    );
    assert_eq!(dedup.expected.dedup_cases[1].dropped, 0);
    assert_eq!(
        dedup.expected.dedup_cases[2].kept_primary_info_hashes,
        string_vec(&[
            "0101010101010101010101010101010101010101",
            "0303030303030303030303030303030303030303",
            "0404040404040404040404040404040404040404",
        ])
    );
    assert_eq!(dedup.expected.dedup_cases[2].dropped, 1);
    assert_eq!(
        dedup.expected.dedup_cases[3].kept_primary_info_hashes,
        string_vec(&[
            "0101010101010101010101010101010101010101",
            "0101010101010101010101010101010101010101",
        ])
    );
    assert_eq!(dedup.expected.dedup_cases[3].dropped, 0);
    assert_eq!(
        dedup.expected.dedup_cases[4].kept_primary_info_hashes,
        string_vec(&[
            "0101010101010101010101010101010101010101",
            "0202020202020202020202020202020202020202",
        ])
    );
    assert_eq!(dedup.expected.dedup_cases[4].dropped, 0);

    let queue = &rows[5];
    let input = queue.input.classifier.as_ref().unwrap();
    let result = queue.expected.classifier.as_ref().unwrap();
    assert_eq!((input.unique_count, input.classify_batch_size), (101, 100));
    assert_eq!(
        input.duplicate_info_hash,
        "0000000000000000000000000000000000000001"
    );
    assert_eq!(
        (input.first_marker.as_str(), input.later_marker.as_str()),
        ("first", "later_duplicate")
    );
    assert_eq!((result.input_count, result.unique_count), (102, 101));
    assert_eq!(
        result.duplicate_info_hashes,
        vec![input.duplicate_info_hash.clone()]
    );
    assert_eq!(result.duplicate_winner_marker, input.first_marker);
    assert_eq!(
        result
            .classifier_groups
            .iter()
            .map(Vec::len)
            .collect::<Vec<_>>(),
        [100, 1]
    );
    let expected_hashes = (1_u64..=101)
        .map(|ordinal| format!("{ordinal:040x}"))
        .collect::<Vec<_>>();
    assert_eq!(result.classifier_groups.concat(), expected_hashes);
    assert_eq!(result.queue_jobs.len(), 2);
    for (index, (job, group)) in result
        .queue_jobs
        .iter()
        .zip(&result.classifier_groups)
        .enumerate()
    {
        let payload: QueuePayload = serde_json::from_str(&job.payload).unwrap();
        assert_eq!(payload.info_hashes, *group);
        let mut hasher = Sha256::new();
        hasher.update(job.queue.as_bytes());
        hasher.update(job.payload.as_bytes());
        assert_eq!(format!("{:x}", hasher.finalize()), job.fingerprint);
        assert_lower_hex(&job.fingerprint, 32);
        assert_eq!(job.queue, "process_torrent");
        assert_eq!(job.status, "pending");
        assert_eq!((job.retries, job.max_retries, job.priority), (0, 2, 0));
        assert_eq!(job.archival_duration_nanoseconds, 604_800_000_000_000);
        assert_eq!(job.delay_millis, 60_000);
        assert!(job.absolute_run_after_excluded);
        assert_eq!(
            job.fingerprint,
            [
                "8af5dc7aa891b1f7d854c57c86e84a581ed40264332c0096bb445fd6326490ad",
                "5c1a40dcb2b884d35289d63e7c3281aa9eef1ecf030ae6b0fe649f07bcfe987c",
            ][index]
        );
    }
    let mut payload_value =
        serde_json::from_str::<serde_json::Value>(&result.queue_jobs[0].payload).unwrap();
    payload_value
        .as_object_mut()
        .unwrap()
        .insert("Unknown".to_owned(), serde_json::Value::Bool(true));
    assert!(serde_json::from_value::<QueuePayload>(payload_value).is_err());

    assert!(RUST_EXECUTION_PARTITION[4]
        .1
        .contains("NO_RUST_DEDUP_EXECUTION"));
    assert!(RUST_EXECUTION_PARTITION[5]
        .1
        .contains("NO_CLASSIFIER_OR_QUEUE_WRITER"));
}
