//! Disabled-by-default `torrent.reprocess` through the Lane P writer kernel.
//!
//! Go's resolver does not enqueue: it runs `Processor.Process` synchronously
//! inside the request (`internal/gql/resolvers/mutation.resolvers.go`), which
//! is the same function the `process_torrent` queue worker calls. This adapter
//! therefore builds Go's exact `MessageParams` from the GraphQL input and runs
//! the existing load → classify → retry-publish → persist kernel.
//!
//! Only the content-attachment-free subset is served: Go's core workflow with
//! `local_search_enabled` and `apis_enabled` both explicitly off, which is what
//! the web UI sends by default. Anything the Rust classifier cannot reproduce
//! (TMDB or local content search, custom workflows from a merged
//! `classifier.yml`) fails closed before any write.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_graphql::{Context, MaybeUndefined, PathSegment, ServerError};
use async_trait::async_trait;
use bitmagnet_db::PgPool;
use bitmagnet_processor::{
    load_writer_plan, persist_writer_plan, BlockingManager as ProcessorBlockingManager, BoxError,
    MaterializeError, Materializer, PersistWriterPlanError, RetryPublishReceipt, RetryPublisher,
    WriterPlanError,
};
use bitmagnet_queue::{
    process_torrent_job, JobError, ProcessTorrentParams, ProtocolId, QueueJob, QueueJobOptions,
    QueueStore, CLASSIFY_MODE_DEFAULT, CLASSIFY_MODE_REMATCH,
};
use futures::future::BoxFuture;
use serde_json::Value;
use thiserror::Error;

use super::inputs::TorrentReprocessInput;

/// Go's `classifier.NewDefaultConfig().Workflow`, the only compiled workflow.
const DEFAULT_WORKFLOW: &str = "default";
/// Workflows Go reserves for validated tape evidence (`tape_plan.go`).
const TAPE_EVIDENCE_WORKFLOWS: [&str; 3] = [
    "tape_evidence_action_entries",
    "tape_evidence_unmatched",
    "tape_evidence_deleted",
];
/// The message gqlgen's default recover function renders for a resolver panic.
const GO_RECOVERED_PANIC_MESSAGE: &str = "internal system error";
/// Exact table grants for the writer, sorted like `information_schema`: the
/// processor loader's reads, the kernel's `torrent_contents` upsert and
/// stale-row delete, tag inserts, and the partial-failure retry insert. No
/// content, torrent-delete or blocking-store authority: those paths are
/// unreachable from the served subset.
const WRITER_TABLE_GRANTS: [(&str, &str); 14] = [
    ("content", "SELECT"),
    ("content_attributes", "SELECT"),
    ("content_collections", "SELECT"),
    ("content_collections_content", "SELECT"),
    ("goose_db_version", "SELECT"),
    ("queue_jobs", "INSERT"),
    ("torrent_contents", "DELETE"),
    ("torrent_contents", "INSERT"),
    ("torrent_contents", "SELECT"),
    ("torrent_contents", "UPDATE"),
    ("torrent_hints", "SELECT"),
    ("torrent_tags", "INSERT"),
    ("torrents", "SELECT"),
    ("torrents_torrent_sources", "SELECT"),
];
/// Column grants beyond the table grants: the idempotent retry insert's
/// `RETURNING id` and its `ON CONFLICT (fingerprint) WHERE status ...` arbiter.
const WRITER_COLUMN_GRANTS: [(&str, &str, &str); 3] = [
    ("queue_jobs", "fingerprint", "SELECT"),
    ("queue_jobs", "id", "SELECT"),
    ("queue_jobs", "status", "SELECT"),
];

/// Normalized request for `torrent.reprocess`.
///
/// `params` is exactly the `processor.MessageParams` value Go's resolver
/// builds: hashes in caller order with duplicates, the unresolved workflow
/// name, and only the flags the caller set.
#[derive(Debug, Clone)]
pub struct TorrentReprocessRequest {
    pub params: ProcessTorrentParams,
}

/// Typed failures from the torrent-reprocess adapter.
#[derive(Debug, Error)]
pub enum TorrentReprocessMutationsError {
    /// The schema was built without the separately authenticated writer.
    #[error("torrent reprocess mutations are disabled")]
    Disabled,
    /// The request needs a classifier path this runtime cannot reproduce.
    #[error("torrent reprocess is not supported by this Rust runtime: {0}")]
    Unsupported(String),
    /// Go's `errors.Join` ledger for a request in which nothing succeeded.
    #[error("{0}")]
    AllFailed(String),
    /// Loading, classification or validation failed closed before any write.
    #[error("torrent reprocess plan failed: {0}")]
    Plan(#[from] WriterPlanError),
    /// Retry publication or persistence failed.
    #[error("torrent reprocess write failed: {0}")]
    Persist(PersistWriterPlanError),
    /// The Go-compatible retry job could not be built.
    #[error("torrent reprocess retry job construction failed: {0}")]
    Job(#[from] JobError),
    /// The kernel's retry ledger held a hash that is not 40 hex characters.
    #[error("torrent reprocess retry ledger has an invalid info hash {0}")]
    InvalidRetryHash(String),
    /// A direct PostgreSQL read failed.
    #[error("torrent reprocess PostgreSQL read failed: {0}")]
    Database(#[from] sqlx::Error),
}

impl From<PersistWriterPlanError> for TorrentReprocessMutationsError {
    fn from(error: PersistWriterPlanError) -> Self {
        match error {
            PersistWriterPlanError::AllFailed { failures } => Self::AllFailed(failures.to_string()),
            other => Self::Persist(other),
        }
    }
}

/// Runtime seam for `torrent.reprocess`.
#[async_trait]
pub trait TorrentReprocessMutationsRuntime: Send + Sync {
    async fn reprocess(
        &self,
        request: TorrentReprocessRequest,
    ) -> Result<(), TorrentReprocessMutationsError>;
}

struct DisabledTorrentReprocessMutationsRuntime;

#[async_trait]
impl TorrentReprocessMutationsRuntime for DisabledTorrentReprocessMutationsRuntime {
    async fn reprocess(
        &self,
        _request: TorrentReprocessRequest,
    ) -> Result<(), TorrentReprocessMutationsError> {
        Err(TorrentReprocessMutationsError::Disabled)
    }
}

/// PostgreSQL implementation backed by a caller-owned, separately authorized pool.
pub struct PgTorrentReprocessMutationsRuntime {
    pool: PgPool,
    queue: QueueStore,
    materializer: Materializer,
}

impl PgTorrentReprocessMutationsRuntime {
    /// Compiles the embedded core classifier once for the process lifetime.
    ///
    /// # Errors
    /// Returns an error if the embedded core classifier fails to compile.
    pub fn new(pool: PgPool) -> Result<Self, MaterializeError> {
        Ok(Self {
            queue: QueueStore::new(pool.clone()),
            pool,
            materializer: Materializer::from_core()?,
        })
    }
}

#[async_trait]
impl TorrentReprocessMutationsRuntime for PgTorrentReprocessMutationsRuntime {
    async fn reprocess(
        &self,
        request: TorrentReprocessRequest,
    ) -> Result<(), TorrentReprocessMutationsError> {
        let params = request.params;
        // Go's `infoHashCriteria` renders an empty list as `FALSE`: nothing is
        // loaded, nothing is missing, and `Process` returns nil.
        if params.info_hashes.is_empty() {
            return Ok(());
        }

        let workflow = if params.classifier_workflow.is_empty() {
            DEFAULT_WORKFLOW
        } else {
            params.classifier_workflow.as_str()
        };
        if workflow != DEFAULT_WORKFLOW {
            return Err(TorrentReprocessMutationsError::AllFailed(
                self.unknown_workflow_failures(&params.info_hashes, workflow)
                    .await?,
            ));
        }
        let classification_params = classification_params(&params)?;

        let plan = load_writer_plan(&self.pool, &self.materializer, &classification_params).await?;
        let retry_job = if plan.retry_job().is_some() {
            Some(go_retry_job(&params, plan.retry_info_hashes())?)
        } else {
            None
        };
        let publisher = GoRetryPublisher {
            store: &self.queue,
            job: retry_job,
        };
        persist_writer_plan(&self.pool, &plan, &publisher, &RefusingBlockingManager).await?;
        Ok(())
    }
}

impl PgTorrentReprocessMutationsRuntime {
    /// Reproduce Go's all-failed ledger for a workflow the runner lacks.
    ///
    /// Every loaded torrent fails with the same classifier error, so the
    /// goroutine completion order Go joins them in cannot be observed. No row
    /// succeeds, so Go writes nothing, retry job included.
    async fn unknown_workflow_failures(
        &self,
        info_hashes: &[ProtocolId],
        workflow: &str,
    ) -> Result<String, TorrentReprocessMutationsError> {
        let requested = info_hashes
            .iter()
            .map(|info_hash| info_hash.as_bytes().to_vec())
            .collect::<Vec<_>>();
        let found = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT info_hash FROM torrents WHERE info_hash = ANY($1::bytea[])",
        )
        .bind(&requested)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();

        let missing = requested
            .iter()
            .filter(|info_hash| !found.contains(*info_hash))
            .count();
        let classifier_error = if TAPE_EVIDENCE_WORKFLOWS.contains(&workflow) {
            format!(
                "workflow {} is reserved for validated classifier tape evidence",
                go_quote(workflow)
            )
        } else {
            format!("workflow not found: {workflow}")
        };
        let mut causes = Vec::with_capacity(found.len() + 1);
        if missing > 0 {
            causes.push(format!("missing {missing} info hashes"));
        }
        causes.extend(std::iter::repeat_n(classifier_error, found.len()));
        Ok(causes.join("\n"))
    }
}

/// The flag image the Rust classifier runs with.
///
/// The writer kernel admits only explicitly attachment-free flags. The core
/// workflow reads `tmdb_enabled` solely as `flags.apis_enabled &&
/// flags.tmdb_enabled`, so with `apis_enabled` off its value cannot change a
/// classification and pinning it off is exact. The guard test
/// `core_workflow_reads_tmdb_enabled_only_behind_apis_enabled` holds that
/// invariant. The retry payload is rebuilt from Go's un-injected params.
fn classification_params(
    params: &ProcessTorrentParams,
) -> Result<ProcessTorrentParams, TorrentReprocessMutationsError> {
    let flags = params.classifier_flags.clone().unwrap_or_default();
    for name in ["local_search_enabled", "apis_enabled"] {
        if flags.get(name) != Some(&Value::Bool(false)) {
            return Err(TorrentReprocessMutationsError::Unsupported(format!(
                "{name} must be disabled (the Rust classifier has no content search or API integration)"
            )));
        }
    }
    let mut flags = flags;
    flags.insert("tmdb_enabled".to_owned(), Value::Bool(false));
    Ok(ProcessTorrentParams {
        classify_mode: params.classify_mode,
        classifier_workflow: DEFAULT_WORKFLOW.to_owned(),
        classifier_flags: Some(flags),
        info_hashes: params.info_hashes.clone(),
    })
}

/// Go's `NewQueueJob(MessageParams{failedHashes, mode, workflowName, flags})`.
///
/// `workflowName` is the resolved default, never empty, and the flags are the
/// caller's map, so the payload and fingerprint match Go's republish exactly.
fn go_retry_job(
    params: &ProcessTorrentParams,
    retry_info_hashes: &[String],
) -> Result<QueueJob, TorrentReprocessMutationsError> {
    let info_hashes = retry_info_hashes
        .iter()
        .map(|info_hash| {
            ProtocolId::from_hex(info_hash)
                .map_err(|_| TorrentReprocessMutationsError::InvalidRetryHash(info_hash.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(process_torrent_job(
        &ProcessTorrentParams {
            classify_mode: params.classify_mode,
            classifier_workflow: DEFAULT_WORKFLOW.to_owned(),
            classifier_flags: params.classifier_flags.clone(),
            info_hashes,
        },
        QueueJobOptions::default(),
    )?)
}

/// Publishes the Go-exact retry job in place of the kernel's flag-pinned one.
struct GoRetryPublisher<'a> {
    store: &'a QueueStore,
    job: Option<QueueJob>,
}

impl RetryPublisher for GoRetryPublisher<'_> {
    fn publish_retry<'a>(
        &'a self,
        _kernel_job: &'a QueueJob,
    ) -> BoxFuture<'a, Result<RetryPublishReceipt, BoxError>> {
        Box::pin(async move {
            let job = self
                .job
                .as_ref()
                .ok_or("torrent reprocess retry job was not prepared")?;
            self.store
                .insert_job_idempotent(job)
                .await
                .map_err(|error| Box::new(error) as BoxError)
        })
    }
}

/// Torrent deletion is unreachable from GraphQL under the core flags.
///
/// Only `delete_xxx` and `delete_content_types` produce a delete outcome, and
/// neither can be set through `TorrentReprocessInput`. Refusing keeps the
/// writer role free of bloom-filter large-object authority.
struct RefusingBlockingManager;

impl ProcessorBlockingManager for RefusingBlockingManager {
    fn block<'a>(&'a self, _info_hashes: &'a [String]) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async {
            Err("torrent reprocess does not delete torrents in this Rust runtime".into())
        })
    }
}

/// GraphQL context wrapper for the torrent-reprocess runtime.
#[derive(Clone)]
pub struct TorrentReprocessMutationsRuntimeData(Arc<dyn TorrentReprocessMutationsRuntime>);

impl TorrentReprocessMutationsRuntimeData {
    /// Wraps an enabled runtime.
    #[must_use]
    pub fn new(runtime: Arc<dyn TorrentReprocessMutationsRuntime>) -> Self {
        Self(runtime)
    }

    /// Constructs the default fail-loud runtime.
    #[must_use]
    pub fn disabled() -> Self {
        Self::new(Arc::new(DisabledTorrentReprocessMutationsRuntime))
    }

    /// Constructs the production PostgreSQL writer runtime.
    ///
    /// # Errors
    /// Returns an error if the embedded core classifier fails to compile.
    pub fn pg(pool: PgPool) -> Result<Self, MaterializeError> {
        Ok(Self::new(Arc::new(
            PgTorrentReprocessMutationsRuntime::new(pool)?,
        )))
    }
}

/// Admit the exact steady-state PostgreSQL authority used by `torrent.reprocess`.
///
/// The writer must be a plain non-inheriting login role with no membership,
/// no object-creation authority, and exactly [`WRITER_TABLE_GRANTS`] plus
/// [`WRITER_COLUMN_GRANTS`] (directly or through `PUBLIC`).
///
/// # Errors
/// Returns an error describing the first authority mismatch.
pub async fn admit_torrent_reprocess_writer_authority(pool: &PgPool) -> anyhow::Result<()> {
    let (role_oid, attributes) = sqlx::query_as::<
        _,
        (
            sqlx::postgres::types::Oid,
            (bool, bool, bool, bool, bool, bool, bool),
        ),
    >(
        "SELECT oid, (rolcanlogin, rolinherit, rolsuper, rolcreatedb, rolcreaterole, \
                rolreplication, rolbypassrls) \
         FROM pg_catalog.pg_roles WHERE rolname = current_user",
    )
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        attributes == (true, false, false, false, false, false, false),
        "GraphQL torrent-reprocess writer must be LOGIN NOINHERIT NOSUPERUSER NOCREATEDB \
         NOCREATEROLE NOREPLICATION NOBYPASSRLS"
    );
    let memberships = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)::bigint FROM pg_catalog.pg_auth_members \
         WHERE member = $1::oid OR roleid = $1::oid",
    )
    .bind(role_oid)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        memberships == 0,
        "GraphQL torrent-reprocess writer must have no role memberships or members"
    );
    let creation = sqlx::query_as::<_, (bool, bool, bool)>(
        "SELECT has_database_privilege(current_user, current_database(), 'CREATE'), \
                has_database_privilege(current_user, current_database(), 'TEMPORARY'), \
                has_schema_privilege(current_user, 'public', 'CREATE')",
    )
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        creation == (false, false, false),
        "GraphQL torrent-reprocess writer must not hold database CREATE/TEMPORARY or \
         public-schema CREATE"
    );

    let table_grants = sqlx::query_as::<_, (String, String)>(
        "SELECT DISTINCT table_name::text, privilege_type::text \
         FROM information_schema.table_privileges \
         WHERE grantee IN (current_user, 'PUBLIC') AND table_schema = 'public' \
         ORDER BY 1, 2",
    )
    .fetch_all(pool)
    .await?;
    anyhow::ensure!(
        table_grants
            .iter()
            .map(|(table, privilege)| (table.as_str(), privilege.as_str()))
            .eq(WRITER_TABLE_GRANTS),
        "GraphQL torrent-reprocess writer does not have the exact table grants; \
         observed {table_grants:?}"
    );
    let column_grants = sqlx::query_as::<_, (String, String, String)>(
        "SELECT DISTINCT cp.table_name::text, cp.column_name::text, cp.privilege_type::text \
         FROM information_schema.column_privileges cp \
         WHERE cp.grantee IN (current_user, 'PUBLIC') AND cp.table_schema = 'public' \
           AND NOT EXISTS ( \
             SELECT 1 FROM information_schema.table_privileges tp \
              WHERE tp.grantee IN (current_user, 'PUBLIC') \
                AND tp.table_schema = cp.table_schema \
                AND tp.table_name = cp.table_name \
                AND tp.privilege_type = cp.privilege_type) \
         ORDER BY 1, 2, 3",
    )
    .fetch_all(pool)
    .await?;
    anyhow::ensure!(
        column_grants
            .iter()
            .map(|(table, column, privilege)| (table.as_str(), column.as_str(), privilege.as_str()))
            .eq(WRITER_COLUMN_GRANTS),
        "GraphQL torrent-reprocess writer does not have the exact column grants; \
         observed {column_grants:?}"
    );
    Ok(())
}

/// A failure positioned where gqlgen reports it.
#[derive(Debug)]
struct ReprocessFailure {
    message: String,
    /// Segments below `torrent.reprocess`; non-empty only for input coercion.
    input_path: Vec<PathSegment>,
}

impl ReprocessFailure {
    fn at_field(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            input_path: Vec::new(),
        }
    }
}

/// Run the mutation and report any failure out of band.
///
/// gqlgen nulls only the failed nullable `reprocess` field and reports an
/// input-coercion error at the argument's own path. async-graphql 7 instead
/// nulls the whole `torrent` selection once every selected field has failed,
/// and positions resolver errors at the field. Recording the error on the
/// context and resolving to `null` reproduces gqlgen's response exactly.
pub(super) async fn resolve(
    ctx: &Context<'_>,
    runtime: &TorrentReprocessMutationsRuntimeData,
    input: TorrentReprocessInput,
) {
    let result = match normalize(input) {
        Ok(request) => runtime
            .0
            .reprocess(request)
            .await
            .map_err(|error| ReprocessFailure::at_field(error.to_string())),
        Err(failure) => Err(failure),
    };
    if let Err(failure) = result {
        let mut error = ctx.set_error_path(ServerError::new(failure.message, None));
        error.path.extend(failure.input_path);
        ctx.add_error(error);
    }
}

/// Build Go's `MessageParams` in the resolver's exact statement order.
///
/// gqlgen hands each optional field over as `Omittable[*T]`; the resolver
/// dereferences `ValueOK()` without a nil check, so an explicit `null` panics
/// and gqlgen recovers it as `internal system error` before any database work.
fn normalize(input: TorrentReprocessInput) -> Result<TorrentReprocessRequest, ReprocessFailure> {
    let info_hashes = input
        .info_hashes
        .into_iter()
        .enumerate()
        .map(|(index, hash)| {
            parse_go_hash20(&hash.0).map_err(|message| ReprocessFailure {
                message,
                input_path: vec![
                    PathSegment::Field("input".to_owned()),
                    PathSegment::Field("infoHashes".to_owned()),
                    PathSegment::Index(index),
                ],
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let panic = || ReprocessFailure::at_field(GO_RECOVERED_PANIC_MESSAGE);
    let classifier_workflow = match input.classifier_workflow {
        MaybeUndefined::Undefined => String::new(),
        MaybeUndefined::Null => return Err(panic()),
        MaybeUndefined::Value(workflow) => workflow,
    };
    let classify_mode = match input.classifier_rematch {
        MaybeUndefined::Undefined | MaybeUndefined::Value(false) => CLASSIFY_MODE_DEFAULT,
        MaybeUndefined::Null => return Err(panic()),
        MaybeUndefined::Value(true) => CLASSIFY_MODE_REMATCH,
    };
    let mut classifier_flags = BTreeMap::new();
    for (name, disabled) in [
        ("apis_enabled", input.apis_disabled),
        ("local_search_enabled", input.local_search_disabled),
    ] {
        match disabled {
            MaybeUndefined::Undefined => {}
            MaybeUndefined::Null => return Err(panic()),
            MaybeUndefined::Value(disabled) => {
                classifier_flags.insert(name.to_owned(), Value::Bool(!disabled));
            }
        }
    }

    Ok(TorrentReprocessRequest {
        params: ProcessTorrentParams {
            classify_mode,
            classifier_workflow,
            // Go always passes a non-nil map; `omitempty` drops it when empty.
            classifier_flags: Some(classifier_flags),
            info_hashes,
        },
    })
}

/// Go's `protocol.ParseID`: trim one `0x`, decode hex of either case, and
/// require exactly 20 bytes, with `encoding/hex`'s exact error messages.
fn parse_go_hash20(raw: &str) -> Result<ProtocolId, String> {
    let src = raw.strip_prefix("0x").unwrap_or(raw).as_bytes();
    let mut decoded = Vec::with_capacity(src.len() / 2);
    let (pairs, remainder) = src.as_chunks::<2>();
    for &[high, low] in pairs {
        let high = hex_value(high).ok_or_else(|| go_invalid_hex_byte(high))?;
        let low = hex_value(low).ok_or_else(|| go_invalid_hex_byte(low))?;
        decoded.push((high << 4) | low);
    }
    if let [last] = remainder {
        // Go reports an invalid trailing byte before the odd length.
        return Err(match hex_value(*last) {
            Some(_) => "encoding/hex: odd length hex string".to_owned(),
            None => go_invalid_hex_byte(*last),
        });
    }
    ProtocolId::try_from(decoded.as_slice()).map_err(|_| "hash string must be 20 bytes".to_owned())
}

const fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// `hex.InvalidByteError.Error()`: `fmt.Sprintf("%#U", rune(byte))`.
fn go_invalid_hex_byte(byte: u8) -> String {
    let printable = matches!(byte, 0x20..=0x7e) || (matches!(byte, 0xa1..=0xff) && byte != 0xad);
    if printable {
        format!(
            "encoding/hex: invalid byte: U+{:04X} '{}'",
            byte,
            char::from(byte)
        )
    } else {
        format!("encoding/hex: invalid byte: U+{byte:04X}")
    }
}

/// Go's `strconv.Quote` for the reserved-workflow message.
fn go_quote(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for character in value.chars() {
        match character {
            '\x07' => quoted.push_str("\\a"),
            '\x08' => quoted.push_str("\\b"),
            '\x0c' => quoted.push_str("\\f"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            '\x0b' => quoted.push_str("\\v"),
            '\\' => quoted.push_str("\\\\"),
            '"' => quoted.push_str("\\\""),
            ' '..='~' => quoted.push(character),
            other if u32::from(other) < 0x20 || u32::from(other) == 0x7f => {
                quoted.push_str(&format!("\\x{:02x}", u32::from(other)));
            }
            other => quoted.push(other),
        }
    }
    quoted.push('"');
    quoted
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_graphql::{value, EmptySubscription};

    use super::*;
    use crate::schema::roots::{Mutation, Query};

    const HASH_A: &str = "0123456789abcdef0123456789abcdef01234567";
    const HASH_B: &str = "1111111111111111111111111111111111111111";

    struct FakeRuntime {
        calls: Arc<Mutex<Vec<TorrentReprocessRequest>>>,
    }

    #[async_trait]
    impl TorrentReprocessMutationsRuntime for FakeRuntime {
        async fn reprocess(
            &self,
            request: TorrentReprocessRequest,
        ) -> Result<(), TorrentReprocessMutationsError> {
            self.calls.lock().expect("calls lock").push(request);
            Ok(())
        }
    }

    fn schema_with_fake(calls: Arc<Mutex<Vec<TorrentReprocessRequest>>>) -> crate::schema::Schema {
        let runtime: Arc<dyn TorrentReprocessMutationsRuntime> = Arc::new(FakeRuntime { calls });
        async_graphql::Schema::build(Query, Mutation, EmptySubscription)
            .data(TorrentReprocessMutationsRuntimeData::new(runtime))
            .finish()
    }

    fn id(raw: &str) -> ProtocolId {
        ProtocolId::from_hex(raw).expect("test hash")
    }

    fn flags(entries: &[(&str, bool)]) -> Option<BTreeMap<String, Value>> {
        Some(
            entries
                .iter()
                .map(|(name, value)| ((*name).to_owned(), Value::Bool(*value)))
                .collect(),
        )
    }

    #[tokio::test]
    async fn graphql_reprocess_builds_go_message_params_and_returns_void() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let response = schema_with_fake(Arc::clone(&calls))
            .execute(format!(
                "mutation {{ torrent {{ reprocess(input: {{ \
                 infoHashes: [\"{HASH_A}\", \"0x{HASH_B}\", \"{HASH_A}\"], \
                 classifierRematch: true, apisDisabled: true, localSearchDisabled: false \
                 }}) }} }}"
            ))
            .await;

        assert!(response.errors.is_empty(), "errors: {:?}", response.errors);
        assert_eq!(response.data, value!({ "torrent": { "reprocess": null } }));
        let calls = calls.lock().expect("calls lock");
        assert_eq!(calls.len(), 1);
        assert_eq!(
            serde_json::to_value(&calls[0].params).expect("params JSON"),
            serde_json::json!({
                "ClassifyMode": 1,
                "ClassifierFlags": { "apis_enabled": false, "local_search_enabled": true },
                "InfoHashes": [HASH_A, HASH_B, HASH_A],
            })
        );
    }

    #[tokio::test]
    async fn omitted_fields_leave_go_defaults_and_an_empty_flag_map() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let response = schema_with_fake(Arc::clone(&calls))
            .execute(
                "mutation { torrent { reprocess(input: { infoHashes: [], \
                 classifierWorkflow: \"custom\", classifierRematch: false }) } }",
            )
            .await;

        assert!(response.errors.is_empty(), "errors: {:?}", response.errors);
        let calls = calls.lock().expect("calls lock");
        let params = &calls[0].params;
        assert_eq!(params.classify_mode, CLASSIFY_MODE_DEFAULT);
        assert_eq!(params.classifier_workflow, "custom");
        assert_eq!(params.classifier_flags, Some(BTreeMap::new()));
        assert!(params.info_hashes.is_empty());
    }

    #[tokio::test]
    async fn explicit_nulls_fail_like_the_recovered_go_nil_dereference() {
        for field in [
            "classifierWorkflow",
            "classifierRematch",
            "apisDisabled",
            "localSearchDisabled",
        ] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let response = schema_with_fake(Arc::clone(&calls))
                .execute(format!(
                    "mutation {{ torrent {{ reprocess(input: {{ infoHashes: [], {field}: null }}) }} }}"
                ))
                .await;
            assert_eq!(response.errors.len(), 1, "{field}");
            assert_eq!(response.errors[0].message, "internal system error");
            assert_eq!(response.data, value!({ "torrent": { "reprocess": null } }));
            assert!(calls.lock().expect("calls lock").is_empty(), "{field}");
        }
    }

    #[test]
    fn hash_parsing_matches_go_protocol_parse_id() {
        assert_eq!(parse_go_hash20(HASH_A), Ok(id(HASH_A)));
        assert_eq!(
            parse_go_hash20(&format!("0x{}", HASH_A.to_uppercase())),
            Ok(id(HASH_A))
        );
        for (raw, message) in [
            ("not-a-hash", "encoding/hex: invalid byte: U+006E 'n'"),
            ("0g", "encoding/hex: invalid byte: U+0067 'g'"),
            ("abc", "encoding/hex: odd length hex string"),
            ("abz", "encoding/hex: invalid byte: U+007A 'z'"),
            ("ab", "hash string must be 20 bytes"),
            ("", "hash string must be 20 bytes"),
            ("0x0x", "encoding/hex: invalid byte: U+0078 'x'"),
            ("\u{e9}0", "encoding/hex: invalid byte: U+00C3 '\u{c3}'"),
            ("\u{1}0", "encoding/hex: invalid byte: U+0001"),
            ("a\u{a0}", "encoding/hex: invalid byte: U+00C2 '\u{c2}'"),
        ] {
            assert_eq!(parse_go_hash20(raw), Err(message.to_owned()), "{raw:?}");
        }
    }

    #[test]
    fn go_quote_matches_strconv_quote() {
        assert_eq!(
            go_quote("tape_evidence_deleted"),
            "\"tape_evidence_deleted\""
        );
        assert_eq!(go_quote("a\"b\\c\n\u{1}é"), "\"a\\\"b\\\\c\\n\\x01é\"");
    }

    #[tokio::test]
    async fn invalid_hash_fails_before_the_runtime() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let response = schema_with_fake(Arc::clone(&calls))
            .execute("mutation { torrent { reprocess(input: { infoHashes: [\"zz\"] }) } }")
            .await;
        assert_eq!(response.errors.len(), 1);
        assert_eq!(
            response.errors[0].message,
            "encoding/hex: invalid byte: U+007A 'z'"
        );
        assert_eq!(
            serde_json::to_value(&response.errors[0].path).expect("path JSON"),
            serde_json::json!(["torrent", "reprocess", "input", "infoHashes", 0])
        );
        assert_eq!(response.data, value!({ "torrent": { "reprocess": null } }));
        assert!(calls.lock().expect("calls lock").is_empty());
    }

    #[test]
    fn classification_params_require_attachment_free_flags_and_pin_tmdb() {
        let base = ProcessTorrentParams {
            classify_mode: CLASSIFY_MODE_REMATCH,
            classifier_workflow: String::new(),
            classifier_flags: flags(&[("apis_enabled", false), ("local_search_enabled", false)]),
            info_hashes: vec![id(HASH_A)],
        };
        let pinned = classification_params(&base).expect("supported flags");
        assert_eq!(pinned.classifier_workflow, "default");
        assert_eq!(pinned.classify_mode, CLASSIFY_MODE_REMATCH);
        assert_eq!(
            pinned.classifier_flags,
            flags(&[
                ("apis_enabled", false),
                ("local_search_enabled", false),
                ("tmdb_enabled", false),
            ])
        );

        for unsupported in [
            flags(&[]),
            flags(&[("apis_enabled", false)]),
            flags(&[("local_search_enabled", false)]),
            flags(&[("apis_enabled", true), ("local_search_enabled", false)]),
            flags(&[("apis_enabled", false), ("local_search_enabled", true)]),
        ] {
            let params = ProcessTorrentParams {
                classifier_flags: unsupported.clone(),
                ..base.clone()
            };
            assert!(
                matches!(
                    classification_params(&params),
                    Err(TorrentReprocessMutationsError::Unsupported(_))
                ),
                "{unsupported:?}"
            );
        }
    }

    #[test]
    fn retry_job_uses_go_params_with_the_resolved_workflow() {
        let params = ProcessTorrentParams {
            classify_mode: CLASSIFY_MODE_REMATCH,
            classifier_workflow: String::new(),
            classifier_flags: flags(&[("apis_enabled", false), ("local_search_enabled", false)]),
            info_hashes: vec![id(HASH_A), id(HASH_B)],
        };
        let job =
            go_retry_job(&params, &[HASH_B.to_owned(), HASH_B.to_owned()]).expect("retry job");
        assert_eq!(job.queue, "process_torrent");
        assert_eq!(job.max_retries, 2);
        assert_eq!(
            job.payload,
            format!(
                "{{\"ClassifyMode\":1,\"ClassifierWorkflow\":\"default\",\
                 \"ClassifierFlags\":{{\"apis_enabled\":false,\"local_search_enabled\":false}},\
                 \"InfoHashes\":[\"{HASH_B}\",\"{HASH_B}\"]}}"
            )
        );
    }

    /// Pins the invariant that makes `classification_params` exact: in the core
    /// classifier every read of `tmdb_enabled` is conjoined with `apis_enabled`.
    #[test]
    fn core_workflow_reads_tmdb_enabled_only_behind_apis_enabled() {
        let core = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../internal/classifier/classifier.core.yml"
        ));
        let guarded = "flags.apis_enabled && flags.tmdb_enabled";
        let reads = core.matches("flags.tmdb_enabled").count();
        assert!(reads > 0, "core classifier no longer reads tmdb_enabled");
        assert_eq!(
            core.matches(guarded).count(),
            reads,
            "a core workflow condition reads tmdb_enabled outside `{guarded}`"
        );
        assert!(
            !core.contains("flags.apis_enabled ||"),
            "apis_enabled is read disjunctively"
        );
    }

    #[tokio::test]
    async fn disabled_runtime_fails_loudly() {
        let schema = async_graphql::Schema::build(Query, Mutation, EmptySubscription)
            .data(TorrentReprocessMutationsRuntimeData::disabled())
            .finish();
        let response = schema
            .execute("mutation { torrent { reprocess(input: { infoHashes: [] }) } }")
            .await;
        assert_eq!(response.errors.len(), 1);
        assert_eq!(
            response.errors[0].message,
            "torrent reprocess mutations are disabled"
        );
    }
}
