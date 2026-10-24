//! Disabled-by-default torrent deletion through the Go-compatible blocking store.

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context as _;
use async_graphql::{Error, Result};
use async_trait::async_trait;
use bitmagnet_blocking::{BlockingError, BlockingManager};
use bitmagnet_db::PgPool;
use bitmagnet_model::InfoHash;
use sqlx::postgres::types::Oid;
use thiserror::Error;

use super::scalars::Hash20;

/// Maximum raw hashes accepted by one `torrent.delete` mutation.
pub const MAX_TORRENT_DELETE_INFO_HASHES: usize = 10_000;
const BLOCKED_TORRENTS_KEY: &str = "blocked_torrents";
const BLOCKED_TORRENTS_FILTER_BYTES: i32 = 25_000_091;
const BLOCKED_TORRENTS_BOUNDED_READ_BYTES: i32 = BLOCKED_TORRENTS_FILTER_BYTES + 1;
const DELETION_TRIGGER_BODY: &str = "
BEGIN
  INSERT INTO deleted_torrents (info_hash, deleted_at)
  VALUES (OLD.info_hash, now())
  ON CONFLICT (info_hash) DO UPDATE SET deleted_at = now();
  RETURN OLD;
END
";

/// The production object identity admitted for the dedicated delete writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TorrentDeleteWriterAdmission {
    /// Existing Go-owned `blocked_torrents` large-object OID.
    pub blocked_torrents_oid: u32,
}

/// Normalized request for `torrent.delete`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentDeleteRequest {
    /// Deduplicated hashes in caller order, matching the Go manager's map buffer.
    pub info_hashes: Vec<InfoHash>,
}

/// Typed failures from the torrent-delete adapter.
#[derive(Debug, Error)]
pub enum TorrentDeleteMutationsError {
    /// The schema was built without the separately authenticated delete writer.
    #[error("torrent delete mutations are disabled")]
    Disabled,
    /// The atomic torrent-delete and bloom-filter checkpoint failed.
    #[error("torrent delete blocking-store write failed: {0}")]
    Blocking(#[from] BlockingError),
}

/// Runtime seam for `torrent.delete`.
#[async_trait]
pub trait TorrentDeleteMutationsRuntime: Send + Sync {
    async fn delete(
        &self,
        request: TorrentDeleteRequest,
    ) -> std::result::Result<(), TorrentDeleteMutationsError>;
}

struct DisabledTorrentDeleteMutationsRuntime;

#[async_trait]
impl TorrentDeleteMutationsRuntime for DisabledTorrentDeleteMutationsRuntime {
    async fn delete(
        &self,
        _request: TorrentDeleteRequest,
    ) -> std::result::Result<(), TorrentDeleteMutationsError> {
        Err(TorrentDeleteMutationsError::Disabled)
    }
}

/// PostgreSQL implementation backed by a caller-owned, separately authorized pool.
pub struct PgTorrentDeleteMutationsRuntime {
    manager: BlockingManager,
}

impl PgTorrentDeleteMutationsRuntime {
    /// Constructs one serialized production blocking manager.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            manager: BlockingManager::new(pool),
        }
    }
}

#[async_trait]
impl TorrentDeleteMutationsRuntime for PgTorrentDeleteMutationsRuntime {
    async fn delete(
        &self,
        request: TorrentDeleteRequest,
    ) -> std::result::Result<(), TorrentDeleteMutationsError> {
        // Go's resolver always forces a checkpoint, including for an empty list.
        self.manager.block(&request.info_hashes, true).await?;
        Ok(())
    }
}

/// GraphQL context wrapper for the torrent-delete runtime.
#[derive(Clone)]
pub struct TorrentDeleteMutationsRuntimeData(Arc<dyn TorrentDeleteMutationsRuntime>);

impl TorrentDeleteMutationsRuntimeData {
    /// Wraps an enabled runtime.
    #[must_use]
    pub fn new(runtime: Arc<dyn TorrentDeleteMutationsRuntime>) -> Self {
        Self(runtime)
    }

    /// Constructs the default fail-loud runtime.
    #[must_use]
    pub fn disabled() -> Self {
        Self::new(Arc::new(DisabledTorrentDeleteMutationsRuntime))
    }

    /// Constructs the production PostgreSQL writer runtime.
    #[must_use]
    pub fn pg(pool: PgPool) -> Self {
        Self::new(Arc::new(PgTorrentDeleteMutationsRuntime::new(pool)))
    }
}

/// Admit the exact steady-state PostgreSQL authority used by `torrent.delete`.
///
/// The GraphQL writer may update only an existing, non-zero, non-writer-owned
/// Go-compatible large object. It cannot create or replace bloom metadata, own
/// tables/schemas or inherit another role. The exact executable large-object
/// routine allowlist excludes object creation, import, descriptor access and
/// unlink operations.
pub async fn admit_torrent_delete_writer_authority(
    pool: &PgPool,
) -> anyhow::Result<TorrentDeleteWriterAdmission> {
    let attributes = sqlx::query_as::<_, (Oid, bool, bool, bool, bool, bool, bool, bool, i32)>(
        "SELECT oid, rolcanlogin, rolinherit, rolsuper, rolcreatedb, rolcreaterole, \
         rolreplication, rolbypassrls, rolconnlimit \
         FROM pg_catalog.pg_roles WHERE rolname = current_user",
    )
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        (
            attributes.1,
            attributes.2,
            attributes.3,
            attributes.4,
            attributes.5,
            attributes.6,
            attributes.7,
            attributes.8,
        ) == (true, false, false, false, false, false, false, 1),
        "GraphQL torrent-delete writer must be LOGIN NOINHERIT NOSUPERUSER NOCREATEDB \
         NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 1"
    );
    let role_oid = attributes.0;

    let memberships = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)::bigint FROM pg_catalog.pg_auth_members \
         WHERE member = $1::oid OR roleid = $1::oid",
    )
    .bind(role_oid)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        memberships == 0,
        "GraphQL torrent-delete writer must have no role memberships or members"
    );

    let authority = sqlx::query_as::<_, (bool, bool, bool, bool)>(
        "SELECT \
           has_database_privilege(current_user, current_database(), 'CREATE'), \
           has_database_privilege(current_user, current_database(), 'TEMPORARY'), \
           has_schema_privilege(current_user, 'public', 'USAGE'), \
           has_schema_privilege(current_user, 'public', 'CREATE')",
    )
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        authority == (false, false, true, false),
        "GraphQL torrent-delete writer requires public USAGE but no database CREATE/TEMPORARY \
         or public-schema CREATE"
    );
    let schema_authority = sqlx::query_as::<_, (String, bool, bool)>(
        "SELECT n.nspname::text, \
                has_schema_privilege(current_user, n.oid, 'USAGE'), \
                has_schema_privilege(current_user, n.oid, 'CREATE') \
         FROM pg_catalog.pg_namespace n \
         WHERE n.nspname <> 'information_schema' \
           AND n.nspname !~ '^pg_' \
           AND (has_schema_privilege(current_user, n.oid, 'USAGE') \
             OR has_schema_privilege(current_user, n.oid, 'CREATE')) \
         ORDER BY n.nspname",
    )
    .fetch_all(pool)
    .await?;
    anyhow::ensure!(
        schema_authority == [("public".to_owned(), true, false)],
        "GraphQL torrent-delete writer must access only the public application schema"
    );
    let schemas = sqlx::query_scalar::<_, Vec<String>>("SELECT current_schemas(false)::text[]")
        .fetch_one(pool)
        .await?;
    anyhow::ensure!(
        schemas == ["public".to_owned()],
        "GraphQL torrent-delete writer search path must resolve only public"
    );

    let owned_objects = sqlx::query_scalar::<_, i64>(
        "SELECT \
           (SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspowner = $1::oid) + \
           (SELECT count(*) FROM pg_catalog.pg_class c \
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relowner = $1::oid \
               AND n.nspname NOT IN ('pg_catalog', 'information_schema'))",
    )
    .bind(role_oid)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        owned_objects == 0,
        "GraphQL torrent-delete writer must not own schemas or application relations"
    );

    let table_grants = sqlx::query_as::<_, (bool, String, String, String, String)>(
        "SELECT grantee = current_user, table_schema, table_name, privilege_type, is_grantable \
         FROM information_schema.table_privileges \
         WHERE grantee IN (current_user, 'PUBLIC') \
           AND table_schema NOT IN ('pg_catalog', 'information_schema') \
         ORDER BY grantee, table_schema, table_name, privilege_type",
    )
    .fetch_all(pool)
    .await?;
    anyhow::ensure!(
        table_grants
            == [
                (
                    true,
                    "public".to_owned(),
                    "goose_db_version".to_owned(),
                    "SELECT".to_owned(),
                    "NO".to_owned(),
                ),
                (
                    true,
                    "public".to_owned(),
                    "torrents".to_owned(),
                    "DELETE".to_owned(),
                    "NO".to_owned(),
                ),
            ],
        "GraphQL torrent-delete writer does not have the exact table grants"
    );

    let column_grants = sqlx::query_as::<_, (bool, String, String, String, String, String)>(
        "SELECT cp.grantee = current_user, cp.table_schema, cp.table_name, \
                cp.column_name, cp.privilege_type, cp.is_grantable \
         FROM information_schema.column_privileges cp \
         WHERE cp.grantee IN (current_user, 'PUBLIC') \
           AND cp.table_schema NOT IN ('pg_catalog', 'information_schema') \
           AND NOT EXISTS ( \
             SELECT 1 FROM information_schema.table_privileges tp \
              WHERE tp.grantee = cp.grantee \
                AND tp.table_schema = cp.table_schema \
                AND tp.table_name = cp.table_name \
                AND tp.privilege_type = cp.privilege_type) \
         ORDER BY cp.grantee, cp.table_schema, cp.table_name, cp.column_name, cp.privilege_type",
    )
    .fetch_all(pool)
    .await?;
    anyhow::ensure!(
        column_grants
            == [
                (
                    true,
                    "public".to_owned(),
                    "bloom_filters".to_owned(),
                    "key".to_owned(),
                    "SELECT".to_owned(),
                    "NO".to_owned(),
                ),
                (
                    true,
                    "public".to_owned(),
                    "bloom_filters".to_owned(),
                    "oid".to_owned(),
                    "SELECT".to_owned(),
                    "NO".to_owned(),
                ),
                (
                    true,
                    "public".to_owned(),
                    "deleted_torrents".to_owned(),
                    "deleted_at".to_owned(),
                    "INSERT".to_owned(),
                    "NO".to_owned(),
                ),
                (
                    true,
                    "public".to_owned(),
                    "deleted_torrents".to_owned(),
                    "deleted_at".to_owned(),
                    "UPDATE".to_owned(),
                    "NO".to_owned(),
                ),
                (
                    true,
                    "public".to_owned(),
                    "deleted_torrents".to_owned(),
                    "info_hash".to_owned(),
                    "INSERT".to_owned(),
                    "NO".to_owned(),
                ),
                (
                    true,
                    "public".to_owned(),
                    "deleted_torrents".to_owned(),
                    "info_hash".to_owned(),
                    "SELECT".to_owned(),
                    "NO".to_owned(),
                ),
                (
                    true,
                    "public".to_owned(),
                    "torrents".to_owned(),
                    "info_hash".to_owned(),
                    "SELECT".to_owned(),
                    "NO".to_owned(),
                ),
            ],
        "GraphQL torrent-delete writer does not have the exact column grants"
    );

    let rls = sqlx::query_as::<_, (bool, bool, bool)>(
        "SELECT \
           row_security_active('public.torrents'::regclass), \
           row_security_active('public.bloom_filters'::regclass), \
           row_security_active('public.deleted_torrents'::regclass)",
    )
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        rls == (false, false, false),
        "GraphQL torrent-delete target tables must not activate row security"
    );

    let triggers = sqlx::query_as::<_, (String, String, bool, String, String, String)>(
        "SELECT t.tgname::text, t.tgenabled::text, \
                (t.tgtype = 9 AND t.tgqual IS NULL AND t.tgnargs = 0 \
                 AND octet_length(t.tgargs) = 0 \
                 AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL \
                 AND NOT p.prosecdef AND l.lanname = 'plpgsql' \
                 AND pg_catalog.pg_get_function_result(p.oid) = 'trigger' \
                 AND p.pronargs = 0), \
                n.nspname::text, p.proname::text, p.prosrc::text \
         FROM pg_catalog.pg_trigger t \
         JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid \
         JOIN pg_catalog.pg_namespace cn ON cn.oid = c.relnamespace \
         JOIN pg_catalog.pg_proc p ON p.oid = t.tgfoid \
         JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
         JOIN pg_catalog.pg_language l ON l.oid = p.prolang \
         WHERE cn.nspname = 'public' AND c.relname = 'torrents' \
           AND NOT t.tgisinternal \
         ORDER BY t.tgname",
    )
    .fetch_all(pool)
    .await?;
    let [trigger] = triggers.as_slice() else {
        anyhow::bail!(
            "GraphQL torrent-delete writer requires exactly one noninternal torrents trigger"
        );
    };
    anyhow::ensure!(
        trigger.0 == "torrents_deletion_audit"
            && trigger.1 == "O"
            && trigger.2
            && trigger.3 == "public"
            && trigger.4 == "record_torrent_deletion"
            && trigger
                .5
                .split_whitespace()
                .eq(DELETION_TRIGGER_BODY.split_whitespace()),
        "GraphQL torrent-delete writer requires exactly the enabled SECURITY INVOKER \
         AFTER DELETE ROW audit trigger and function body"
    );

    let settings = sqlx::query_as::<_, (String, String)>(
        "SELECT current_setting('transaction_read_only'), \
                current_setting('lo_compat_privileges')",
    )
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        settings == ("off".to_owned(), "off".to_owned()),
        "GraphQL torrent-delete writer requires read-write transactions and enforced large-object ACLs"
    );

    let routines = sqlx::query_as::<_, (String, String)>(
        "SELECT p.proname::text, pg_catalog.pg_get_function_identity_arguments(p.oid) \
         FROM pg_catalog.pg_proc p \
         JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
         WHERE n.nspname = 'pg_catalog' AND p.prokind = 'f' \
           AND (p.proname LIKE 'lo!_%' ESCAPE '!' OR p.proname IN ('loread', 'lowrite')) \
           AND has_function_privilege(current_user, p.oid, 'EXECUTE') \
         ORDER BY p.proname, pg_catalog.pg_get_function_identity_arguments(p.oid)",
    )
    .fetch_all(pool)
    .await?;
    anyhow::ensure!(
        routines
            == [
                ("lo_get".to_owned(), "oid, bigint, integer".to_owned(),),
                ("lo_put".to_owned(), "oid, bigint, bytea".to_owned(),),
            ],
        "GraphQL torrent-delete writer must execute exactly lo_get(oid,bigint,integer) \
         and lo_put(oid,bigint,bytea) among PostgreSQL large-object routines; \
         observed {routines:?}"
    );

    let writer_owned_large_objects = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)::bigint FROM pg_catalog.pg_largeobject_metadata \
         WHERE lomowner = $1::oid",
    )
    .bind(role_oid)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        writer_owned_large_objects == 0,
        "GraphQL torrent-delete writer must not own any large object"
    );

    let (blocked_oid, owner_oid, encoded) = sqlx::query_as::<_, (Oid, Oid, Vec<u8>)>(
        "SELECT bf.oid, lom.lomowner, \
                    pg_catalog.lo_get(bf.oid, 0::bigint, $2::integer) \
             FROM public.bloom_filters bf \
             JOIN pg_catalog.pg_largeobject_metadata lom ON lom.oid = bf.oid \
             WHERE bf.key = $1::text AND bf.oid IS NOT NULL",
    )
    .bind(BLOCKED_TORRENTS_KEY)
    .bind(BLOCKED_TORRENTS_BOUNDED_READ_BYTES)
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        blocked_oid.0 != 0,
        "blocked_torrents large-object OID must be non-zero"
    );
    anyhow::ensure!(
        owner_oid != role_oid,
        "GraphQL torrent-delete writer must not own the rollback-protected large object"
    );
    anyhow::ensure!(
        encoded.len() == BLOCKED_TORRENTS_FILTER_BYTES as usize,
        "blocked_torrents large object must have the exact Go-compatible encoded length"
    );
    bitmagnet_blocking::validate_go_blocked_torrents_filter(&encoded)
        .map_err(anyhow::Error::new)
        .context(
            "blocked_torrents large object must strictly decode with Go production geometry",
        )?;

    let role_lo_acl = sqlx::query_as::<_, (String, bool)>(
        "SELECT acl.privilege_type, acl.is_grantable \
         FROM pg_catalog.pg_largeobject_metadata lom \
         CROSS JOIN LATERAL pg_catalog.aclexplode(lom.lomacl) acl \
         WHERE lom.oid = $1::oid AND acl.grantee = $2::oid \
         ORDER BY acl.privilege_type",
    )
    .bind(blocked_oid)
    .bind(role_oid)
    .fetch_all(pool)
    .await?;
    anyhow::ensure!(
        role_lo_acl == [("SELECT".to_owned(), false), ("UPDATE".to_owned(), false),],
        "GraphQL torrent-delete writer requires direct non-grantable SELECT and UPDATE \
         on the exact blocked_torrents large object"
    );

    let public_large_object_grants = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)::bigint \
         FROM pg_catalog.pg_largeobject_metadata lom \
         CROSS JOIN LATERAL pg_catalog.aclexplode(lom.lomacl) acl \
         WHERE acl.grantee = 0::oid",
    )
    .fetch_one(pool)
    .await?;
    anyhow::ensure!(
        public_large_object_grants == 0,
        "PUBLIC must have no direct large-object privileges"
    );

    let accessible_large_objects = sqlx::query_scalar::<_, Oid>(
        "SELECT DISTINCT lom.oid \
         FROM pg_catalog.pg_largeobject_metadata lom \
         CROSS JOIN LATERAL pg_catalog.aclexplode(lom.lomacl) acl \
         WHERE acl.grantee IN (0::oid, $1::oid) \
           AND acl.privilege_type IN ('SELECT', 'UPDATE') \
         ORDER BY lom.oid",
    )
    .bind(role_oid)
    .fetch_all(pool)
    .await?;
    anyhow::ensure!(
        accessible_large_objects == [blocked_oid],
        "GraphQL torrent-delete writer must access only the exact blocked_torrents large object"
    );

    Ok(TorrentDeleteWriterAdmission {
        blocked_torrents_oid: blocked_oid.0,
    })
}

pub(super) async fn resolve(
    runtime: &TorrentDeleteMutationsRuntimeData,
    info_hashes: Vec<Hash20>,
) -> Result<()> {
    let request = normalize(info_hashes)?;
    runtime
        .0
        .delete(request)
        .await
        .map_err(|error| Error::new(error.to_string()))
}

fn normalize(raw: Vec<Hash20>) -> Result<TorrentDeleteRequest> {
    if raw.len() > MAX_TORRENT_DELETE_INFO_HASHES {
        return Err(Error::new(format!(
            "torrent.delete infoHashes has more than {MAX_TORRENT_DELETE_INFO_HASHES} entries"
        )));
    }

    let mut seen = HashSet::with_capacity(raw.len());
    let mut info_hashes = Vec::with_capacity(raw.len());
    for Hash20(value) in raw {
        let hash = InfoHash::from_str(&value)
            .map_err(|error| Error::new(format!("invalid Hash20: {error}")))?;
        if seen.insert(hash) {
            info_hashes.push(hash);
        }
    }
    Ok(TorrentDeleteRequest { info_hashes })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_graphql::{value, EmptySubscription};

    use super::*;
    use crate::schema::roots::{Mutation, Query};

    struct FakeRuntime {
        calls: Arc<Mutex<Vec<TorrentDeleteRequest>>>,
    }

    #[async_trait]
    impl TorrentDeleteMutationsRuntime for FakeRuntime {
        async fn delete(
            &self,
            request: TorrentDeleteRequest,
        ) -> std::result::Result<(), TorrentDeleteMutationsError> {
            self.calls.lock().expect("calls lock").push(request);
            Ok(())
        }
    }

    fn schema_with_fake(calls: Arc<Mutex<Vec<TorrentDeleteRequest>>>) -> crate::schema::Schema {
        let runtime: Arc<dyn TorrentDeleteMutationsRuntime> = Arc::new(FakeRuntime { calls });
        async_graphql::Schema::build(Query, Mutation, EmptySubscription)
            .data(TorrentDeleteMutationsRuntimeData::new(runtime))
            .finish()
    }

    fn hash(raw: &str) -> InfoHash {
        raw.parse().expect("test hash")
    }

    #[tokio::test]
    async fn graphql_delete_deduplicates_calls_and_returns_void() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let schema = schema_with_fake(Arc::clone(&calls));
        let first = "0123456789abcdef0123456789abcdef01234567";
        let second = "1111111111111111111111111111111111111111";
        let response = schema
            .execute(format!(
                "mutation {{ torrent {{ delete(infoHashes: [\"{first}\", \"{first}\", \"{second}\"]) }} }}"
            ))
            .await;

        assert!(response.errors.is_empty(), "errors: {:?}", response.errors);
        assert_eq!(response.data, value!({ "torrent": { "delete": null } }));
        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![TorrentDeleteRequest {
                info_hashes: vec![hash(first), hash(second)],
            }]
        );
    }

    #[tokio::test]
    async fn empty_delete_reaches_runtime_like_go() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let response = schema_with_fake(Arc::clone(&calls))
            .execute("mutation { torrent { delete(infoHashes: []) } }")
            .await;

        assert!(response.errors.is_empty(), "errors: {:?}", response.errors);
        assert_eq!(
            *calls.lock().expect("calls lock"),
            vec![TorrentDeleteRequest {
                info_hashes: Vec::new(),
            }]
        );
    }

    #[tokio::test]
    async fn invalid_and_oversized_inputs_fail_before_the_runtime() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let schema = schema_with_fake(Arc::clone(&calls));
        let invalid = schema
            .execute("mutation { torrent { delete(infoHashes: [\"not-a-hash\"]) } }")
            .await;
        assert_eq!(invalid.errors.len(), 1);
        assert!(invalid.errors[0].message.contains("invalid Hash20"));

        let oversized = vec![
            Hash20("0123456789abcdef0123456789abcdef01234567".to_owned());
            MAX_TORRENT_DELETE_INFO_HASHES + 1
        ];
        let error = normalize(oversized).expect_err("oversized delete must fail");
        assert!(error.message.contains("has more than"));
        assert!(calls.lock().expect("calls lock").is_empty());
    }

    #[tokio::test]
    async fn disabled_runtime_fails_loudly() {
        let schema = async_graphql::Schema::build(Query, Mutation, EmptySubscription)
            .data(TorrentDeleteMutationsRuntimeData::disabled())
            .finish();
        let response = schema
            .execute("mutation { torrent { delete(infoHashes: []) } }")
            .await;
        assert_eq!(response.errors.len(), 1);
        assert!(response.errors[0]
            .message
            .contains("torrent delete mutations are disabled"));
    }
}
