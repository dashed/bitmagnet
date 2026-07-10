//! Regression: a backfilled pathsearch index MUST survive being reopened by a
//! new writer process (the serving pod after the backfill Job exits), and must
//! survive a subsequent restart. The prod D4 smoke found the 100k backfill docs
//! were wiped when the serving Deployment scaled 0->1.

use bitmagnet_search::pathsearch::document::PathDocument;
use bitmagnet_search::pathsearch::index::{open_or_create, reader, writer};
use bitmagnet_search::pathsearch::indexer::upsert;
use bitmagnet_search::pathsearch::schema::Fields;
use bitmagnet_search::pathsearch::PathSearchServer;
use bitmagnet_search::proto::path_search_service_server::PathSearchService;
use bitmagnet_search::proto::HealthCheckRequest;
use std::path::{Path, PathBuf};
use tonic::Request;

const HEAP: usize = 256 * 1024 * 1024;

fn unique_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bitmagnet-pathsearch-durability-{}-{}",
        std::process::id(),
        tag
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn doc(i: u32) -> PathDocument {
    let mut info_hash = vec![0u8; 20];
    info_hash[0..4].copy_from_slice(&i.to_le_bytes());
    PathDocument {
        info_hash,
        paths: vec![format!("Season{}/Episode.{i}.1080p.mkv", i % 10)],
        size: 1_000 + u64::from(i),
        files_count: 1,
        seeders: 0,
        published_at: 1_600_000_000,
    }
}

/// Simulate the backfill PROCESS: a self-contained `Index` + `IndexWriter` that
/// indexes `n` docs with a periodic commit cadence (like the real backfill's
/// `--commit-interval`), commits, and then exits — both the writer and the
/// index are dropped at the end of this function.
fn run_backfill_process(dir: &Path, n: u32, commit_every: u32) {
    let index = open_or_create(dir).expect("backfill open");
    let fields = Fields::from_schema(&index.schema()).expect("fields");
    let mut w = writer(&index, HEAP, 1).expect("backfill writer");
    for i in 0..n {
        upsert(&w, &fields, &doc(i)).expect("index doc");
        if (i + 1) % commit_every == 0 {
            w.commit().expect("interval commit");
        }
    }
    w.commit().expect("final commit");
    // Backfill process exits here: writer + index dropped.
}

async fn server_doc_count(server: &PathSearchServer) -> u64 {
    server
        .health_check(Request::new(HealthCheckRequest {}))
        .await
        .expect("health")
        .into_inner()
        .doc_count
}

#[tokio::test]
async fn backfilled_index_survives_reopen_and_restart() {
    let dir = unique_dir("reopen");
    let n = 3_000;
    run_backfill_process(&dir, n, 200);

    // Bisection probe: a reader-only reopen (no writer) must see the committed
    // docs. This proves the backfill data is durably on disk.
    {
        let index = open_or_create(&dir).expect("reader reopen");
        let r = reader(&index).expect("reader");
        r.reload().expect("reload");
        assert_eq!(
            u64::from(u32::try_from(r.searcher().num_docs()).unwrap()),
            u64::from(n),
            "reader-only reopen must see all committed backfill docs"
        );
    }

    // The serving pod reopens the same dir and creates its writer. This MUST NOT
    // drop the committed segments.
    let server = PathSearchServer::open(&dir, HEAP, 1, None).expect("server open");
    assert_eq!(
        server_doc_count(&server).await,
        u64::from(n),
        "serving-pod reopen (new writer process) must preserve the backfill docs"
    );
    drop(server);

    // A second restart (serving pod restarts again) must also preserve them.
    let server = PathSearchServer::open(&dir, HEAP, 1, None).expect("server reopen");
    assert_eq!(
        server_doc_count(&server).await,
        u64::from(n),
        "a serving-pod restart must preserve the index"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// TRUE separate-process build half: builds the index and lets THIS process
/// exit (the `cargo test` process terminates, killing any in-flight Tantivy
/// merge thread — exactly like the backfill Job pod completing). Driven by env
/// so it can be invoked as its own OS process:
///   BITMAGNET_PS_XPROC_DIR=/tmp/x BITMAGNET_PS_XPROC_N=3000 \
///     cargo test -p bitmagnet-search --test pathsearch_durability -- --ignored --exact xprocess_build_half
#[test]
#[ignore = "separate-process repro half; invoked by the durability harness with BITMAGNET_PS_XPROC_DIR set"]
fn xprocess_build_half() {
    let dir = PathBuf::from(std::env::var("BITMAGNET_PS_XPROC_DIR").expect("XPROC dir"));
    let n: u32 = std::env::var("BITMAGNET_PS_XPROC_N")
        .expect("XPROC n")
        .parse()
        .unwrap();
    // Commit cadence is env-tunable so the harness can mimic the prod backfill's
    // larger segments (commit-interval 10000) and maximise the chance a big
    // auto-merge is still in-flight when this process exits.
    let commit_every: u32 = std::env::var("BITMAGNET_PS_XPROC_COMMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let _ = std::fs::remove_dir_all(&dir);
    run_backfill_process(&dir, n, commit_every);
    // No explicit cleanup / no wait_merging_threads: the process exits here,
    // killing any in-flight merge thread — exactly like the backfill Job pod.
}

/// TRUE separate-process open half: opens the dir built by a PRIOR process and
/// asserts the docs survived. Run after `xprocess_build_half` against the same
/// dir with the same N.
#[tokio::test]
#[ignore = "separate-process repro half; invoked by the durability harness with BITMAGNET_PS_XPROC_DIR set"]
async fn xprocess_open_half() {
    let dir = PathBuf::from(std::env::var("BITMAGNET_PS_XPROC_DIR").expect("XPROC dir"));
    let n: u64 = std::env::var("BITMAGNET_PS_XPROC_N")
        .expect("XPROC n")
        .parse()
        .unwrap();
    let server = PathSearchServer::open(&dir, HEAP, 1, None).expect("server open");
    assert_eq!(
        server_doc_count(&server).await,
        n,
        "a backfill built by a SEPARATE process that has fully exited must survive reopen"
    );
}

/// SIGKILL repro — CHURN half. Opens the index, writes a watermark file INSIDE
/// the index dir (exactly like prod), and commits one doc per upsert in a tight
/// loop — mimicking the follow loop's per-upsert commit + GC churn. After each
/// successful commit it records the committed count to `<dir>.count` (OUTSIDE
/// the index dir). Runs forever; the harness `kill -9`s it mid-churn to leave
/// the writer's Drop UNRUN (the prod SIGTERM→immediate-death condition).
#[tokio::test]
#[ignore = "SIGKILL repro half; the harness runs it as its own process and kill -9's it"]
async fn xprocess_churn_half() {
    let dir = PathBuf::from(std::env::var("BITMAGNET_PS_XPROC_DIR").expect("XPROC dir"));
    let _ = std::fs::remove_dir_all(&dir);
    let server = PathSearchServer::open(&dir, HEAP, 1, None).expect("server open");
    // Prod writes the follow watermark file inside the Tantivy index dir.
    std::fs::write(dir.join("watermark"), "1600000000\n").expect("watermark");
    let count_file = dir.with_extension("count");
    let mut i: u32 = 0;
    loop {
        server.upsert_document(&doc(i)).await.expect("upsert");
        i += 1;
        // Record the count of docs that have been committed AND reader-reloaded.
        let _ = std::fs::write(&count_file, i.to_string());
    }
}

/// SIGKILL repro — REPORT half. Reopens the dir left by a `kill -9`ed churn
/// process and prints the surviving doc_count for the harness to compare against
/// the recorded committed count. A wipe shows DOC_COUNT collapsing to ~0.
#[tokio::test]
#[ignore = "SIGKILL repro half; reopens a kill -9'ed index dir and reports doc_count"]
async fn xprocess_report_half() {
    let dir = PathBuf::from(std::env::var("BITMAGNET_PS_XPROC_DIR").expect("XPROC dir"));
    let server = PathSearchServer::open(&dir, HEAP, 1, None).expect("server open");
    println!("DOC_COUNT={}", server_doc_count(&server).await);
}

/// The prod serving pod runs `follow=true`: after opening the backfilled index
/// it commits via its own writer (the follow loop's upsert/delete + commit).
/// That post-reopen commit MUST NOT drop the backfill segments.
#[tokio::test]
async fn serving_writer_commit_preserves_backfill() {
    let dir = unique_dir("follow-commit");
    let n = 3_000;
    run_backfill_process(&dir, n, 200);

    // Mimic prod exactly: the follow loop writes a watermark file *inside* the
    // index directory before its first commit.
    std::fs::write(dir.join("watermark"), "1600000000\n").expect("write watermark");

    let server = PathSearchServer::open(&dir, HEAP, 1, None).expect("server open");
    assert_eq!(
        server_doc_count(&server).await,
        u64::from(n),
        "freshly-opened server must see all backfill docs"
    );

    // The follow loop's first action: upsert a freshly-crawled torrent, which
    // commits through the serving writer (and triggers Tantivy GC).
    server
        .upsert_document(&doc(1_000_000))
        .await
        .expect("follow upsert");
    assert_eq!(
        server_doc_count(&server).await,
        u64::from(n) + 1,
        "a follow-loop commit must not wipe the backfill segments"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
