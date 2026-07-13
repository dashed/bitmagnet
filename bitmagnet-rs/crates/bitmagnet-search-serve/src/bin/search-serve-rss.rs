//! Reproducible production-shaped allocator/RSS probe for the L1 composer.

use std::collections::HashMap;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bitmagnet_model::{serialize_files, BlobFile, FilesStatus, InfoHash, Torrent};
use bitmagnet_proto::v1::{
    PathCandidate, PathCandidatesRequest, PathCandidatesResponse, PathSearchHealth, SortBy,
    SuggestRequest, SuggestResponse,
};
use bitmagnet_search_serve::{
    Aggregations, CandidateSource, Composer, ComposerConfig, Filters, PgSearchBackend,
    QueryOptions, SearchOptions, SearchServe, TorrentContentResult, TorrentContentResultItem,
    DEFAULT_MAX_CANDIDATES, DEFAULT_MAX_CHUNK_TORRENTS, DEFAULT_MAX_DECODE_CANDIDATES,
    DEFAULT_MAX_REFINE_FILES, DEFAULT_REFINE_FILE_BUDGET, DEFAULT_RETAINED_FILE_BUDGET,
};

const LIVE_MAX_FILES_PER_TORRENT: usize = 88_561;
const CHUNK_TORRENTS: usize = 3;
const RETAINED_TORRENTS: usize = 12;
const HARNESS_ROUTE_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Chunk,
    Retained,
}

impl Scenario {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "chunk" => Ok(Self::Chunk),
            "retained" => Ok(Self::Retained),
            _ => Err(format!(
                "invalid scenario {value:?}; expected chunk or retained"
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Chunk => "chunk",
            Self::Retained => "retained",
        }
    }

    fn torrent_count(self) -> usize {
        match self {
            Self::Chunk => CHUNK_TORRENTS,
            Self::Retained => RETAINED_TORRENTS,
        }
    }

    fn decoded_file_upper_bound(self) -> usize {
        match self {
            Self::Chunk => CHUNK_TORRENTS * LIVE_MAX_FILES_PER_TORRENT,
            // Eleven torrents fit below the retained cap. The twelfth is
            // decoded as one bounded lookahead before the composer stops.
            Self::Retained => RETAINED_TORRENTS * LIVE_MAX_FILES_PER_TORRENT,
        }
    }
}

#[derive(Debug, Clone)]
struct Fixture {
    info_hash: InfoHash,
    blob_path: PathBuf,
    file_count: usize,
}

struct FixtureCandidates {
    fixtures: Vec<Fixture>,
}

#[async_trait::async_trait]
impl CandidateSource for FixtureCandidates {
    async fn path_candidates(
        &self,
        request: PathCandidatesRequest,
    ) -> bitmagnet_search_serve::Result<PathCandidatesResponse> {
        let limit = usize::try_from(request.limit).unwrap_or(usize::MAX);
        Ok(PathCandidatesResponse {
            candidates: self
                .fixtures
                .iter()
                .take(limit)
                .map(|fixture| PathCandidate {
                    info_hash: fixture.info_hash.as_slice().to_vec(),
                    ..PathCandidate::default()
                })
                .collect(),
            candidate_total: u64::try_from(self.fixtures.len()).unwrap_or(u64::MAX),
            estimated: true,
        })
    }

    async fn suggest(
        &self,
        _request: SuggestRequest,
    ) -> bitmagnet_search_serve::Result<SuggestResponse> {
        Ok(SuggestResponse::default())
    }

    async fn health_check(&self) -> bitmagnet_search_serve::Result<PathSearchHealth> {
        Ok(PathSearchHealth::default())
    }
}

struct DiskPg {
    fixtures: HashMap<InfoHash, Fixture>,
}

#[async_trait::async_trait]
impl PgSearchBackend for DiskPg {
    async fn torrent_content(
        &self,
        options: SearchOptions,
    ) -> bitmagnet_search_serve::Result<TorrentContentResult> {
        if !options.hydrate_files {
            return Ok(TorrentContentResult {
                aggregations: Aggregations::empty(),
                ..TorrentContentResult::default()
            });
        }

        let mut items = Vec::with_capacity(options.info_hash_in.len());
        for info_hash in options.info_hash_in {
            let fixture = self.fixtures.get(&info_hash).ok_or_else(|| {
                bitmagnet_search_serve::Error::Pg(format!(
                    "RSS fixture missing for candidate {info_hash}"
                ))
            })?;
            let files_data = std::fs::read(&fixture.blob_path).map_err(|error| {
                bitmagnet_search_serve::Error::Pg(format!(
                    "read RSS fixture {}: {error}",
                    fixture.blob_path.display()
                ))
            })?;
            items.push(TorrentContentResultItem {
                info_hash,
                torrent: Torrent {
                    info_hash,
                    name: format!("production-shaped-{info_hash}"),
                    size: u64::try_from(fixture.file_count).unwrap_or(u64::MAX) * 1_048_576,
                    private: false,
                    files_status: FilesStatus::Multi,
                    extension: None,
                    files_count: u32::try_from(fixture.file_count).ok(),
                    files_data: Some(files_data),
                    file_extensions: vec!["mkv".to_owned()],
                },
                refine_files: Vec::new(),
            });
        }

        Ok(TorrentContentResult {
            items,
            ..TorrentContentResult::default()
        })
    }

    async fn file_counts(
        &self,
        ids: &[InfoHash],
    ) -> bitmagnet_search_serve::Result<HashMap<InfoHash, i64>> {
        Ok(ids
            .iter()
            .filter_map(|info_hash| {
                self.fixtures.get(info_hash).map(|fixture| {
                    (
                        *info_hash,
                        i64::try_from(fixture.file_count).unwrap_or(i64::MAX),
                    )
                })
            })
            .collect())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--child") {
        return child_main(&args).await;
    }
    parent_main(&args)
}

fn parent_main(args: &[String]) -> Result<(), Box<dyn Error>> {
    let requested = arg_value(args, "--scenario").unwrap_or("all");
    let scenarios = match requested {
        "all" => vec![Scenario::Chunk, Scenario::Retained],
        value => vec![Scenario::parse(value)?],
    };

    let fixture_dir =
        std::env::temp_dir().join(format!("bitmagnet-search-serve-rss-{}", std::process::id()));
    if fixture_dir.exists() {
        std::fs::remove_dir_all(&fixture_dir)?;
    }
    std::fs::create_dir_all(&fixture_dir)?;

    let result = (|| -> Result<(), Box<dyn Error>> {
        generate_fixtures(&fixture_dir, RETAINED_TORRENTS)?;
        let executable = std::env::current_exe()?;
        for scenario in scenarios {
            let output = Command::new(&executable)
                .args(["--child", "--scenario", scenario.name(), "--fixture-dir"])
                .arg(&fixture_dir)
                .output()?;
            if !output.status.success() {
                return Err(format!(
                    "{} child failed with {}: {}",
                    scenario.name(),
                    output.status,
                    String::from_utf8_lossy(&output.stderr)
                )
                .into());
            }
            print!("{}", String::from_utf8(output.stdout)?);
        }
        Ok(())
    })();

    std::fs::remove_dir_all(&fixture_dir)?;
    result
}

async fn child_main(args: &[String]) -> Result<(), Box<dyn Error>> {
    let scenario =
        Scenario::parse(arg_value(args, "--scenario").ok_or("child requires --scenario")?)?;
    let fixture_dir =
        PathBuf::from(arg_value(args, "--fixture-dir").ok_or("child requires --fixture-dir")?);
    let fixtures = read_fixture_metadata(&fixture_dir, scenario.torrent_count())?;
    let raw_blob_bytes = fixtures.iter().try_fold(0_u64, |total, fixture| {
        Ok::<_, std::io::Error>(total.saturating_add(std::fs::metadata(&fixture.blob_path)?.len()))
    })?;

    let candidates = Arc::new(FixtureCandidates {
        fixtures: fixtures.clone(),
    });
    let pg = Arc::new(DiskPg {
        fixtures: fixtures
            .iter()
            .cloned()
            .map(|fixture| (fixture.info_hash, fixture))
            .collect(),
    });
    let config = ComposerConfig {
        max_candidates: DEFAULT_MAX_CANDIDATES,
        max_decode_candidates: DEFAULT_MAX_DECODE_CANDIDATES,
        max_refine_files: DEFAULT_MAX_REFINE_FILES,
        refine_file_budget: DEFAULT_REFINE_FILE_BUDGET,
        max_chunk_torrents: DEFAULT_MAX_CHUNK_TORRENTS,
        retained_file_budget: DEFAULT_RETAINED_FILE_BUDGET,
        route_timeout: HARNESS_ROUTE_TIMEOUT,
        max_concurrent_refines: 1,
        ..ComposerConfig::default()
    };
    let composer = Composer::new(candidates, pg, config, None);
    let options = QueryOptions {
        combined: SearchOptions {
            hydrate_files: true,
            ..SearchOptions::default()
        },
        refine: Some(SearchOptions {
            hydrate_files: true,
            ..SearchOptions::default()
        }),
        agg: SearchOptions {
            with_facets: true,
            ..SearchOptions::default()
        },
    };

    let baseline_rss_kib = proc_status_kib("VmRSS")?;
    let started = Instant::now();
    let (result, served) = composer
        .torrent_content(
            Filters {
                query: "inception".to_owned(),
                ..Filters::default()
            },
            options,
            50,
            0,
            Vec::<SortBy>::new(),
        )
        .await?;
    let elapsed_ms = started.elapsed().as_millis();
    if !served {
        return Err("RSS harness route unexpectedly fell back".into());
    }

    let retained_files = result
        .items
        .iter()
        .map(|item| item.refine_files.len())
        .sum::<usize>();
    let expected_retained = match scenario {
        Scenario::Chunk => CHUNK_TORRENTS * LIVE_MAX_FILES_PER_TORRENT,
        Scenario::Retained => 11 * LIVE_MAX_FILES_PER_TORRENT,
    };
    if retained_files != expected_retained {
        return Err(format!(
            "{} retained {retained_files} files; expected {expected_retained}",
            scenario.name()
        )
        .into());
    }
    if result
        .items
        .iter()
        .any(|item| item.torrent.files_data.is_some())
    {
        return Err("served RSS harness item retained its raw blob".into());
    }

    let final_rss_kib = proc_status_kib("VmRSS")?;
    let peak_rss_kib = proc_status_kib("VmHWM")?;
    let peak_delta_kib = peak_rss_kib.saturating_sub(baseline_rss_kib);
    let rss_delta_bytes_per_peak_file =
        (peak_delta_kib as f64 * 1024.0) / scenario.decoded_file_upper_bound() as f64;

    println!(
        concat!(
            "{{\"scenario\":\"{}\",\"target_os\":\"{}\",\"target_arch\":\"{}\",",
            "\"profile\":\"{}\",\"allocator\":\"std-system\",\"fixture_source\":",
            "\"live p50=6 p90=54 p99=743 max=88561; high-fanout max-files stress\",",
            "\"torrents\":{},\"files_per_torrent\":{},\"raw_blob_bytes\":{},",
            "\"max_refine_files\":{},\"chunk_file_budget\":{},",
            "\"retained_file_budget\":{},\"route_timeout_seconds\":{},",
            "\"decoded_file_upper_bound\":{},\"retained_files\":{},",
            "\"baseline_rss_kib\":{},\"final_rss_kib\":{},\"peak_rss_kib\":{},",
            "\"peak_delta_kib\":{},\"rss_delta_bytes_per_peak_file\":{:.3},",
            "\"elapsed_ms\":{},\"served\":true}}"
        ),
        scenario.name(),
        std::env::consts::OS,
        std::env::consts::ARCH,
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        scenario.torrent_count(),
        LIVE_MAX_FILES_PER_TORRENT,
        raw_blob_bytes,
        DEFAULT_MAX_REFINE_FILES,
        DEFAULT_REFINE_FILE_BUDGET,
        DEFAULT_RETAINED_FILE_BUDGET,
        HARNESS_ROUTE_TIMEOUT.as_secs(),
        scenario.decoded_file_upper_bound(),
        retained_files,
        baseline_rss_kib,
        final_rss_kib,
        peak_rss_kib,
        peak_delta_kib,
        rss_delta_bytes_per_peak_file,
        elapsed_ms,
    );

    std::hint::black_box(result);
    Ok(())
}

fn generate_fixtures(directory: &Path, count: usize) -> Result<(), Box<dyn Error>> {
    for torrent_index in 0..count {
        let files = (0..LIVE_MAX_FILES_PER_TORRENT)
            .map(|file_index| BlobFile {
                index: u32::try_from(file_index).unwrap_or(u32::MAX),
                path: format!("inception/collection-{torrent_index:02}/disc-{file_index:06}.mkv"),
                extension: "mkv".to_owned(),
                size: 734_003_200_u64.saturating_add(file_index as u64),
            })
            .collect::<Vec<_>>();
        let blob = serialize_files(&files)?;
        std::fs::write(
            directory.join(format!("torrent-{torrent_index:02}.blob")),
            blob,
        )?;
    }
    Ok(())
}

fn read_fixture_metadata(directory: &Path, count: usize) -> Result<Vec<Fixture>, Box<dyn Error>> {
    (0..count)
        .map(|index| {
            let blob_path = directory.join(format!("torrent-{index:02}.blob"));
            if !blob_path.is_file() {
                return Err(format!("missing fixture {}", blob_path.display()).into());
            }
            Ok(Fixture {
                info_hash: fixture_info_hash(index),
                blob_path,
                file_count: LIVE_MAX_FILES_PER_TORRENT,
            })
        })
        .collect()
}

fn fixture_info_hash(index: usize) -> InfoHash {
    let mut bytes = [0_u8; bitmagnet_model::INFO_HASH_LEN];
    bytes[..8].copy_from_slice(&u64::try_from(index + 1).unwrap_or(u64::MAX).to_be_bytes());
    InfoHash::new(bytes)
}

fn arg_value<'a>(args: &'a [String], key: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|window| window[0] == key)
        .map(|window| window[1].as_str())
}

fn proc_status_kib(field: &str) -> Result<u64, Box<dyn Error>> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let line = status
        .lines()
        .find(|line| line.starts_with(field))
        .ok_or_else(|| format!("/proc/self/status has no {field} field"))?;
    let value = line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| format!("malformed {field} line: {line}"))?
        .parse::<u64>()?;
    Ok(value)
}
