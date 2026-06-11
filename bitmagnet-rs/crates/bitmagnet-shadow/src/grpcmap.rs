//! The sidecar side of each pair — `FileSearchService` requests + response
//! normalization into the comparison domain.

use anyhow::{Context, Result};
use bitmagnet_proto::v1 as proto;
use proto::file_search_service_client::FileSearchServiceClient;
use tonic::transport::Channel;

use crate::{PairSpec, Shape, ShapeResult};

/// Build the proto filters for a pair (field-for-field with the PG mirror).
fn filters(spec: &PairSpec) -> proto::FileFilters {
    proto::FileFilters {
        extensions: spec.extensions.clone(),
        content_types: Vec::new(),
        size_min: spec.size_min,
        size_max: spec.size_max,
        path_query: spec.path_query.clone(),
        include_padding: spec.include_padding,
    }
}

fn sort(spec: &PairSpec) -> Vec<proto::FileSortBy> {
    match &spec.sort_field {
        // No sort in the request → the server default (size DESC), which the
        // PG mirror reproduces via `resolved_sort`.
        None => Vec::new(),
        Some(f) => vec![proto::FileSortBy {
            field: f.clone(),
            descending: spec.sort_desc,
        }],
    }
}

/// Run the pair's RPC and normalize. `find`/`collapse` keep response order
/// (the sort is a total order via tiebreaks); facets become a map.
pub async fn run(
    client: &mut FileSearchServiceClient<Channel>,
    spec: &PairSpec,
) -> Result<ShapeResult> {
    Ok(match spec.shape {
        Shape::Find | Shape::Collapse => {
            let resp = client
                .search_files(proto::SearchFilesRequest {
                    filters: Some(filters(spec)),
                    pagination: Some(proto::FilePagination {
                        limit: spec.limit,
                        cursor: String::new(),
                    }),
                    sort: sort(spec),
                    collapse_to_torrent: spec.shape == Shape::Collapse,
                    // Previews are per-group point lookups, not part of the
                    // parity domain — skip the cheapest way the proto allows.
                    preview_limit: 1,
                })
                .await
                .context("SearchFiles RPC")?
                .into_inner();
            if spec.shape == Shape::Find {
                ShapeResult::Rows(
                    resp.files
                        .into_iter()
                        .map(|f| crate::FileRowN {
                            info_hash: f.info_hash,
                            file_index: f.file_index,
                            path: f.path,
                            extension: f.extension, // "" = NULL already
                            size: f.size,
                        })
                        .collect(),
                )
            } else {
                ShapeResult::Groups(
                    resp.groups
                        .into_iter()
                        .map(|g| crate::GroupN {
                            info_hash: g.info_hash,
                            matching_file_count: g.matching_file_count,
                            matching_total_size: g.matching_total_size,
                            matching_max_size: g.matching_max_size,
                        })
                        .collect(),
                )
            }
        }
        Shape::CountFiles | Shape::CountTorrents => {
            let resp = client
                .count_files(proto::CountFilesRequest {
                    filters: Some(filters(spec)),
                    collapse_to_torrent: spec.shape == Shape::CountTorrents,
                })
                .await
                .context("CountFiles RPC")?
                .into_inner();
            ShapeResult::Count {
                count: resp.count,
                estimated: resp.estimated,
            }
        }
        Shape::Facet => {
            let resp = client
                .facets(proto::FacetsRequest {
                    filters: Some(filters(spec)),
                    facet_fields: vec!["extension".to_owned()],
                })
                .await
                .context("Facets RPC")?
                .into_inner();
            let mut m = std::collections::BTreeMap::new();
            for facet in resp.facets {
                if facet.field == "extension" {
                    for b in facet.buckets {
                        m.insert(b.value, (b.count, b.total_size));
                    }
                }
            }
            ShapeResult::Facet(m)
        }
    })
}
