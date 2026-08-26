use bitmagnet_queue::ProcessTorrentParams;

const REQUIRED_FALSE_FLAGS: [&str; 3] = ["local_search_enabled", "apis_enabled", "tmdb_enabled"];

pub(crate) fn has_explicit_default_workflow(params: &ProcessTorrentParams) -> bool {
    params.classifier_workflow == "default"
}

pub(crate) fn has_explicit_attach_flags_off(params: &ProcessTorrentParams) -> bool {
    let Some(flags) = params.classifier_flags.as_ref() else {
        return false;
    };
    REQUIRED_FALSE_FLAGS
        .iter()
        .all(|name| flags.get(*name).and_then(serde_json::Value::as_bool) == Some(false))
}
