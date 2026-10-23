use bitmagnet_queue::ProcessTorrentParams;

const ATTACH_FLAGS: [&str; 3] = ["local_search_enabled", "apis_enabled", "tmdb_enabled"];

pub(crate) fn has_explicit_default_workflow(params: &ProcessTorrentParams) -> bool {
    params.classifier_workflow == "default"
}

pub(crate) fn has_explicit_attach_flags_off(params: &ProcessTorrentParams) -> bool {
    has_explicit_attach_flags(params, false)
}

/// All three attach flags explicitly JSON `true`: the flags-on subset, which
/// needs a live content resolver.
pub(crate) fn has_explicit_attach_flags_on(params: &ProcessTorrentParams) -> bool {
    has_explicit_attach_flags(params, true)
}

fn has_explicit_attach_flags(params: &ProcessTorrentParams, value: bool) -> bool {
    let Some(flags) = params.classifier_flags.as_ref() else {
        return false;
    };
    ATTACH_FLAGS
        .iter()
        .all(|name| flags.get(*name).and_then(serde_json::Value::as_bool) == Some(value))
}
