//! String-case conversion compatible with bitmagnet's Go configuration walker.

/// Convert a Go-style identifier to `snake_case`, matching
/// `iancoleman/strcase` v0.3.0 `ToSnake`.
#[must_use]
pub fn to_snake(s: &str) -> String {
    let s = s.trim();
    let bytes = s.as_bytes();
    let mut output = Vec::with_capacity(s.len() + 2);

    for (index, &original) in bytes.iter().enumerate() {
        let is_cap = original.is_ascii_uppercase();
        let is_low = original.is_ascii_lowercase();
        let value = if is_cap {
            original.to_ascii_lowercase()
        } else {
            original
        };

        if let Some(&next) = bytes.get(index + 1) {
            let is_num = value.is_ascii_digit();
            let next_is_cap = next.is_ascii_uppercase();
            let next_is_low = next.is_ascii_lowercase();
            let next_is_num = next.is_ascii_digit();
            let is_transition = (is_cap && (next_is_low || next_is_num))
                || (is_low && (next_is_cap || next_is_num))
                || (is_num && (next_is_cap || next_is_low));

            if is_transition {
                if is_cap && next_is_low && index > 0 && bytes[index - 1].is_ascii_uppercase() {
                    output.push(b'_');
                }

                output.push(value);
                if is_low || is_num || next_is_num {
                    output.push(b'_');
                }
                continue;
            }
        }

        if matches!(value, b' ' | b'_' | b'-' | b'.') {
            output.push(b'_');
        } else {
            output.push(value);
        }
    }

    String::from_utf8(output).expect("ASCII-only byte edits preserve UTF-8")
}

#[cfg(test)]
mod tests {
    use super::to_snake;

    #[test]
    fn matches_go_strcase_v0_3_0_oracle() {
        const CASES: &[(&str, &str)] = &[
            ("ScalingFactor", "scaling_factor"),
            ("CORS", "cors"),
            ("AllowedHeaders", "allowed_headers"),
            ("IntervalMs", "interval_ms"),
            ("RateLimitBurst", "rate_limit_burst"),
            ("DefaultFrontend", "default_frontend"),
            ("MaxKeys", "max_keys"),
            ("TTL", "ttl"),
            ("PeerGraphqlURLs", "peer_graphql_ur_ls"),
            ("PeerGraphqlUrls", "peer_graphql_urls"),
            ("JSONData", "json_data"),
            ("HTTPServer", "http_server"),
            ("SaveFilesThreshold", "save_files_threshold"),
            (
                "ReseedBootstrapNodesInterval",
                "reseed_bootstrap_nodes_interval",
            ),
            ("DeleteXxx", "delete_xxx"),
            ("SavePieces", "save_pieces"),
            ("QueryTimeout", "query_timeout"),
            ("SleepBetweenBatchesMs", "sleep_between_batches_ms"),
            ("SampleSize", "sample_size"),
            ("ID", "id"),
            ("UserID", "user_id"),
            ("APIKey", "api_key"),
            ("OAuth2Token", "o_auth_2_token"),
            ("HTML5Parser", "html_5_parser"),
            ("A", "a"),
            ("AB", "ab"),
            ("Ab", "ab"),
            ("aB", "a_b"),
            ("Already_Snake", "already_snake"),
            ("with-dash", "with_dash"),
            ("dot.name", "dot_name"),
            ("IPv4Addr", "i_pv_4_addr"),
            ("V2Foundation", "v_2_foundation"),
            ("Port80Number", "port_80_number"),
        ];

        for &(input, expected) in CASES {
            assert_eq!(to_snake(input), expected, "input: {input}");
        }
    }

    #[test]
    fn trims_space_before_converting_bytes() {
        assert_eq!(to_snake(" \tHTTPServer\n"), "http_server");
    }
}
