use bitmagnet_classifier::core_config_digest;

#[test]
fn core_effective_config_digest_matches_go() {
    assert_eq!(
        core_config_digest().expect("digest embedded core classifier"),
        "sha256:95ffc278681f50fbcee2a3498e4388378ffe78156bc432d403d2acc3c2c809ae"
    );
}
