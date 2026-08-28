use std::net::SocketAddr;

use md5::{Digest, Md5};

use crate::{ByteString, Id20};

/// Secret-bearing deployed announce-token derivation.
///
/// Deliberately does not implement `Debug`: formatted responder state must not
/// expose the process-lifetime token secret.
#[derive(Clone)]
pub(crate) struct AnnounceToken {
    secret: [u8; 20],
}

impl AnnounceToken {
    pub(crate) const fn new(secret: [u8; 20]) -> Self {
        Self { secret }
    }

    pub(crate) fn issue(
        &self,
        local_id: Id20,
        info_hash: Id20,
        requester_id: Id20,
        source: SocketAddr,
    ) -> ByteString {
        let mut hasher = Md5::new();
        hasher.update(self.secret);
        hasher.update(local_id.as_bytes());
        hasher.update(info_hash.as_bytes());
        hasher.update(requester_id.as_bytes());
        hasher.update(go_source_ip_string(source).as_bytes());
        ByteString::new(hex::encode(hasher.finalize()).into_bytes())
    }
}

fn go_source_ip_string(source: SocketAddr) -> String {
    match source {
        SocketAddr::V4(source) => source.ip().to_string(),
        SocketAddr::V6(source) if source.scope_id() == 0 => source.ip().to_string(),
        SocketAddr::V6(source) => format!("{}%{}", source.ip(), source.scope_id()),
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

    use super::*;

    fn id(byte: u8) -> Id20 {
        Id20::from_slice(&[byte; 20]).unwrap()
    }

    #[test]
    fn source_ip_matches_go_netip_string_and_excludes_port_and_flowinfo() {
        assert_eq!(
            go_source_ip_string(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(192, 0, 2, 1),
                6881,
            ))),
            "192.0.2.1"
        );
        assert_eq!(
            go_source_ip_string(SocketAddr::V6(SocketAddrV6::new(
                "2001:db8::1".parse::<Ipv6Addr>().unwrap(),
                6881,
                17,
                0,
            ))),
            "2001:db8::1"
        );
        assert_eq!(
            go_source_ip_string(SocketAddr::V6(SocketAddrV6::new(
                "fe80::1".parse::<Ipv6Addr>().unwrap(),
                6881,
                17,
                7,
            ))),
            "fe80::1%7"
        );
    }

    #[test]
    fn token_is_lowercase_hex_and_depends_on_go_concatenation_order() {
        let issuer = AnnounceToken::new([0x11; 20]);
        let source = "192.0.2.1:6881".parse().unwrap();
        let token = issuer.issue(id(0x22), id(0x33), id(0x44), source);
        assert_eq!(token.as_bytes().len(), 32);
        assert!(token.as_bytes().iter().all(u8::is_ascii_hexdigit));
        assert!(token
            .as_bytes()
            .iter()
            .all(|byte| !byte.is_ascii_uppercase()));
        assert_eq!(
            token,
            issuer.issue(
                id(0x22),
                id(0x33),
                id(0x44),
                "192.0.2.1:65535".parse().unwrap(),
            )
        );
        assert_ne!(token, issuer.issue(id(0x33), id(0x22), id(0x44), source));
    }

    #[test]
    fn fixed_ipv4_token_matches_the_real_go_responder_golden() {
        let issuer = AnnounceToken::new(*b"0123456789abcdefghij");
        let local_id = Id20::from_hex("00112233445566778899aabbccddeeff10203040").unwrap();
        let info_hash = Id20::from_hex("11223344556677889900aabbccddeeff01020304").unwrap();
        let requester_id = Id20::from_hex("ffeeddccbbaa0099887766554433221100abcdef").unwrap();
        let token = issuer.issue(
            local_id,
            info_hash,
            requester_id,
            "192.0.2.1:6881".parse().unwrap(),
        );
        assert_eq!(token.as_bytes(), b"266127f80b327ff927362ec21a79e923");

        let mapped = issuer.issue(
            local_id,
            info_hash,
            requester_id,
            "[::ffff:192.0.2.1]:6881".parse().unwrap(),
        );
        assert_eq!(mapped.as_bytes(), b"f9a3b8d02c30d597f2928f713fc3e18d");

        let scoped = issuer.issue(
            local_id,
            info_hash,
            requester_id,
            SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), 6881, 0, 7)),
        );
        assert_eq!(scoped.as_bytes(), b"ae68f1ca191c1d377a9f02576136ecb6");
    }
}
