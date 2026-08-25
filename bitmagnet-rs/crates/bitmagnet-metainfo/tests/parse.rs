use bitmagnet_metainfo::{
    parse_info_bytes, FileTree, MetaVersion, ParseInfoError, MAX_INFO_NESTING_DEPTH,
};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;

const PURE_V2_TORRENT: &[u8] =
    include_bytes!("../../../../internal/protocol/metainfo/testdata/bittorrent-v2-test.torrent");
const HYBRID_TORRENT: &[u8] = include_bytes!(
    "../../../../internal/protocol/metainfo/testdata/bittorrent-v2-hybrid-test.torrent"
);

fn decode_hex<const N: usize>(value: &str) -> [u8; N] {
    assert_eq!(value.len(), N * 2);
    let mut decoded = [0; N];
    for (index, byte) in decoded.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).unwrap();
    }
    decoded
}

fn sha1(raw: &[u8]) -> [u8; 20] {
    Sha1::digest(raw).into()
}

fn sha256(raw: &[u8]) -> [u8; 32] {
    Sha256::digest(raw).into()
}

fn synthetic_v1() -> Vec<u8> {
    let mut raw =
        b"d6:lengthi4096e4:name20:synthetic-single.bin12:piece lengthi32768e6:pieces20:".to_vec();
    raw.extend_from_slice(&[0; 20]);
    raw.push(b'e');
    raw
}

fn extract_info_dictionary(torrent: &[u8]) -> &[u8] {
    assert_eq!(torrent.first(), Some(&b'd'));
    let mut cursor = 1;
    while torrent[cursor] != b'e' {
        let (key, value_start) = byte_string(torrent, cursor);
        let value_end = skip_value(torrent, value_start, 0);
        if key == b"info" {
            return &torrent[value_start..value_end];
        }
        cursor = value_end;
    }
    panic!("torrent fixture has no info dictionary")
}

fn byte_string(input: &[u8], start: usize) -> (&[u8], usize) {
    let colon = input[start..]
        .iter()
        .position(|byte| *byte == b':')
        .map(|offset| start + offset)
        .unwrap();
    let length = std::str::from_utf8(&input[start..colon])
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let value_start = colon + 1;
    let value_end = value_start + length;
    (&input[value_start..value_end], value_end)
}

fn skip_value(input: &[u8], start: usize, depth: usize) -> usize {
    assert!(depth < 256);
    match input[start] {
        b'i' => input[start + 1..]
            .iter()
            .position(|byte| *byte == b'e')
            .map(|offset| start + offset + 2)
            .unwrap(),
        b'l' => {
            let mut cursor = start + 1;
            while input[cursor] != b'e' {
                cursor = skip_value(input, cursor, depth + 1);
            }
            cursor + 1
        }
        b'd' => {
            let mut cursor = start + 1;
            while input[cursor] != b'e' {
                let (_, value_start) = byte_string(input, cursor);
                cursor = skip_value(input, value_start, depth + 1);
            }
            cursor + 1
        }
        b'0'..=b'9' => byte_string(input, start).1,
        byte => panic!("invalid fixture bencode byte {byte:#x}"),
    }
}

fn tree_stats(tree: &FileTree) -> (usize, i64) {
    if tree.is_dir() {
        tree.children()
            .values()
            .map(tree_stats)
            .fold((0, 0), |left, right| (left.0 + right.0, left.1 + right.1))
    } else {
        (1, tree.file().length())
    }
}

#[test]
fn parses_synthetic_v1_identity_and_raw_fields() {
    let raw = synthetic_v1();
    let expected = decode_hex("345b04b60b9afeb8d1e1209c19b0f625b3e7a8f8");
    assert_eq!(raw.len(), 98);
    assert_eq!(sha1(&raw), expected);

    let parsed = parse_info_bytes(expected, &raw).unwrap();
    assert_eq!(parsed.meta_version(), MetaVersion::V1);
    assert_eq!(parsed.info_hash_v1(), Some(expected));
    assert_eq!(parsed.info_hash_v2(), None);
    assert_eq!(parsed.info().name(), b"synthetic-single.bin");
    assert_eq!(parsed.info().best_name(), b"synthetic-single.bin");
    assert_eq!(parsed.info().piece_length(), 32_768);
    assert_eq!(parsed.info().length(), 4_096);
    assert_eq!(parsed.info().pieces(), &[0; 20]);
    assert_eq!(parsed.info().files(), None);
}

#[test]
fn parses_pure_v2_fixture_with_full_identity_and_file_tree() {
    assert_eq!(
        sha256(PURE_V2_TORRENT),
        decode_hex("e729044e5bb92be09963c9584f129bc2af1a830363a50e2bd01ff988b948fdeb")
    );
    let raw = extract_info_dictionary(PURE_V2_TORRENT);
    let requested = decode_hex("caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa");
    let full = decode_hex("caf1e1c30e81cb361b9ee167c4aa64228a7fa4fa9f6105232b28ad099f3a302e");
    assert_eq!(raw.len(), 1_278);

    let parsed = parse_info_bytes(requested, raw).unwrap();
    assert_eq!(parsed.meta_version(), MetaVersion::V2);
    assert_eq!(parsed.info_hash_v1(), None);
    assert_eq!(parsed.info_hash_v2(), Some(full));
    assert_eq!(parsed.info().name(), b"bittorrent-v2-test");
    assert_eq!(parsed.info().piece_length(), 4_194_304);
    assert!(parsed.info().pieces().is_empty());
    assert_eq!(parsed.info().files(), None);
    assert_eq!(tree_stats(parsed.info().file_tree()), (11, 1_534_222_888));
}

#[test]
fn parses_hybrid_fixture_through_either_discovery_identity() {
    assert_eq!(
        sha256(HYBRID_TORRENT),
        decode_hex("8ba7575e64e9046cac74ca6523bff6445ff5c3e369d5d132607a793a1834e93f")
    );
    let raw = extract_info_dictionary(HYBRID_TORRENT);
    let v1 = decode_hex("631a31dd0a46257d5078c0dee4e66e26f73e42ac");
    let v2 = decode_hex("d8dd32ac93357c368556af3ac1d95c9d76bd0dff6fa9833ecdac3d53134efabb");
    let v2_short = decode_hex("d8dd32ac93357c368556af3ac1d95c9d76bd0dff");
    assert_eq!(raw.len(), 36_333);

    let from_v1 = parse_info_bytes(v1, raw).unwrap();
    let from_v2 = parse_info_bytes(v2_short, raw).unwrap();
    assert_eq!(from_v1, from_v2);
    assert_eq!(from_v1.meta_version(), MetaVersion::V2);
    assert_eq!(from_v1.info_hash_v1(), Some(v1));
    assert_eq!(from_v1.info_hash_v2(), Some(v2));
    assert_eq!(from_v1.info().name(), b"bittorrent-v1-v2-hybrid-test");
    assert_eq!(from_v1.info().piece_length(), 524_288);
    assert_eq!(from_v1.info().pieces().len(), 34_300);
    assert_eq!(from_v1.info().files().unwrap().len(), 17);
    assert_eq!(tree_stats(from_v1.info().file_tree()), (9, 895_544_883));
}

#[test]
fn raw_hash_verification_precedes_decode() {
    for original in [
        synthetic_v1(),
        extract_info_dictionary(PURE_V2_TORRENT).to_vec(),
        extract_info_dictionary(HYBRID_TORRENT).to_vec(),
    ] {
        let requested = if original.len() == 1_278 {
            sha256(&original)[..20].try_into().unwrap()
        } else {
            sha1(&original)
        };
        let mut tampered = original;
        *tampered.last_mut().unwrap() ^= 0xff;
        assert_eq!(
            parse_info_bytes(requested, &tampered),
            Err(ParseInfoError::WrongHash)
        );
    }

    let malformed = b"d4:name3:abc";
    assert!(matches!(
        parse_info_bytes(sha1(malformed), malformed),
        Err(ParseInfoError::Bencode(_))
    ));
}

#[test]
fn rejects_wrong_hash_trailing_object_and_noncanonical_dictionary() {
    let raw = synthetic_v1();
    assert_eq!(
        parse_info_bytes([0; 20], &raw),
        Err(ParseInfoError::WrongHash)
    );

    let mut trailing = raw;
    trailing.extend_from_slice(b"i0e");
    assert_eq!(
        parse_info_bytes(sha1(&trailing), &trailing),
        Err(ParseInfoError::TrailingObject)
    );

    let unsorted = b"d1:bi1e1:ai2ee";
    assert!(matches!(
        parse_info_bytes(sha1(unsorted), unsorted),
        Err(ParseInfoError::Bencode(_))
    ));
}

#[test]
fn bounds_depth_and_rejects_known_fields_with_wrong_types() {
    let wrong_type = b"d4:namei1ee";
    assert!(matches!(
        parse_info_bytes(sha1(wrong_type), wrong_type),
        Err(ParseInfoError::InvalidField { field, expected })
            if field == "info.name" && expected == "a byte string"
    ));

    let mut deep = b"d1:x".to_vec();
    deep.extend(std::iter::repeat_n(b'l', MAX_INFO_NESTING_DEPTH + 1));
    deep.extend(std::iter::repeat_n(b'e', MAX_INFO_NESTING_DEPTH + 1));
    deep.push(b'e');
    assert!(matches!(
        parse_info_bytes(sha1(&deep), &deep),
        Err(ParseInfoError::Bencode(_))
    ));
}

#[test]
fn retains_raw_names_and_files_presence_while_ignoring_unknown_fields() {
    let raw_name = b"d4:name2:\xff\x0010:name.utf-82:\xfe\x001:x4:keepe";
    let parsed = parse_info_bytes(sha1(raw_name), raw_name).unwrap();
    assert_eq!(parsed.info().name(), &[0xff, 0]);
    assert_eq!(parsed.info().name_utf8(), &[0xfe, 0]);
    assert_eq!(parsed.info().best_name(), &[0xfe, 0]);

    let omitted = b"d12:meta versioni2ee";
    let parsed = parse_info_bytes(sha1(omitted), omitted).unwrap();
    assert!(!parsed.info().has_v1());
    assert!(parsed.info().has_v2());
    assert_eq!(parsed.info().files(), None);

    let present_empty = b"d5:filesle12:meta versioni2ee";
    let parsed = parse_info_bytes(sha1(present_empty), present_empty).unwrap();
    assert!(parsed.info().has_v1());
    assert!(parsed.info().has_v2());
    assert_eq!(parsed.info().files(), Some([].as_slice()));
}
