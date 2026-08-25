use bitmagnet_metainfo::{
    parse_info_bytes, InfoFilesError, MetaVersion, ParseInfoError, MAX_INFO_NESTING_DEPTH,
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

fn encoded_bytes(value: &[u8]) -> Vec<u8> {
    let mut encoded = value.len().to_string().into_bytes();
    encoded.push(b':');
    encoded.extend_from_slice(value);
    encoded
}

fn encoded_integer(value: i64) -> Vec<u8> {
    format!("i{value}e").into_bytes()
}

fn encoded_list(values: Vec<Vec<u8>>) -> Vec<u8> {
    let mut encoded = vec![b'l'];
    for value in values {
        encoded.extend(value);
    }
    encoded.push(b'e');
    encoded
}

fn encoded_dict(mut entries: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<u8> {
    entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let mut encoded = vec![b'd'];
    for (key, value) in entries {
        encoded.extend(encoded_bytes(&key));
        encoded.extend(value);
    }
    encoded.push(b'e');
    encoded
}

fn entry(key: &[u8], value: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
    (key.to_vec(), value)
}

fn v1_file(length: i64, path: &[&[u8]], path_utf8: &[&[u8]]) -> Vec<u8> {
    let mut entries = vec![
        entry(b"length", encoded_integer(length)),
        entry(
            b"path",
            encoded_list(path.iter().map(|part| encoded_bytes(part)).collect()),
        ),
    ];
    if !path_utf8.is_empty() {
        entries.push(entry(
            b"path.utf-8",
            encoded_list(path_utf8.iter().map(|part| encoded_bytes(part)).collect()),
        ));
    }
    encoded_dict(entries)
}

fn v2_leaf(length: i64, pieces_root: &[u8]) -> Vec<u8> {
    let mut properties = vec![entry(b"length", encoded_integer(length))];
    if !pieces_root.is_empty() {
        properties.push(entry(b"pieces root", encoded_bytes(pieces_root)));
    }
    encoded_dict(vec![entry(b"", encoded_dict(properties))])
}

fn v2_info(piece_length: i64, children: Vec<(&[u8], Vec<u8>)>) -> Vec<u8> {
    encoded_dict(vec![
        entry(
            b"file tree",
            encoded_dict(
                children
                    .into_iter()
                    .map(|(name, tree)| entry(name, tree))
                    .collect(),
            ),
        ),
        entry(b"meta version", encoded_integer(2)),
        entry(b"piece length", encoded_integer(piece_length)),
    ])
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
    assert!(!parsed.info().is_dir());
    let files = parsed.info().upverted_files().unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].length(), 4_096);
    assert!(files[0].path().is_empty());
    assert!(files[0].path_utf8().is_empty());
    assert_eq!(files[0].pieces_root(), None);
    assert_eq!(files[0].torrent_offset(), 0);
    assert_eq!(parsed.info().total_length(), Ok(4_096));
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
    assert!(parsed.info().is_dir());
    assert_eq!(parsed.info().upverted_files().unwrap().len(), 11);
    assert_eq!(parsed.info().total_length(), Ok(1_534_222_888));
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
    assert!(from_v1.info().is_dir());
    let projected = from_v1.info().upverted_files().unwrap();
    assert_eq!(projected.len(), 9);
    assert!(projected
        .iter()
        .all(|file| { file.path().iter().all(|part| part.as_slice() != b".pad") }));
    assert_eq!(from_v1.info().total_length(), Ok(895_544_883));
}

#[test]
fn normalizes_v1_multifile_paths_and_offsets_in_source_order() {
    let raw = encoded_dict(vec![
        entry(
            b"files",
            encoded_list(vec![
                v1_file(3, &[b"raw", &[0xff, b'x']], &["ümlaut.bin".as_bytes()]),
                v1_file(5, &[b"z.bin"], &[]),
            ]),
        ),
        entry(b"name", encoded_bytes(b"raw-root")),
        entry(b"name.utf-8", encoded_bytes("utf8-root".as_bytes())),
        entry(b"piece length", encoded_integer(4)),
    ]);
    let parsed = parse_info_bytes(sha1(&raw), &raw).unwrap();

    assert!(parsed.info().is_dir());
    assert_eq!(parsed.info().best_name(), b"utf8-root");
    let files = parsed.info().upverted_files().unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!(files[0].length(), 3);
    assert_eq!(files[0].path(), [b"raw".to_vec(), vec![0xff, b'x']]);
    assert_eq!(files[0].path_utf8(), ["ümlaut.bin".as_bytes().to_vec()]);
    assert_eq!(files[0].best_path(), files[0].path_utf8());
    assert_eq!(files[0].torrent_offset(), 0);
    assert_eq!(files[1].length(), 5);
    assert_eq!(files[1].best_path(), [b"z.bin".to_vec()]);
    assert_eq!(files[1].torrent_offset(), 3);
    assert_eq!(parsed.info().total_length(), Ok(8));
}

#[test]
fn normalizes_v2_in_byte_sorted_order_with_piece_aligned_offsets() {
    let root_a = [0x11; 32];
    let root_z = [0x22; 32];
    let raw = v2_info(
        4,
        vec![(b"z", v2_leaf(5, &root_z)), (b"a", v2_leaf(3, &root_a))],
    );
    let parsed = parse_info_bytes(sha1(&raw), &raw).unwrap();
    let files = parsed.info().upverted_files().unwrap();

    assert_eq!(files.len(), 2);
    assert_eq!(files[0].path(), [b"a".to_vec()]);
    assert_eq!(files[0].path_utf8(), files[0].path());
    assert_eq!(files[0].pieces_root(), Some(root_a));
    assert_eq!(files[0].torrent_offset(), 0);
    assert_eq!(files[1].path(), [b"z".to_vec()]);
    assert_eq!(files[1].pieces_root(), Some(root_z));
    assert_eq!(files[1].torrent_offset(), 4);
    assert_eq!(parsed.info().total_length(), Ok(8));
}

#[test]
fn v2_directory_properties_are_ignored_and_empty_child_is_a_zero_length_leaf() {
    let parent = encoded_dict(vec![
        entry(
            b"",
            encoded_dict(vec![entry(b"length", encoded_integer(99))]),
        ),
        entry(b"leaf", encoded_dict(Vec::new())),
    ]);
    let raw = v2_info(4, vec![(b"parent", parent)]);
    let parsed = parse_info_bytes(sha1(&raw), &raw).unwrap();

    let files = parsed.info().upverted_files().unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path(), [b"parent".to_vec(), b"leaf".to_vec()]);
    assert_eq!(files[0].length(), 0);
    assert_eq!(files[0].pieces_root(), None);
    assert_eq!(files[0].torrent_offset(), 0);
    assert_eq!(parsed.info().total_length(), Ok(0));
}

#[test]
fn rejects_negative_lengths_for_v1_and_v2_projections() {
    let v1 = encoded_dict(vec![
        entry(b"length", encoded_integer(-1)),
        entry(b"name", encoded_bytes(b"negative")),
    ]);
    let v2 = v2_info(16, vec![(b"negative", v2_leaf(-1, &[]))]);

    for raw in [v1, v2] {
        let parsed = parse_info_bytes(sha1(&raw), &raw).unwrap();
        assert!(matches!(
            parsed.info().upverted_files(),
            Err(InfoFilesError::NegativeFileLength { length: -1, .. })
        ));
        assert!(matches!(
            parsed.info().total_length(),
            Err(InfoFilesError::NegativeFileLength { length: -1, .. })
        ));
    }
}

#[test]
fn rejects_nonpositive_v2_piece_lengths() {
    for piece_length in [0, -1] {
        let raw = v2_info(piece_length, vec![(b"file", v2_leaf(1, &[]))]);
        let parsed = parse_info_bytes(sha1(&raw), &raw).unwrap();
        assert_eq!(
            parsed.info().upverted_files(),
            Err(InfoFilesError::NonPositiveV2PieceLength(piece_length))
        );
        assert_eq!(
            parsed.info().total_length(),
            Err(InfoFilesError::NonPositiveV2PieceLength(piece_length))
        );
    }
}

#[test]
fn rejects_invalid_v2_pieces_root_lengths_without_panicking() {
    for pieces_root in [vec![0x11], vec![0x22; 33]] {
        let raw = v2_info(16, vec![(b"file", v2_leaf(1, &pieces_root))]);
        let parsed = parse_info_bytes(sha1(&raw), &raw).unwrap();
        let expected = pieces_root.len();
        assert!(matches!(
            parsed.info().upverted_files(),
            Err(InfoFilesError::InvalidPiecesRootLength { actual, .. }) if actual == expected
        ));
        assert!(matches!(
            parsed.info().total_length(),
            Err(InfoFilesError::InvalidPiecesRootLength { actual, .. }) if actual == expected
        ));
    }
}

#[test]
fn rejects_offset_alignment_and_total_length_overflow() {
    let v1 = encoded_dict(vec![entry(
        b"files",
        encoded_list(vec![
            v1_file(i64::MAX, &[b"first"], &[]),
            v1_file(1, &[b"second"], &[]),
        ]),
    )]);
    let parsed = parse_info_bytes(sha1(&v1), &v1).unwrap();
    assert!(matches!(
        parsed.info().upverted_files(),
        Err(InfoFilesError::TorrentOffsetOverflow { .. })
    ));
    assert!(matches!(
        parsed.info().total_length(),
        Err(InfoFilesError::TotalLengthOverflow { .. })
    ));

    let v2 = v2_info(i64::MAX - 1, vec![(b"file", v2_leaf(i64::MAX, &[]))]);
    let parsed = parse_info_bytes(sha1(&v2), &v2).unwrap();
    assert!(matches!(
        parsed.info().upverted_files(),
        Err(InfoFilesError::TorrentOffsetOverflow { .. })
    ));
    assert_eq!(parsed.info().total_length(), Ok(i64::MAX));
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

    let present_empty_v1 = b"d5:fileslee";
    let parsed = parse_info_bytes(sha1(present_empty_v1), present_empty_v1).unwrap();
    assert!(parsed.info().has_v1());
    assert!(!parsed.info().has_v2());
    assert!(!parsed.info().is_dir());
    assert_eq!(parsed.info().upverted_files().unwrap().len(), 1);
    assert_eq!(parsed.info().total_length(), Ok(0));
}
