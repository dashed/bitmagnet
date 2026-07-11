//! Lane G parity-golden smoke tests.

use std::{fs, io::Cursor, path::Path};

use quick_xml::{events::Event, Reader};

#[test]
fn lane_g_torznab_goldens_parse_when_present() {
    let golden_directory =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../testdata/parity/torznab");

    if !golden_directory.is_dir() {
        eprintln!(
            "skipping Lane G Torznab goldens: {} is absent",
            golden_directory.display()
        );
        return;
    }

    let mut golden_files = fs::read_dir(&golden_directory)
        .expect("Torznab golden directory is readable")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".golden.xml"))
        })
        .collect::<Vec<_>>();
    golden_files.sort();

    if golden_files.is_empty() {
        eprintln!(
            "skipping Lane G Torznab goldens: no *.golden.xml files in {}",
            golden_directory.display()
        );
        return;
    }

    for golden_file in golden_files {
        let bytes = fs::read(&golden_file).expect("Torznab golden file is readable");
        assert_parses(&bytes, &golden_file);
    }
}

fn assert_parses(bytes: &[u8], path: &Path) {
    let mut reader = Reader::from_reader(Cursor::new(bytes));
    let mut buffer = Vec::new();
    let mut saw_element = false;

    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Eof) => break,
            Ok(Event::Start(_) | Event::Empty(_)) => {
                saw_element = true;
                buffer.clear();
            }
            Ok(_) => buffer.clear(),
            Err(error) => panic!("{} is not valid XML: {error}", path.display()),
        }
    }

    assert!(saw_element, "{} contains no XML element", path.display());
}
