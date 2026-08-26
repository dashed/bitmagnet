use std::fs;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Deserialize;

pub(crate) fn goldens_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../testdata/parity/torznab")
}

pub(crate) fn load_corpus(path: &Path) -> Vec<CorpusQuery> {
    read_jsonl(path)
}

pub(crate) fn read_jsonl<T: DeserializeOwned>(path: &Path) -> Vec<T> {
    let text = fs::read_to_string(path).expect("jsonl is readable");
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("jsonl line parses"))
        .collect()
}

#[derive(Debug, Deserialize)]
pub(crate) struct CorpusQuery {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) path: String,
    #[serde(default, rename = "expectIds")]
    pub(crate) expect_ids: Option<Vec<String>>,
}

impl CorpusQuery {
    pub(crate) fn golden_name(&self) -> String {
        if self.id == "caps" {
            "caps.golden.xml".to_owned()
        } else {
            format!("q-{}.golden.xml", self.id)
        }
    }
}

#[derive(Debug)]
enum Child {
    Element(Element),
    Text(String),
}

#[derive(Debug)]
struct Element {
    name: String,
    attrs: Vec<(String, String)>,
    children: Vec<Child>,
}

/// Canonical XML normalizer ported from `internal/parity/torznab_xml.go`.
pub(crate) fn normalize(raw: &[u8]) -> Vec<u8> {
    use quick_xml::events::Event;
    use quick_xml::reader::Reader;

    let mut reader = Reader::from_reader(raw);
    let mut buf = Vec::new();
    let mut stack: Vec<Element> = Vec::new();
    let mut root: Option<Element> = None;

    loop {
        match reader.read_event_into(&mut buf).expect("valid XML") {
            Event::Eof => break,
            Event::Start(start) => stack.push(element_from(&start)),
            Event::Empty(empty) => {
                let element = element_from(&empty);
                attach(&mut stack, &mut root, element);
            }
            Event::End(_) => {
                let element = stack.pop().expect("balanced end tag");
                attach(&mut stack, &mut root, element);
            }
            Event::Text(text) => {
                let value = unescape_bytes(&text.into_inner());
                if value.trim().is_empty() {
                    continue;
                }
                let parent = stack.last_mut().expect("text within an element");
                parent.children.push(Child::Text(value));
            }
            Event::CData(cdata) => {
                let value =
                    String::from_utf8(cdata.into_inner().into_owned()).expect("utf-8 cdata");
                if let Some(parent) = stack.last_mut() {
                    parent.children.push(Child::Text(value));
                }
            }
            // XML declaration, comments, processing instructions, doctype: the
            // canonical tree carries only elements and text.
            _ => {}
        }
        buf.clear();
    }

    let root = root.expect("document has a root element");
    let mut out = String::new();
    write_element(&mut out, &root, 0);
    out.into_bytes()
}

fn element_from(start: &quick_xml::events::BytesStart<'_>) -> Element {
    let name = String::from_utf8(start.name().as_ref().to_vec()).expect("utf-8 element name");
    let attrs = start
        .attributes()
        .map(|attr| {
            let attr = attr.expect("well-formed attribute");
            let key = String::from_utf8(attr.key.as_ref().to_vec()).expect("utf-8 attr name");
            let value = unescape_bytes(&attr.value);
            (key, value)
        })
        .collect();
    Element {
        name,
        attrs,
        children: Vec::new(),
    }
}

/// Decode predefined and numeric XML escapes written by Go's `encoding/xml`.
fn unescape_bytes(bytes: &[u8]) -> String {
    let raw = std::str::from_utf8(bytes).expect("utf-8 xml text");
    quick_xml::escape::unescape(raw)
        .expect("valid xml escapes")
        .into_owned()
}

fn attach(stack: &mut [Element], root: &mut Option<Element>, element: Element) {
    match stack.last_mut() {
        Some(parent) => parent.children.push(Child::Element(element)),
        None => *root = Some(element),
    }
}

fn write_element(out: &mut String, element: &Element, depth: usize) {
    let indent = "  ".repeat(depth);
    out.push_str(&indent);
    out.push('<');
    out.push_str(&element.name);

    let mut attrs = element.attrs.clone();
    attrs.sort_by(|left, right| left.0.cmp(&right.0));
    for (name, value) in &attrs {
        out.push(' ');
        out.push_str(name);
        out.push_str("=\"");
        push_escaped(out, value, true);
        out.push('"');
    }

    if element.children.is_empty() {
        out.push_str("/>\n");
        return;
    }

    let has_text = element
        .children
        .iter()
        .any(|child| matches!(child, Child::Text(_)));
    let has_element = element
        .children
        .iter()
        .any(|child| matches!(child, Child::Element(_)));
    assert!(
        !(has_text && has_element),
        "mixed content in <{}> is unsupported",
        element.name
    );

    if has_text {
        out.push('>');
        for child in &element.children {
            if let Child::Text(text) = child {
                push_escaped(out, text, false);
            }
        }
        out.push_str("</");
        out.push_str(&element.name);
        out.push_str(">\n");
        return;
    }

    out.push_str(">\n");
    for child in &element.children {
        if let Child::Element(element) = child {
            write_element(out, element, depth + 1);
        }
    }
    out.push_str(&indent);
    out.push_str("</");
    out.push_str(&element.name);
    out.push_str(">\n");
}

/// Canonical escaping: `& < >` always and `"` only in attribute values.
fn push_escaped(out: &mut String, value: &str, attribute: bool) {
    for character in value.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' if attribute => out.push_str("&quot;"),
            _ => out.push(character),
        }
    }
}

pub(crate) fn first_diff(actual: &[u8], expected: &[u8]) -> String {
    let actual = String::from_utf8_lossy(actual);
    let expected = String::from_utf8_lossy(expected);
    for (index, (actual_line, expected_line)) in actual.lines().zip(expected.lines()).enumerate() {
        if actual_line != expected_line {
            return format!(
                "  first diff at line {}:\n  actual:   {actual_line:?}\n  expected: {expected_line:?}",
                index + 1
            );
        }
    }
    if actual.lines().count() != expected.lines().count() {
        return format!(
            "  line count differs: actual {} vs expected {}",
            actual.lines().count(),
            expected.lines().count(),
        );
    }
    "  (bytes differ only in trailing whitespace)".to_owned()
}
