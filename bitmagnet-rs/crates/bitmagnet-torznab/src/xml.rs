//! Deterministic XML rendering compatible with Go's `encoding/xml` output.

use std::fmt::{self, Display, Write as _};

use thiserror::Error;

use crate::response::{Caps, CapsSearch, Category, Channel, Item, SearchResult, TorznabError};

const XML_HEADER: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
const ATOM_NAMESPACE: &str = "http://www.w3.org/2005/Atom";
const TORZNAB_NAMESPACE: &str = "http://torznab.com/schemas/2015/feed";
const NEWZNAB_NAMESPACE: &str = "http://www.newznab.com/DTD/2010/feeds/attributes/";

/// Failure while formatting a Torznab XML document.
#[derive(Debug, Error)]
pub enum XmlError {
    #[error("failed to format XML")]
    Format(#[from] fmt::Error),
}

impl Caps {
    /// Serializes this caps response with Go-compatible whitespace and escaping.
    pub fn to_xml(&self) -> Result<Vec<u8>, XmlError> {
        render_caps(self)
    }

    /// Alias matching the Go response method's name.
    pub fn xml(&self) -> Result<Vec<u8>, XmlError> {
        self.to_xml()
    }
}

impl SearchResult {
    /// Serializes this RSS response with Go-compatible whitespace and escaping.
    pub fn to_xml(&self) -> Result<Vec<u8>, XmlError> {
        render_search_result(self)
    }

    /// Alias matching the Go response method's name.
    pub fn xml(&self) -> Result<Vec<u8>, XmlError> {
        self.to_xml()
    }
}

impl TorznabError {
    /// Serializes this Torznab error with Go-compatible whitespace and escaping.
    pub fn to_xml(&self) -> Result<Vec<u8>, XmlError> {
        render_error(self)
    }

    /// Alias matching the Go response method's name.
    pub fn xml(&self) -> Result<Vec<u8>, XmlError> {
        self.to_xml()
    }
}

#[derive(Clone, Copy)]
enum AttributeValue<'a> {
    Text(&'a str),
    I32(i32),
    U32(u32),
}

type Attribute<'a> = (&'a str, AttributeValue<'a>);

struct XmlWriter {
    output: String,
    depth: usize,
}

impl XmlWriter {
    fn new() -> Self {
        Self {
            output: XML_HEADER.to_owned(),
            depth: 0,
        }
    }

    fn root_start(&mut self, name: &str, attributes: &[Attribute<'_>]) -> fmt::Result {
        self.write_start_tag(name, attributes)?;
        self.depth += 1;
        Ok(())
    }

    fn root_expanded(&mut self, name: &str, attributes: &[Attribute<'_>]) -> fmt::Result {
        self.write_start_tag(name, attributes)?;
        self.write_end_tag(name);
        Ok(())
    }

    fn start(&mut self, name: &str, attributes: &[Attribute<'_>]) -> fmt::Result {
        self.start_line();
        self.write_start_tag(name, attributes)?;
        self.depth += 1;
        Ok(())
    }

    fn expanded(&mut self, name: &str, attributes: &[Attribute<'_>]) -> fmt::Result {
        self.start_line();
        self.write_start_tag(name, attributes)?;
        self.write_end_tag(name);
        Ok(())
    }

    fn text(&mut self, name: &str, value: &str) -> fmt::Result {
        self.start_line();
        self.write_start_tag(name, &[])?;
        push_escaped_xml(&mut self.output, value);
        self.write_end_tag(name);
        Ok(())
    }

    fn display(&mut self, name: &str, value: impl Display) -> fmt::Result {
        self.start_line();
        self.write_start_tag(name, &[])?;
        write!(self.output, "{value}")?;
        self.write_end_tag(name);
        Ok(())
    }

    fn end(&mut self, name: &str) -> fmt::Result {
        self.depth = self.depth.checked_sub(1).ok_or(fmt::Error)?;
        self.start_line();
        self.write_end_tag(name);
        Ok(())
    }

    fn finish(self) -> Vec<u8> {
        self.output.into_bytes()
    }

    fn start_line(&mut self) {
        self.output.push('\n');
        for _ in 0..self.depth {
            self.output.push_str("  ");
        }
    }

    fn write_start_tag(&mut self, name: &str, attributes: &[Attribute<'_>]) -> fmt::Result {
        self.output.push('<');
        self.output.push_str(name);

        for (attribute_name, attribute_value) in attributes {
            self.output.push(' ');
            self.output.push_str(attribute_name);
            self.output.push_str("=\"");
            match attribute_value {
                AttributeValue::Text(value) => push_escaped_xml(&mut self.output, value),
                AttributeValue::I32(value) => write!(self.output, "{value}")?,
                AttributeValue::U32(value) => write!(self.output, "{value}")?,
            }
            self.output.push('"');
        }

        self.output.push('>');
        Ok(())
    }

    fn write_end_tag(&mut self, name: &str) {
        self.output.push_str("</");
        self.output.push_str(name);
        self.output.push('>');
    }
}

fn push_escaped_xml(output: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&#34;"),
            '\'' => output.push_str("&#39;"),
            '\t' => output.push_str("&#x9;"),
            '\n' => output.push_str("&#xA;"),
            '\r' => output.push_str("&#xD;"),
            _ => output.push(character),
        }
    }
}

fn render_caps(caps: &Caps) -> Result<Vec<u8>, XmlError> {
    let mut writer = XmlWriter::new();
    writer.root_start("caps", &[])?;
    writer.expanded(
        "server",
        &[("title", AttributeValue::Text(&caps.server.title))],
    )?;

    let mut limit_attributes = Vec::with_capacity(2);
    if caps.limits.max != 0 {
        limit_attributes.push(("max", AttributeValue::U32(caps.limits.max)));
    }
    if caps.limits.default != 0 {
        limit_attributes.push(("default", AttributeValue::U32(caps.limits.default)));
    }
    writer.expanded("limits", &limit_attributes)?;

    writer.start("searching", &[])?;
    render_caps_search(&mut writer, "search", &caps.searching.search)?;
    render_caps_search(&mut writer, "tv-search", &caps.searching.tv_search)?;
    render_caps_search(&mut writer, "movie-search", &caps.searching.movie_search)?;
    render_caps_search(&mut writer, "music-search", &caps.searching.music_search)?;
    render_caps_search(&mut writer, "audio-search", &caps.searching.audio_search)?;
    render_caps_search(&mut writer, "book-search", &caps.searching.book_search)?;
    writer.end("searching")?;

    if caps.categories.is_empty() {
        writer.expanded("categories", &[])?;
    } else {
        writer.start("categories", &[])?;
        for category in &caps.categories {
            render_category(&mut writer, category)?;
        }
        writer.end("categories")?;
    }
    writer.text("tags", &caps.tags)?;
    writer.end("caps")?;

    Ok(writer.finish())
}

fn render_caps_search(
    writer: &mut XmlWriter,
    element_name: &str,
    search: &CapsSearch,
) -> fmt::Result {
    let mut attributes = Vec::with_capacity(2);
    attributes.push(("available", AttributeValue::Text(search.available.as_str())));
    if !search.supported_params.is_empty() {
        attributes.push((
            "supportedParams",
            AttributeValue::Text(search.supported_params.as_str()),
        ));
    }

    writer.expanded(element_name, &attributes)
}

fn render_category(writer: &mut XmlWriter, category: &Category) -> fmt::Result {
    let attributes = [
        ("id", AttributeValue::I32(category.id)),
        ("name", AttributeValue::Text(&category.name)),
    ];

    if category.subcat.is_empty() {
        writer.expanded("category", &attributes)?;
        return Ok(());
    }

    writer.start("category", &attributes)?;
    for subcategory in &category.subcat {
        writer.expanded(
            "subcat",
            &[
                ("id", AttributeValue::I32(subcategory.id)),
                ("name", AttributeValue::Text(&subcategory.name)),
            ],
        )?;
    }
    writer.end("category")
}

fn render_search_result(result: &SearchResult) -> Result<Vec<u8>, XmlError> {
    let mut writer = XmlWriter::new();
    writer.root_start(
        "rss",
        &[
            ("version", AttributeValue::Text("2.0")),
            ("xmlns:atom", AttributeValue::Text(ATOM_NAMESPACE)),
            ("xmlns:torznab", AttributeValue::Text(TORZNAB_NAMESPACE)),
        ],
    )?;
    render_channel(&mut writer, &result.channel)?;
    writer.end("rss")?;

    Ok(writer.finish())
}

fn render_channel(writer: &mut XmlWriter, channel: &Channel) -> fmt::Result {
    writer.start("channel", &[])?;
    render_optional_text(writer, "title", channel.title.as_deref())?;
    render_optional_text(writer, "link", channel.link.as_deref())?;
    render_optional_text(writer, "description", channel.description.as_deref())?;
    render_optional_text(writer, "language", channel.language.as_deref())?;
    writer.text("pubDate", &channel.pub_date.format())?;
    writer.text("lastBuildDate", &channel.last_build_date.format())?;
    render_optional_text(writer, "docs", channel.docs.as_deref())?;
    render_optional_text(writer, "generator", channel.generator.as_deref())?;

    let mut response_attributes = Vec::with_capacity(3);
    response_attributes.push(("xmlns", AttributeValue::Text(NEWZNAB_NAMESPACE)));
    if channel.response.offset != 0 {
        response_attributes.push(("offset", AttributeValue::U32(channel.response.offset)));
    }
    if channel.response.total != 0 {
        response_attributes.push(("total", AttributeValue::U32(channel.response.total)));
    }
    writer.expanded("response", &response_attributes)?;

    for item in &channel.items {
        render_item(writer, item)?;
    }
    writer.end("channel")
}

fn render_optional_text(
    writer: &mut XmlWriter,
    element_name: &str,
    value: Option<&str>,
) -> fmt::Result {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        writer.text(element_name, value)?;
    }
    Ok(())
}

fn render_item(writer: &mut XmlWriter, item: &Item) -> fmt::Result {
    writer.start("item", &[])?;
    writer.text("title", &item.title)?;
    render_optional_text(writer, "guid", item.guid.as_deref())?;
    writer.text("pubDate", &item.pub_date.format())?;
    render_optional_text(writer, "category", item.category.as_deref())?;
    render_optional_text(writer, "link", item.link.as_deref())?;
    writer.display("size", item.size)?;
    render_optional_text(writer, "description", item.description.as_deref())?;
    render_optional_text(writer, "comments", item.comments.as_deref())?;
    writer.expanded(
        "enclosure",
        &[
            ("url", AttributeValue::Text(&item.enclosure.url)),
            ("length", AttributeValue::Text(&item.enclosure.length)),
            ("type", AttributeValue::Text(&item.enclosure.type_)),
        ],
    )?;

    for attribute in &item.torznab_attrs {
        writer.expanded(
            "torznab:attr",
            &[
                ("name", AttributeValue::Text(&attribute.name)),
                ("value", AttributeValue::Text(&attribute.value)),
            ],
        )?;
    }
    writer.end("item")
}

fn render_error(error: &TorznabError) -> Result<Vec<u8>, XmlError> {
    let mut writer = XmlWriter::new();
    writer.root_expanded(
        "error",
        &[
            ("error", AttributeValue::I32(error.code)),
            ("description", AttributeValue::Text(&error.description)),
        ],
    )?;

    Ok(writer.finish())
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::push_escaped_xml;
    use crate::response::{
        caps, Channel, Enclosure, Item, Response, RssDate, SearchResult, TorznabAttr, TorznabError,
    };

    const CAPS_DEFAULT: &[u8] = include_bytes!("../tests/fixtures/caps_default.xml");
    const ERROR_MISSING_PARAMETER: &[u8] =
        include_bytes!("../tests/fixtures/error_missing_parameter.xml");
    const SEARCH_EMPTY: &[u8] = include_bytes!("../tests/fixtures/search_empty.xml");

    #[test]
    fn hand_built_fixtures_are_byte_exact() {
        let cases = [
            (
                "caps_default",
                caps("bitmagnet", 100, 100)
                    .to_xml()
                    .expect("caps XML renders"),
                CAPS_DEFAULT,
            ),
            (
                "search_empty",
                SearchResult::default()
                    .to_xml()
                    .expect("search XML renders"),
                SEARCH_EMPTY,
            ),
        ];

        for (name, actual, expected) in cases {
            assert_eq!(actual.as_slice(), expected, "fixture {name}");
            assert!(!actual.ends_with(b"\n"), "fixture {name}");
        }
    }

    #[test]
    fn error_response_matches_the_go_attribute_names_and_order() {
        let error = TorznabError {
            code: 200,
            description: "missing parameter (t)".to_owned(),
        };
        assert_eq!(
            error.to_xml().expect("error XML renders"),
            ERROR_MISSING_PARAMETER
        );
    }

    #[test]
    fn populated_item_preserves_element_and_torznab_attribute_order() {
        let date = rss_date("2024-01-02T03:04:05+00:00");
        let result = SearchResult {
            channel: Channel {
                title: Some("bitmagnet".to_owned()),
                pub_date: date.clone(),
                last_build_date: date.clone(),
                response: Response {
                    offset: 5,
                    total: 10,
                },
                items: vec![Item {
                    title: "Example".to_owned(),
                    guid: Some("012345".to_owned()),
                    pub_date: date,
                    category: Some("Movies".to_owned()),
                    link: Some("magnet:?xt=urn:btih:012345".to_owned()),
                    size: 42,
                    description: Some("Description".to_owned()),
                    comments: Some("Comments".to_owned()),
                    enclosure: Enclosure {
                        url: "magnet:?xt=urn:btih:012345".to_owned(),
                        length: "42".to_owned(),
                        type_: "application/x-bittorrent;x-scheme-handler/magnet".to_owned(),
                    },
                    torznab_attrs: vec![
                        TorznabAttr {
                            name: "infohash".to_owned(),
                            value: "012345".to_owned(),
                        },
                        TorznabAttr {
                            name: "size".to_owned(),
                            value: "42".to_owned(),
                        },
                    ],
                }],
                ..Channel::default()
            },
        };
        let expected = concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
            "<rss version=\"2.0\" xmlns:atom=\"http://www.w3.org/2005/Atom\" xmlns:torznab=\"http://torznab.com/schemas/2015/feed\">\n",
            "  <channel>\n",
            "    <title>bitmagnet</title>\n",
            "    <pubDate>Tue, 02 Jan 2024 03:04:05 +0000</pubDate>\n",
            "    <lastBuildDate>Tue, 02 Jan 2024 03:04:05 +0000</lastBuildDate>\n",
            "    <response xmlns=\"http://www.newznab.com/DTD/2010/feeds/attributes/\" offset=\"5\" total=\"10\"></response>\n",
            "    <item>\n",
            "      <title>Example</title>\n",
            "      <guid>012345</guid>\n",
            "      <pubDate>Tue, 02 Jan 2024 03:04:05 +0000</pubDate>\n",
            "      <category>Movies</category>\n",
            "      <link>magnet:?xt=urn:btih:012345</link>\n",
            "      <size>42</size>\n",
            "      <description>Description</description>\n",
            "      <comments>Comments</comments>\n",
            "      <enclosure url=\"magnet:?xt=urn:btih:012345\" length=\"42\" type=\"application/x-bittorrent;x-scheme-handler/magnet\"></enclosure>\n",
            "      <torznab:attr name=\"infohash\" value=\"012345\"></torznab:attr>\n",
            "      <torznab:attr name=\"size\" value=\"42\"></torznab:attr>\n",
            "    </item>\n",
            "  </channel>\n",
            "</rss>"
        );

        assert_eq!(
            result.to_xml().expect("populated search XML renders"),
            expected.as_bytes()
        );
    }

    #[test]
    fn text_and_attributes_use_go_compatible_escaping() {
        let result = SearchResult {
            channel: Channel {
                items: vec![Item {
                    title: "& < > \" '".to_owned(),
                    enclosure: Enclosure {
                        url: "&<>\"'\t\n\r".to_owned(),
                        ..Enclosure::default()
                    },
                    ..Item::default()
                }],
                ..Channel::default()
            },
        };
        let xml = String::from_utf8(result.to_xml().expect("search XML renders"))
            .expect("rendered XML is UTF-8");

        assert!(xml.contains("<title>&amp; &lt; &gt; &#34; &#39;</title>"));
        assert!(xml.contains(
            "<enclosure url=\"&amp;&lt;&gt;&#34;&#39;&#x9;&#xA;&#xD;\" length=\"\" type=\"\"></enclosure>"
        ));
    }

    #[test]
    fn escaper_handles_all_go_character_replacements_in_one_pass() {
        let mut escaped = String::new();
        push_escaped_xml(&mut escaped, "&<>\"'\t\n\r");

        assert_eq!(escaped, "&amp;&lt;&gt;&#34;&#39;&#x9;&#xA;&#xD;");
    }

    #[test]
    fn empty_optional_strings_are_omitted() {
        let result = SearchResult {
            channel: Channel {
                title: Some(String::new()),
                link: Some(String::new()),
                items: vec![Item {
                    guid: Some(String::new()),
                    category: Some(String::new()),
                    ..Item::default()
                }],
                ..Channel::default()
            },
        };
        let xml = String::from_utf8(result.to_xml().expect("search XML renders"))
            .expect("rendered XML is UTF-8");

        assert!(!xml.contains("<guid>"));
        assert!(!xml.contains("<category>"));
        assert!(!xml.contains("<link>"));
    }

    #[test]
    fn zero_caps_limits_are_omitted_from_the_expanded_element() {
        let xml = String::from_utf8(caps("bitmagnet", 0, 0).to_xml().expect("caps XML renders"))
            .expect("rendered XML is UTF-8");

        assert!(xml.contains("<limits></limits>"));
    }

    #[test]
    fn rss_date_uses_the_go_layout_and_numeric_offset() {
        assert_eq!(
            RssDate::default().format(),
            "Mon, 01 Jan 0001 00:00:00 +0000"
        );
        assert_eq!(
            rss_date("2024-01-02T03:04:05+05:30").format(),
            "Tue, 02 Jan 2024 03:04:05 +0530"
        );
    }

    fn rss_date(value: &str) -> RssDate {
        RssDate(DateTime::parse_from_rfc3339(value).expect("valid test date"))
    }
}
