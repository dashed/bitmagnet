package parity

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"sort"
	"strings"
)

// NormalizeTorznabXML applies the shared canonical Torznab XML normal form.
//
// The normalization rules, verbatim from the Lane G design, are:
//
//  1. Drop the XML declaration (`<?xml ...?>`). The canonical form has none.
//  2. Re-serialize from the parsed tree, pretty-printed: 2-space indent per
//     depth, one element per line, LF endings, single trailing newline (mirrors the
//     SDL golden's deterministic layout).
//  3. Attributes are sorted by name (full name including any `xmlns:`/prefix),
//     with values escaped canonically (`& < > "` → `&amp; &lt; &gt; &quot;`).
//     Attribute order is not semantically meaningful, so sorting is safe and
//     erases writer differences (e.g. namespace-attr ordering on `<rss>`).
//  4. Child element order is preserved (document order). Ordering is the parity
//     contract for `<item>`s (ranked results) and reflects struct field order
//     elsewhere — never sort children.
//  5. Whitespace-only text nodes are dropped (indentation is insignificant).
//     Leaf text is preserved verbatim after XML-unescape → canonical re-escape.
//     Torznab output has no mixed content. CDATA is normalized to escaped text
//     (defensive; bitmagnet emits none).
//  6. Empty elements render uniformly as `<name/>` regardless of whether the
//     source wrote `<name/>` or `<name></name>`.
//  7. `omitempty` presence is not synthesized. The normalizer never adds or
//     removes elements; element presence is the contract and each side must
//     match Go's `omitempty` semantics (Lane T's job).
func NormalizeTorznabXML(raw []byte) ([]byte, error) {
	root, err := parseTorznabXML(raw)
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer
	if err := writeTorznabXMLElement(&out, root, 0); err != nil {
		return nil, err
	}

	return out.Bytes(), nil
}

// ExtractInfohashes returns one lower-case infohash per item, in document
// order. It prefers torznab:attr name="infohash" and falls back to guid.
func ExtractInfohashes(raw []byte) ([]string, error) {
	root, err := parseTorznabXML(raw)
	if err != nil {
		return nil, err
	}

	var hashes []string
	var visit func(*torznabXMLElement)
	visit = func(element *torznabXMLElement) {
		if localXMLName(element.name) == "item" {
			var guid string
			var infohash string
			var hasInfohash bool

			for _, child := range element.children {
				if child.element == nil {
					continue
				}

				switch child.element.name {
				case "torznab:attr":
					name, hasName := torznabXMLAttributeValue(child.element, "name")
					value, hasValue := torznabXMLAttributeValue(child.element, "value")
					if !hasInfohash && hasName && name == "infohash" && hasValue {
						infohash = value
						hasInfohash = true
					}
				case "guid":
					if guid == "" {
						guid = torznabXMLElementText(child.element)
					}
				}
			}

			if !hasInfohash {
				infohash = guid
			}

			hashes = append(hashes, strings.ToLower(strings.TrimSpace(infohash)))
			return
		}

		for _, child := range element.children {
			if child.element != nil {
				visit(child.element)
			}
		}
	}

	visit(root)

	return hashes, nil
}

type torznabXMLAttribute struct {
	name  string
	value string
}

type torznabXMLChild struct {
	element *torznabXMLElement
	text    string
}

type torznabXMLElement struct {
	name     string
	attrs    []torznabXMLAttribute
	children []torznabXMLChild
}

func parseTorznabXML(raw []byte) (*torznabXMLElement, error) {
	decoder := xml.NewDecoder(bytes.NewReader(raw))
	decoder.Strict = true
	// RawToken leaves a lexical prefix in Name.Space instead of resolving it
	// to a namespace URI. It does not match start/end tags, so the stack below
	// performs that strict check explicitly.

	var root *torznabXMLElement
	var stack []*torznabXMLElement

	for {
		token, err := decoder.RawToken()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, fmt.Errorf("decode Torznab XML: %w", err)
		}

		switch value := token.(type) {
		case xml.StartElement:
			element := &torznabXMLElement{
				name:  qualifiedXMLName(value.Name),
				attrs: make([]torznabXMLAttribute, 0, len(value.Attr)),
			}
			seenAttrs := make(map[string]struct{}, len(value.Attr))
			for _, attr := range value.Attr {
				name := qualifiedXMLName(attr.Name)
				if _, exists := seenAttrs[name]; exists {
					return nil, fmt.Errorf("decode Torznab XML: duplicate attribute %q on <%s>", name, element.name)
				}
				seenAttrs[name] = struct{}{}
				element.attrs = append(element.attrs, torznabXMLAttribute{name: name, value: attr.Value})
			}

			if len(stack) == 0 {
				if root != nil {
					return nil, fmt.Errorf("decode Torznab XML: multiple root elements")
				}
				root = element
			} else {
				parent := stack[len(stack)-1]
				parent.children = append(parent.children, torznabXMLChild{element: element})
			}

			stack = append(stack, element)
		case xml.EndElement:
			if len(stack) == 0 {
				return nil, fmt.Errorf("decode Torznab XML: unexpected closing element </%s>", qualifiedXMLName(value.Name))
			}

			got := qualifiedXMLName(value.Name)
			want := stack[len(stack)-1].name
			if got != want {
				return nil, fmt.Errorf("decode Torznab XML: closing element </%s> does not match <%s>", got, want)
			}
			stack = stack[:len(stack)-1]
		case xml.CharData:
			text := string(value)
			if strings.TrimSpace(text) == "" {
				continue
			}
			if len(stack) == 0 {
				return nil, fmt.Errorf("decode Torznab XML: text outside root element")
			}

			parent := stack[len(stack)-1]
			if len(parent.children) > 0 && parent.children[len(parent.children)-1].element == nil {
				parent.children[len(parent.children)-1].text += text
			} else {
				parent.children = append(parent.children, torznabXMLChild{text: text})
			}
		case xml.Comment, xml.Directive, xml.ProcInst:
			// The canonical tree contains only elements and text. The XML
			// declaration is consequently dropped along with other markup.
		default:
			return nil, fmt.Errorf("decode Torznab XML: unsupported token %T", token)
		}
	}

	if len(stack) != 0 {
		return nil, fmt.Errorf("decode Torznab XML: unclosed element <%s>", stack[len(stack)-1].name)
	}
	if root == nil {
		return nil, fmt.Errorf("decode Torznab XML: document has no root element")
	}

	return root, nil
}

func writeTorznabXMLElement(out *bytes.Buffer, element *torznabXMLElement, depth int) error {
	indent := strings.Repeat("  ", depth)
	out.WriteString(indent)
	out.WriteByte('<')
	out.WriteString(element.name)

	attrs := append([]torznabXMLAttribute(nil), element.attrs...)
	sort.Slice(attrs, func(i, j int) bool {
		return attrs[i].name < attrs[j].name
	})
	for _, attr := range attrs {
		out.WriteByte(' ')
		out.WriteString(attr.name)
		out.WriteString("=\"")
		writeCanonicalXMLEscaped(out, attr.value, true)
		out.WriteByte('"')
	}

	if len(element.children) == 0 {
		out.WriteString("/>\n")
		return nil
	}

	hasText := false
	hasElements := false
	for _, child := range element.children {
		if child.element == nil {
			hasText = true
		} else {
			hasElements = true
		}
	}
	if hasText && hasElements {
		return fmt.Errorf("normalize Torznab XML: mixed content in <%s> is unsupported", element.name)
	}

	if hasText {
		out.WriteByte('>')
		for _, child := range element.children {
			writeCanonicalXMLEscaped(out, child.text, false)
		}
		out.WriteString("</")
		out.WriteString(element.name)
		out.WriteString(">\n")
		return nil
	}

	out.WriteString(">\n")
	for _, child := range element.children {
		if err := writeTorznabXMLElement(out, child.element, depth+1); err != nil {
			return err
		}
	}
	out.WriteString(indent)
	out.WriteString("</")
	out.WriteString(element.name)
	out.WriteString(">\n")

	return nil
}

func qualifiedXMLName(name xml.Name) string {
	if name.Space == "" {
		return name.Local
	}

	return name.Space + ":" + name.Local
}

func localXMLName(name string) string {
	if index := strings.LastIndexByte(name, ':'); index >= 0 {
		return name[index+1:]
	}

	return name
}

func writeCanonicalXMLEscaped(out *bytes.Buffer, value string, attribute bool) {
	for _, r := range value {
		switch r {
		case '&':
			out.WriteString("&amp;")
		case '<':
			out.WriteString("&lt;")
		case '>':
			out.WriteString("&gt;")
		case '"':
			if attribute {
				out.WriteString("&quot;")
			} else {
				out.WriteRune(r)
			}
		default:
			out.WriteRune(r)
		}
	}
}

func torznabXMLAttributeValue(element *torznabXMLElement, name string) (string, bool) {
	for _, attr := range element.attrs {
		if attr.name == name {
			return attr.value, true
		}
	}

	return "", false
}

func torznabXMLElementText(element *torznabXMLElement) string {
	var text strings.Builder
	for _, child := range element.children {
		if child.element == nil {
			text.WriteString(child.text)
		}
	}

	return text.String()
}
