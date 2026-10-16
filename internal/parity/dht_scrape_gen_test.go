package parity

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net"
	"strconv"
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
)

const dhtScrapeBloomSubsystem = "dht_scrape_bloom"

type dhtScrapeRange struct {
	Base  dhtBytes `json:"base"`
	Count int      `json:"count"`
}

type dhtScrapeFilterInput struct {
	RawIPs []dhtBytes       `json:"rawIps,omitempty"`
	Ranges []dhtScrapeRange `json:"ranges,omitempty"`
}

type dhtScrapeInput struct {
	Seeders *dhtScrapeFilterInput `json:"seeders,omitempty"`
	Peers   *dhtScrapeFilterInput `json:"peers,omitempty"`
}

type dhtScrapeFilterExpected struct {
	BloomHex         string  `json:"bloomHex"`
	EstimateCount    float64 `json:"estimateCount"`
	ApproximatedSize uint32  `json:"approximatedSize"`
}

type dhtScrapeExpected struct {
	WireHex string                   `json:"wireHex"`
	Seeders *dhtScrapeFilterExpected `json:"seeders,omitempty"`
	Peers   *dhtScrapeFilterExpected `json:"peers,omitempty"`
}

type dhtScrapeCompatibilityInput struct {
	WireHex string `json:"wireHex"`
}

type dhtScrapeCompatibilityExpected struct {
	GoAccepted         bool   `json:"goAccepted"`
	RustAccepted       bool   `json:"rustAccepted"`
	GoCanonicalWireHex string `json:"goCanonicalWireHex,omitempty"`
	Reason             string `json:"reason"`
}

func TestGenerateDHTScrapeFixtures(t *testing.T) {
	empty := &dhtScrapeFilterInput{}
	ipv4 := &dhtScrapeFilterInput{RawIPs: []dhtBytes{{127, 0, 0, 1}}}
	// `net.IPv4` is a 16-byte slice unless the caller explicitly uses To4.
	mappedIPv4 := &dhtScrapeFilterInput{RawIPs: []dhtBytes{{
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 127, 0, 0, 1,
	}}}
	ipv6 := &dhtScrapeFilterInput{RawIPs: []dhtBytes{
		{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
	}}
	duplicate := &dhtScrapeFilterInput{RawIPs: []dhtBytes{
		{192, 0, 2, 9},
		{192, 0, 2, 9},
	}}
	reference := &dhtScrapeFilterInput{Ranges: []dhtScrapeRange{
		{Base: dhtBytes{192, 0, 2, 0}, Count: 256},
		{Base: dhtBytes{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, Count: 1000},
	}}

	rawEmpty := &dhtScrapeFilterInput{RawIPs: []dhtBytes{{}}}
	typed := []struct {
		id    string
		input dhtScrapeInput
	}{
		{"absent_filters", dhtScrapeInput{}},
		{"present_empty_filters", dhtScrapeInput{Seeders: empty, Peers: empty}},
		{"raw_empty_ip_bytes", dhtScrapeInput{Seeders: rawEmpty}},
		{"single_ipv4_4_byte", dhtScrapeInput{Seeders: ipv4}},
		{"single_ipv4_mapped_16_byte", dhtScrapeInput{Seeders: mappedIPv4}},
		{"single_ipv6_16_byte", dhtScrapeInput{Seeders: ipv6}},
		{"distinct_seeders_and_peers", dhtScrapeInput{Seeders: ipv4, Peers: ipv6}},
		{"duplicate_insertion", dhtScrapeInput{Seeders: duplicate}},
		{"go_bep33_reference_corpus", dhtScrapeInput{Seeders: reference}},
	}

	fixtures := make([]Fixture, 0, len(typed)+4)
	for _, scenario := range typed {
		seeders := buildGoScrapeFilter(t, scenario.input.Seeders)
		peers := buildGoScrapeFilter(t, scenario.input.Peers)
		message := dht.Msg{
			T: "bs",
			Y: dht.YResponse,
			R: &dht.Return{ID: dhtID(0x21), BFsd: seeders, BFpe: peers},
		}
		wire := mustBencode(t, message)
		var decoded dht.Msg
		if err := bencode.Unmarshal(wire, &decoded); err != nil {
			t.Fatalf("%s: decode generated wire: %v", scenario.id, err)
		}
		if canonical := mustBencode(t, decoded); !bytes.Equal(wire, canonical) {
			t.Fatalf("%s: valid scrape wire is not Go round-trip stable", scenario.id)
		}
		expected := dhtScrapeExpected{
			WireHex: hex.EncodeToString(wire),
			Seeders: describeGoScrapeFilter(seeders),
			Peers:   describeGoScrapeFilter(peers),
		}
		fixtures = append(fixtures, mustDHTScrapeFixture(t, scenario.id, scenario.input, expected))
	}

	for _, scenario := range []struct {
		id, reason string
		wire       []byte
		rustAccept bool
	}{
		{"empty_width", "Go zero-pads an empty array string; Rust requires the BEP-33 exact 256-byte width", scrapeWidthWire("BFsd", nil), false},
		{"short_width", "Go zero-pads a 255-byte array string; Rust requires the BEP-33 exact 256-byte width", scrapeWidthWire("BFsd", make([]byte, 255)), false},
		{"long_width", "Go truncates a 257-byte array string; Rust requires the BEP-33 exact 256-byte width", scrapeWidthWire("BFsd", make([]byte, 257)), false},
		{"wrong_type", "Both codecs reject a non-byte-string BEP-33 filter", []byte("d1:rd4:BFsdi1e2:id20:00000000000000000000e1:t2:bs1:y1:re"), false},
	} {
		var decoded dht.Msg
		err := bencode.Unmarshal(scenario.wire, &decoded)
		expected := dhtScrapeCompatibilityExpected{
			GoAccepted:   err == nil,
			RustAccepted: scenario.rustAccept,
			Reason:       scenario.reason,
		}
		if err == nil {
			expected.GoCanonicalWireHex = hex.EncodeToString(mustBencode(t, decoded))
		}
		fixtures = append(fixtures, mustDHTScrapeFixture(
			t,
			"compat_"+scenario.id,
			dhtScrapeCompatibilityInput{WireHex: hex.EncodeToString(scenario.wire)},
			expected,
		))
	}

	reconcileDHTFixtures(t, "scrape_bloom.jsonl", fixtures)
}

func buildGoScrapeFilter(t *testing.T, input *dhtScrapeFilterInput) *dht.ScrapeBloomFilter {
	t.Helper()
	if input == nil {
		return nil
	}
	filter := new(dht.ScrapeBloomFilter)
	for _, rawIP := range input.RawIPs {
		filter.AddIP(net.IP(rawIP))
	}
	for _, valueRange := range input.Ranges {
		for offset := range valueRange.Count {
			filter.AddIP(net.IP(addBigEndian(t, valueRange.Base, uint64(offset))))
		}
	}
	return filter
}

func addBigEndian(t *testing.T, base []byte, offset uint64) []byte {
	t.Helper()
	result := append([]byte(nil), base...)
	carry := offset
	for index := len(result) - 1; index >= 0 && carry != 0; index-- {
		sum := uint64(result[index]) + carry
		result[index] = byte(sum)
		carry = sum >> 8
	}
	if carry != 0 {
		t.Fatal("scrape range overflows its address width")
	}
	return result
}

func describeGoScrapeFilter(filter *dht.ScrapeBloomFilter) *dhtScrapeFilterExpected {
	if filter == nil {
		return nil
	}
	return &dhtScrapeFilterExpected{
		BloomHex:         hex.EncodeToString(filter[:]),
		EstimateCount:    filter.EstimateCount(),
		ApproximatedSize: filter.ToBloomFilter().ApproximatedSize(),
	}
}

func scrapeWidthWire(name string, value []byte) []byte {
	var wire bytes.Buffer
	wire.WriteString("d1:rd")
	wire.WriteString(strconv.Itoa(len(name)))
	wire.WriteByte(':')
	wire.WriteString(name)
	wire.WriteString(strconv.Itoa(len(value)))
	wire.WriteByte(':')
	wire.Write(value)
	wire.WriteString("2:id20:00000000000000000000e1:t2:bs1:y1:re")
	return wire.Bytes()
}

func mustDHTScrapeFixture(t *testing.T, id string, input, expected any) Fixture {
	t.Helper()
	inputJSON, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	return Fixture{
		ID: id, Subsystem: dhtScrapeBloomSubsystem,
		Input: inputJSON, Expected: expectedJSON,
	}
}
