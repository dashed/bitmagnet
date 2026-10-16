package parity

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
)

const dhtInboundSubsystem = "dht_krpc_inbound"

type dhtInboundInput struct {
	WireHex              string `json:"wireHex,omitempty"`
	PaddingDatagramBytes int    `json:"paddingDatagramBytes,omitempty"`
}

type dhtInboundExpected struct {
	GoAccepted         bool            `json:"goAccepted"`
	RustAccepted       bool            `json:"rustAccepted"`
	GoDecoded          *dhtKRPCMessage `json:"goDecoded,omitempty"`
	GoCanonicalWireHex string          `json:"goCanonicalWireHex,omitempty"`
	RustProjectionLoss bool            `json:"rustProjectionLoss,omitempty"`
	RustErrorClass     string          `json:"rustErrorClass,omitempty"`
}

type dhtInboundScenario struct {
	id, wire, rustErrorClass string
	rustAccepted             bool
	rustProjectionLoss       bool
	paddingDatagramBytes     int
}

func TestGenerateDHTInboundFixtures(t *testing.T) {
	zero20 := "20:" + strings.Repeat("0", 20)
	zero256 := "256:" + string(make([]byte, 256))
	valid := "d1:ad2:id" + zero20 + "e1:q4:ping1:t2:aa1:y1:qe"
	scenarios := []dhtInboundScenario{
		{id: "canonical_query", wire: valid, rustAccepted: true},
		{id: "args_presence_empty_and_zero_ids", wire: "d1:ad2:id" + zero20 + "9:info_hash" + zero20 + "6:target" + zero20 + "4:wantlee1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "return_presence_empty_and_blooms", wire: "d1:rd4:BFpe" + zero256 + "4:BFsd" + zero256 + "2:id" + zero20 + "5:nodes0:6:nodes60:7:samples0:5:token0:6:valueslee1:t2:aa1:y1:re", rustAccepted: true},
		{id: "top_t_wrong_type", wire: "d1:ti1e1:y1:qe", rustErrorClass: "shape"},
		{id: "top_y_wrong_type", wire: "d1:t2:aa1:yi1ee", rustErrorClass: "shape"},
		{id: "top_q_wrong_type", wire: "d1:qi1e1:t2:aa1:y1:qe", rustErrorClass: "shape"},
		{id: "top_a_wrong_type", wire: "d1:a0:1:t2:aa1:y1:qe", rustErrorClass: "shape"},
		{id: "top_r_wrong_type", wire: "d1:r0:1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "top_e_wrong_type", wire: "d1:ei1e1:t2:aa1:y1:ee", rustErrorClass: "shape"},
		{id: "top_ip_wrong_type", wire: "d2:ipi1e1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "top_ip_short_hardening", wire: "d2:ip3:abc1:t2:aa1:y1:re", rustErrorClass: "compact"},
		{id: "top_ro_wrong_type", wire: "d2:rod1:a0:e1:t2:aa1:y1:qe", rustErrorClass: "shape"},
		{id: "top_v_wrong_type", wire: "d1:t2:aa1:vi1e1:y1:qe", rustErrorClass: "shape"},
		{id: "unsorted_top", wire: "d1:y1:q1:t2:aa1:q4:pinge", rustAccepted: true},
		{id: "duplicate_top_last_wins", wire: "d1:t2:aa1:y1:q1:t2:bb1:y1:re", rustAccepted: true},
		{id: "malformed_earlier_duplicate_rejects", wire: "d1:ti1e1:t2:aa1:y1:qe", rustErrorClass: "shape"},
		{id: "unsorted_args", wire: "d1:ad5:token1:x2:id" + zero20 + "e1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "duplicate_args_last_wins", wire: "d1:ad2:id" + zero20 + "5:token1:a5:token1:be1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "duplicate_args_malformed_earlier", wire: "d1:ad2:idi1e2:id" + zero20 + "e1:t2:aa1:y1:qe", rustErrorClass: "shape"},
		{id: "args_id_wrong_type", wire: "d1:ad2:idi1ee1:t2:aa1:y1:qe", rustErrorClass: "shape"},
		{id: "args_id_short", wire: "d1:ad2:id1:xe1:t2:aa1:y1:qe", rustErrorClass: "compact"},
		{id: "args_info_hash_wrong_type", wire: "d1:ad2:id" + zero20 + "9:info_hashi1ee1:t2:aa1:y1:qe", rustErrorClass: "shape"},
		{id: "args_target_wrong_type", wire: "d1:ad2:id" + zero20 + "6:targeti1ee1:t2:aa1:y1:qe", rustErrorClass: "shape"},
		{id: "args_token_wrong_type", wire: "d1:ad2:id" + zero20 + "5:tokeni1ee1:t2:aa1:y1:qe", rustErrorClass: "shape"},
		{id: "args_port_wrong_type", wire: "d1:ad2:id" + zero20 + "4:port1:xe1:t2:aa1:y1:qe", rustErrorClass: "shape"},
		{id: "args_implied_port_wrong_type", wire: "d1:ad2:id" + zero20 + "12:implied_portd1:a0:ee1:t2:aa1:y1:qe", rustErrorClass: "shape"},
		{id: "args_want_wrong_entry", wire: "d1:ad2:id" + zero20 + "4:wantli1eee1:t2:aa1:y1:qe", rustErrorClass: "shape"},
		{id: "args_noseed_wrong_type", wire: "d1:ad2:id" + zero20 + "6:noseed1:xe1:t2:aa1:y1:qe", rustErrorClass: "shape"},
		{id: "args_scrape_wrong_type", wire: "d1:ad2:id" + zero20 + "6:scrape1:xe1:t2:aa1:y1:qe", rustErrorClass: "shape"},
		{id: "unsorted_return", wire: "d1:rd5:token1:x2:id" + zero20 + "e1:t2:aa1:y1:re", rustAccepted: true},
		{id: "duplicate_return_last_wins", wire: "d1:rd2:id" + zero20 + "5:token1:a5:token1:be1:t2:aa1:y1:re", rustAccepted: true},
		{id: "duplicate_return_malformed_earlier", wire: "d1:rd2:idi1e2:id" + zero20 + "e1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "return_id_wrong_type", wire: "d1:rd2:idi1ee1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "return_nodes_wrong_type", wire: "d1:rd2:id" + zero20 + "5:nodesi1ee1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "return_nodes_misaligned", wire: "d1:rd2:id" + zero20 + "5:nodes1:xe1:t2:aa1:y1:re", rustErrorClass: "compact"},
		{id: "return_nodes6_wrong_type", wire: "d1:rd2:id" + zero20 + "6:nodes6i1ee1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "return_token_wrong_type", wire: "d1:rd2:id" + zero20 + "5:tokeni1ee1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "return_values_wrong_type", wire: "d1:rd2:id" + zero20 + "6:valuesi1ee1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "return_values_wrong_entry", wire: "d1:rd2:id" + zero20 + "6:valuesli1eee1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "return_interval_wrong_type", wire: "d1:rd2:id" + zero20 + "8:interval1:xe1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "return_num_wrong_type", wire: "d1:rd2:id" + zero20 + "3:num1:xe1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "return_samples_wrong_type", wire: "d1:rd2:id" + zero20 + "7:samplesi1ee1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "return_samples_misaligned", wire: "d1:rd2:id" + zero20 + "7:samples19:xxxxxxxxxxxxxxxxxxxe1:t2:aa1:y1:re", rustErrorClass: "compact"},
		{id: "return_bfsd_wrong_type", wire: "d1:rd4:BFsdi1e2:id" + zero20 + "e1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "return_bfpe_wrong_type", wire: "d1:rd4:BFpei1e2:id" + zero20 + "e1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "unknown_sorted_dictionary", wire: "d1:t2:aa1:y1:q1:zd1:a0:1:b0:ee", rustAccepted: true},
		{id: "unknown_unsorted_dictionary", wire: "d1:t2:aa1:y1:q1:zd1:b0:1:a0:ee", rustErrorClass: "syntax"},
		{id: "unknown_duplicate_dictionary", wire: "d1:t2:aa1:y1:q1:zd1:a0:1:a0:ee", rustErrorClass: "syntax"},
		{id: "bool_empty_integer", wire: "d2:roie1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "bool_dash_integer", wire: "d2:roi-e1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "bool_nested_singleton", wire: "d2:roll4:trueee1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "singleton_top_t", wire: "d1:tl2:aae1:y1:qe", rustAccepted: true},
		{id: "singleton_top_y", wire: "d1:t2:aa1:yl1:qee", rustAccepted: true},
		{id: "singleton_top_q", wire: "d1:ql4:pinge1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "singleton_top_v", wire: "d1:t2:aa1:vl4:UT01e1:y1:qe", rustAccepted: true},
		{id: "singleton_top_dictionary_key", wire: "dl1:te2:aa1:y1:qe", rustAccepted: true},
		{id: "singleton_args_dictionary_key", wire: "d1:adl2:ide" + zero20 + "e1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "singleton_return_dictionary_key", wire: "d1:rdl2:ide" + zero20 + "e1:t2:aa1:y1:re", rustAccepted: true},
		{id: "singleton_args_id", wire: "d1:ad2:idl" + zero20 + "ee1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "singleton_args_info_hash", wire: "d1:ad2:id" + zero20 + "9:info_hashl" + zero20 + "ee1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "singleton_args_target", wire: "d1:ad2:id" + zero20 + "6:targetl" + zero20 + "ee1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "singleton_args_token", wire: "d1:ad2:id" + zero20 + "5:tokenl1:xee1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "singleton_args_port", wire: "d1:ad2:id" + zero20 + "4:portli7eee1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "singleton_args_noseed", wire: "d1:ad2:id" + zero20 + "6:noseedli1eee1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "singleton_args_scrape", wire: "d1:ad2:id" + zero20 + "6:scrapeli1eee1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "singleton_args_want_entry", wire: "d1:ad2:id" + zero20 + "4:wantll2:n4eee1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "singleton_args_seq", wire: "d1:ad2:id" + zero20 + "3:seqli7eee1:t2:aa1:y1:qe", rustAccepted: true, rustProjectionLoss: true},
		{id: "singleton_args_cas", wire: "d1:ad3:casli7ee2:id" + zero20 + "e1:t2:aa1:y1:qe", rustAccepted: true, rustProjectionLoss: true},
		{id: "singleton_return_id", wire: "d1:rd2:idl" + zero20 + "ee1:t2:aa1:y1:re", rustAccepted: true},
		{id: "singleton_return_token", wire: "d1:rd2:id" + zero20 + "5:tokenl1:xee1:t2:aa1:y1:re", rustAccepted: true},
		{id: "singleton_return_interval", wire: "d1:rd2:id" + zero20 + "8:intervalli7eee1:t2:aa1:y1:re", rustAccepted: true},
		{id: "singleton_return_num", wire: "d1:rd2:id" + zero20 + "3:numli7eee1:t2:aa1:y1:re", rustAccepted: true},
		{id: "singleton_return_nodes", wire: "d1:rd2:id" + zero20 + "5:nodesl0:ee1:t2:aa1:y1:re", rustAccepted: true},
		{id: "singleton_return_samples", wire: "d1:rd2:id" + zero20 + "7:samplesl0:ee1:t2:aa1:y1:re", rustAccepted: true},
		{id: "singleton_return_seq", wire: "d1:rd2:id" + zero20 + "3:seqli7eee1:t2:aa1:y1:re", rustAccepted: true, rustProjectionLoss: true},
		{id: "singleton_return_values_entry_not_coerced", wire: "d1:rd2:id" + zero20 + "6:valuesll6:\x7f\x00\x00\x01\x1a\xe1eee1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "singleton_ip_not_coerced", wire: "d2:ipl6:\x7f\x00\x00\x01\x1a\xe1e1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "singleton_bloom_not_coerced", wire: "d1:rd4:BFsdl" + zero256 + "e2:id" + zero20 + "e1:t2:aa1:y1:re", rustErrorClass: "shape"},
		{id: "bool_noncanonical_integer", wire: "d2:roi00e1:t2:aa1:y1:qe", rustErrorClass: "syntax"},
		{id: "known_integer_empty", wire: "d1:ad2:id" + zero20 + "4:portiee1:t2:aa1:y1:qe", rustErrorClass: "syntax"},
		{id: "known_integer_i64_max", wire: "d1:ad2:id" + zero20 + "4:porti9223372036854775807ee1:t2:aa1:y1:qe", rustAccepted: true},
		{id: "known_integer_i64_overflow", wire: "d1:ad2:id" + zero20 + "4:porti9223372036854775808ee1:t2:aa1:y1:qe", rustErrorClass: "shape"},
		{id: "unknown_giant_integer", wire: "d1:t2:aa1:y1:q1:zi184467440737095516160000ee", rustAccepted: true},
		{id: "args_v_canonical_excluded", wire: "d1:ad2:id" + zero20 + "1:v3:fooe1:t2:aa1:y1:qe", rustErrorClass: "unsupported"},
		{id: "args_v_unsorted_dictionary", wire: "d1:ad2:id" + zero20 + "1:vd1:b0:1:a0:ee1:t2:aa1:y1:qe", rustErrorClass: "syntax"},
		{id: "args_v_duplicate_dictionary", wire: "d1:ad2:id" + zero20 + "1:vd1:a0:1:a0:ee1:t2:aa1:y1:qe", rustErrorClass: "syntax"},
		{id: "args_v_noncanonical_integer", wire: "d1:ad2:id" + zero20 + "1:vi00ee1:t2:aa1:y1:qe", rustErrorClass: "syntax"},
		{id: "args_v_malformed_pair", wire: "d1:ad2:id" + zero20 + "1:vd1:aee1:t2:aa1:y1:qe", rustErrorClass: "syntax"},
		{id: "return_v_canonical_excluded", wire: "d1:rd2:id" + zero20 + "1:v3:fooe1:t2:aa1:y1:re", rustErrorClass: "unsupported"},
		{id: "return_v_unsorted_dictionary_excluded", wire: "d1:rd2:id" + zero20 + "1:vd1:b0:1:a0:ee1:t2:aa1:y1:re", rustErrorClass: "unsupported"},
		{id: "return_v_duplicate_dictionary_excluded", wire: "d1:rd2:id" + zero20 + "1:vd1:a0:1:a0:ee1:t2:aa1:y1:re", rustErrorClass: "unsupported"},
		{id: "return_v_noncanonical_integer_excluded", wire: "d1:rd2:id" + zero20 + "1:vi00ee1:t2:aa1:y1:re", rustErrorClass: "unsupported"},
		{id: "return_v_malformed_pair_excluded", wire: "d1:rd2:id" + zero20 + "1:vd1:aee1:t2:aa1:y1:re", rustErrorClass: "unsupported"},
		{id: "legacy_error", wire: "d1:e4:oops1:t2:aa1:y1:ee", rustAccepted: true},
		{id: "error_extra_validated", wire: "d1:eli201e4:oopsd1:a0:ee1:t2:aa1:y1:ee", rustAccepted: true},
		{id: "error_too_short", wire: "d1:eli201ee1:t2:aa1:y1:ee", rustErrorClass: "shape"},
		{id: "error_wrong_message", wire: "d1:eli201ei1ee1:t2:aa1:y1:ee", rustErrorClass: "shape"},
		{id: "trailing_value", wire: valid + "0:", rustErrorClass: "syntax"},
		{id: "truncated", wire: valid[:len(valid)-1], rustErrorClass: "syntax"},
		{id: "depth_eight", wire: nestedUnknownLists(7), rustAccepted: true},
		{id: "depth_nine_hardening", wire: nestedUnknownLists(8), rustErrorClass: "limit"},
		{id: "short_peer_hardening", wire: "d1:rd2:id" + zero20 + "6:valuesl3:abcee1:t2:aa1:y1:re", rustErrorClass: "compact"},
		{id: "short_bloom_hardening", wire: "d1:rd4:BFsd255:" + strings.Repeat("x", 255) + "2:id" + zero20 + "e1:t2:aa1:y1:re", rustErrorClass: "scrape_bloom"},
		{id: "max_datagram", paddingDatagramBytes: 65_507, rustAccepted: true},
		{id: "oversize_datagram_hardening", paddingDatagramBytes: 65_508, rustErrorClass: "limit"},
	}

	fixtures := make([]Fixture, 0, len(scenarios))
	for _, scenario := range scenarios {
		wire := []byte(scenario.wire)
		input := dhtInboundInput{WireHex: hex.EncodeToString(wire)}
		if scenario.paddingDatagramBytes != 0 {
			wire = inboundPaddingDatagram(t, scenario.paddingDatagramBytes)
			input = dhtInboundInput{PaddingDatagramBytes: scenario.paddingDatagramBytes}
		}
		var decoded dht.Msg
		err := bencode.Unmarshal(wire, &decoded)
		expected := dhtInboundExpected{
			GoAccepted:         err == nil,
			RustAccepted:       scenario.rustAccepted,
			RustProjectionLoss: scenario.rustProjectionLoss,
			RustErrorClass:     scenario.rustErrorClass,
		}
		if err == nil {
			projection := projectDHTMessage(decoded)
			expected.GoDecoded = &projection
			expected.GoCanonicalWireHex = hex.EncodeToString(mustBencode(t, decoded))
		}
		if scenario.rustAccepted && err != nil {
			t.Fatalf("%s: scenario claims shared acceptance but Go rejected: %v", scenario.id, err)
		}
		fixtures = append(fixtures, mustInboundFixture(t, scenario.id, input, expected))
	}
	reconcileDHTFixtures(t, "inbound.jsonl", fixtures)
}

func nestedUnknownLists(count int) string {
	return "d1:t2:aa1:y1:q1:z" + strings.Repeat("l", count) + "0:" + strings.Repeat("e", count) + "e"
}

func inboundPaddingDatagram(t *testing.T, target int) []byte {
	t.Helper()
	padding := target
	for {
		wire := []byte(fmt.Sprintf("d1:t2:aa1:y1:q1:z%d:%se", padding, strings.Repeat("x", padding)))
		switch {
		case len(wire) == target:
			return wire
		case len(wire) > target:
			padding -= len(wire) - target
		default:
			padding += target - len(wire)
		}
		if padding < 0 {
			t.Fatalf("cannot construct %d-byte padding datagram", target)
		}
	}
}

func mustInboundFixture(t *testing.T, id string, input, expected any) Fixture {
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
		ID: id, Subsystem: dhtInboundSubsystem,
		Input: inputJSON, Expected: expectedJSON,
	}
}

func FuzzDHTInboundGoDecoder(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte("de"),
		[]byte("d1:t2:aa1:y1:qe"),
		[]byte("d1:y1:q1:t2:aae"),
		[]byte("d1:t2:aa1:t2:bb1:y1:re"),
		[]byte("d1:t2:aa1:y1:q1:zd1:b0:1:a0:ee"),
		[]byte("d2:roie1:t2:aa1:y1:qe"),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, wire []byte) {
		if len(wire) > 65_507 {
			t.Skip()
		}
		var decoded dht.Msg
		if bencode.Unmarshal(wire, &decoded) != nil {
			return
		}
		canonical := mustBencode(t, decoded)
		var roundTrip dht.Msg
		if err := bencode.Unmarshal(canonical, &roundTrip); err != nil {
			t.Fatalf("canonical Go re-decode: %v", err)
		}
		if !bytes.Equal(mustBencode(t, roundTrip), canonical) {
			t.Fatal("accepted Go projection did not stabilize after canonical re-encode")
		}
	})
}
