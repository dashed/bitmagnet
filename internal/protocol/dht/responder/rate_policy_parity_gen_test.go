package responder

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"golang.org/x/time/rate"
)

var updateDHTResponderRatePolicyParity = flag.Bool(
	"update-dht-responder-rate-policy-parity",
	false,
	"rewrite the Rust DHT responder rate-policy parity fixture",
)

const responderRatePolicyGoldenSHA256 = "a0134b10ebe37bd87059058e38c8bed984ca24cca3c77187ef186a8861990255"

var responderRatePolicyFixtureIDs = [...]string{
	"inner_per_ip_denial_precedes_global",
	"inner_exact_ip_string_keys",
	"outer_denial_and_delegate_effects",
}

type responderRatePolicyFixture struct {
	ID                 string                      `json:"id"`
	Subsystem          string                      `json:"subsystem"`
	Runtime            responderRatePolicyRuntime  `json:"runtime"`
	ProductionDefaults responderRatePolicyDefaults `json:"productionDefaults"`
	Input              responderRatePolicyInput    `json:"input"`
	Expected           responderRatePolicyExpected `json:"expected"`
}

type responderRatePolicyRuntime struct {
	Implementation string `json:"implementation"`
	Clock          string `json:"clock"`
}

type responderRatePolicyDefaults struct {
	OverallEveryNanos int64 `json:"overallEveryNanos"`
	OverallBurst      int   `json:"overallBurst"`
	PerIPEveryNanos   int64 `json:"perIpEveryNanos"`
	PerIPBurst        int   `json:"perIpBurst"`
	PerIPCapacity     int   `json:"perIpCapacity"`
	PerIPTTLNanos     int64 `json:"perIpTtlNanos"`
}

type responderRatePolicyInput struct {
	Layer               string   `json:"layer"`
	Addresses           []string `json:"addresses"`
	ScriptedPerIPAllows []bool   `json:"scriptedPerIpAllows"`
	GlobalBurst         int      `json:"globalBurst"`
	ScriptedOuterAllows []bool   `json:"scriptedOuterAllows"`
	DelegateOutcomes    []string `json:"delegateOutcomes"`
}

type responderRatePolicyExpected struct {
	Events []responderRatePolicyEvent `json:"events"`
}

type responderRatePolicyEvent struct {
	Call                   int      `json:"call"`
	Address                string   `json:"address"`
	Allowed                bool     `json:"allowed"`
	PerIPKeys              []string `json:"perIpKeys"`
	GlobalTokensBefore     int      `json:"globalTokensBefore"`
	GlobalTokensAfter      int      `json:"globalTokensAfter"`
	DelegateCalls          int      `json:"delegateCalls"`
	ReturnIDHex            string   `json:"returnIdHex"`
	ErrorCode              int      `json:"errorCode"`
	ErrorMessage           string   `json:"errorMessage"`
	ErrorIsTooManyRequests bool     `json:"errorIsTooManyRequests"`
	ErrorIsDelegate        bool     `json:"errorIsDelegate"`
}

type responderRatePolicyScriptedKeyed struct {
	results []bool
	keys    []string
}

var _ concurrency.KeyedLimiter = (*responderRatePolicyScriptedKeyed)(nil)

func (s *responderRatePolicyScriptedKeyed) Allow(key string) bool {
	s.keys = append(s.keys, key)
	if len(s.results) == 0 {
		panic("unexpected keyed Allow call")
	}
	result := s.results[0]
	s.results = s.results[1:]
	return result
}

func (*responderRatePolicyScriptedKeyed) Wait(context.Context, string) error {
	panic("unexpected keyed Wait call")
}

type responderRatePolicyScriptedLimiter struct {
	results []bool
}

func (s *responderRatePolicyScriptedLimiter) Allow(netip.Addr) bool {
	if len(s.results) == 0 {
		panic("unexpected outer Allow call")
	}
	result := s.results[0]
	s.results = s.results[1:]
	return result
}

type responderRatePolicyDelegate struct {
	outcomes []string
	calls    int
	err      error
	ret      dht.Return
}

func (d *responderRatePolicyDelegate) Respond(context.Context, dht.RecvMsg) (dht.Return, error) {
	d.calls++
	if len(d.outcomes) == 0 {
		panic("unexpected responder delegate call")
	}
	outcome := d.outcomes[0]
	d.outcomes = d.outcomes[1:]
	if outcome == "error" {
		return dht.Return{}, d.err
	}
	return d.ret, nil
}

func TestGenerateDHTResponderRatePolicyParity(t *testing.T) {
	fixtures := generateResponderRatePolicyFixtures(t)
	if len(fixtures) != len(responderRatePolicyFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(responderRatePolicyFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != responderRatePolicyFixtureIDs[index] || fixture.Subsystem != "dht_responder_limiter" {
			t.Fatalf("unstable responder rate fixture %d: %#v", index, fixture)
		}
	}
	reconcileResponderRatePolicyFixture(t, fixtures)
}

func generateResponderRatePolicyFixtures(t *testing.T) []responderRatePolicyFixture {
	t.Helper()
	defaults := responderRatePolicyDefaults{
		OverallEveryNanos: int64(time.Second / 50), OverallBurst: 20,
		PerIPEveryNanos: int64(time.Second), PerIPBurst: 10,
		PerIPCapacity: 1000, PerIPTTLNanos: int64(20 * time.Second),
	}
	runtime := responderRatePolicyRuntime{
		Implementation: "production responderLimiter and limiter",
		Clock:          "rate.Limit(0) makes global token observations independent of wall time",
	}

	fixtures := []responderRatePolicyFixture{
		generateResponderInnerOrderingFixture(t, runtime, defaults),
		generateResponderInnerKeysFixture(t, runtime, defaults),
		generateResponderOuterFixture(t, runtime, defaults),
	}
	return fixtures
}

func generateResponderInnerOrderingFixture(
	t *testing.T,
	runtime responderRatePolicyRuntime,
	defaults responderRatePolicyDefaults,
) responderRatePolicyFixture {
	t.Helper()
	input := responderRatePolicyInput{
		Layer: "inner", Addresses: []string{"192.0.2.1", "192.0.2.1", "192.0.2.1"},
		ScriptedPerIPAllows: []bool{false, true, true}, GlobalBurst: 1,
		ScriptedOuterAllows: []bool{}, DelegateOutcomes: []string{},
	}
	keyed := &responderRatePolicyScriptedKeyed{results: append([]bool(nil), input.ScriptedPerIPAllows...)}
	production := &limiter{keyedLimiter: keyed, limiter: rate.NewLimiter(0, input.GlobalBurst)}
	events := make([]responderRatePolicyEvent, 0, len(input.Addresses))
	for index, text := range input.Addresses {
		addr := netip.MustParseAddr(text)
		before := int(production.limiter.Tokens())
		allowed := production.Allow(addr)
		after := int(production.limiter.Tokens())
		events = append(events, responderRatePolicyEvent{
			Call: index + 1, Address: text, Allowed: allowed,
			PerIPKeys:          append([]string(nil), keyed.keys...),
			GlobalTokensBefore: before, GlobalTokensAfter: after,
		})
	}
	wantAllowed := []bool{false, true, false}
	wantBefore := []int{1, 1, 0}
	wantAfter := []int{1, 0, 0}
	for index, event := range events {
		if event.Allowed != wantAllowed[index] || event.GlobalTokensBefore != wantBefore[index] || event.GlobalTokensAfter != wantAfter[index] {
			t.Fatalf("inner ordering event %d changed: %#v", index, event)
		}
	}
	return responderRatePolicyFixture{
		ID: "inner_per_ip_denial_precedes_global", Subsystem: "dht_responder_limiter",
		Runtime: runtime, ProductionDefaults: defaults, Input: input,
		Expected: responderRatePolicyExpected{Events: events},
	}
}

func generateResponderInnerKeysFixture(
	t *testing.T,
	runtime responderRatePolicyRuntime,
	defaults responderRatePolicyDefaults,
) responderRatePolicyFixture {
	t.Helper()
	input := responderRatePolicyInput{
		Layer:               "inner",
		Addresses:           []string{"192.0.2.1", "::ffff:192.0.2.1", "fe80::1%7", "fe80::1%8"},
		ScriptedPerIPAllows: []bool{true, true, true, true}, GlobalBurst: 4,
		ScriptedOuterAllows: []bool{}, DelegateOutcomes: []string{},
	}
	keyed := &responderRatePolicyScriptedKeyed{results: append([]bool(nil), input.ScriptedPerIPAllows...)}
	production := &limiter{keyedLimiter: keyed, limiter: rate.NewLimiter(0, input.GlobalBurst)}
	events := make([]responderRatePolicyEvent, 0, len(input.Addresses))
	for index, text := range input.Addresses {
		allowed := production.Allow(netip.MustParseAddr(text))
		events = append(events, responderRatePolicyEvent{
			Call: index + 1, Address: text, Allowed: allowed,
			PerIPKeys:          append([]string(nil), keyed.keys...),
			GlobalTokensBefore: input.GlobalBurst - index,
			GlobalTokensAfter:  input.GlobalBurst - index - 1,
		})
	}
	if len(keyed.keys) != len(input.Addresses) {
		t.Fatalf("keyed call count = %d, want %d", len(keyed.keys), len(input.Addresses))
	}
	for index := range keyed.keys {
		if keyed.keys[index] != input.Addresses[index] || !events[index].Allowed {
			t.Fatalf("exact key event %d changed: %#v", index, events[index])
		}
	}
	return responderRatePolicyFixture{
		ID: "inner_exact_ip_string_keys", Subsystem: "dht_responder_limiter",
		Runtime: runtime, ProductionDefaults: defaults, Input: input,
		Expected: responderRatePolicyExpected{Events: events},
	}
}

func generateResponderOuterFixture(
	t *testing.T,
	runtime responderRatePolicyRuntime,
	defaults responderRatePolicyDefaults,
) responderRatePolicyFixture {
	t.Helper()
	input := responderRatePolicyInput{
		Layer: "outer", Addresses: []string{"192.0.2.9", "192.0.2.9", "192.0.2.9"},
		ScriptedPerIPAllows: []bool{}, GlobalBurst: 0,
		ScriptedOuterAllows: []bool{false, true, true}, DelegateOutcomes: []string{"success", "error"},
	}
	delegateErr := errors.New("fixed delegate error")
	returnID := protocol.ID{0xaa, 0xbb, 0xcc}
	delegate := &responderRatePolicyDelegate{
		outcomes: append([]string(nil), input.DelegateOutcomes...), err: delegateErr,
		ret: dht.Return{ID: returnID},
	}
	production := responderLimiter{
		responder: delegate,
		limiter:   &responderRatePolicyScriptedLimiter{results: append([]bool(nil), input.ScriptedOuterAllows...)},
	}
	events := make([]responderRatePolicyEvent, 0, len(input.Addresses))
	for index, text := range input.Addresses {
		ret, err := production.Respond(context.Background(), dht.RecvMsg{From: netip.MustParseAddrPort(text + ":6881")})
		event := responderRatePolicyEvent{
			Call: index + 1, Address: text, Allowed: input.ScriptedOuterAllows[index],
			PerIPKeys: []string{}, DelegateCalls: delegate.calls,
			ReturnIDHex:            hex.EncodeToString(ret.ID[:]),
			ErrorIsTooManyRequests: errors.Is(err, ErrTooManyRequests),
			ErrorIsDelegate:        err == delegateErr,
		}
		var dhtErr dht.Error
		if errors.As(err, &dhtErr) {
			event.ErrorCode = dhtErr.Code
			event.ErrorMessage = dhtErr.Msg
		} else if err != nil {
			event.ErrorMessage = err.Error()
		}
		events = append(events, event)
	}
	if !events[0].ErrorIsTooManyRequests || events[0].DelegateCalls != 0 || events[0].ErrorCode != 201 || events[0].ErrorMessage != "too many requests" {
		t.Fatalf("outer denial changed: %#v", events[0])
	}
	if events[1].DelegateCalls != 1 || events[1].ReturnIDHex != "aabbcc0000000000000000000000000000000000" || events[1].ErrorMessage != "" {
		t.Fatalf("outer success changed: %#v", events[1])
	}
	if events[2].DelegateCalls != 2 || !events[2].ErrorIsDelegate || events[2].ErrorMessage != "fixed delegate error" {
		t.Fatalf("outer error changed: %#v", events[2])
	}
	return responderRatePolicyFixture{
		ID: "outer_denial_and_delegate_effects", Subsystem: "dht_responder_limiter",
		Runtime: runtime, ProductionDefaults: defaults, Input: input,
		Expected: responderRatePolicyExpected{Events: events},
	}
}

func reconcileResponderRatePolicyFixture(t *testing.T, fixtures []responderRatePolicyFixture) {
	t.Helper()
	var generated bytes.Buffer
	encoder := json.NewEncoder(&generated)
	encoder.SetEscapeHTML(false)
	for _, fixture := range fixtures {
		if err := encoder.Encode(fixture); err != nil {
			t.Fatal(err)
		}
	}
	generatedBytes := generated.Bytes()
	path := filepath.Join("..", "..", "..", "..", "testdata", "parity", "dht", "responder_limiter.jsonl")
	if *updateDHTResponderRatePolicyParity {
		if err := os.WriteFile(path, generatedBytes, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	digest := sha256.Sum256(generatedBytes)
	if digestHex := hex.EncodeToString(digest[:]); responderRatePolicyGoldenSHA256 != "TODO" && digestHex != responderRatePolicyGoldenSHA256 {
		t.Fatalf("generated responder rate fixture digest = %s, want %s", digestHex, responderRatePolicyGoldenSHA256)
	}
	checked, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read responder rate fixture; rerun with -update-dht-responder-rate-policy-parity: %v", err)
	}
	if !bytes.Equal(checked, generatedBytes) {
		t.Fatal("DHT responder rate fixture is stale; rerun with -update-dht-responder-rate-policy-parity")
	}
}
