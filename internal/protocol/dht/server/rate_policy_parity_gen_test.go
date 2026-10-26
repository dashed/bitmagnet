package server

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"golang.org/x/time/rate"
)

var updateDHTQueryLimiterParity = flag.Bool(
	"update-dht-query-limiter-parity",
	false,
	"rewrite the Rust DHT query-limiter parity fixture",
)

const queryLimiterGoldenSHA256 = "37ea6d399bc44873052e540de1ceb9b2a2018c2596e37f1293b20efeb07e0a1b"

var queryLimiterFixtureIDs = [...]string{
	"wait_barrier_precedes_delegate",
	"wait_error_short_circuits_exact",
	"delegate_error_identity_after_wait",
	"pre_canceled_actual_keyed_limiter",
	"expired_deadline_actual_keyed_limiter",
	"future_deadline_rejected_before_delegate",
	"exact_ip_string_keys",
}

type queryLimiterFixture struct {
	ID                 string               `json:"id"`
	Subsystem          string               `json:"subsystem"`
	Runtime            queryLimiterRuntime  `json:"runtime"`
	ProductionDefaults queryLimiterDefaults `json:"productionDefaults"`
	Input              queryLimiterInput    `json:"input"`
	Expected           queryLimiterExpected `json:"expected"`
}

type queryLimiterRuntime struct {
	Implementation string `json:"implementation"`
	Clock          string `json:"clock"`
}

type queryLimiterDefaults struct {
	PerIPEveryNanos int64 `json:"perIpEveryNanos"`
	PerIPBurst      int   `json:"perIpBurst"`
	PerIPCapacity   int   `json:"perIpCapacity"`
	PerIPTTLNanos   int64 `json:"perIpTtlNanos"`
}

type queryLimiterInput struct {
	LimiterKind   string   `json:"limiterKind"`
	ContextKind   string   `json:"contextKind"`
	Addresses     []string `json:"addresses"`
	ScriptedWaits []string `json:"scriptedWaits"`
	Delegate      string   `json:"delegate"`
}

type queryLimiterExpected struct {
	Events []queryLimiterEvent `json:"events"`
}

type queryLimiterEvent struct {
	Call                    int      `json:"call"`
	Address                 string   `json:"address"`
	WaitKeys                []string `json:"waitKeys"`
	Sequence                []string `json:"sequence"`
	DelegateCalls           int      `json:"delegateCalls"`
	DelegateBeforeWaitEnded int      `json:"delegateBeforeWaitEnded"`
	ReturnIDHex             string   `json:"returnIdHex"`
	ErrorMessage            string   `json:"errorMessage"`
	ErrorIsWaitSentinel     bool     `json:"errorIsWaitSentinel"`
	ErrorIsDelegateSentinel bool     `json:"errorIsDelegateSentinel"`
	ErrorIsCanceled         bool     `json:"errorIsCanceled"`
	ErrorIsDeadlineExceeded bool     `json:"errorIsDeadlineExceeded"`
}

type queryLimiterScriptedKeyed struct {
	waits    []error
	keys     []string
	sequence *[]string
	entered  chan struct{}
	release  chan struct{}
}

var _ concurrency.KeyedLimiter = (*queryLimiterScriptedKeyed)(nil)

func (*queryLimiterScriptedKeyed) Allow(string) bool {
	panic("unexpected query-limiter Allow call")
}

func (s *queryLimiterScriptedKeyed) Wait(_ context.Context, key string) error {
	s.keys = append(s.keys, key)
	if s.sequence != nil {
		*s.sequence = append(*s.sequence, "wait")
	}
	if s.entered != nil {
		close(s.entered)
		<-s.release
	}
	if len(s.waits) == 0 {
		return nil
	}
	err := s.waits[0]
	s.waits = s.waits[1:]
	return err
}

type queryLimiterScriptedServer struct {
	calls    atomic.Int32
	sequence *[]string
	err      error
	ret      dht.RecvMsg
}

func (*queryLimiterScriptedServer) start() error { return nil }
func (*queryLimiterScriptedServer) stop()        {}

func (s *queryLimiterScriptedServer) Query(
	context.Context,
	netip.AddrPort,
	string,
	dht.MsgArgs,
) (dht.RecvMsg, error) {
	s.calls.Add(1)
	if s.sequence != nil {
		*s.sequence = append(*s.sequence, "delegate")
	}
	return s.ret, s.err
}

func TestGenerateDHTQueryLimiterParity(t *testing.T) {
	fixtures := generateQueryLimiterFixtures(t)
	if len(fixtures) != len(queryLimiterFixtureIDs) {
		t.Fatalf("fixture count = %d, want %d", len(fixtures), len(queryLimiterFixtureIDs))
	}
	for index, fixture := range fixtures {
		if fixture.ID != queryLimiterFixtureIDs[index] || fixture.Subsystem != "dht_query_limiter" {
			t.Fatalf("unstable query-limiter fixture %d: %#v", index, fixture)
		}
	}
	reconcileQueryLimiterFixture(t, fixtures)
}

func generateQueryLimiterFixtures(t *testing.T) []queryLimiterFixture {
	t.Helper()
	runtime := queryLimiterRuntime{
		Implementation: "production queryLimiter with actual keyed limiter where identified",
		Clock:          "channel barriers or already-decided contexts/reservations; no sleeps",
	}
	defaults := queryLimiterDefaults{
		PerIPEveryNanos: int64(time.Second), PerIPBurst: 4,
		PerIPCapacity: 1000, PerIPTTLNanos: int64(20 * time.Second),
	}
	return []queryLimiterFixture{
		generateQueryLimiterBarrierFixture(t, runtime, defaults),
		generateQueryLimiterWaitErrorFixture(t, runtime, defaults),
		generateQueryLimiterDelegateErrorFixture(t, runtime, defaults),
		generateQueryLimiterPreCanceledFixture(t, runtime, defaults),
		generateQueryLimiterExpiredDeadlineFixture(t, runtime, defaults),
		generateQueryLimiterFutureDeadlineFixture(t, runtime, defaults),
		generateQueryLimiterKeysFixture(t, runtime, defaults),
	}
}

func queryLimiterBaseFixture(
	id string,
	runtime queryLimiterRuntime,
	defaults queryLimiterDefaults,
	input queryLimiterInput,
	events []queryLimiterEvent,
) queryLimiterFixture {
	return queryLimiterFixture{
		ID: id, Subsystem: "dht_query_limiter", Runtime: runtime,
		ProductionDefaults: defaults, Input: input,
		Expected: queryLimiterExpected{Events: events},
	}
}

func queryLimiterAddr(text string) netip.AddrPort {
	return netip.AddrPortFrom(netip.MustParseAddr(text), 6881)
}

func queryLimiterReturnIDHex(ret dht.RecvMsg) string {
	if ret.Msg.R == nil {
		return ""
	}
	return hex.EncodeToString(ret.Msg.R.ID[:])
}

func queryLimiterEventFrom(
	call int,
	address string,
	waiter *queryLimiterScriptedKeyed,
	delegate *queryLimiterScriptedServer,
	sequence []string,
	ret dht.RecvMsg,
	err error,
	waitErr error,
	delegateErr error,
) queryLimiterEvent {
	event := queryLimiterEvent{
		Call: call, Address: address,
		WaitKeys: append([]string{}, waiter.keys...), Sequence: append([]string{}, sequence...),
		DelegateCalls: int(delegate.calls.Load()), ReturnIDHex: queryLimiterReturnIDHex(ret),
		ErrorIsWaitSentinel:     waitErr != nil && err == waitErr,
		ErrorIsDelegateSentinel: delegateErr != nil && err == delegateErr,
		ErrorIsCanceled:         errors.Is(err, context.Canceled),
		ErrorIsDeadlineExceeded: errors.Is(err, context.DeadlineExceeded),
	}
	if err != nil {
		event.ErrorMessage = err.Error()
	}
	return event
}

func generateQueryLimiterBarrierFixture(t *testing.T, runtime queryLimiterRuntime, defaults queryLimiterDefaults) queryLimiterFixture {
	t.Helper()
	address := "192.0.2.11"
	input := queryLimiterInput{
		LimiterKind: "scripted_barrier", ContextKind: "background", Addresses: []string{address},
		ScriptedWaits: []string{"barrier_then_success"}, Delegate: "success",
	}
	sequence := []string{}
	waiter := &queryLimiterScriptedKeyed{sequence: &sequence, entered: make(chan struct{}), release: make(chan struct{})}
	returnID := protocol.ID{0x11, 0x22}
	delegate := &queryLimiterScriptedServer{sequence: &sequence, ret: dht.RecvMsg{Msg: dht.Msg{R: &dht.Return{ID: returnID}}}}
	production := queryLimiter{server: delegate, queryLimiter: waiter}
	type result struct {
		ret dht.RecvMsg
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		ret, err := production.Query(context.Background(), queryLimiterAddr(address), dht.QPing, dht.MsgArgs{})
		resultCh <- result{ret: ret, err: err}
	}()
	<-waiter.entered
	beforeWaitEnded := int(delegate.calls.Load())
	close(waiter.release)
	resultValue := <-resultCh
	event := queryLimiterEventFrom(1, address, waiter, delegate, sequence, resultValue.ret, resultValue.err, nil, nil)
	event.DelegateBeforeWaitEnded = beforeWaitEnded
	if beforeWaitEnded != 0 || event.DelegateCalls != 1 || len(event.Sequence) != 2 || event.Sequence[0] != "wait" || event.Sequence[1] != "delegate" {
		t.Fatalf("barrier ordering changed: %#v", event)
	}
	return queryLimiterBaseFixture("wait_barrier_precedes_delegate", runtime, defaults, input, []queryLimiterEvent{event})
}

func generateQueryLimiterWaitErrorFixture(t *testing.T, runtime queryLimiterRuntime, defaults queryLimiterDefaults) queryLimiterFixture {
	t.Helper()
	address := "192.0.2.12"
	input := queryLimiterInput{
		LimiterKind: "scripted", ContextKind: "background", Addresses: []string{address},
		ScriptedWaits: []string{"fixed wait error"}, Delegate: "must_not_run",
	}
	waitErr := errors.New("fixed wait error")
	sequence := []string{}
	waiter := &queryLimiterScriptedKeyed{waits: []error{waitErr}, sequence: &sequence}
	delegate := &queryLimiterScriptedServer{sequence: &sequence, ret: dht.RecvMsg{Msg: dht.Msg{R: &dht.Return{}}}}
	production := queryLimiter{server: delegate, queryLimiter: waiter}
	ret, err := production.Query(context.Background(), queryLimiterAddr(address), dht.QPing, dht.MsgArgs{})
	event := queryLimiterEventFrom(1, address, waiter, delegate, sequence, ret, err, waitErr, nil)
	if !event.ErrorIsWaitSentinel || event.DelegateCalls != 0 || event.ErrorMessage != "fixed wait error" {
		t.Fatalf("wait error identity changed: %#v", event)
	}
	return queryLimiterBaseFixture("wait_error_short_circuits_exact", runtime, defaults, input, []queryLimiterEvent{event})
}

func generateQueryLimiterDelegateErrorFixture(t *testing.T, runtime queryLimiterRuntime, defaults queryLimiterDefaults) queryLimiterFixture {
	t.Helper()
	address := "192.0.2.13"
	input := queryLimiterInput{
		LimiterKind: "scripted", ContextKind: "background", Addresses: []string{address},
		ScriptedWaits: []string{"success"}, Delegate: "fixed delegate error",
	}
	delegateErr := errors.New("fixed delegate error")
	sequence := []string{}
	waiter := &queryLimiterScriptedKeyed{sequence: &sequence}
	delegate := &queryLimiterScriptedServer{sequence: &sequence, err: delegateErr, ret: dht.RecvMsg{Msg: dht.Msg{R: &dht.Return{}}}}
	production := queryLimiter{server: delegate, queryLimiter: waiter}
	ret, err := production.Query(context.Background(), queryLimiterAddr(address), dht.QPing, dht.MsgArgs{})
	event := queryLimiterEventFrom(1, address, waiter, delegate, sequence, ret, err, nil, delegateErr)
	if !event.ErrorIsDelegateSentinel || event.DelegateCalls != 1 || len(event.Sequence) != 2 || event.Sequence[0] != "wait" || event.Sequence[1] != "delegate" {
		t.Fatalf("delegate error identity changed: %#v", event)
	}
	return queryLimiterBaseFixture("delegate_error_identity_after_wait", runtime, defaults, input, []queryLimiterEvent{event})
}

func generateQueryLimiterPreCanceledFixture(t *testing.T, runtime queryLimiterRuntime, defaults queryLimiterDefaults) queryLimiterFixture {
	t.Helper()
	address := "192.0.2.14"
	input := queryLimiterInput{
		LimiterKind: "actual_keyed_rate_1_per_hour_burst_1_ttl_0", ContextKind: "pre_canceled_then_background",
		Addresses: []string{address, address}, ScriptedWaits: []string{}, Delegate: "success",
	}
	delegate := &queryLimiterScriptedServer{ret: dht.RecvMsg{Msg: dht.Msg{R: &dht.Return{ID: protocol.ID{0x14}}}}}
	production := queryLimiter{
		server:       delegate,
		queryLimiter: concurrency.NewKeyedLimiter(rate.Every(time.Hour), 1, 8, 0),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ret1, err1 := production.Query(ctx, queryLimiterAddr(address), dht.QPing, dht.MsgArgs{})
	event1 := queryLimiterEvent{
		Call: 1, Address: address, WaitKeys: []string{}, Sequence: []string{},
		DelegateCalls: int(delegate.calls.Load()), ReturnIDHex: queryLimiterReturnIDHex(ret1),
		ErrorMessage: err1.Error(), ErrorIsCanceled: errors.Is(err1, context.Canceled),
	}
	ret2, err2 := production.Query(context.Background(), queryLimiterAddr(address), dht.QPing, dht.MsgArgs{})
	event2 := queryLimiterEvent{
		Call: 2, Address: address, WaitKeys: []string{}, Sequence: []string{},
		DelegateCalls: int(delegate.calls.Load()), ReturnIDHex: queryLimiterReturnIDHex(ret2),
		ErrorIsCanceled: errors.Is(err2, context.Canceled), ErrorIsDeadlineExceeded: errors.Is(err2, context.DeadlineExceeded),
	}
	if !event1.ErrorIsCanceled || event1.ErrorMessage != context.Canceled.Error() || event1.DelegateCalls != 0 || err2 != nil || event2.DelegateCalls != 1 {
		t.Fatalf("pre-canceled behavior changed: %#v %#v", event1, event2)
	}
	return queryLimiterBaseFixture("pre_canceled_actual_keyed_limiter", runtime, defaults, input, []queryLimiterEvent{event1, event2})
}

func generateQueryLimiterExpiredDeadlineFixture(t *testing.T, runtime queryLimiterRuntime, defaults queryLimiterDefaults) queryLimiterFixture {
	t.Helper()
	address := "192.0.2.15"
	input := queryLimiterInput{
		LimiterKind: "actual_keyed_rate_1_per_hour_burst_1_ttl_0", ContextKind: "expired_deadline",
		Addresses: []string{address}, ScriptedWaits: []string{}, Delegate: "must_not_run",
	}
	delegate := &queryLimiterScriptedServer{ret: dht.RecvMsg{Msg: dht.Msg{R: &dht.Return{}}}}
	production := queryLimiter{server: delegate, queryLimiter: concurrency.NewKeyedLimiter(rate.Every(time.Hour), 1, 8, 0)}
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancel()
	ret, err := production.Query(ctx, queryLimiterAddr(address), dht.QPing, dht.MsgArgs{})
	event := queryLimiterEvent{
		Call: 1, Address: address, WaitKeys: []string{}, Sequence: []string{},
		DelegateCalls: int(delegate.calls.Load()), ReturnIDHex: queryLimiterReturnIDHex(ret),
		ErrorMessage: err.Error(), ErrorIsCanceled: errors.Is(err, context.Canceled),
		ErrorIsDeadlineExceeded: errors.Is(err, context.DeadlineExceeded),
	}
	if !event.ErrorIsDeadlineExceeded || event.ErrorMessage != context.DeadlineExceeded.Error() || event.DelegateCalls != 0 {
		t.Fatalf("expired deadline behavior changed: %#v", event)
	}
	return queryLimiterBaseFixture("expired_deadline_actual_keyed_limiter", runtime, defaults, input, []queryLimiterEvent{event})
}

func generateQueryLimiterFutureDeadlineFixture(t *testing.T, runtime queryLimiterRuntime, defaults queryLimiterDefaults) queryLimiterFixture {
	t.Helper()
	address := "192.0.2.16"
	input := queryLimiterInput{
		LimiterKind: "actual_keyed_rate_1_per_hour_burst_1_ttl_0", ContextKind: "background_then_1_second_deadline",
		Addresses: []string{address, address}, ScriptedWaits: []string{}, Delegate: "first_only",
	}
	delegate := &queryLimiterScriptedServer{ret: dht.RecvMsg{Msg: dht.Msg{R: &dht.Return{ID: protocol.ID{0x16}}}}}
	production := queryLimiter{server: delegate, queryLimiter: concurrency.NewKeyedLimiter(rate.Every(time.Hour), 1, 8, 0)}
	ret1, err1 := production.Query(context.Background(), queryLimiterAddr(address), dht.QPing, dht.MsgArgs{})
	event1 := queryLimiterEvent{
		Call: 1, Address: address, WaitKeys: []string{}, Sequence: []string{}, DelegateCalls: int(delegate.calls.Load()),
		ReturnIDHex: queryLimiterReturnIDHex(ret1),
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
	defer cancel()
	ret2, err2 := production.Query(ctx, queryLimiterAddr(address), dht.QPing, dht.MsgArgs{})
	event2 := queryLimiterEvent{
		Call: 2, Address: address, WaitKeys: []string{}, Sequence: []string{}, DelegateCalls: int(delegate.calls.Load()),
		ReturnIDHex: queryLimiterReturnIDHex(ret2), ErrorMessage: err2.Error(),
		ErrorIsCanceled: errors.Is(err2, context.Canceled), ErrorIsDeadlineExceeded: errors.Is(err2, context.DeadlineExceeded),
	}
	if err1 != nil || event1.DelegateCalls != 1 || event2.ErrorMessage != "rate: Wait(n=1) would exceed context deadline" || event2.DelegateCalls != 1 || event2.ErrorIsDeadlineExceeded {
		t.Fatalf("future deadline reservation behavior changed: %#v %#v", event1, event2)
	}
	return queryLimiterBaseFixture("future_deadline_rejected_before_delegate", runtime, defaults, input, []queryLimiterEvent{event1, event2})
}

func generateQueryLimiterKeysFixture(t *testing.T, runtime queryLimiterRuntime, defaults queryLimiterDefaults) queryLimiterFixture {
	t.Helper()
	addresses := []string{"192.0.2.1", "::ffff:192.0.2.1", "fe80::1%7", "fe80::1%8"}
	input := queryLimiterInput{
		LimiterKind: "scripted", ContextKind: "background", Addresses: addresses,
		ScriptedWaits: []string{"success", "success", "success", "success"}, Delegate: "success",
	}
	waiter := &queryLimiterScriptedKeyed{}
	delegate := &queryLimiterScriptedServer{ret: dht.RecvMsg{Msg: dht.Msg{R: &dht.Return{}}}}
	production := queryLimiter{server: delegate, queryLimiter: waiter}
	events := make([]queryLimiterEvent, 0, len(addresses))
	for index, address := range addresses {
		ret, err := production.Query(context.Background(), queryLimiterAddr(address), dht.QPing, dht.MsgArgs{})
		event := queryLimiterEventFrom(index+1, address, waiter, delegate, []string{}, ret, err, nil, nil)
		events = append(events, event)
	}
	if len(waiter.keys) != len(addresses) {
		t.Fatalf("Wait keys = %d, want %d", len(waiter.keys), len(addresses))
	}
	for index := range addresses {
		if waiter.keys[index] != addresses[index] || events[index].DelegateCalls != index+1 {
			t.Fatalf("exact query key event %d changed: %#v", index, events[index])
		}
	}
	return queryLimiterBaseFixture("exact_ip_string_keys", runtime, defaults, input, events)
}

func reconcileQueryLimiterFixture(t *testing.T, fixtures []queryLimiterFixture) {
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
	path := filepath.Join("..", "..", "..", "..", "testdata", "parity", "dht", "query_limiter.jsonl")
	if *updateDHTQueryLimiterParity {
		if err := os.WriteFile(path, generatedBytes, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	digest := sha256.Sum256(generatedBytes)
	if digestHex := hex.EncodeToString(digest[:]); queryLimiterGoldenSHA256 != "TODO" && digestHex != queryLimiterGoldenSHA256 {
		t.Fatalf("generated query limiter fixture digest = %s, want %s", digestHex, queryLimiterGoldenSHA256)
	}
	checked, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read query limiter fixture; rerun with -update-dht-query-limiter-parity: %v", err)
	}
	if !bytes.Equal(checked, generatedBytes) {
		t.Fatal("DHT query limiter fixture is stale; rerun with -update-dht-query-limiter-parity")
	}
}
