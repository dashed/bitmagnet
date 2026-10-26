package server

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"go.uber.org/zap"
)

var updateDHTTransactionParity = flag.Bool(
	"update-dht-transaction-parity",
	false,
	"rewrite the Rust DHT transaction parity fixture",
)

type transactionParitySocket struct {
	mu           sync.Mutex
	server       *server
	sends        []transactionParitySend
	sendObserved chan struct{}
	sendErr      error
}

type transactionParitySend struct {
	TID              string `json:"tid"`
	Addr             string `json:"addr"`
	WireHex          string `json:"wireHex"`
	RegisteredAtSend bool   `json:"registeredAtSend"`
}

func (*transactionParitySocket) Open(netip.AddrPort) error { return nil }
func (*transactionParitySocket) Close() error              { return nil }
func (*transactionParitySocket) Receive([]byte) (int, netip.AddrPort, error) {
	return 0, netip.AddrPort{}, context.Canceled
}

func (s *transactionParitySocket) Send(addr netip.AddrPort, wire []byte) error {
	var msg dht.Msg
	if err := bencode.Unmarshal(wire, &msg); err != nil {
		return err
	}

	s.server.mutex.Lock()
	pending, registered := s.server.queries[msg.T]
	registered = registered && addrMatches(pending.addr, addr)
	s.server.mutex.Unlock()

	s.mu.Lock()
	s.sends = append(s.sends, transactionParitySend{
		TID:              hex.EncodeToString([]byte(msg.T)),
		Addr:             addr.String(),
		WireHex:          hex.EncodeToString(wire),
		RegisteredAtSend: registered,
	})
	s.mu.Unlock()
	s.sendObserved <- struct{}{}

	return s.sendErr
}

type transactionParityFixture struct {
	ID        string                    `json:"id"`
	Subsystem string                    `json:"subsystem"`
	Input     transactionParityInput    `json:"input"`
	Expected  transactionParityExpected `json:"expected"`
}

type transactionParityInput struct {
	IssuerTIDs   []string                   `json:"issuerTids"`
	Remotes      []string                   `json:"remotes"`
	Query        string                     `json:"query"`
	AddressCases []transactionAddressCase   `json:"addressCases"`
	Deliveries   []transactionDeliveryInput `json:"deliveries"`
}

type transactionAddressCase struct {
	Left  string `json:"left"`
	Right string `json:"right"`
}

type transactionDeliveryInput struct {
	Kind     string `json:"kind"`
	TID      string `json:"tid"`
	From     string `json:"from"`
	ClientID string `json:"clientId,omitempty"`
}

type transactionParityExpected struct {
	Sends                  []transactionParitySend `json:"sends"`
	PendingWhileSent       int                     `json:"pendingWhileSent"`
	PendingAfterCancel     int                     `json:"pendingAfterCancel"`
	SendFailureWasReturned bool                    `json:"sendFailureWasReturned"`
	PendingAfterSendError  int                     `json:"pendingAfterSendError"`
	AddressMatches         []bool                  `json:"addressMatches"`
	DeliveryObservations   []string                `json:"deliveryObservations"`
	FirstClientID          string                  `json:"firstClientId"`
	PendingAfterDelivery   int                     `json:"pendingAfterDelivery"`
	TerminalCases          []transactionTerminal   `json:"terminalCases"`
}

type transactionTerminal struct {
	Name         string `json:"name"`
	Outcome      string `json:"outcome"`
	PendingAfter int    `json:"pendingAfter"`
}

func newTransactionParityServer(ids []string, socket *transactionParitySocket) *server {
	s := &server{
		stopped:      make(chan struct{}),
		socket:       socket,
		queryTimeout: time.Minute,
		queries:      make(map[string]pendingQuery),
		idIssuer:     &scriptedIssuer{ids: ids},
		logger:       zap.NewNop().Sugar(),
	}
	socket.server = s

	return s
}

func runTransactionTerminal(
	t *testing.T,
	name string,
	tid string,
	responseMessage *dht.Msg,
	cancelBefore bool,
	cancelAfterSend bool,
	timeout time.Duration,
) transactionTerminal {
	t.Helper()
	socket := &transactionParitySocket{sendObserved: make(chan struct{}, 1)}
	s := newTransactionParityServer([]string{tid}, socket)
	s.queryTimeout = timeout
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if cancelBefore {
		cancel()
	}
	addr := netip.MustParseAddrPort("1.2.3.4:6881")
	result := runQuery(ctx, s, addr, dht.QPing)
	select {
	case <-socket.sendObserved:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not reach fake Send", name)
	}
	if cancelAfterSend {
		cancel()
	}
	if responseMessage != nil {
		s.handleResponse(dht.RecvMsg{From: addr, Msg: *responseMessage})
	}
	queryResult := <-result
	if cancelAfterSend {
		// A late response after the defer cleanup exercises the real unknown-TID
		// path without reintroducing a map entry.
		s.handleResponse(response(addr, tid))
	}

	outcome := "success"
	switch {
	case errors.Is(queryResult.err, context.Canceled):
		outcome = "cancelled"
	case errors.Is(queryResult.err, context.DeadlineExceeded):
		outcome = "timeout"
	case queryResult.err != nil && reflect.ValueOf(queryResult.err).Kind() == reflect.Ptr && reflect.ValueOf(queryResult.err).IsNil():
		outcome = "typed_nil_error"
	case queryResult.err != nil:
		outcome = queryResult.err.Error()
	}
	s.mutex.Lock()
	pendingAfter := len(s.queries)
	s.mutex.Unlock()
	socket.mu.Lock()
	if len(socket.sends) != 1 || !socket.sends[0].RegisteredAtSend {
		socket.mu.Unlock()
		t.Fatalf("%s was not registered synchronously inside its only Send", name)
	}
	socket.mu.Unlock()

	return transactionTerminal{Name: name, Outcome: outcome, PendingAfter: pendingAfter}
}

func TestGenerateDHTTransactionParity(t *testing.T) {
	ids := []string{"A1", "A1", "B2"}
	remotes := []netip.AddrPort{
		netip.MustParseAddrPort("1.2.3.4:6881"),
		netip.MustParseAddrPort("[2001:db8::1]:6881"),
	}
	socket := &transactionParitySocket{sendObserved: make(chan struct{}, 4)}
	s := newTransactionParityServer(ids, socket)
	contexts := make([]context.CancelFunc, 0, 2)
	results := make([]chan queryResult, 0, 2)
	for _, remote := range remotes {
		ctx, cancel := context.WithCancel(context.Background())
		contexts = append(contexts, cancel)
		results = append(results, runQuery(ctx, s, remote, dht.QPing))
		select {
		case <-socket.sendObserved:
		case <-time.After(2 * time.Second):
			t.Fatal("query did not reach fake Send")
		}
	}

	s.mutex.Lock()
	pendingWhileSent := len(s.queries)
	s.mutex.Unlock()
	for _, cancel := range contexts {
		cancel()
	}
	for _, result := range results {
		if err := (<-result).err; !errors.Is(err, context.Canceled) {
			t.Fatalf("expected canceled query, got %v", err)
		}
	}
	s.mutex.Lock()
	pendingAfterCancel := len(s.queries)
	s.mutex.Unlock()

	sendFailure := errors.New("oracle send failure")
	failureSocket := &transactionParitySocket{
		sendObserved: make(chan struct{}, 1),
		sendErr:      sendFailure,
	}
	failureServer := newTransactionParityServer([]string{"C3"}, failureSocket)
	_, failureErr := failureServer.Query(
		context.Background(),
		netip.MustParseAddrPort("1.2.3.4:6881"),
		dht.QPing,
		dht.MsgArgs{},
	)
	failureServer.mutex.Lock()
	pendingAfterSendError := len(failureServer.queries)
	failureServer.mutex.Unlock()
	failureSocket.mu.Lock()
	if len(failureSocket.sends) != 1 || !failureSocket.sends[0].RegisteredAtSend {
		failureSocket.mu.Unlock()
		t.Fatal("send-failure query was not registered synchronously inside Send")
	}
	failureSocket.mu.Unlock()

	addressCases := []transactionAddressCase{
		{Left: "1.2.3.4:6881", Right: "[::ffff:1.2.3.4]:6881"},
		{Left: "1.2.3.4:6881", Right: "1.2.3.4:6882"},
		{Left: "[fe80::1%3]:6881", Right: "[fe80::1%3]:6881"},
		{Left: "[fe80::1%3]:6881", Right: "[fe80::1%4]:6881"},
	}
	addressMatchesExpected := make([]bool, 0, len(addressCases))
	for _, tc := range addressCases {
		addressMatchesExpected = append(addressMatchesExpected, addrMatches(
			netip.MustParseAddrPort(tc.Left),
			netip.MustParseAddrPort(tc.Right),
		))
	}

	deliveryServer, _, _ := newTestServer(&scriptedIssuer{ids: []string{"D4"}})
	deliveryAddr := netip.MustParseAddrPort("1.2.3.4:6881")
	deliveryChannel := make(chan dht.RecvMsg, 1)
	deliveryServer.queries["D4"] = pendingQuery{ch: deliveryChannel, addr: deliveryAddr}
	deliveries := []transactionDeliveryInput{
		{Kind: "unknown", TID: "5539", From: deliveryAddr.String()},
		{Kind: "address_mismatch", TID: "4434", From: "1.2.3.4:6882"},
		{Kind: "delivered", TID: "4434", From: deliveryAddr.String(), ClientID: "6669727374"},
		{Kind: "duplicate", TID: "4434", From: deliveryAddr.String(), ClientID: "7365636f6e64"},
	}
	observations := make([]string, 0, len(deliveries))
	for _, delivery := range deliveries {
		tid, err := hex.DecodeString(delivery.TID)
		if err != nil {
			t.Fatal(err)
		}
		clientID, err := hex.DecodeString(delivery.ClientID)
		if err != nil {
			t.Fatal(err)
		}
		before := len(deliveryChannel)
		deliveryServer.handleResponse(dht.RecvMsg{
			From: netip.MustParseAddrPort(delivery.From),
			Msg:  dht.Msg{T: string(tid), Y: dht.YResponse, R: &dht.Return{}, ClientID: string(clientID)},
		})
		after := len(deliveryChannel)
		switch {
		case delivery.Kind == "delivered" && before == 0 && after == 1:
			observations = append(observations, "delivered")
		case delivery.Kind == "duplicate" && before == 1 && after == 1:
			observations = append(observations, "duplicate")
		case before == after:
			observations = append(observations, delivery.Kind)
		default:
			t.Fatalf("unexpected channel transition for %s: %d -> %d", delivery.Kind, before, after)
		}
	}
	first := <-deliveryChannel
	deliveryServer.mutex.Lock()
	pendingAfterDelivery := len(deliveryServer.queries)
	deliveryServer.mutex.Unlock()
	terminalCases := []transactionTerminal{
		runTransactionTerminal(t, "happy", "H1", &dht.Msg{
			T: "H1", Y: dht.YResponse, R: &dht.Return{},
		}, false, false, time.Minute),
		runTransactionTerminal(t, "remote_error", "E1", &dht.Msg{
			T: "E1", Y: dht.YError, E: &dht.Error{Code: 201, Msg: "remote"},
		}, false, false, time.Minute),
		runTransactionTerminal(t, "missing_error_body", "E2", &dht.Msg{
			T: "E2", Y: dht.YError,
		}, false, false, time.Minute),
		runTransactionTerminal(t, "missing_return_body", "R0", &dht.Msg{
			T: "R0", Y: dht.YResponse,
		}, false, false, time.Minute),
		runTransactionTerminal(t, "pre_cancelled", "C0", nil, true, false, time.Minute),
		runTransactionTerminal(t, "timeout", "T0", nil, false, false, time.Millisecond),
		runTransactionTerminal(t, "late_after_cancel", "L0", nil, false, true, time.Minute),
	}

	socket.mu.Lock()
	sends := append([]transactionParitySend(nil), socket.sends...)
	socket.mu.Unlock()
	fixture := transactionParityFixture{
		ID:        "go_server_transaction_core",
		Subsystem: "dht_transaction",
		Input: transactionParityInput{
			IssuerTIDs:   []string{"4131", "4131", "4232"},
			Remotes:      []string{remotes[0].String(), remotes[1].String()},
			Query:        dht.QPing,
			AddressCases: addressCases,
			Deliveries:   deliveries,
		},
		Expected: transactionParityExpected{
			Sends:                  sends,
			PendingWhileSent:       pendingWhileSent,
			PendingAfterCancel:     pendingAfterCancel,
			SendFailureWasReturned: errors.Is(failureErr, sendFailure),
			PendingAfterSendError:  pendingAfterSendError,
			AddressMatches:         addressMatchesExpected,
			DeliveryObservations:   observations,
			FirstClientID:          hex.EncodeToString([]byte(first.Msg.ClientID)),
			PendingAfterDelivery:   pendingAfterDelivery,
			TerminalCases:          terminalCases,
		},
	}

	encoded, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	if *updateDHTTransactionParity {
		_, source, _, ok := runtime.Caller(0)
		if !ok {
			t.Fatal("resolve generator source")
		}
		path := filepath.Clean(filepath.Join(
			filepath.Dir(source),
			"../../../../testdata/parity/dht/transaction.jsonl",
		))
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}

	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve generator source")
	}
	path := filepath.Clean(filepath.Join(
		filepath.Dir(source),
		"../../../../testdata/parity/dht/transaction.jsonl",
	))
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read generated fixture; rerun with -update-dht-transaction-parity: %v", err)
	}
	if string(want) != string(encoded) {
		t.Fatal("transaction fixture is stale; rerun with -update-dht-transaction-parity")
	}
}
