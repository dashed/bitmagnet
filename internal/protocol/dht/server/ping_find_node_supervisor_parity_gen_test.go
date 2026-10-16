package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	dhtresponder "github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/responder"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

var updateDHTPingFindNodeSupervisorParity = flag.Bool(
	"update-dht-ping-find-node-supervisor-parity",
	false,
	"rewrite the Rust DHT ping/find-node finite supervisor fixture",
)

type pingFindNodeSupervisorFixture struct {
	ID        string                         `json:"id"`
	Subsystem string                         `json:"subsystem"`
	Input     pingFindNodeSupervisorInput    `json:"input"`
	Expected  pingFindNodeSupervisorExpected `json:"expected"`
}

type pingFindNodeSupervisorInput struct {
	WireHex      string                        `json:"wireHex,omitempty"`
	Source       pingFindNodeSupervisorAddress `json:"source"`
	ReceiveFails bool                          `json:"receiveFails,omitempty"`
	SendFails    bool                          `json:"sendFails,omitempty"`
}

type pingFindNodeSupervisorExpected struct {
	GoTerminal          string `json:"goTerminal"`
	RustTerminal        string `json:"rustTerminal"`
	GoReceiveCalls      int    `json:"goReceiveCalls"`
	GoSendCalls         int    `json:"goSendCalls"`
	GoReplyWireHex      string `json:"goReplyWireHex,omitempty"`
	GoPanicked          bool   `json:"goPanicked,omitempty"`
	PanicRetained       bool   `json:"panicRetainedTransport,omitempty"`
	SendFailureLogged   bool   `json:"sendFailureLogged,omitempty"`
	SendFailureRetained bool   `json:"sendFailureRetainedTransport,omitempty"`
}

type pingFindNodeSupervisorAddress struct {
	IP   string `json:"ip"`
	Port uint16 `json:"port"`
}

type pingFindNodeSupervisorScenario struct {
	id, goTerminal, rustTerminal string
	wire                         []byte
	source                       netip.AddrPort
	receiveErr                   error
	sendErr                      error
	response                     dht.Return
	responseErr                  error
}

type pingFindNodeSupervisorSocket struct {
	mu           sync.Mutex
	wire         []byte
	source       netip.AddrPort
	receiveErr   error
	sendErr      error
	cancel       context.CancelFunc
	completed    <-chan struct{}
	completeSend func()
	served       bool
	receiveCalls int
	sends        [][]byte
}

func (*pingFindNodeSupervisorSocket) Open(netip.AddrPort) error { return nil }
func (*pingFindNodeSupervisorSocket) Close() error              { return nil }

func (s *pingFindNodeSupervisorSocket) Receive(buffer []byte) (int, netip.AddrPort, error) {
	s.mu.Lock()
	s.receiveCalls++
	if !s.served {
		s.served = true
		wire := append([]byte(nil), s.wire...)
		source := s.source
		err := s.receiveErr
		s.mu.Unlock()
		copy(buffer, wire)
		return len(wire), source, err
	}
	s.mu.Unlock()

	<-s.completed
	s.cancel()
	return 0, netip.AddrPort{}, context.Canceled
}

func (s *pingFindNodeSupervisorSocket) Send(_ netip.AddrPort, wire []byte) error {
	s.mu.Lock()
	s.sends = append(s.sends, append([]byte(nil), wire...))
	err := s.sendErr
	s.mu.Unlock()
	if err == nil {
		s.completeSend()
	}
	return err
}

type pingFindNodeSupervisorResponder struct {
	response dht.Return
	err      error
}

func (r pingFindNodeSupervisorResponder) Respond(
	context.Context,
	dht.RecvMsg,
) (dht.Return, error) {
	return r.response, r.err
}

func TestGenerateDHTPingFindNodeSupervisorParity(t *testing.T) {
	remote := netip.MustParseAddrPort("192.0.2.1:6881")
	localID := pingFindNodeSupervisorID(0x90)
	query := func(tid, method string) []byte {
		wire, err := bencode.Marshal(dht.Msg{
			T: tid,
			Y: dht.YQuery,
			Q: method,
			A: &dht.MsgArgs{ID: pingFindNodeSupervisorID(1)},
		})
		if err != nil {
			t.Fatal(err)
		}
		return wire
	}
	sentinel := errors.New("supervisor transport sentinel")
	scenarios := []pingFindNodeSupervisorScenario{
		{
			id: "unowned_go_204_rust_pause", goTerminal: "method_unknown_reply_sent",
			rustTerminal: "unowned_query", wire: query("U1", dht.QGetPeers), source: remote,
			responseErr: dhtresponder.ErrMethodUnknown,
		},
		{
			id: "send_failure_go_swallow_rust_stop", goTerminal: "send_failure_swallowed",
			rustTerminal: "failed_send", wire: query("S1", dht.QPing), source: remote,
			response: dht.Return{ID: localID}, sendErr: sentinel,
		},
		{
			id: "receive_failure_go_panic_rust_stop", goTerminal: "receive_panic",
			rustTerminal: "failed_receive", source: remote, receiveErr: sentinel,
		},
	}

	fixtures := make([]pingFindNodeSupervisorFixture, 0, len(scenarios))
	for _, scenario := range scenarios {
		fixture := runPingFindNodeSupervisorScenario(t, scenario)
		fixtures = append(fixtures, fixture)
	}
	reconcilePingFindNodeSupervisorFixtures(t, fixtures)
}

func runPingFindNodeSupervisorScenario(
	t *testing.T,
	scenario pingFindNodeSupervisorScenario,
) pingFindNodeSupervisorFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	completed := make(chan struct{})
	var completeOnce sync.Once
	complete := func() { completeOnce.Do(func() { close(completed) }) }

	socket := &pingFindNodeSupervisorSocket{
		wire: scenario.wire, source: scenario.source, receiveErr: scenario.receiveErr,
		sendErr: scenario.sendErr, cancel: cancel, completed: completed, completeSend: complete,
	}
	loggedSendFailure := false
	logCore, observedLogs := observer.New(zap.DebugLevel)
	logger := zap.New(
		zapcore.NewTee(
			logCore,
			zapcore.NewCore(
				zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
				zapcore.AddSync(io.Discard),
				zap.DebugLevel,
			),
		),
		zap.Hooks(func(entry zapcore.Entry) error {
			if entry.Message == "could not send response" {
				loggedSendFailure = true
				complete()
			}
			return nil
		}),
	).Sugar()
	srv := &server{
		socket:  socket,
		queries: make(map[string]pendingQuery),
		responder: pingFindNodeSupervisorResponder{
			response: scenario.response,
			err:      scenario.responseErr,
		},
		responderTimeout: time.Minute,
		logger:           logger,
	}

	panicked, recovered := pingFindNodeSupervisorRunRead(srv, ctx)
	panicRetained := false
	if recoveredErr, ok := recovered.(error); ok {
		panicRetained = errors.Is(recoveredErr, scenario.receiveErr)
	}
	socket.mu.Lock()
	receiveCalls := socket.receiveCalls
	sends := append([][]byte(nil), socket.sends...)
	socket.mu.Unlock()
	if scenario.receiveErr != nil {
		if !panicked || !panicRetained || receiveCalls != 1 || len(sends) != 0 {
			t.Fatalf("receive failure evidence: panic=%v receives=%d sends=%d", panicked, receiveCalls, len(sends))
		}
	} else if panicked || receiveCalls != 2 || len(sends) != 1 {
		t.Fatalf("completed read evidence: panic=%v receives=%d sends=%d", panicked, receiveCalls, len(sends))
	}
	if scenario.sendErr != nil && !loggedSendFailure {
		t.Fatal("actual handleQuery did not log the swallowed send failure")
	}
	sendFailureRetained := false
	for _, entry := range observedLogs.FilterMessage("could not send response").All() {
		for _, field := range entry.Context {
			if field.Key == "retErr" && field.Interface == scenario.sendErr {
				sendFailureRetained = true
			}
		}
	}
	if scenario.sendErr != nil && !sendFailureRetained {
		t.Fatal("actual handleQuery did not retain the exact send sentinel in its completion log")
	}

	replyHex := ""
	if len(sends) == 1 {
		replyHex = hex.EncodeToString(sends[0])
	}
	return pingFindNodeSupervisorFixture{
		ID: scenario.id, Subsystem: "dht_ping_find_node_supervisor",
		Input: pingFindNodeSupervisorInput{
			WireHex: hex.EncodeToString(scenario.wire),
			Source: pingFindNodeSupervisorAddress{
				IP: scenario.source.Addr().String(), Port: scenario.source.Port(),
			},
			ReceiveFails: scenario.receiveErr != nil,
			SendFails:    scenario.sendErr != nil,
		},
		Expected: pingFindNodeSupervisorExpected{
			GoTerminal: scenario.goTerminal, RustTerminal: scenario.rustTerminal,
			GoReceiveCalls: receiveCalls, GoSendCalls: len(sends),
			GoReplyWireHex: replyHex, GoPanicked: panicked,
			PanicRetained: panicRetained, SendFailureLogged: loggedSendFailure,
			SendFailureRetained: sendFailureRetained,
		},
	}
}

func pingFindNodeSupervisorRunRead(
	srv *server,
	ctx context.Context,
) (panicked bool, recovered any) {
	defer func() {
		recovered = recover()
		panicked = recovered != nil
	}()
	srv.read(ctx)
	return false, nil
}

func pingFindNodeSupervisorID(last byte) protocol.ID {
	var id protocol.ID
	id[19] = last
	return id
}

func reconcilePingFindNodeSupervisorFixtures(
	t *testing.T,
	fixtures []pingFindNodeSupervisorFixture,
) {
	t.Helper()
	var encoded bytes.Buffer
	for _, fixture := range fixtures {
		line, err := json.Marshal(fixture)
		if err != nil {
			t.Fatal(err)
		}
		encoded.Write(line)
		encoded.WriteByte('\n')
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve generator source")
	}
	path := filepath.Clean(filepath.Join(
		filepath.Dir(source),
		"../../../../testdata/parity/dht/ping_find_node_supervisor.jsonl",
	))
	if *updateDHTPingFindNodeSupervisorParity {
		if err := os.WriteFile(path, encoded.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture; rerun with -update-dht-ping-find-node-supervisor-parity: %v", err)
	}
	if !bytes.Equal(want, encoded.Bytes()) {
		t.Fatal("ping/find-node supervisor fixture is stale; rerun with -update-dht-ping-find-node-supervisor-parity")
	}
}
