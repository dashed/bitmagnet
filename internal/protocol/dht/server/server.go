package server

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync"
	"time"

	"github.com/anacrolix/torrent/bencode"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/responder"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type Server interface {
	start() error
	stop()
	Query(ctx context.Context, addr netip.AddrPort, q string, args dht.MsgArgs) (dht.RecvMsg, error)
}

// pendingQuery holds the delivery channel and the address a query was sent to,
// so that responses can be verified to originate from the queried node.
type pendingQuery struct {
	ch   chan dht.RecvMsg
	addr netip.AddrPort
}

type server struct {
	stopped          chan struct{}
	mutex            sync.Mutex
	localAddr        netip.AddrPort
	socket           Socket
	queryTimeout     time.Duration
	queries          map[string]pendingQuery
	responder        responder.Responder
	responderTimeout time.Duration
	idIssuer         IDIssuer
	responseDropped  *prometheus.CounterVec
	logger           *zap.SugaredLogger
}

// addrMatches reports whether two addresses refer to the same node. Both sides
// are Unmap()-ed so that an IPv4-in-IPv6 address (e.g. a bootstrap addr resolved
// via net.ResolveUDPAddr) compares equal to the plain IPv4 address that a node
// actually sends its responses from.
func addrMatches(a, b netip.AddrPort) bool {
	return a.Port() == b.Port() && a.Addr().Unmap() == b.Addr().Unmap()
}

func (s *server) start() error {
	if err := s.socket.Open(s.localAddr); err != nil {
		return fmt.Errorf("could not open socket: %w", err)
	}

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		go s.read(ctx)
		<-s.stopped
		cancel()

		_ = s.socket.Close()
	}()

	return nil
}

func (s *server) stop() {
	close(s.stopped)
}

func (s *server) read(ctx context.Context) {
	/*   The field size sets a theoretical limit of 65,535 bytes (8 byte header + 65,527 bytes of
	 * data) for a UDP datagram. However the actual limit for the data length, which is imposed by
	 * the underlying IPv4 protocol, is 65,507 bytes (65,535 − 8 byte UDP header − 20 byte IP
	 * header).
	 *
	 *   In IPv6 jumbograms it is possible to have UDP packets of size greater than 65,535 bytes.
	 * RFC 2675 specifies that the length field is set to zero if the length of the UDP header plus
	 * UDP data is greater than 65,535.
	 *
	 * https://en.wikipedia.org/wiki/User_Datagram_Protocol
	 */
	buffer := make([]byte, 65507)

	for {
		if ctx.Err() != nil {
			return
		}

		n, from, err := s.socket.Receive(buffer)
		if err != nil {
			// Socket is probably closed; if we're not shutting down then panic
			if ctx.Err() == nil {
				panic(fmt.Errorf("socket read error: %w", err))
			}

			return
		}

		if n == 0 {
			/* Datagram sockets in various domains  (e.g., the UNIX and Internet domains) permit
			 * zero-length datagrams. When such a datagram is received, the return value (n) is 0.
			 */
			continue
		}

		var msg dht.Msg

		err = bencode.Unmarshal(buffer[:n], &msg)
		if err != nil {
			s.logger.Debugw("could not unmarshal packet data", "error", err)
			continue
		}

		recvMsg := dht.RecvMsg{
			Msg:  msg,
			From: from,
		}

		switch msg.Y {
		case dht.YQuery:
			go s.handleQuery(ctx, recvMsg)
		case dht.YResponse, dht.YError:
			go s.handleResponse(recvMsg)
		}
	}
}

func (s *server) handleQuery(ctx context.Context, msg dht.RecvMsg) {
	ctx, cancel := context.WithTimeout(ctx, s.responderTimeout)
	defer cancel()

	res := dht.Msg{
		T: msg.Msg.T,
		Y: dht.YResponse,
	}

	ret, retErr := s.responder.Respond(ctx, msg)
	if retErr != nil {
		dhtErr := &dht.Error{}
		if ok := errors.As(retErr, dhtErr); ok {
			res.E = dhtErr
		} else {
			res.E = &dht.Error{
				Code: dht.ErrorCodeServerError,
				Msg:  "server error",
			}

			s.logger.Errorw("server error", "msg", msg, "retErr", retErr)
		}
	} else {
		res.R = &ret
	}

	if sendErr := s.send(msg.From, res); sendErr != nil {
		s.logger.Debugw("could not send response", "msg", msg, "retErr", sendErr)
	}
}

func (s *server) handleResponse(msg dht.RecvMsg) {
	transactionID := msg.Msg.T

	s.mutex.Lock()
	pending, ok := s.queries[transactionID]
	s.mutex.Unlock()

	if !ok {
		// No in-flight query with this transaction ID; either it already
		// completed/timed out or this is an unsolicited/forged response.
		s.dropResponse("unknown_tid")
		return
	}

	if !addrMatches(msg.From, pending.addr) {
		// Off-path response injection: the source address does not match the
		// address we sent the query to. Drop it.
		s.logger.Debugw(
			"dropped response from unexpected addr",
			"tid", transactionID,
			"from", msg.From,
			"expected", pending.addr,
		)
		s.dropResponse("addr_mismatch")

		return
	}

	// Non-blocking send: the channel has capacity 1, so if a duplicate accepted
	// response races in after the first was delivered, we must not block here.
	select {
	case pending.ch <- msg:
	default:
	}
}

func (s *server) dropResponse(reason string) {
	if s.responseDropped != nil {
		s.responseDropped.WithLabelValues(reason).Inc()
	}
}

func (s *server) Query(
	ctx context.Context,
	addr netip.AddrPort,
	q string,
	args dht.MsgArgs,
) (r dht.RecvMsg, err error) {
	ch := make(chan dht.RecvMsg, 1)

	// Issue the transaction ID and register the pending query atomically, with a
	// collision-retry so that all in-flight transaction IDs are unique. This
	// makes responses harder to forge (the ID is unpredictable, see idIssuer)
	// and prevents a colliding ID from delivering to the wrong query.
	s.mutex.Lock()
	var transactionID string

	for {
		transactionID = s.idIssuer.Issue()
		if _, exists := s.queries[transactionID]; !exists {
			break
		}
	}

	s.queries[transactionID] = pendingQuery{ch: ch, addr: addr}
	s.mutex.Unlock()

	defer (func() {
		s.mutex.Lock()
		delete(s.queries, transactionID)
		s.mutex.Unlock()
	})()

	msg := dht.Msg{
		Q: q,
		T: transactionID,
		A: &args,
		Y: dht.YQuery,
	}
	if sendErr := s.send(addr, msg); sendErr != nil {
		s.logger.Debugw("could not send query", "msg", msg, "sendErr", sendErr)
		err = sendErr

		return
	}

	queryCtx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()
	select {
	case <-queryCtx.Done():
		err = queryCtx.Err()
		return
	case res, ok := <-ch:
		if !ok {
			err = errors.New("channel closed")
			return
		}

		r = res

		if res.Msg.Y == dht.YError {
			err = res.Msg.E
			if err == nil {
				err = errors.New("error missing from response")
			}
		} else if r.Msg.R == nil {
			err = errors.New("return data missing from response")
		}

		return
	}
}

func (s *server) send(addr netip.AddrPort, msg dht.Msg) error {
	data, encodeErr := bencode.Marshal(msg)
	if encodeErr != nil {
		return encodeErr
	}

	sendErr := s.socket.Send(addr, data)
	if sendErr != nil {
		return sendErr
	}

	return nil
}
