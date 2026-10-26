package server

import (
	"crypto/rand"
)

type IDIssuer interface {
	Issue() string
}

// cryptoIDIssuer issues unpredictable 2-byte KRPC transaction IDs using a
// cryptographically secure random source. Unpredictable transaction IDs make
// off-path response injection infeasible: an attacker cannot guess the "t"
// value to forge a matching response. Uniqueness among in-flight queries is
// enforced separately by the collision-retry loop in server.Query; that loop
// terminates in practice because the 2-byte TID space (65536 values) is vastly
// larger than the number of realistically concurrent in-flight queries, so a
// fresh draw almost always misses the in-flight set on the first attempt.
type cryptoIDIssuer struct{}

func (cryptoIDIssuer) Issue() string {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand.Read should never fail; if it does the system entropy
		// source is broken and we cannot safely continue. Panic, consistent
		// with the socket read-error handling in server.read.
		panic("dht: could not read random transaction ID: " + err.Error())
	}

	return string(b[:])
}
