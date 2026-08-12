# First DHT milestone: pure KRPC wire parity

This crate owns only the offline byte boundary needed before a Rust DHT runtime
can be designed. Go remains the production implementation and the source of
truth. `internal/parity/dht_krpc_gen_test.go` runs the real Go bencode codec and
writes the checked fixture consumed by the Rust differential test.

The bounded contract includes:

- raw byte strings for transaction ID, message type, query, token, client ID,
  and error message; no UTF-8 assumption enters the wire;
- the Go KRPC envelope and its arguments/return projection, without imposing a
  later server's method/argument semantic validation;
- IPv4/IPv6 compact peer addresses, 26/38-byte compact nodes, and concatenated
  20-byte BEP-51 samples;
- exact canonical bencode bytes for typed fixtures; and
- Go-preserving presence: explicitly empty response tokens, samples, `want`,
  peer-value lists, and compact-node lists remain different from absent fields
  on encode. Go's custom compact-node decoder alone collapses an empty compact
  byte string back to nil; the fixture records that lossy canonical re-encode
  and Rust matches it.

Go accepts more bencode syntax than the pinned `bendy =0.6.1` decoder. The
fixture therefore probes unsorted/duplicate dictionary keys, noncanonical
integers, unknown fields, trailing values, missing `t`/`y`, `ro=0`, and the
legacy bare-string error. It also records Go's unusual boolean compatibility:
integers, byte strings, and recursively singleton lists canonicalize to an
integer flag. Rust intentionally rejects unsorted and duplicate
keys at the strict syntax boundary even though Go accepts them. This known
compatibility difference is gated. Both codecs reject non-20-byte IDs. The
oracle also records one intentional shape hardening: Rust rejects non-6/18-byte
compact peer addresses that Go's generic IP decoding accepts. These known
differences mean this crate is **not yet admitted as an inbound live-network codec**.
Unknown keys remain forward-compatible and are ignored like Go; accepted
noncanonical projections re-encode canonically.

Excluded from this milestone: UDP/TCP sockets, transaction-ID issuance and
address correlation, timeouts, routing tables, responders, BEP-33 scrape bloom
filters, BEP-44 mutable/immutable storage and signing, BEP-51 scheduling,
BEP-9/10 metadata transfer, crawler orchestration, PostgreSQL, queues, images,
and deployment. No live DHT behavior changes.
