# Pure KRPC wire, scrape-bloom, and transaction-correlation parity

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

The second bounded source-only slice adds BEP-33 without adding a client or
runtime:

- the exact 2,048-bit filter, two SHA-1-derived indexes, little-endian 16-bit
  index assembly, and least-significant-bit-first bit placement;
- raw IP-byte insertion, preserving Go's important distinction between a
  four-byte IPv4 and the sixteen-byte `net.IPv4` representation;
- Go's floating `EstimateCount` and the rounded finite `ApproximatedSize` used
  by persistence; an empty filter estimates about half an item and a full
  filter estimates positive infinity; and
- pointer-sensitive `BFsd` (seeders) and `BFpe` (peers) return fields, including
  the difference between omission and a present all-zero 256-byte filter.

The third bounded source-only slice adds only the transaction-correlation core:

- fallible cryptographic two-byte transaction IDs, injectable deterministic
  issuance, a bounded 65,536-attempt collision loop, and distinct source,
  full-space, and collision-exhaustion failures;
- atomic register-before-send typestate: `RegisteredQuery` owns an already
  inserted exact query message and canonical wire bytes, then transfers its
  generation receipt to `PendingTransaction` only after the send succeeds;
- generation-checked cleanup on send failure, cancellation, timeout, registry
  close, task abort, and either typestate guard being dropped, preventing a
  stale guard from deleting a later query that reused the same TID;
- response/error type gating, exact TID lookup, normalized source-address
  gating, and explicit first-wins delivery in that order. Plain IPv4 aliases
  only a zero-scope IPv4-mapped address; Rust preserves a nonzero mapped scope
  fail-closed, while native IPv6 requires equal scope and ignores flowinfo; and
- typed success, remote-error, missing-body, timeout, cancellation, and closed
  outcomes that retain the accepted source address and full response envelope.

`internal/protocol/dht/server/transaction_parity_gen_test.go` exercises the
actual Go `Query`, `send`, `handleResponse`, and `addrMatches` paths. Its fake
socket asserts synchronously inside `Send` that the TID and destination are
already registered, and the checked fixture locks canonical query bytes,
collision retry, address normalization, duplicate delivery, and every terminal
cleanup path to the Rust differential tests.

The fourth bounded source-only slice admits a separate parser for future
untrusted inbound datagrams without adding a socket or receive loop:

- `KrpcMessage::decode_inbound` is a single-pass borrowed cursor with fixed,
  non-caller-configurable limits: 65,507 datagram bytes (the exact production
  Go UDP buffer), eight container levels, and 32,768 visited values;
- the typed top-level, argument, and return dictionaries match Go's struct
  decoder by accepting unsorted and duplicate fields, validating every
  occurrence, and retaining the last successfully decoded value;
- typed dictionary keys, scalar byte strings, signed integers (including the
  excluded BEP-44 sequence fields), IDs, `want` entries, and the compact-node/
  sample custom decoders recursively unwrap an exact singleton list as Go
  does; compact addresses, peer-value entries, and fixed-array bloom filters
  retain Go's actual non-coercing behavior;
- unknown values are syntax-validated and discarded without a generic value
  tree. Unknown dictionaries match Go's interface decoder by requiring strict
  byte-key ordering and uniqueness;
- trailing bytes, truncation, malformed lengths and integers, excessive depth,
  wrong known-field shapes, and missing dictionary values return typed limit,
  syntax, shape, compact-value, or scrape-bloom errors without panics; and
- Go's boolean integer/string/singleton-list coercion, legacy error string,
  error-list extras, optional-field presence, and compact-value behavior are
  locked by a real anacrolix `bencode.Unmarshal` oracle. Deterministic corpus
  mutation and every-prefix truncation tests additionally exercise the Rust
  no-panic boundary.

The existing `KrpcMessage::decode` remains the strict canonical fixture codec;
its acceptance and encoding bytes are unchanged. The inbound parser is the
admitted bounded wire boundary for a future receive loop, not authorization to
dispatch a message or mutate DHT state.

Go accepts more bencode syntax than the pinned `bendy =0.6.1` strict decoder. The
fixture therefore probes unsorted/duplicate dictionary keys, noncanonical
integers, unknown fields, trailing values, missing `t`/`y`, `ro=0`, and the
legacy bare-string error. It also records Go's unusual boolean compatibility:
integers, byte strings, and recursively singleton lists canonicalize to an
integer flag. Rust intentionally rejects unsorted and duplicate
keys at the strict syntax boundary even though Go accepts them. This known
compatibility difference is gated. Both codecs reject non-20-byte IDs. The
oracle also records one intentional shape hardening: Rust rejects non-6/18-byte
compact peer addresses that Go's generic IP decoding accepts. It likewise
records that Go's generic fixed-array decoder pads/truncates non-256-byte
BEP-33 filters while Rust requires the exact protocol width. The bounded
inbound parser deliberately retains these protocol-width checks and rejects
depth beyond eight or datagrams beyond the production receive ceiling; the Go
oracle records each intentional hardening difference.
Unknown keys remain forward-compatible and are ignored like Go; accepted
noncanonical projections re-encode canonically.

BEP-44 values are not admitted in this slice. The oracle distinguishes the Go
argument `v` interface decoder (canonical generic dictionaries) from the Go
return `v` raw-byte decoder, which accepts unsorted/duplicate dictionaries,
noncanonical integers, and even a structurally terminated dictionary with an
unpaired key. Rust validates each form with the corresponding bounded scanner
and returns a typed `Unsupported` error instead of accepting and then losing
signature-sensitive raw bytes.
The excluded BEP-44 `seq`/`cas` integer fields are nevertheless shape-validated
with Go's singleton coercion before being discarded. Their fixtures mark the
intentional projection loss and compare acceptance plus the projected message,
not a Rust re-encode that cannot retain those excluded fields.

The fifth bounded source-only slice adds a fakeable one-datagram receive and
dispatch harness, still without opening a socket or owning a loop:

- each async `receive_one` supplies one fixed 65,507-byte reusable buffer to a
  `DatagramReceiver`; transport failures and impossible overreported lengths
  are distinct typed errors, and a zero-length datagram is a typed no-op;
- malformed datagrams return the bounded inbound error without disturbing a
  pending transaction. Query and unknown-type envelopes are returned as owned
  messages, so a later buffer overwrite cannot mutate them;
- response and error envelopes move directly into `TransactionRegistry`.
  That existing core remains the only transaction-ID, normalized-source,
  first-wins duplicate, closed-registry, and waiter-gone authority; unknown
  message types never consult it; and
- the same-package Go oracle drives the real `server.read` with a deterministic
  fake socket and fake responder observation. Its checked fixture covers zero,
  malformed, canonical/permissive/minimal query shapes, response, error,
  unknown or missing type, wrong source, duplicate, unknown transaction,
  mapped IPv4, and native IPv6 scopes. Go's acceptance of one- and three-byte
  registered transaction IDs is recorded as a deliberate Rust hardening
  delta. A two-datagram Go test and a multi-call Rust test prove message
  ownership across receive-buffer reuse. The registry's existing unit gate
  covers `WaiterGone`, which safe public harness ownership cannot construct:
  dropping the public pending handle also removes its registration by RAII.

The harness dispatches exactly one supplied datagram per call. It does not
spawn handlers, execute a production responder, or provide socket lifecycle,
retry, backpressure, concurrency, or shutdown policy.

The sixth bounded source-only slice ports only the production Go routing
btree's pure 160-bit behavior:

- `RoutingTree` accepts only the crate's exact-width `Id20`, stores an
  empty/leaf/branch trie over `id XOR origin`, and keeps Go's separate
  leading-zero bucket counts. The origin is rejected, a duplicate is detected
  before capacity, and `k = 0` rejects every non-origin insertion;
- capacity and optional splitting follow Go mechanically, including its
  recursive `countCloserThanSubpath` leaf predicate. This predicate is not
  replaced with an intuitive numeric comparison: at origin zero with `k = 1`,
  inserting distance `80..` then `c0..` accepts both, while the reverse order
  rejects `80..`;
- deletion updates the matching leading-zero bucket and collapses empty or
  singleton branch structure exactly. Closest traversal preserves Go's exact
  preferred-subtree then zero-first sibling order and is independent of
  insertion order. The sibling phase is deliberately not re-sorted into global
  numeric XOR order. Limit zero and a limit larger than the tree are bounded
  without allocating from the caller's limit; and
- a checked real-Go `btree.New` trace covers the original production `k = 4`
  split and unsplit matrices at 20 bytes, capacity boundaries `0`, `1`, `4`,
  and `80`, a nonzero origin, deepest-bit branching, rejection/duplicate state
  preservation, missing/repeated drops, branch collapse, capacity reopening,
  drain/reuse, and exact/missing/origin closest targets. Every operation records
  exact count, target membership, and a full ordered membership snapshot.
  Rust-only invariant gates additionally verify cached subtree counts, unique
  leaves, absence of empty/singleton branches, total count, and the complete
  bucket histogram after mutations.

The seventh bounded source-only slice adds only the deterministic current-state
node keyspace that directly consumes `RoutingTree`:

- `NodeTable` is fixed to Go's node capacity `80`, splitting enabled, and
  closest limit `8`. `RoutingNode` carries only an exact `Id20` and owned
  `SocketAddr`; IPv4-mapped IPv6 and native IPv6 numeric scopes remain distinct,
  IPv6 flowinfo is always zero, port zero is retained, and two IDs may share an
  endpoint independently;
- the origin is rejected. A new accepted ID installs its address; an existing
  ID returns `AlreadyExists` and updates its address even when the bucket is
  full; a capacity rejection changes neither routing nor payload state. Drop
  removes both states and a successful drop reopens capacity;
- an exact closest target returns only that node, matching Go's keyspace
  shortcut. An absent target uses the routing tree's exact traversal and
  returns at most eight payloads; and
- a same-package checked oracle runs the real Go `ktable.New`/`PutNode`/
  `DropNode`/`GetClosestNodes` path. It records exact sorted state after every
  operation across an empty table, a nonzero origin, the full 80-entry bucket
  and rejected 81st entry, duplicate address update, drop/reopen, forward and
  reverse insertion, Go's nonnumeric sibling traversal, drain/reuse, shared
  endpoints, IPv4, mapped IPv4, native scoped IPv6, and port zero. Go's invalid
  zero `netip.AddrPort` rejection is recorded as a typed boundary: safe Rust
  `SocketAddr` has no corresponding invalid value.

The eighth bounded source-only slice adds a deliberately partial pure responder
over that node table:

- `PingFindNodeResponder` owns only the exact raw byte methods `ping` and
  `find_node`. It decides ownership before inspecting arguments and returns
  `None` for empty, binary, case-changed, or any other method. A future full
  router owns method-unknown error `204`;
- either owned method with no argument dictionary returns the exact protocol
  error `203`, `missing arguments`. Ping otherwise returns only the local node
  ID. Find-node also requires a present nonzero target, then returns the node
  table's exact singleton-or-closest-eight result. The responder does not
  validate envelope type, transaction ID, sender ID, `want`, or unrelated
  arguments, matching the separation in Go;
- successful compact-node output remains only in Go's IPv4 `nodes` field and
  never invents `nodes6`. IPv4-mapped IPv6 is canonicalized to the exact IPv4
  wire address. A native or scoped native IPv6 result returns the typed local
  `NativeIpv6Node` failure with no partial response or protocol error. This is
  an explicit hardening of Go's later compact-IPv4 encoder panic, and Rust is
  gated never to panic; and
- a same-package oracle invokes the real Go responder over a real Go ktable,
  then wraps its result in a fixed response envelope. Checked fixtures cover
  argument precedence, ping, missing/zero targets, empty/exact/origin/absent
  lookups, the full 80-node table and eight-node ceiling, ignored wants, port
  zero, mapped IPv4, and native/scoped/mixed IPv6. They compare canonical
  response and protocol-error bytes and explicitly record the native-IPv6 Go
  panic/Rust typed-error boundary.

Excluded from this milestone: UDP/TCP sockets and receive loops, message-method
or dispatch validation beyond the two explicitly owned methods, live query
wiring, the combined Kademlia table and hash keyspace,
hash values, the shared reverse-address map, response clocks and node options,
BEP-51 eligibility/scheduling, batch commands, time/random eviction policy,
metrics,
concurrency, full responder routing and wrappers, a BEP-33
scrape client or scheduler, BEP-44 value interpretation/storage/signing,
BEP-9/10 metadata transfer, crawler orchestration,
PostgreSQL, queues, images, and deployment. Unknown and excluded extension
values other than the explicitly unsupported BEP-44 `v` are syntax-validated
and discarded. Neither the pure registry nor the new parser is connected to
production, so no live DHT behavior changes.
