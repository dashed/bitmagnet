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

The ninth bounded source-only slice composes that partial responder into an
offline dispatch envelope without sending it:

- `PingFindNodeDispatcher` still owns only the exact raw `ping` and
  `find_node` methods. Ownership is resolved before arguments, and envelope
  type is deliberately not revalidated. Every other method returns `None` so
  the unchanged request can be offered to a future router;
- a successful result becomes an exact `y=r`, `r=<return>` envelope. Protocol
  error `203` becomes a normal `y=r`, `e=<error>` reply. This unusual response
  type on an error is current Go `handleQuery` behavior and is preserved;
- a local `NativeIpv6Node` failure is retained in a `LocalFailure` outcome,
  while the peer-visible reply is exactly Go's generic `202`, `server error`
  envelope. No compact nodes or other partial response fields leak into that
  reply, and neither dispatch nor encoding panics;
- the response echoes transaction bytes at any width, including empty and
  binary values. All other request-only envelope fields are cleared. The reply
  destination is the exact supplied `SocketAddr`, including mapped form,
  native scope, and even caller-visible IPv6 flowinfo; this pure layer does not
  normalize or open a socket; and
- a same-package Go oracle invokes the actual server `handleQuery`
  synchronously with a scripted responder and capture-only fake socket. It
  locks success, direct/wrapped protocol errors, the wrapped-pointer and
  generic-error `202` boundary, raw transaction widths, exact IPv4/mapped/
  scoped destinations, mixed request fields, and the native-IPv6 Go panic plus
  Go-generated fallback bytes. The Rust differential also replays all fourteen
  prior real-responder fixtures through the dispatcher and proves all three
  native cases retain their cause, emit exact `202`, and leave the table
  unchanged.

The tenth bounded source-only slice adds only a fakeable prepared-reply send
seam:

- `send_ping_find_node_reply` borrows the already composed reply, encodes its
  complete datagram before creating a sender future, then awaits exactly one
  `DatagramSender::send` call. An encode error makes zero sender calls; a
  transport error is returned as the sender's original typed value. There is
  no implicit retry;
- the exact reply destination is passed through without normalization,
  including IPv4-mapped form, native IPv6 scope, and Rust-visible flowinfo.
  The owned encoded buffer remains alive for the entire borrowed send future;
- the async boundary preserves backpressure: the helper cannot complete while
  the sender future is pending. It adds no timeout, queue, task, or detached
  work; and
- borrowing permits a caller to send the peer-visible generic `202` reply from
  a `LocalFailure` while retaining the typed `NativeIpv6Node` cause. A
  same-package Go oracle calls the actual server `send` method with capture and
  failing sockets to lock canonical bytes, destinations, exact transport-error
  propagation, and the native/mixed compact-node panic before any socket call.
  Rust turns those encoder panics into typed zero-call encode errors and
  composes the helper across the prior dispatch and responder matrices.

The eleventh bounded source-only slice joins the existing offline boundaries
for exactly one datagram:

- `PingFindNodeDriver::drive_one` awaits exactly one `ReceiveDispatcher`
  receive. Only its owned `Query` outcome is offered to the partial
  `PingFindNodeDispatcher`; response and error delivery, zero-length,
  decode-rejected, ignored, and typed receive failures never reach the
  responder or sender;
- an unowned query is returned intact as a typed `NoReply` for a future router.
  An owned query produces the already specified response, protocol-error, or
  local-failure envelope and awaits exactly one `DatagramSender` call;
- successful sends and send/encode failures return the original atomic
  `PingFindNodeDispatchOutcome`, so a peer reply can never be mismatched with
  an optional local cause. Failures also retain the exact underlying send
  error. The helper inherits exact destination/wire,
  encode-before-send, and no-retry behavior from the sender seam; and
- deterministic fake receiver/sender gates prove call counts, receive-before-
  send ordering, and pending sender backpressure without sleeps. A same-package
  Go oracle exercises the actual `server.read` to `handleQuery`/`handleResponse`
  to `server.send` path with channel-observed completion. The bounded partial
  router intentionally leaves unowned queries unsent even though Go's full
  router returns method-unknown `204`.

The twelfth bounded source-only slice joins outbound transaction registration
to the existing fakeable datagram sender:

- `register_and_send_query` atomically registers one raw query and its expected
  source before it fully encodes the canonical query. Only then does it create
  and await exactly one `DatagramSender::send` future. The sender receives the
  caller's exact address, including mapped form, native scope, and Rust-visible
  IPv6 flowinfo; response correlation alone uses the registry's established
  normalization;
- `RegisteredQuery` remains the sole owner of the live generation guard until
  the send succeeds and becomes `PendingTransaction`. Registration, encoding,
  and transport failures are distinct typed outcomes. Error returns, future
  cancellation or drop, and unwinding all remove only that generation's
  registration, while the exact transport error value is preserved;
- a response delivered during sender backpressure stays buffered in the live
  `Delivered` registration and can be consumed after the send succeeds. If the
  same send then fails, the transport failure wins and the buffered response
  and registration are discarded, matching Go `server.Query`; and
- a same-package Go oracle invokes the actual `server.Query` method with a
  scripted issuer and fake socket. It locks registration-before-`Send`, exact
  raw query bytes and arguments, collision handling, destinations, response-
  during-send behavior, send-error precedence, single-call behavior, and
  terminal pending-map cleanup. Rust-only gates add deterministic async sender
  backpressure, cancellation, abort, panic, bounded registration failures,
  and the outbound-address versus response-normalization boundary.

The thirteenth bounded source-only slice adds only the production adapter's
typed `ping` and `find_node` client projection:

- `PingFindNodeClient` builds the exact raw `ping` arguments (local ID only)
  and `find_node` arguments (local ID plus target), then composes
  `register_and_send_query` with the existing `PendingTransaction::wait`.
  A zero target follows Go's non-pointer zero-value encoding and is omitted
  from canonical wire. The fixed query timeout begins only after the one
  datagram send succeeds, so sender backpressure is deliberately outside it;
- registration, encoding, and the original typed transport failure remain
  nested under `QuerySend`, with an explicit standard error source. Remote
  KRPC errors, response/error envelopes missing their corresponding body,
  timeout, and registry closure are distinct typed outcomes. Missing-body and
  remote-error outcomes retain the accepted source and complete envelope;
- ping projects only the responding ID. Find-node projects the responding ID
  and the ordered `r.nodes` entries, retaining duplicates and port boundaries.
  Absent and explicitly empty `nodes` both become an empty vector, while
  `nodes6`, peer values, tokens, scrape/BEP-51 data, and other extensions are
  ignored exactly like Go's `serverAdapter`. Compact response addresses are
  owned `SocketAddr` values with IPv6 flowinfo and scope fixed to zero. Neither
  method validates a response ID or mutates a node table; and
- a client-package checked oracle invokes the actual unexported Go
  `serverAdapter` through a scripted embedded `server.Server`. It locks exact
  destination, method, arguments, canonical query wire, ID-only ping
  projection, ordered/duplicate node projection, nil/empty nodes, ignored
  IPv6 and extension fields, zero target omission, and pointer-identical query
  errors with Go's zero result. Rust no-socket gates additionally cover a
  response delivered during send, transport failure after buffered delivery,
  blocked-send timeout ordering, zero timeout under paused time, wrong-source
  timeout, remote and missing-body errors, registry close, task abort during
  send or wait, and exact outbound versus normalized response addresses.

The typed Rust boundary intentionally differs where Go depends on
`context.Context`: caller cancellation is task/future drop or abort rather than
a returned context error, while the client owns only its fixed post-send
timeout. Go enters `server.Query` and performs its send before observing an
already-cancelled context; an unpolled Rust future performs neither
registration nor send. Rust additionally exposes registry closure as a
first-class outcome. Go's missing-error-body path assigns a typed nil
`*dht.Error` to `error`, yielding a non-nil interface whose dynamic pointer is
nil; Rust instead returns the explicit `MissingErrorBody` variant with the
accepted envelope. The Go oracle locks both of these differences. A response
and timeout becoming ready in the same scheduler turn has nondeterministic Go
`select` ordering and is deliberately excluded; tests establish only strict
before/after boundaries.
Safe `SocketAddr` cannot represent Go's invalid zero `netip.AddrPort`, and it
can retain outbound IPv6 flowinfo that Go's address type cannot express. Rust
also deliberately collapses nil and non-nil empty result node slices to one
owned empty vector; the checked fixture retains the two Go inputs and proves
their equal client projection.

Excluded from this milestone: UDP/TCP sockets and receive loops, message-method
or dispatch validation beyond the two explicitly owned methods, live query
wiring and production transport adapters, the combined Kademlia table and hash keyspace,
hash values, the shared reverse-address map, response clocks and node options,
BEP-51 eligibility/scheduling, batch commands, time/random eviction policy,
metrics,
concurrency, send retry/timeout/queueing/backpressure policy, socket lifecycle,
looping or spawning policy, logging and runtime wiring,
full responder routing and runtime wrappers, a BEP-33
scrape client or scheduler, BEP-44 value interpretation/storage/signing,
BEP-9/10 metadata transfer, crawler orchestration,
PostgreSQL, queues, images, and deployment. Unknown and excluded extension
values other than the explicitly unsupported BEP-44 `v` are syntax-validated
and discarded. Neither the pure registry nor the new parser is connected to
production, so no live DHT behavior changes.
