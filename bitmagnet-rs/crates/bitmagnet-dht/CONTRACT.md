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

The fourteenth bounded source-only slice adds a finite supervisor around the
existing fake one-datagram driver, still without opening a socket or creating
an unbounded receive loop:

- `PingFindNodeSupervisor::drive_batch` accepts an exact nonzero `u8` budget,
  retains fully settled outcomes in receive order, and performs at most 255
  sequential steps. Zero-length, decode-rejected, ignored, response, and error
  datagrams each consume one step. A reply send must finish before the next
  receive starts, so sender backpressure bounds both work and memory;
- a biased `tokio::select!` checks the caller's shutdown future first before
  every step. Shutdown cancels at most one in-flight driver future and returns
  the settled prefix. `DatagramReceiver` and `DatagramSender` implementations
  admitted to this supervisor must therefore both be cancellation-safe:
  dropping either pending future may not poison its transport or make a later
  batch unusable. A cancelled send is not settled, is absent from the returned
  prefix, and is never implicitly retried; the transport must make the effect
  of dropping its own pending send future safe and explicit;
- exhausting the exact budget, shutdown, an intact unowned query, and a typed
  driver failure are separate terminal states. Unowned queries stop before a
  later receive and remain a single owned `ReceiveDispatchOutcome::Query` for
  a future full router. Failures retain the ordered prefix plus the complete
  receive error or inseparable prepared reply/local cause and send error; and
- a same-package Go oracle exercises the actual `server.read` boundaries with
  channel barriers. It records three deliberate lifecycle differences: Go's
  full router sends method-unknown `204` where the partial Rust supervisor
  pauses intact; Go logs and continues after a reply-send error where Rust
  stops with the typed error; and Go panics on an active receive error where
  Rust stops with the typed transport failure. No Go goroutine ordering is
  claimed. Rust-only gates prove biased shutdown, receiver and sender
  cancellation-safe reuse, shutdown/drop/task-abort at each pending boundary,
  no implicit retry or double send,
  strict sequential backpressure, batches of one and 255, resume across
  batches, terminal-prefix retention, response/error registry delivery, local
  failure identity, and a concurrent fake client ping/find-node round trip
  ending with no pending transaction.

The fifteenth bounded source-only slice adds the first real transport
primitive, but still does not connect it to a production loop or binary:

- `TokioIpv4UdpTransport::bind` opens exactly one IPv4 Tokio UDP socket and
  caches the actual bound IPv4 address. Consuming it yields exactly one
  non-cloneable `TokioIpv4UdpReceiver` and a cloneable
  `TokioIpv4UdpSender`; both retain the same socket and cached address. Only
  those respective types implement the receive and send traits;
- receive requires the full 65,507-byte protocol buffer before touching the
  socket, so a too-small buffer is rejected without consuming a datagram.
  Tokio documents both `recv_from` and `send_to` as cancellation-safe: a
  dropped pending operation has no datagram effect and either handle remains
  reusable, satisfying the finite supervisor's explicit admission contract;
- send accepts only an IPv4 destination and rejects a payload over 65,507
  bytes before the syscall. Every admitted payload is passed to exactly one
  `send_to`, with no retry. A platform may still reject an admitted payload
  (notably near the maximum) as a typed `SendIo` retaining both the exact
  IPv4 destination and original `io::Error`; no errno or `ErrorKind` is part
  of the portable contract. A short successful syscall is a distinct error;
- stable real-socket parity rows use the actual Go AF_INET socket at zero,
  binary, and deterministic 8,192-byte payloads. Same-package boolean gates
  prove Go's native and mapped IPv6 destinations are both rejected without
  pinning platform error text. Rust gates additionally prove local preflight
  rejection leaves no peer datagram, maximum-size success or preserved
  platform failure, cancellation-safe reuse without duplicate delivery, and
  a bounded real-loopback typed-client-to-supervisor ping ending with an empty
  transaction registry.

The sixteenth bounded source-only slice adds Go's current-state hash keyspace
and its shared reverse-address behavior without adding clocks, synchronization,
or a runtime:

- `KTableCore` composes the existing fixed-capacity node table with a second
  capacity-80 splitting routing tree for info hashes. New hashes may contain no
  peers and are still exact `Found` results. Duplicate puts accumulate peers;
  IP-only identity makes the last port in one or later updates win. A rejected
  hash put changes neither payload nor reverse state, including at capacity;
- missing-hash lookup returns the node table's exact closest-node traversal,
  while a present empty hash never falls through. Public hash peers and reverse
  hash IDs are sorted only to replace Go map iteration nondeterminism with a
  stable projection; membership and last-write semantics remain Go-exact;
- the reverse key ignores peer ports and IPv6 flowinfo, preserves mapped IPv4
  as distinct from plain IPv4, and includes native IPv6 scope. Filtering
  preserves every unknown input's original order, duplicates, port, scope,
  and flowinfo even though only the reverse key decides membership;
- production quirks are deliberately fixture-locked. A newly accepted node is
  absent from the reverse map until an already-existing-ID update. The all-zero
  node ID remains the reverse map's no-peer sentinel, so its entry can be known
  yet cannot be dropped by address. Changing a node's full address or dropping
  a node deletes the entire IP-keyed entry, including hash associations and a
  different node's newer binding. Two node IDs sharing one IP can therefore
  leave the older node alive but unindexed after the newer binding is dropped;
  and
- a same-package oracle runs the real Go `ktable.New`, node/hash put and drop,
  `DropAddr`, `FilterKnownAddrs`, and `GetHashOrClosestNodes` paths. Its checked
  trace includes first-put omission, same-IP port changes, shared-entry
  destruction, zero-ID sentinel behavior, IPv4/mapped/native/scoped identity,
  empty and accumulating hashes, last-port wins, capacity rejection and
  duplicate update, input-preserving filtering, and closest fallback. Full
  node, hash, and reverse state is sorted after every mutation before the Rust
  differential compares it.

The seventeenth bounded source-only slice layers a shared, clocked KTable over
that current-state core without adding a runtime:

- cloneable `KTable` values share one `Arc<RwLock<State>>` and one
  `Arc<dyn KTableClock>`. `new` uses `SystemKTableClock`; `with_clock` admits a
  deterministic fake. Every public table mutation or query takes one short
  synchronous lock, and there is no async work in this layer. The facade
  delegates origin and all counts, reverse lookup, node handle lookup,
  node/hash put and lookup, node/address drop, closest nodes, hash-or-closest,
  and known-address filtering;
- the short-lock guarantee depends on the `KTableClock` safety contract:
  `now` must be monotonic, fast, nonblocking, non-panicking, and non-reentrant
  into any table, clone, or handle sharing that clock. `SystemKTableClock`
  satisfies this contract. A violating clock panic poisons the top-level lock
  and any node lock held at that instant; all later access to affected state
  deliberately panics instead of recovering potentially mismatched indexes.
  This is fail-closed unusability, not transactional rollback: a completed
  command or batch prefix may already have mutated otherwise inaccessible
  state;
- each current node has one generation-specific live handle. Duplicate puts
  update all clones, dropping marks retained clones, and re-adding the same ID
  creates a distinct clean generation. The core remains the current
  node/hash/reverse index; its only new seams are crate-private reverse-ID and
  sorted-hash enumeration helpers;
- node options represent Go operations: `Responded`, last-write-wins BEP-51
  support, and a signed sample response with a supplied next time. Accepted and
  duplicate puts apply options in slice order; rejected puts apply none and
  consume no clock reads. `Responded` reads the clock once. A productive sample
  reads no clock and stores its supplied next time; an empty sample reads once
  at its exact option position and stores `max(next, now) + 5 minutes`;
- sampled, last-discovered, and total counters are `i64`. Accumulation uses
  explicit `wrapping_add`, matching deployed 64-bit Go `int` overflow and
  preserving negative inputs. The empty-response time addition saturates at
  the greatest representable `Instant` within the five-minute interval, so
  neither build profile exposes a time-arithmetic overflow panic;
- oldest eligibility is strictly before the cutoff, with never-responded nodes
  oldest. Candidate eligibility requires support other than `No`, a next time
  strictly before now, and a response strictly more than five seconds old.
  Each visited candidate consumes its own clock read until the positive limit
  is filled, as Go does. A retained dropped handle's standalone predicate still
  ignores dropped state, while table queries enumerate only current handles;
- equal-time oldest order and Go candidate map traversal are undefined, so Rust
  normalizes them by ID. `Option<NonZeroUsize>` expresses uncapped versus
  positive oldest limits, and candidate queries require `NonZeroUsize`; Go's
  surprising zero/negative candidate-limit behavior remains excluded;
- `KTableCommand` covers node put/drop, address drop, and hash put. A void
  `batch_command` holds one write lock across the complete sequence, matching
  Go `BatchCommand`; a barrier-backed concurrency test proves table observers
  cannot acquire a partial batch view;
- `sample_hashes_and_nodes` implements Go's actual policy: take up to 20 hashes,
  then take up to `40 - selected_hashes` live nodes, and return the exact total
  hash count. Go chooses arbitrary map prefixes; Rust deterministically takes
  ID-sorted hash snapshots and generation-live node handles. A same-package Go
  oracle verifies the exact cardinalities plus uniqueness/current-subset
  invariants rather than pinning undefined members or order;
- the same oracle drives real Go node options, signed counter wrapping, drops,
  strict oldest and candidate queries, actual mixed `BatchCommand`, capacity
  rejection, duplicate updates at capacity, and both small and over-20 hash
  samples. Scripted Rust clocks additionally lock clock call order, rejected
  no-consumption, strict time boundaries, and near-limit time saturation; and
- this layer uses only the existing standard library and crate graph. No Cargo
  manifest dependency or `Cargo.lock` change belongs to this milestone.

The eighteenth bounded source-only slice extends only the outbound typed client
with production Go's `get_peers`, BEP-33 scrape, and `sample_infohashes`
adapter projections:

- `DhtClient` is the common five-method client over the existing
  register/send/wait boundary. The original borrowed `PingFindNodeClient<'a,
  I>` remains a true two-method compatibility wrapper with its const
  constructor, and `PingFindNodeClientError` retains its prior variants and
  display text. Existing lifetime annotations, calls, error matches, and error
  rendering therefore remain compatible while the wrapper delegates the
  shared ping/find-node behavior. This slice adds no socket, receive loop, rate
  limiter, retry, scheduler, responder ownership, or table mutation;
- ordinary peer lookup sends raw method `get_peers` with only local `id` and
  `info_hash`. Scrape uses that same method and arguments plus the exact signed
  integer `scrape=1`; it does not invent a `scrape` method, `want`, or `noseed`.
  Sampling sends raw method `sample_infohashes` with only local `id` and
  `target`. Zero info-hash and target values reach the codec and are omitted
  there exactly like Go rather than being rejected by the client;
- peer lookup projects only responder ID, ordered peer values, and ordered
  IPv4 `nodes`. Scrape projects those fields and maps `BFpe` to the peer filter
  and `BFsd` to the seeder filter. Sampling projects responder ID, signed
  64-bit `num` and `interval`, ordered samples, and ordered IPv4 `nodes`.
  Duplicates and port boundaries are retained. `nodes6`, token, and all valid
  unrelated extension fields are ignored after the complete return dictionary
  has passed the existing inbound validation boundary;
- absent and present-empty peer/node collections both become owned empty
  vectors. Sampling deliberately retains the pointer-sensitive distinction:
  absent `samples` is `None`, while an advertised zero-length field is
  `Some(empty)`. Missing `num` and `interval` are successful signed zeroes;
  zero, negative, and both signed extremes are not range-validated against
  sample count or scheduling policy;
- scrape alone adds a post-transaction semantic check. Both bloom fields must
  be present, but two present all-zero 256-byte filters are valid. Missing one
  or both returns `MissingScrapeBloomFilters`, retaining the accepted source,
  complete response envelope, and separate missing-peer/missing-seeder flags.
  The pending transaction has already completed and is removed before this
  semantic error is returned. Transport, remote-error, and missing-envelope
  outcomes still take precedence;
- a checked client-package Go oracle invokes the actual unexported
  `serverAdapter` through the sealed scripted-server seam. Its fixture locks
  exact destination, method, typed arguments, canonical query bytes, zero-ID
  omission, nil/empty presence, ordered and duplicate address/sample
  projection, ignored valid fields, signed boundaries, distinct bloom mapping,
  the three missing-bloom combinations, pointer-identical query errors,
  pre-cancelled calls, and Go's typed-nil error result. The checked runtime
  metadata requires a 64-bit Go `int`, making the `int64`-to-`int` projection
  deterministic for zero, negative, minimum, and maximum fixture values;
- Rust-only no-socket gates additionally lock response delivery during sender
  backpressure, transport-error precedence over an already buffered response,
  unpolled and polled future drop, task abort during send and wait, timeout
  start after send, exact zero timeout, wrong-source timeout, registry close,
  remote and missing-body errors, and two mixed methods delivered out of order
  through one registry. Every terminal path ends with no pending transaction;
  and
- a raw receive/client composition proves that malformed `nodes6` remains a
  decode rejection even for peer lookup, which does not project that field.
  The rejected datagram does not consume the pending transaction; its client
  waits until the configured timeout and then cleans up. This preserves the
  general rule that method-level projection never bypasses validation of known
  return fields.

The real-Go adapter oracle is intentionally not a socket oracle. Its
pre-cancelled context proves that the adapter invokes `Server.Query`; it does
not claim that production's outer IP-keyed limiter sends a datagram. Rust uses
future drop or task abort for caller cancellation and exposes registry closure
as a typed result. The existing inbound hardenings also remain in force:
non-6/18-byte peer addresses, non-256-byte blooms, over-limit structures, and
unsupported BEP-44 `v` can make a response time out even when the selected
adapter method would not project that field. None of these deliberate deltas
is relaxed by the typed client.

The nineteenth bounded source-only slice extends only the pure responder over
the shared KTable with `get_peers`, `announce_peer`, and
`sample_infohashes`:

- `DhtResponder` is the full five-method pure router. It owns exact raw `ping`,
  `find_node`, `get_peers`, `announce_peer`, and `sample_infohashes`; every other
  raw method receives Go's protocol error `204`, `method Unknown`. Global
  argument presence is checked before method dispatch, so a missing dictionary
  receives `203`, `missing arguments` even for an unknown method. Peer lookup
  and announce additionally require a present nonzero info hash; find-node
  requires a present nonzero target; sampling deliberately does not require or
  inspect its target. Envelope type, transaction ID, requester ID, `want`,
  `noseed`, scrape, and unrelated arguments are not revalidated;
- peer lookup performs exactly one hash-or-closest query. A found hash returns
  a present peer-value list even when empty, while an absent hash leaves values
  absent and returns the existing closest-node projection. Every successful
  lookup returns a present announce token. Rust preserves KTable peer and
  closest-node order; the KTable's deterministic ID order is a normalization
  of production Go map iteration, so exact members and order from a populated
  real Go map are deliberately not claimed;
- announce validation is ordered: nonzero info hash, exact token, then one
  synchronous hash put containing the source IP and selected port. The token is
  the exact lowercase MD5 hex of Go's concatenated secret, local ID, info hash,
  requester ID, and textual source IP. It binds neither source port nor time,
  has no rotation or expiry in this slice, and is compared as an exact byte
  string. An absent explicit port falls back to the datagram source port,
  `implied_port` always selects that source port, and every explicit signed
  value otherwise wraps modulo 65,536 as Go's 64-bit `int` to `uint16` cast;
- announce returns success after attempting the void put even when KTable
  capacity rejects a new hash. Invalid arguments or token leave the table
  unchanged. A successful put retains the existing KTable semantics: peers are
  identified by IP, the last port wins, and a later deterministic projection
  may sort the resulting membership without changing it;
- sampling performs exactly one read-only KTable sample. It always returns
  present `samples`, `num`, and `interval` fields, including a present empty
  sample. The KTable chooses up to 20 hashes and enough nodes to target 40
  combined results; its signed interval is returned unchanged and its
  nonnegative native total is projected to `i64`. Signed fixture extremes are
  covered at the responder's injected sample-result seam without changing KTable
  storage or scheduling semantics;
- compact output remains in `nodes`, never `nodes6`. Mapped IPv4 is accepted as
  IPv4. Native or scoped native IPv6 in a node result returns the existing typed
  local hardening instead of reproducing Go's compact-IPv4 encoder panic. Peer
  values may still contain either IPv4 or IPv6 because they use the compact
  peer-address representation;
- the narrow synchronous `DhtResponderTable` trait mirrors only the five table
  operations used by Go's injected responder dependency. `KTable` implements
  it, and the production constructor clones that shared table. The explicit
  backend-and-secret constructor admits deterministic oracle replay, including
  duplicate ordered results and signed totals that a real KTable cannot
  represent, without widening KTable itself;
- a same-package checked Go oracle drives the production responder with injected,
  deterministic table results and records exact partial returns, protocol
  errors, field presence, token bytes and input sensitivity, port selection,
  ignored arguments, ordered table call traces, and before/after table state.
  All 40 checked rows explicitly require no normalization; three native/scoped
  IPv6 rows additionally lock Go's zone-losing return projection and Rust's
  typed local hardening. The Rust integration replays every row. A seven-party
  barrier releases two lookup, two sample, and two announce workers immediately
  before their first responder call; the cloned-KTable concurrency gate then
  requires every worker to finish and verifies final hash/peer/count invariants;
  and
- the original `PingFindNodeResponder`, `PingFindNodeClient`, and
  `PingFindNodeClientError` remain separate compatibility surfaces. Their
  existing exhaustive public compile and behavioral gates remain authoritative;
  the full responder does not compose or borrow the legacy responder, and this
  slice neither widens legacy method ownership nor its error enums.

This pure responder is not connected to dispatch, reply construction, a UDP
transport, or production runtime wiring. In particular, the slice makes no
claim about Go's unusual `y=r` server-error envelope, limiter precedence,
metrics, logging, asynchronous node discovery, handler cancellation or
timeouts, send failure and retry policy, or mutation rollback after a failed
send. BEP-33 scrape arguments remain ignored exactly as in the current Go core;
no bloom generation or scrape scheduler is added.

Excluded from this milestone: production socket construction or runtime
wiring, external-network traffic, and unbounded receive loops; server envelope
or dispatch validation beyond the five exact pure-responder methods, live query
wiring, hash removal/expiry, peer expiry, hash options, discovered-at clocks,
drop-reason payloads, time/random eviction policy, metrics,
concurrent handler fan-out, send retry/timeout/queueing policy, socket lifecycle,
production looping or spawning policy, logging and runtime wiring,
responder server/runtime wrappers, a BEP-33 scrape scheduler,
BEP-44 value interpretation/storage/signing,
BEP-9/10 metadata transfer, crawler orchestration,
PostgreSQL, queues, images, and deployment. Unknown and excluded extension
values other than the explicitly unsupported BEP-44 `v` are syntax-validated
and discarded. Neither the pure registry nor the new parser is connected to
production, so no live DHT behavior changes.

The twentieth bounded source-only slice composes the full pure responder with
one offline response envelope and one fakeable datagram send:

- `DhtDispatcher` owns one `DhtResponder` and therefore owns all five exact raw
  methods plus the method-unknown response. It does not compose or call the
  partial ping/find-node dispatcher. Each call invokes the responder at most
  once, so an `announce_peer` request can perform at most one table mutation;
- dispatch assumes that an outer server has already classified the envelope as
  a query. Like Go's `handleQuery`, it deliberately ignores the supplied `y`
  value and unrelated mixed request fields. Calling it directly on an
  unclassified response or error envelope is outside this contract and could
  execute a valid embedded `announce_peer`; receive routing is not added here;
- every reply is rebuilt from scratch at the exact supplied `SocketAddr`. It
  echoes the transaction byte string without a UTF-8 or two-byte restriction,
  emits `y=r`, clears query, arguments, observed address, read-only, client ID,
  and all other request bodies, and contains exactly one of `r` or `e`. Go's
  unusual protocol-error shape remains `y=r` with `e`, not `y=e`;
- responder protocol errors become peer-visible error replies and discard the
  responder's prospective partial return. The native-IPv6 projection failure
  remains a typed local cause paired with a clean generic `202`, `server error`
  reply. Actual Go panics while encoding that native compact-IPv4 result and
  sends nothing; the Rust `202` is the existing explicit hardening, not a claim
  that Go emitted those fallback bytes;
- the prepared-reply helper borrows the reply, fully encodes it, then constructs
  and awaits exactly one `DatagramSender` future. Encoding failure creates no
  sender future, while a transport failure preserves the sender's exact typed
  value. Destination normalization, datagram size admission, retry, timeout,
  queueing, and cancellation safety remain sender or caller policy. In
  particular, an oversized but encodable reply reaches the sender for its own
  rejection;
- dispatch is synchronous. A valid announce mutation is complete before the
  prepared send is created and is never rolled back after an unpolled send,
  encode failure, pending-send drop, task abort, sender-construction or
  sender-poll panic, or typed transport failure. Panics unwind rather than
  becoming a send error, and neither cancellation nor failure triggers a retry;
- the exact fake-seam destination retains IPv4-mapped form, native IPv6 scope,
  and Rust-visible flowinfo. This is not a production IPv6 transport claim:
  the current Tokio production adapter is IPv4-only. The responder separately
  retains its established token and announce-address rules;
- a checked same-package Go oracle exercises 25 actual `handleQuery` paths plus
  one direct `server.send` encoder-error path. Rust exactly replays the five
  concrete-responder-compatible envelope rows for ping, populated find-node,
  populated and empty sampling, and method-unknown, including empty, binary,
  and 257-byte transaction IDs and exact IPv4-mapped, native, and scoped
  destinations. Arbitrary scripted peer tokens, non-production present-empty
  find-node output, injected error provenance, context state, logging and
  swallowed send failures, and Go panics remain explicitly classified as outer
  runtime evidence rather than Rust behavior. The direct-send row records Go's
  returned `*bencode.MarshalTypeError` for unsupported argument
  `dht.MsgArgs.V` metadata and zero socket calls. `handleQuery` clears `A`
  before sending, so this direct-send encode error is not a swallowed
  `handleQuery` outcome;
- independently, Rust composes all 40 production-responder oracle rows through
  fresh full dispatchers and scripted tables, checking canonical wire,
  destination, field exclusivity, arbitrary transaction widths, exact table
  calls and effects, partial-return discard, and protocol/local outcomes.
  Rust-only deterministic lifecycle gates cover backpressure, cancellation,
  abort, panic, exact one-call behavior, sender-owned oversize rejection,
  retained local causes, and mutation-without-rollback; and
- all legacy ping/find-node responder, dispatcher, reply, send, driver, client,
  and error surfaces remain unchanged. Exhaustive enum matches and const
  constructor gates prevent the new full types from widening or aliasing those
  compatibility contracts.

This slice still adds no receive call, receive loop, combined driver,
supervisor, limiter, handler task, responder context or timeout, logger,
metrics, node-discovery wrapper, production socket wiring, shutdown policy, or
external-network behavior. Go's outer `handleQuery` logs and swallows a send
failure, whereas the bounded Rust helper returns it; this slice compares that
helper to Go's lower `server.send` boundary and makes no runtime error-policy
claim. `DatagramSender::Error` remains value-preserving without requiring a
standard error source, so encode errors have their typed wire-error source
while transport source chaining remains implementation-independent.
