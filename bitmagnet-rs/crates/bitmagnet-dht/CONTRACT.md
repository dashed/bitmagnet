# KRPC, scrape-bloom, transaction, and owned-runtime parity

This crate began with the offline byte boundary needed to design a Rust DHT
runtime and now also owns its initial Tokio IPv4 runtime composition. Go remains
the production implementation and the source of truth.
`internal/parity/dht_krpc_gen_test.go` runs the real Go bencode codec and writes
the checked fixture consumed by the Rust differential test.

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

The twenty-first bounded source-only slice connects the existing receive
classifier, full dispatcher, and one-send helper into a finite full-DHT driver
and supervisor:

- `DhtDriver::from_dispatcher` owns one receiver, the caller's shared
  transaction registry, one sender, and an already-configured
  `DhtDispatcher`. `drive_one` performs exactly one receive and at most one
  send. Queries reach the full responder, so all five exact methods plus
  method-unknown and missing-arguments replies are owned; responses and KRPC
  errors are delivered to the registry, while ignored, rejected, zero-length,
  and other non-query outcomes return without responder table effects or a
  send;
- one successful reply returns the complete `DhtDispatchOutcome`, preserving a
  typed local responder cause alongside its peer-visible fallback. Receive
  failures retain the exact `ReceiveDispatchError`; send failures retain both
  the exact prepared dispatch and the nested `DhtSendError`. The driver's
  standard `Error` implementation exposes that immediate typed boundary when
  the caller's receiver and sender errors implement `Error`; transport values
  remain explicit enum payloads rather than asserted transitive sources.
  Constructing the driver requires neither cloneable transports nor cloneable
  table backends;
- backpressure is inherited by awaiting the single sender future before
  another receive can begin. Dropping or aborting that future performs no
  retry. An accepted announce mutates synchronously during dispatch, before
  sender construction, and is not rolled back by a typed send failure,
  backpressure, future drop, supervisor shutdown, task abort, or sender
  construction/poll panic. The receiver and sender cancellation gates require
  their scripted handles to remain reusable after a dropped in-flight future;
- `DhtSupervisor::from_driver` wraps that exact driver without adding another
  routing or transport layer. `drive_batch` accepts a nonzero one-byte budget,
  runs sequentially, and returns only `BudgetExhausted`, biased `Shutdown`, or
  `Failed`. Every completed reply or no-reply outcome consumes one unit; a
  failure does not enter the retained prefix. Budgets 1 and 255, repeated
  resumed batches, an unknown method followed by later work, sender
  backpressure, exact failure prefixes, and shutdown while receiving and
  sending are locked by deterministic lifecycle gates;
- a checked 64-bit Go runtime-bridge oracle covers 12 production
  read/handle/respond/send observations: ping, populated find-node, peer hit
  and miss, successful and failed announce, populated sampling, unknown and
  missing-argument protocol errors, duplicate unsorted query decoding,
  receive failure, and receive-length overreport. Fully typed Rust fixture
  structs reject unknown fields at every object level, fix the row count and
  ordered ID set, and exhaustively consume every serialized field. Ten
  datagram rows replay through a fresh `DhtDriver` and scripted table with
  exact destination, canonical response bytes, call trace, send-time state,
  final state, and transport-error identity; the two receive failures replay
  as their exact typed Rust boundaries;
- the bridge also records deliberate outer-loop differences. Production Go
  enters a second receive after each handled datagram, logs and swallows its
  one scripted send failure, and panics on receive error or an overreported
  length. This finite Rust driver performs only one receive, returns send
  failure, and turns both receive conditions into typed errors. Those are
  explicit hardenings, not wire-parity claims. Go's concurrent handler
  goroutines, context timeout, log policy, and unbounded continuation remain
  outer runtime evidence; and
- one bounded Tokio IPv4 loopback composes the production adapter with the full
  supervisor for a ping request and exact response routing. The original
  `PingFindNodeDriver`, `PingFindNodeSupervisor`, their outcomes/errors/exits,
  and every legacy responder, dispatcher, send, and client surface remain
  distinct and exhaustively compile-gated; the full supervisor has no legacy
  `UnownedQuery` exit because the full dispatcher owns unknown methods.

This slice still does not install a production DHT runtime. It adds no socket
binding policy, receive-loop spawn, concurrent handler fan-out, limiter,
responder timeout, retry, send queue, metrics, logging, node discovery,
shutdown signal owner, task supervision, crawler orchestration, external
network traffic, or deployment wiring. The batch is caller-bounded and
sequential, and its shutdown future and transport lifetime remain caller
owned.

The twenty-second bounded source-only slice adds the two production DHT rate
policies as independent, cloneable values, without wiring either policy into a
client, responder, driver, supervisor, or socket runtime:

- `DhtInboundRateLimiter::new` and `Default` construct an initially full
  per-IP one-token-per-second bucket with burst ten, bounded to 1,000 keys with
  a fixed 20-second lifetime, followed by one shared 50-per-second bucket with
  burst twenty. `allow(SocketAddr)` consumes the per-IP token before consulting
  the global bucket, so a later global denial retains the Go production
  wrapper's consumption; a per-IP denial short-circuits without consuming a
  global token;
- `DhtOutboundRateLimiter::new` and `Default` construct initially full
  per-IP one-token-per-second buckets with burst four and the same 1,000-key,
  fixed-20-second bound. `wait(SocketAddr)` is the no-deadline/no-cancellation
  convenience. `wait_until(SocketAddr, Instant)` and `wait_with(SocketAddr,
  Option<Instant>, Future<Output = ()>)` return the exhaustive
  `DhtRateLimitWaitError::{Cancelled, WouldExceedDeadline}` boundary. All three
  reserve in bucket lock-acquisition order and await the exact action time
  without holding either cache or bucket mutex. Clones share the same cache,
  buckets, reservations, and global inbound state;
- keys discard the socket port and IPv6 flow information. IPv4, IPv4-mapped
  IPv6, and native IPv6 remain distinct; nonzero numeric IPv6 scope IDs use
  Go's exact `%N` suffix, so `fe80::1%7` and `fe80::1%8` are distinct public
  Rust keys as well as distinct real-Go oracle observations;
- cache hits refresh capacity recency but not expiry. Capacity replacement is
  least-recently-used. An entry remains valid at its exact insertion deadline
  and is replaced on the first access strictly after 20 seconds with a full
  bucket. Rust performs this expiry lazily during access and owns no cleanup
  task. Go's expirable LRU uses `time.Now` and a non-injectable background
  reaper; its positive-TTL oracle therefore locks only immediate pre-expiry
  identity. The deterministic Rust boundary and reset are independently
  locked with Tokio paused time;
- Rust's generic token-to-duration helper saturates non-finite or
  greater-than-or-equal-to-MaxInt64 nanosecond results. Go's `x/time/rate`
  guard uses strict greater-than before its float-to-`time.Duration`
  conversion, so behavior at that astronomical exact float boundary is not a
  parity claim. The fixed production one-token intervals cannot reach this
  delta;
- dropping an uncompleted outbound wait cancels only the reservation tokens
  that Go's `x/time/rate` calculation permits. Dropping or aborting the latest
  wait removes its extra scheduled delay but does not manufacture an immediate
  token; a replacement still acts at the original next-token instant. An
  older canceled reservation cannot over-restore across a later reservation,
  and a completed wait is committed. `wait_with` polls cancellation first, so
  a pre-ready cancellation wins before cache lookup or deadline validation and
  a post-reservation cancellation ready alongside admission wins the biased
  tie; cancellation while sleeping rolls back the eligible reservation. An
  expired deadline and a reservation strictly after a future deadline return
  `WouldExceedDeadline` without reserving, while an action exactly at the
  deadline is accepted. Rust preserves these Go outcome classes as a typed
  local error instead of exposing Go context and string errors;
- poisoned internal cache and bucket mutexes recover the protected state
  instead of cascading another panic. Source-unit poison gates and public
  high-contention clone gates cover that behavior. Concurrent outbound waits
  lock reservation order, one completion per one-second refill, task abort,
  and reuse; and
- four fully typed, unknown-field-denying real-Go JSONL oracles are consumed
  with fixed row counts and ordered IDs. The four generic token-bucket rows and
  five generic keyed-LRU rows are exhaustive primitive evidence because the
  Rust public API intentionally exposes only fixed production policies. Three
  responder-limiter rows lock production defaults, per-IP-before-global
  ordering, exact textual keys, denial, and delegate effects. Seven query-
  limiter rows lock wait-before-delegate ordering, exact wait/delegate error
  identity, decided cancellation/deadline cases, textual keys, and outbound
  production defaults. The Rust public-default lifecycle gates separately
  exercise exact bursts, refill times, ports/flow ignored, mapped/native/scoped
  address identities, 1,000-key eviction, strict TTL, typed pre-cancellation,
  expired and insufficient deadlines, in-flight cancellation rollback, simple
  future drop, abort, clone sharing, and contention. Go responder/query
  delegate effects remain outer composition evidence because this slice
  exports policy primitives rather than wrapper types.

No existing responder, client, query-send, driver, supervisor, transport,
transaction, KTable, or legacy ping/find-node type is wrapped or changed by
this slice, and it adds no Cargo dependency. In particular, admission is not
yet enforced on inbound dispatch and outbound waits are not yet consulted by
`DhtClient` or `register_and_send_query`. This slice adds no continuous receive
loop, responder timeout, metrics, health, logging, discovery, crawler
scheduling, socket binding, configuration, deployment wiring, or external
network behavior.

The twenty-third slice adds the first owned Tokio DHT runtime composition:

- `DhtRuntimeConfig::default` locks Go's `0.0.0.0:3334` bind policy,
  four-second post-send query timeout, and responder BEP-51 interval ten.
  `DhtRuntime::start` generates a cryptographic 20-byte node ID whose final
  eight bytes are `-BM0001-`, creates the shared production `KTable`, responder,
  transaction registry, dispatcher, driver, and supervisor, eagerly binds one
  IPv4 UDP socket, and spawns one owned receive task;
- the task repeatedly drives finite 255-datagram supervisor batches but owns
  the overall continuous lifetime. Each batch remains sequential and retains
  its typed receive and reply-send failure. Eager typed bind and task exit are
  deliberate hardenings over Go's lazy server construction, detached read
  goroutine, swallowed reply-send errors, and live receive panic;
- `DhtRuntimeClient` mirrors all five typed outbound query methods. Clones
  share the transaction registry and production outbound per-IP limiter, and
  every query waits for limiter admission before registration and send. The
  client owns only `TokioIpv4UdpWeakSender`; one per-send upgrade is retained
  until that send settles, but retained client clones cannot extend the bound
  socket's lifetime;
- consuming `shutdown` requests graceful task completion, while consuming
  `wait` observes a natural typed task exit. Dropping either the handle or one
  of those futures closes the registry and aborts rather than detaching the
  task. A task-local drop guard also closes every pending query on normal
  shutdown, driver failure, panic, or abort. Graceful shutdown drops the final
  strong socket owners before returning, so the exact address can be rebound
  even while weak client handles remain; and
- a fully typed, unknown-field-denying Rust consumer locks the checked Go
  lifecycle fixture's public defaults, 64 real suffixed-ID observations, and
  source-derived lazy construction, task ownership, stop, error-policy, and
  pending-query gaps. The fixture explicitly records that it opens no socket,
  starts no goroutine, and makes no timing or detached-completion-order claim;
  and
- focused release gates cover the production defaults and suffix, an actual
  self-ping through the shared loopback socket, graceful shutdown, pending
  registry closure, retained-client failure, task Drop, and exact port release.

This initial runtime deliberately does not yet add bounded concurrent handler
fan-out. A backpressured inbound reply can therefore delay delivery of a later
query response until the send settles. The inbound limiter, five-second Go
responder deadline, responder discovery notifications, metrics, health,
logging, crawler scheduling and persistence, application configuration/Fx
wiring, deployment wiring, and external bootstrap traffic also remain outside
this slice. Those are subsequent runtime milestones, not claims made by this
checkpoint.

The twenty-fourth slice replaces that sequential runtime loop with bounded
query fan-out while leaving every legacy finite driver and supervisor intact:

- `DhtConcurrentSupervisor` owns one `ReceiveDispatcher`, one cloneable sender
  prototype, one shared dispatcher, and one `JoinSet`. Its continuous `run`
  loop biases shutdown before a nonempty handler join and a handler join before
  the next receive. Response and error envelopes are therefore correlated
  inline without consuming query capacity, even while every admitted query
  reply is backpressured;
- each admitted query owns one sender clone and one handler task through
  dispatch and its exact awaited send. `DhtRuntime` uses a fixed capacity of 64.
  At capacity, Rust drops the newest query before responder dispatch, so it
  causes no KTable mutation and produces no reply. This fixed bound and
  drop-newest overload policy are deliberate hardenings over Go's unbounded
  goroutine per decoded query;
- shutdown stops admission, aborts every handler, and fully drains the owned
  task set before returning. The first observed receive or reply-send failure
  performs the same sibling cleanup and retains the existing exact
  `DhtDriverError`. A handler panic resumes its original payload after cleanup,
  so the runtime's existing Tokio `JoinError` boundary remains exhaustive.
  Accepted responder mutations still precede send and are never rolled back by
  backpressure, send failure, or shutdown; and
- deterministic channel gates hold one reply send pending while delivering a
  later registered response, prove capacity-one drop-before-mutation and later
  slot reuse, retain an exact send failure while aborting a blocked sibling,
  preserve a handler panic payload, and prove shutdown drains the blocked
  handler. The checked Go concurrency/inbound oracle separately records Go's
  actual blocked-send/later-response partial order and the exact production
  limiter rejection seam used by the following slice.

This slice does not yet consult `DhtInboundRateLimiter`, send an overload
rejection, expose the capacity as configuration, or count dropped queries. It
also does not add Go's five-second responder deadline, swallowed reply-send
policy, discovery, metrics, health, logging, crawler/persistence wiring, or
external bootstrap traffic. Inbound enforcement and a nonblocking rejection
path are the next runtime checkpoint.

The twenty-fifth slice adds bounded inbound admission and rejection to
`DhtConcurrentSupervisor` and wires the production policy into `DhtRuntime`:

- `DhtConcurrentSupervisor::with_inbound_policy` checks the bounded handler
  capacity before consulting the policy. A capacity rejection therefore
  consumes neither the peer's per-IP token nor a global token. Among queries
  that are capacity-eligible, the synchronous policy is evaluated in receive
  order before responder dispatch. Go instead invokes its limiter as an
  in-handler responder wrapper; moving Rust's check ahead of the responder is
  a deliberate bounded-admission hardening. A denied query never calls the
  Rust `DhtResponder` or mutates the KTable; when the bounded rejection lane
  accepts it, the query reaches the Go-compatible protocol-error composition;
- the production `DhtInboundRateLimiter` implementation supplies Go's deployed
  defaults: one token per second with burst ten for each source-IP identity,
  a bounded 1,000-key cache with strict twenty-second TTL, followed by a shared
  fifty-per-second bucket with burst twenty. Ports and IPv6 flow information
  are excluded from the per-IP key, while IPv4, mapped IPv4, native IPv6, and
  numeric IPv6 zones retain their distinct Go-compatible identities;
- both a handler-capacity denial and a typed per-IP or global rate denial select
  the same overload path. When its bounded lane accepts the work, that path
  prepares the exact Go response: a `y=r` envelope containing `e=[201, "too
  many requests"]`, the original transaction ID, no return dictionary, and no
  Rust responder call. One owned FIFO rejection worker awaits sends
  sequentially. An independent permit bound counts the active rejection plus
  every queued rejection; `DhtRuntime` fixes it at 64 total. Once exhausted,
  the newest denial is counted and dropped before reply construction, without
  cloning or polling a sender and without responder or table effects;
- Rust deliberately treats a rejection encode or transport failure as the
  exact terminal `DhtDriverError::Send`, retaining the prepared 201 outcome and
  original error. Go's production handler logs and swallows its reply-send
  failure instead. Shutdown, receive failure, either send failure, or a child
  panic stops admission, aborts, and fully drains both owned task sets before
  returning or resuming the original panic payload. No rejection worker is
  detached;
- response and error envelopes continue to bypass handler capacity, the
  inbound policy, and the rejection lane, reaching the transaction registry
  inline even while handler and rejection sends are backpressured; and
- cloneable `DhtInboundStats` exposes nine saturating monotonic counters.
  `admitted` counts capacity-eligible policy admissions;
  `denied_per_ip`, `denied_global`, and `denied_handler_capacity` classify the
  three denial boundaries; `rejection_queued` counts FIFO acceptance;
  `rejection_queue_full_dropped` counts newest rejections refused by the total
  bound; `rejection_sent` counts completed successful sends; and
  `rejection_encode_failed` and `rejection_transport_failed` classify the two
  terminal rejection-send failures. A snapshot uses independent relaxed loads
  and is explicitly not transactional across fields. The legacy
  `DhtConcurrentSupervisor::from_dispatcher` path consults no policy and leaves
  all nine counters at zero.

`DhtRuntime::start` now constructs the production inbound limiter and selects
`DhtConcurrentSupervisor::with_inbound_policy` with the existing fixed 64
handler slots and a fixed 64 total outstanding rejections. It retains a clone
of the shared stats and exposes that live handle through `inbound_stats`.
This supersedes slice twenty-four's silent capacity drop only for the runtime's
policy-enabled constructor. The legacy `from_dispatcher` path retains that
silent drop and its all-zero stats.

Deterministic supervisor gates cover capacity-before-policy token
preservation, per-IP/global receive order, exact 201 wire shape and
drop-before-mutation, response delivery and admitted-query progress while a
rejection blocks, the active-plus-queued bound and drop-newest recovery, exact
terminal rejection-send error retention, stats accounting, sibling cleanup,
and panic-payload preservation. The checked real-Go concurrency/inbound oracle
records the production server read/handler path, its blocked-send partial
order, and the exact direct-handler rejection envelope through its documented
limiter adapter. The responder-limiter and rate-policy oracles separately lock
the deployed defaults and per-IP-before-global ordering; the runtime-bridge and
dispatch/send evidence lock Go's swallowed reply-send failure. A raw loopback
gate freezes the limiter clock, bursts ten fixed-ID queries from one socket and
observes ten normal replies in arbitrary handler-completion order, then locks
the eleventh query's exact 201 wire, complete stats snapshot, graceful
shutdown, and exact port rebind.
Release verification ran the focused gate repeatedly without relying on the
client's outbound wait.

This slice still excludes Go's five-second responder timeout, configurable
handler or rejection capacities, reply-send logging, Prometheus integration,
responder discovery, crawler scheduling, persistence, external bootstrap
traffic, application/Fx or deployment wiring, and any production rollout.

The twenty-sixth slice adds caller-controlled outbound admission to the owned
runtime client without changing its existing convenience surface:

- `DhtRuntimeClient` now exposes additive `ping_with_admission`,
  `find_node_with_admission`, `get_peers_with_admission`,
  `get_peers_scrape_with_admission`, and
  `sample_infohashes_with_admission` methods. Each accepts the existing typed
  query arguments followed by an optional Tokio admission deadline and an
  arbitrary `Future<Output = ()>` admission cancellation. The original five
  methods, their unbounded production admission, and the
  `DhtRuntimeClientError` alias are unchanged;
- controlled calls return the exhaustive
  `DhtRuntimeControlledQueryError::{Admission, Query}` boundary.
  `Admission` retains the exact typed `DhtRateLimitWaitError`; `Query` retains
  the existing concrete `DhtRuntimeClientError`, including registration,
  encode, weak-transport, remote-envelope, semantic, timeout, and registry-close
  data and sources. No admission variant was added to the closed generic
  `DhtClientError` or either legacy ping/find-node error;
- every client clone continues to share the runtime's production outbound
  limiter: one token per second per exact IP identity, burst four, a
  1,000-key access-recency bound, and strict twenty-second TTL. A controlled
  method first awaits `wait_with`. Only successful admission clones the weak
  sender and immediately enters the selected `DhtClient` method. That method
  issues and registers its two-byte TID before encode and send, and starts the
  configured response timeout only after the send succeeds. Admission failure
  therefore creates no sender clone or future, registration, TID, encoding,
  send, or response timer;
- the supplied deadline and cancellation govern only the outbound admission
  wait. Pre-ready cancellation wins before deadline validation or cache access;
  an expired deadline and a reservation scheduled strictly after its future
  deadline both project to Rust's typed `WouldExceedDeadline`, while an action
  exactly at the deadline is admitted. Once admission succeeds, its token is
  committed and the supplied cancellation future is dropped. It cannot cancel
  registration, send, or response waiting, and the admission deadline is not a
  total-operation deadline. Dropping or selecting away the complete controlled
  method remains the whole-operation cancellation mechanism: during admission
  it cancels the eligible reservation without registration, while after
  admission it drops the exact registration guard without refunding the
  committed token;
- Go's production `queryLimiter` obtains or refreshes the keyed limiter before
  `rate.Limiter.Wait` observes a pre-cancelled or already-expired context. Rust
  deliberately detects those two decided conditions before keyed-cache lookup,
  so their error outcomes short-circuit equally but their cache-touch behavior
  is not a parity claim. A future deadline that cannot accommodate the next
  reservation reaches the keyed limiter in both implementations; Go reports
  its `rate: Wait(n=1) would exceed context deadline` string, while Rust groups
  that case with the already-expired case under `WouldExceedDeadline`;
- Go passes the same context through its limiter wrapper into the delegated
  server query, so that context remains able to cancel response waiting. Rust's
  caller-supplied admission future is intentionally phase-local and is dropped
  at admission; callers that need cancellation across both phases select or
  drop the complete controlled method instead. Similarly, shutting down the
  runtime closes the transaction registry and socket owners but does not cancel
  a caller-owned future still blocked solely in the shared limiter. If it later
  becomes admitted, the existing query path observes the closed registry before
  send. This combined queued-admission shutdown behavior is source-derived from
  the independently gated limiter and runtime-ownership boundaries; no current
  gate directly holds admission pending across runtime shutdown; and
- this slice intentionally exposes no detachable admission permit. The current
  limiter commits a token when `wait_with` returns, so retaining many completed
  permits could defeat the burst bound by sending them together, while dropping
  an unused permit would waste a committed token. Keeping admission and query
  in one future preserves the deployed pacing boundary.

Focused source gates lock pre-cancellation and expired-deadline precedence over
an already-closed registry, exact nested closed-registry and stopped-weak-sender
query errors, zero registrations throughout a blocked admission, in-flight
cancellation rollback to the exact next one-second action, dropping the
admission cancellation after success, whole-query registration cleanup without
token refund, all five typed forwarding paths, and exhaustive error matching.
The seven strictly consumed real-Go `query_limiter.jsonl` rows remain the
oracle for wait-before-delegate ordering, decided contexts, insufficient future
deadline text, exact IP keys, and production defaults. Existing client,
query-send, peer/sample, and runtime lifecycle gates retain exact wire,
projection, sender, timeout, registry, drop, abort, and shutdown evidence; this
slice composes those checked boundaries rather than claiming a new Go runtime
wire oracle.

This slice adds no reservation-guard or detachable-permit API, total-operation
context or deadline abstraction, new runtime configuration, retry, responder
timeout, discovery, metrics, health, crawler or persistence scheduling,
application wiring, deployment wiring, external traffic, or production
rollout.

The twenty-seventh slice adds the responder-to-runtime node-discovery handoff:

- At the shared responder seam, Rust and Go agree on the discovery success
  predicate and event payload. A successful call for `ping`, `find_node`,
  `get_peers`, `announce_peer`, or `sample_infohashes` offers the requester's
  exact ID and source address. A zero requester ID is still offered, duplicate
  successes produce duplicate offers, and read-only requests are not excluded.
  Representative protocol failures produce no event. The checked real-Go
  responder oracle covers those successes and failures, exact IPv4,
  IPv4-mapped IPv6, native IPv6, and scoped-IPv6 source values, and the Go-only
  defaults and evidence boundaries recorded by its fixture. Separate Go
  responder-limiter and rate fixtures establish that limiter denial does not
  reach the responder. Rust gates establish discovery silence for its shared
  per-IP/global denial path and its capacity-first denial; the latter is Rust
  hardening with no direct Go capacity-denial counterpart;
- only the fixed ingress capacity is a queue-configuration parity claim. Rust
  uses 1,000 slots, matching Go's `100 * ScalingFactor` with the Go code default
  scaling factor of ten. Go starts a detached goroutine after each
  successful responder call; that goroutine can wait for queue capacity for up
  to one second, so a full queue can accumulate blocked producers. Rust starts
  no task or timer: the discovery-enabled dispatcher performs one synchronous
  `try_send` and immediately drops the newest event when the queue is full or
  closed. This is deliberate bounded-runtime hardening, not Go queue-behavior
  parity;
- Go's crawler drains at most ten nodes per batch or every ten milliseconds,
  and its channel behavior can backpressure the detached producers. That
  batching and backpressure behavior is not implemented in this slice. Rust
  exposes one unbatched, take-once receiver; any reference here to a downstream
  consumer means explicitly future work, not a crawler implementation;
- Rust offers the event after the responder has completed any table mutation
  and before reply composition returns to the send boundary. An accepted event
  therefore deterministically survives a later reply encode or transport-send
  failure. Go launches its enqueue goroutine before the wrapper returns to
  reply encoding and sending, so enqueue-versus-send ordering is scheduler
  dependent. Rust also suppresses discovery when the responder returns its
  typed native-IPv6 compact-node failure. At Go's direct responder seam, the
  wrapper source ordering permits discovery to launch after core success,
  while separate dispatch evidence shows that later compact-node encoding can
  panic. This is a source-derived composition rather than one combined-oracle
  result. The fixture's native and scoped IPv6 requester-source rows are
  direct-seam evidence because the production Go server receives on an IPv4
  socket; that socket caveat does not apply to the separate native-IPv6 node
  returned by a table;
- `DhtDiscoverySender` is cloneable and offers without awaiting. Its saturating
  monotonic counters classify every attempt as `offered` and exactly one of
  `queued`, `full_dropped`, or `receiver_closed_dropped`. Snapshots use
  independent relaxed loads and are not transactional across fields. A full
  open queue drops newest and accepts again after a drain; a closed receiver
  rejects immediately while preserving already queued items for draining;
- `DhtRuntime::start` always wires a discovery sender into its dispatcher with
  the fixed 1,000-slot capacity. `take_discovered_nodes` transfers the sole
  receiver at most once, while `discovery_stats` returns a cloneable read-only
  handle that owns no sender and cannot delay EOF. Graceful shutdown and
  runtime drop close a taken receiver after owned dispatcher tasks and sender
  clones are gone, while the stats handle remains readable. Go instead shares
  one responder/crawler multi-producer batching service; it has no corresponding
  server-runtime take-once receiver or coordinated server-shutdown EOF
  boundary; and
- deterministic dispatcher, supervisor, driver, and raw IPv4 UDP gates cover
  exact payloads, duplicates, protocol and native-IPv6 failures, full and
  closed queues, cloned dispatchers, capacity and rate denial silence, event
  survival across transport failure, take-once ownership, graceful and drop
  EOF, and port rebind. In particular, the capacity-one supervisor gate proves
  that the first admitted query offers once, the capacity-rejected query adds
  no offer, and the later admitted query offers the second exact event. The raw
  runtime gate directly proves per-IP denial silence; global denial silence is
  composed from the same shared pre-dispatch denial branch and the separate
  typed global-denial gates rather than a discovery-attached global-denial
  runtime case.

This slice does not add crawler batching, IP deduplication or filtering, random
routing among ping/find-node/sample workers, crawler-driven KTable mutation,
triage, database persistence, Prometheus or other external metrics integration
or export, application or deployment wiring, external bootstrap traffic, or
production rollout. It supersedes the twenty-sixth slice only where that slice
lists discovery as excluded; the older statement remains as the historical
boundary of that earlier slice.

The twenty-eighth slice consumes that take-once discovery receiver in an owned,
bounded scheduler and freezes its relationship to the real Go crawler:

- `DhtDiscoveredNodeScheduler` is taskless until its consuming `run` future is
  polled. Its fixed constructor defaults retain Go's maximum batch size ten,
  nominal ten-millisecond tick, and downstream
  ping/find-node/sample-infohashes queue capacities 10/100/100. The first tick
  is anchored when the scheduler is
  constructed, input-versus-tick selection is unbiased, an empty tick emits no
  batch, and both a size flush and nonempty tick reset the interval before any
  downstream wait. Tokio's missed-tick policy is `Skip`; neither implementation
  promises a strict per-item ten-millisecond deadline or a deterministic winner
  at an exact input/tick tie. Zero and monotonic-clock-out-of-range intervals
  fail construction through typed configuration errors;
- Go implements batching in a detached goroutine with a 1,000-slot input, one
  queued output batch, and one additional batch that can be held by the blocked
  output send. Rust deliberately keeps batching and routing in one owned future
  and adds no second batch channel or task. When explicitly composed with slice
  twenty-seven's fixed 1,000-slot runtime discovery receiver, Rust stops
  draining that ingress while one node waits for downstream capacity; the
  responder handoff would therefore reach its documented drop-newest bound
  earlier than Go's extra output-stage buffering. The frozen Go batching row
  remains source behavior and backpressure evidence, not a claim that Rust
  reproduces its intermediate-buffer choreography;
- each nonempty Rust batch preserves input order and keeps the first full
  `RoutingNode` for each structural address key. IPv4 and IPv4-mapped IPv6 stay
  distinct; native IPv6 retains its numeric scope ID. Node ID, port, and IPv6
  flow information do not participate in deduplication. Before filtering, Rust
  projects the winners to port zero and IPv6 flowinfo zero, matching Go's
  `netip.Addr` input. It calls the real shared `KTable::filter_known_addrs`
  synchronously exactly once, retains its returned order, maps each result back
  to the original first-winning node, and never holds a table lock across an
  await. The scheduler performs no table mutation and does not recheck table
  state after filtering;
- each unknown node is routed sequentially to exactly one open bounded lane.
  Selection among simultaneously available ping, find-node, and
  sample-infohashes permits is unbiased. A winning reserved permit commits its
  node synchronously, so cancellation cannot land between capacity acquisition
  and enqueue. A full lane remains eligible but pending, an individually closed
  lane is disabled, and closing all three lanes is a typed terminal state even
  while the scheduler is idle. This replaces Go's selected-send-to-closed-worker
  panic with explicit ownership and closure handling. The queues in this slice
  contain work only; Go's concurrent query workers are not yet implemented;
- at each asynchronous selection point, a ready caller shutdown is polled
  before intake/timer or route-capacity progress; synchronous deduplication,
  filtering, and reserved-permit commit are not preempted. A winning shutdown
  preserves every already committed delivery, abandons the exact unfiltered
  local partial batch or remaining filtered routing suffix, closes the ingress,
  synchronously drains accepted queued ingress nodes, and reports their
  combined `pending_dropped` count.
  All-route closure performs the same bounded close-and-drain accounting.
  Absent competing shutdown or all-route closure, producer EOF routes the final
  partial batch and returns `InputClosed`. That coordinated flush-and-EOF
  behavior is deliberate hardening over Go's unlabelled closed-input `break`,
  which can spin and leaves partial-buffer and output-close behavior
  unspecified. `run` spawns no work; every route receiver reaches drain-then-EOF
  when the consuming scheduler future returns or is dropped;
- sender-free scheduler stats expose saturating monotonic `received`, `batches`,
  `duplicate_dropped`, `known_filtered`, completed `filter_calls`, entered
  `route_attempts`, the three committed route counts, and exact shutdown and
  all-route-close drop counts. `route_attempts` means a filtered node entered
  shutdown-or-capacity selection; it does not claim a permit was acquired.
  Snapshots use independent relaxed loads and are not transactional across
  fields. These counters are downstream of, and distinct from, the discovery
  sender's offered/queued/full/closed counters; and
- a strict Rust consumer locks the ten ordered real-Go oracle rows, fixture
  SHA-256, every nested schema field, optional branch, factory/source fact, and
  five embedded Go source digests. It executes all eight crawler rows against
  the real Rust scheduler and KTable, including first-IP wins, the first-put
  reverse-map quirk, hash-peer filtering, cross-batch dedupe reset, all-known
  continuation, forced and multiple-ready lanes, full-route ping release, and
  both cancellation barriers. Private unit-test prefill makes the full-route
  barriers observable; completed filter and entered-route counters prove that
  cancellation occurs only after the fixture's required phase. Go's output
  capacity one, worker concurrency values, broken close loops, and cancellation
  tie outcomes remain explicitly classified as Go source evidence or known
  deltas rather than Rust runtime claims.

This slice does not wire the scheduler into `DhtRuntime` or an application
supervisor, start ping/find-node/sample-infohashes query workers, recursively
feed their discoveries back into the ingress, mutate the KTable from crawler
responses, manage bootstrap or oldest-node work, triage info hashes, run
get-peers/scrape/metainfo stages, persist torrents or sources, export these
counters through Prometheus, add health or logging integration, change
application/deployment configuration, contact external DHT nodes, or roll out
to production. It supersedes slice twenty-seven only where that slice lists
crawler batching, deduplication, filtering, and routing among the
ping/find-node/sample-infohashes lanes as wholly absent; all later crawler and
deployment exclusions remain in force.

The twenty-ninth slice consumes only the scheduler's discovered-node ping lane
with an owned, bounded query worker:

- `DhtDiscoveredNodePingWorker::new` owns the route receiver, the supplied
  production `DhtRuntimeClient` handle, the shared `KTable`, and every spawned
  query.
  Its default maximum in-flight count is ten, matching Go's default ping
  concurrency, while the scheduler's ping route retains its separate capacity
  of ten. `with_config` changes only the nonzero concurrency bound. The worker
  does not poll or dequeue the route at capacity. Within the worker and its
  default input route, retention is therefore at most ten active queries plus
  ten queued nodes, with no hidden eleventh acquire waiter. A node and filtered
  suffix held by the scheduler while it awaits route capacity, and any upstream
  discovery ingress, remain governed by slice twenty-eight and are outside this
  worker-local bound. Go instead dequeues before acquiring its semaphore and
  can retain capacity plus concurrency plus one waiter;
- the worker accepts state-free `RoutingNode` values produced by slice
  twenty-eight. It queries the exact input address and intentionally does not
  look the ID up in the table before the request. Go's `runPing` first rejects
  a dropped live `KTableNodeHandle` and skips a handle whose timestamp is
  strictly after `now - oldPeerThreshold`; the factory fixes that threshold at
  fifteen minutes. Those guards belong to the future periodic old-node producer
  and are not inferred from a state-free discovered node. This slice therefore
  neither implements nor claims those two handle behaviors;
- a successful reply to a zero advertised ID puts the response ID at the input
  address with `Responded`. A matching nonzero response does the same. A
  mismatching response to a nonzero advertised ID drops the advertised ID and
  increments the mismatch counter. A client error deliberately retains the
  deployed Go quirk: it attempts to drop the zero ID, not the advertised ID.
  Consequently an existing advertised node can survive the error unchanged.
  Rust retains the decision and table effect but has no KTable command reason
  or error-identity field; the oracle's exact Go reason and error identity are
  Go-only evidence. There is no await between a completed query result and its
  table command;
- route EOF stops intake, waits for every owned query and returns typed
  `InputClosed`. Caller shutdown is biased ahead of a ready join or receive: it
  closes and synchronously drains the route, aborts all unresolved query tasks,
  joins the complete task set, and returns typed `Shutdown` with exact queued
  drops and cancellations actually observed. A task that has already completed
  its synchronous table command survives shutdown and is not counted as
  cancelled; the oracle's cancel-after-success row directly freezes that
  boundary. A child panic stops intake, aborts and drains its siblings, then
  resumes the original payload. Dropping the worker closes intake and aborts
  its owned task set, so no query is deliberately detached. These are Rust
  lifecycle hardenings over Go's spawned, unjoined callbacks, swallowed lane
  error, repeated closed-channel receive, and ping-lane nil-node panic;
- sender-free `DhtDiscoveredNodePingStatsHandle` snapshots nine saturating
  monotonic counters: dequeued nodes; started, successful, and failed client
  futures; ID mismatches; attempted put and drop commands; and shutdown-queued
  drops and observed query cancellations. Successful replies include ID
  mismatches. Command counters include rejected or no-op table commands, so
  the Go-compatible zero-ID drop is observable even when zero was absent.
  `queries_started` proves only that a local client future entered the owned
  task set, not rate-limit admission or UDP transmission; similarly, cancelling
  a local future does not prove that it sent no datagram. Each live snapshot is
  a set of independent relaxed loads. Cross-field conservation is promised only
  after a normal terminal exit; and
- the strict ping consumer freezes nine ordered real-Go rows, the fixture
  SHA-256
  `26d403becff0caeb0a27ec9027a366d51e19cdb7129ff05715cf24a6d2e1b040`,
  every nested field and optional shape, and eight embedded Go source digests.
  It executes the five state-free worker rows through the real private Rust
  core, binding each scripted outcome to the exact put/drop decision, query
  address, exit, complete terminal stats, and KTable state. The two
  dropped/recent rows are explicitly deferred live-handle evidence; the lane
  error row is Go-only and is paired with Rust's empty-route `InputClosed`
  delta; and the remaining row freezes source, factory, capacity, concurrency,
  and lifecycle facts. Go accessor counts, context identity, exact batch and
  option traces, command reasons, and error identity remain explicitly Go-only
  metadata rather than Rust-observed parity.

This slice does not compose the scheduler or worker into `DhtRuntime` or an
application supervisor, produce periodic old-node or bootstrap work, implement
the find-node or sample-infohashes workers, rotate a find target, recursively
feed response nodes into discovery, triage info hashes, run
get-peers/scrape/metainfo stages, persist torrents or sources, expose
Prometheus or other external metrics, add health or logging integration,
change application or deployment configuration, contact external DHT nodes,
or roll out to production. It supersedes slice twenty-eight only where that
slice says the discovered-node ping query worker is absent; all other scheduler,
crawler, application, and deployment exclusions remain in force.

The thirtieth slice adds only the shared crawler node-ID target and its owned
rotation prerequisite for future find-node and sample-infohashes workers:

- `DhtCrawlerTarget` is a public, cloneable, `Send + Sync` read handle over one
  `Arc<RwLock<Id20>>`. Its explicit `new` constructor accepts any complete ID,
  including zero, while `current` returns one stable whole-value snapshot.
  Module-owned replacement is private. Read and write poison is recovered
  because the protected invariant is exactly one copyable ID; no lock guard is
  retained across an await. Constructing this handle alone performs no entropy
  read, starts no timer or task, and does not imply that a rotator exists;
- `DhtCrawlerTargetRotator::new` synchronously fills a fresh raw twenty-byte
  candidate with `getrandom`, applies no Bitmagnet client suffix, and returns
  the `(target, rotator)` pair only after that initial fill succeeds. Zero and a
  repeat of the prior ID remain valid random outcomes and are neither rejected
  nor retried. An initial partial-fill failure discards the local candidate and
  returns `DhtCrawlerTargetError::Entropy` without publishing either component.
  The rotator is deliberately non-cloneable and is the only public construction
  path that carries the private replacement capability for its paired target;
- the rotator's consuming `run` future owns and spawns no task. It creates a
  fresh ten-second sleep for each iteration only after the preceding
  replacement has completed, so there is no immediate rotation, periodic-timer
  catch-up, or promised strict wall-clock cadence. An already-ready shutdown is
  biased ahead of a ready sleep and returns `Ok(())`. Once the sleep wins,
  entropy generation and replacement are synchronous and cancellation-unaware:
  shutdown becoming ready during a successful fill does not suppress that
  completed replacement and wins on the next loop. A rotation entropy failure
  publishes no partial candidate, preserves the last successful target, and
  returns the typed error without retry. Dropping a pending `run` future drops
  its armed delay and detaches no work; surviving target handles remain readable
  at their last published value after either shutdown or failure;
- Go stores the corresponding value in one shared
  `*concurrency.AtomicValue[protocol.ID]`, whose read and exclusive write locks
  return and replace the complete array. The production factory initializes it
  with `RandomNodeID` before starting the crawler, and the exact find-node and
  sample-infohashes call sites each read that same field at client-call time.
  Those two consumer bindings are source evidence, not a claim that the Rust
  workers are present. Go starts rotation as a detached, unjoined goroutine,
  uses a fresh `time.After(10 * time.Second)` after each set, and leaves a
  simultaneously ready timer/cancellation winner unspecified. Rust instead
  owns the future and fixes the ready tie in favor of shutdown;
- both implementations use twenty raw target bytes without a client suffix and
  do not guarantee a nonzero or changing value. Go ignores both results from
  `crypto/rand.Read`; if that call returns an error, it can install a new ID
  containing any written prefix and a zero-initialized remainder, replacing the
  previous target. Rust deliberately requires a complete fill before either
  initial publication or rotation and exposes failure while retaining the last
  successfully published target. Neither implementation can cancel its
  synchronous entropy call after the timer branch has won; and
- the strict Rust consumer freezes all five ordered sought-target oracle rows,
  fixture SHA-256
  `683162fe0da0c9fe8f39b80fffaaa3aae4f98683a0c1579b521eeb69f9aa1ea4`,
  every nested schema field, the seven explicit Rust hardening classifications,
  and six embedded Go source digests. It replays the four actual Go
  `AtomicValue` rows against the real Rust holder: explicit zero, exact set/get,
  shared-alias A-to-B replacement, and channel-gated cross-thread handoff. Its
  mandatory total `Get`/`current` counts are exactly `[0, 1, 1, 3, 3]` across
  the source row and four runtime rows, including the cross-thread final read.
  The remaining row freezes factory, shared-consumer, delay, lifecycle, and
  random source shapes through exact Go AST and source freshness; it explicitly
  marks Go wall-clock and entropy execution as unobserved. Separate
  deterministic Rust unit gates use injected fills and delays plus paused Tokio time to cover
  raw and zero initialization, the exact first delay, no catch-up, biased ready
  ties, shutdown during generation, partial-fill retention, and pending-future
  drop, while a compile-fail doctest freezes the non-cloneable writer surface.

This slice does not implement or compose the find-node or sample-infohashes
workers, periodic oldest-node producers, recursive discovery, crawler-driven
KTable response mutation, scheduler or `DhtRuntime` ownership, application
supervision, entropy retry, backoff or restart, configurable rotation cadence,
change notifications, rotation counters, Prometheus or other external metrics,
health or logging integration, application or deployment configuration,
external DHT traffic, or production rollout. It supersedes slice twenty-nine
only where that slice excludes rotating a find target; the exclusions of both
query workers and all crawler, runtime, application, and deployment composition
remain in force.

The thirty-first slice, landed by `2c7ce48a` and strictly consumed by
`36de3a8b`, adds only the owned discovered-node `find_node` worker:

- `DhtDiscoveredNodeFindWorker::new` owns the scheduler route receiver, the
  supplied production `DhtRuntimeClient`, shared `KTable`, cloneable
  `DhtDiscoverySender`, and cloneable `DhtCrawlerTarget`. It returns the worker
  together with a sender-free `DhtDiscoveredNodeFindStatsHandle`.
  `DhtDiscoveredNodeFindWorkerConfig` exposes only a nonzero `max_inflight`;
  `new` fixes it at 100, while `with_config` changes only that bound. The
  scheduler's distinct find-node route capacity remains 100. The controller
  does not poll or dequeue that route at capacity, so the worker and its input
  route retain at most 100 accepted tasks plus 100 queued nodes, without Go's
  extra item dequeued before semaphore acquisition. Nodes or a filtered suffix
  still held by the scheduler, upstream discovery ingress, and other future
  producers are outside this worker-local bound;
- after each route dequeue, the controller reads `DhtCrawlerTarget::current`
  exactly once, immediately before constructing the query future and spawning
  its owned task. It performs no target read for work still queued at capacity.
  Once accepted, Rust performs no KTable lookup, scheduler-eligibility recheck,
  or other table-state guard before constructing that query. The exact Go
  source likewise performs no eligibility recheck inside `runFindNode`; its
  callback queries the supplied live node directly. This shared fact does not
  implement Go's separate oldest-node producer or its selection policy.
  Go instead reads the shared target inside each concurrently spawned callback
  at the `FindNode` client call. Both obtain one target per accepted query, but
  their controller-versus-callback scheduling point is an explicit delta: no
  cross-query ordering is inferred from the shared holder or its ten-second
  rotator;
- the accepted `RoutingNode` is one immutable ID/address snapshot. Rust queries
  that exact address and, on success, issues exactly one put of the same
  advertised ID and address with exactly `Responded`. Go holds a live
  `ktable.Node`, calls `Addr` for the query, and calls it again for the put; the
  oracle's mutable A-to-B row proves that Go can query A and store B. Rust
  deliberately queries and attempts to store A. A query error attempts exactly
  one drop of the advertised ID. Rust's typed
  `Result` cannot retain the response ID and nodes alongside an error, and its
  KTable command has no Go reason or wrapped-error-identity field. On success,
  the response ID is deliberately ignored: it is never used to select a table
  operation, and the fixture's distinct response IDs remain absent. Only the
  advertised ID is supplied to the responded put. There is no await between
  the completed query result and its table command;
- after the successful synchronous put, Rust preserves the response-node
  sequence, duplicates, exact addresses, and numeric IPv6 scope IDs. It awaits
  one `DhtDiscoverySender::reserve` at a time and synchronously commits each
  acquired permit before reserving the next. Full live discovery therefore
  backpressures the same owned query task rather than dropping newest. EOF also
  waits through that backpressure. Shutdown can preserve an already committed
  prefix and abandon the exact blocked suffix. A first receiver-closed result
  classifies the whole remaining worker suffix as
  `recursive_nodes_closed_dropped`, including nodes for which no discovery
  delivery was attempted. The channel-global `DhtDiscoveryStats`, by contrast,
  counts only actual offer or reserved-delivery attempts and aggregates every
  sender clone; it does not inherit the worker's unattempted suffix count. A
  permit acquired before explicit receiver close retains the discovery seam's
  documented synchronous-commit behavior, while receiver destruction can
  still reject that commit;
- route EOF stops intake, joins every accepted query and fanout task, and
  returns `DhtDiscoveredNodeFindWorkerExit::InputClosed`. A ready caller
  shutdown is biased ahead of another join or receive, closes and synchronously
  drains queued route work, marks cancellation as shutdown-caused, aborts the
  complete task set, and joins it before returning `Shutdown` with exact
  `queued_dropped`, observed `tasks_cancelled`, and
  `recursive_nodes_dropped`. The query future and its successful completion
  code are one task poll: if the query makes shutdown ready immediately before
  returning success, the advertised put still completes synchronously before
  the first capacity wait, after which shutdown can cancel the fanout. A
  successful query can therefore increment success and put counters while its
  unfinished fanout task is later counted as cancelled. Ordinary worker or
  `run`-future drop before caller shutdown has won closes route intake and
  aborts the owned task set without setting graceful-shutdown counters. A child
  panic observed through the non-shutdown join path stops intake, aborts and
  drains its siblings, leaves those graceful-shutdown counters at zero, and
  resumes the original panic payload. Dropping the `run` future after shutdown
  has already set its cause marker is not a normal terminal exit and does not
  carry a zero-counter promise. No query or fanout is deliberately detached;
- `DhtDiscoveredNodeFindStatsHandle` snapshots thirteen saturating monotonic
  counters: dequeued nodes; started and completed tasks; successful and failed
  queries; attempted put and drop commands; total, queued, and receiver-closed
  recursive nodes; and shutdown-queued, shutdown-task, and shutdown-recursive
  drops. Command counters include table rejection or no-op. A started task spans
  both its query and any recursive fanout, so `shutdown_tasks_cancelled` is not
  a claim that the client query itself remained unresolved. Each live snapshot
  is a set of independent relaxed loads. Every terminal sum below uses
  `saturating_add`, matching the counters rather than assuming mathematical
  overflow. After normal `InputClosed`, the invariants are
  `dequeued == queries_started`, `put_commands == queries_succeeded`,
  `drop_commands == queries_failed`, `queries_started == tasks_completed`,
  `tasks_completed ==
  queries_succeeded.saturating_add(queries_failed)`, and `recursive_nodes ==
  recursive_nodes_queued.saturating_add(recursive_nodes_closed_dropped)`, with
  all shutdown counters zero. After normal `Shutdown`, the same common
  equalities are explicit: `dequeued == queries_started`,
  `put_commands == queries_succeeded`, and
  `drop_commands == queries_failed`. In addition, `queries_started ==
  tasks_completed.saturating_add(shutdown_tasks_cancelled)` and
  `recursive_nodes == recursive_nodes_queued.saturating_add(
  recursive_nodes_closed_dropped).saturating_add(
  shutdown_recursive_nodes_dropped)`. The exit's `queued_dropped`,
  `tasks_cancelled`, and `recursive_nodes_dropped` are the same per-worker
  amounts applied as saturating increments to `shutdown_queued_dropped`,
  `shutdown_tasks_cancelled`, and `shutdown_recursive_nodes_dropped`,
  respectively. The same-poll success/cancellation oracle deliberately
  exercises the shutdown invariant with one success and put, zero completed
  tasks, one cancelled task, four returned nodes, and four
  shutdown-abandoned nodes; and
- the strict child-module consumer freezes all eight ordered real-Go rows and
  fixture SHA-256
  `e126ad26fd342b14ae0416b3610d991f927dbe9381ac11609ebeba96d67870b7`.
  Their classifications, in order, are `SOURCE_ONLY`, `RUNTIME_EXACT`,
  `RUNTIME_WITH_IMMUTABLE_ADDR_DELTA`, `RUNTIME_EXACT`,
  `RUNTIME_WITH_SHUTDOWN_BACKPRESSURE_DELTA`,
  `RUNTIME_WITH_SHUTDOWN_BACKPRESSURE_DELTA`, `RUNTIME_EXACT`, and
  `GO_ONLY_LANE`. Every nested schema rejects unknown fields. Runtime rows bind
  independently hard-coded exits, complete worker and channel-global stats,
  query calls, target-read adjacency, exact table admission and retained-handle
  state, response-ID absence, put-before-fanout ordering, ordered duplicate and
  scoped discovery, same-poll success/shutdown overlap, and one-prefix/three-
  suffix cancellation. The Go runtime rows use a manual unbuffered discovery
  input with capacity zero. Rust's deterministic backpressure harness instead
  uses a capacity-one Tokio discovery channel and, where the first reservation
  must remain pending, prefills its one slot with a sentinel. That sentinel is
  included explicitly in channel-global stats. Capacity one plus prefill is a
  test-harness delta, not unbuffered-channel or production-buffer choreography
  parity. The swallowed Go lane-error row remains Go-only; a
  separate Rust gate proves typed empty-route EOF rather than pretending that
  EOF replays a Go lane error. Go accessor counts and mutable returns,
  unbuffered capacity zero, callback context identity, exact batch and option
  traces, reasons and error identity, discovery-node temporal/candidate state,
  and an error result's otherwise populated response payload remain explicitly
  Go-only metadata.

The consumer also binds each fixture source entry and the current embedded Go
source to these thirteen independent SHA-256 anchors:

- `internal/concurrency/atomic.go`:
  `09cc4842dbdf516f8574f26b411130daba526f69dbf217e1f2867e829f781a4f`;
- `internal/concurrency/batching_channel.go`:
  `72b3c9fd5fbc8ecbfb0ba2bc2ed5e6c1d45de01f03d3e015b2467f114ec70975`;
- `internal/concurrency/buffered_concurrent_channel.go`:
  `4be882800ec66d0c1709319fe029d61773c3f4a37bdb409e3a2f7d5d415d954c`;
- `internal/dhtcrawler/config.go`:
  `b3cac15378cdca0f21c5f21f37aeb0679815d5bacd16bfa0c3bac2af56db87ef`;
- `internal/dhtcrawler/crawler.go`:
  `ae6ca2484a57231a08351629c21fdc0a875f2272bfd4ad42a4e5386be86500b6`;
- `internal/dhtcrawler/discovered_nodes.go`:
  `22806cabf39173df71010a54d874a4319458f1715308834be828dbdb99767027`;
- `internal/dhtcrawler/factory.go`:
  `ed34129835773817736d70e74c7c884e5b9197e35741dee922ee9a5d691288a6`;
- `internal/dhtcrawler/find_node.go`:
  `cd5fab8aa078ad40ed82331dbbfd141a38badc018287dd13211d221b230087bb`;
- `internal/protocol/dht/client/interface.go`:
  `477139d727ea685538bccfb0be114ab4fa43556cbdb70d5492a074f24482389f`;
- `internal/protocol/dht/ktable/command.go`:
  `575e58a01856db0746281c3a66a95d6d5483452fb8ab20dc6379ffbc45cedf11`;
- `internal/protocol/dht/ktable/node.go`:
  `93ed9a76a7cd0f50ee3ad255c6e77a8d19e5fe17081edc6238c5efab4983b3c3`;
- `internal/protocol/dht/ktable/query.go`:
  `103ec27a7904bdbbbd91f3ea1dae1f4d6ea3b3d6652757a6ab8ddbf598a7060e`;
  and
- `internal/protocol/dht/ktable/table.go`:
  `68e3caf4394b2692fd9358224cce2b70ae3d90d920097bd28885b6b3bb77848f`.

The source row remains source and exact-AST evidence rather than Rust runtime
execution. In particular, Go's default scaling factor ten, buffered concurrent
lane capacity and concurrency 100, dequeue-before-acquire extra waiter,
unjoined callbacks, repeated closed-input receive, shared context, production
discovery input 1,000 with batch size ten, ten-millisecond interval and output
capacity one, and target factory/rotation call sites are not reclassified as
combined runtime parity. The Go oldest-node producer performs its first query
before delay, asks for at most ten nodes older than five seconds, sends them in
order, then waits on a cancellation-unaware one-second `time.After`; with an
empty table it can continue querying and sleeping after cancellation. Those
producer facts are digest and AST evidence only. Slice thirty remains the
runtime contract for the Rust target holder and rotator; this slice consumes
the holder but does not widen the rotator's ownership or timing claims.

This slice does not implement the periodic oldest-node producer, a second or
multi-producer find-route sender seam, bootstrap injection, or the
sample-infohashes worker. It does not compose the scheduler, find worker,
target rotator, discovery feedback, or ping worker into one crawler supervisor,
`DhtRuntime`, application, or process lifecycle. It adds no restart policy,
database persistence, torrent or source ingestion, Prometheus or other
external metrics export, health or logging integration, application or
deployment configuration, external DHT traffic, live deployment, or production
rollout. It supersedes slice thirty where that slice says the Rust find-node
worker is absent, its find-node target read is source evidence only, recursive
discovery is wholly absent, and no crawler-driven KTable response mutation
exists. The new scope is only this worker's advertised-node put/drop and its
ordered response-node offers to discovery. It does not compose those offers
back through the scheduler or establish a recursive crawler feedback loop. All
shared-target, rotation, sample-infohashes, producer, broader recursive
composition, runtime, application, deployment, and rollout boundaries remain
in force.

The thirty-second slice, landed by `667201cf`, adds only a shared producer
capability for the existing bounded `find_node` route:

- `DhtDiscoveredNodeScheduler::find_node_input(&self)` lazily clones the
  scheduler's existing find-route sender and returns a public, cloneable,
  `Send + Sync` `DhtDiscoveredNodeFindInput`. Merely constructing the scheduler
  creates no external handle. The capability adds no task, queue, or capacity:
  scheduler-origin and externally supplied nodes share the one configured
  `find_node` queue, whose default capacity remains 100;
- `DhtDiscoveredNodeFindInput::send(&self, node)` asynchronously waits for that
  shared capacity and returns
  `Result<(), DhtDiscoveredNodeFindInputClosed>`. A successful return means the
  node was synchronously committed to the queue, not that the worker has
  dequeued or consumed it. Receiver closure returns the exact uncommitted
  `RoutingNode`, recoverable through
  `DhtDiscoveredNodeFindInputClosed::into_node`;
- sequential awaited sends by one producer preserve program order. Competing
  send or reserve futures enter Tokio's FIFO waiter queue in runtime
  registration order. This seam assigns no source priority and promises no
  deterministic cross-producer or scheduler-versus-external source order;
- each pending `send` future owns one node outside the bounded queue. The seam
  does not bound how many such futures callers may create, so the queue's
  capacity is not a whole-composition retention bound. Dropping a pending
  future commits nothing, drops its future-owned node, and loses its waiter
  position; a caller that needs to retry must retain a separate copy. Once a
  send succeeds, cancellation cannot retract the queued node;
- without a requested find-input handle, scheduler return or drop retains the
  prior drain-then-EOF behavior for all three routes. A live find-input handle
  or clone intentionally keeps only the find route open after the
  scheduler-owned sender is gone; dropping the last external clone then permits
  drain-then-EOF. Explicitly closing or dropping the unique find-route receiver
  wakes registered and later sends with the typed closed error. The owned find
  worker's shutdown closes that receiver before draining already queued work:
  a node from a pending external send is returned as uncommitted and is absent
  from the worker's queued-drop accounting, while a previously committed queue
  item remains worker-owned and is drained and counted under slice thirty-one;
- direct find-input sends bypass the scheduler's discovery batching, address
  deduplication, address projection, KTable known-address filter, and routing
  choice. They also bypass all scheduler counters. In particular,
  `routed_find_node` continues to count only scheduler-origin commits. The find
  worker consumes one source-free `RoutingNode` stream and its dequeued, query,
  table-command, recursive-discovery, and terminal counters therefore aggregate
  committed work from every source without attributing producer provenance;
  and
- focused Rust gates preserve the lazy default EOF, clone-extended EOF, shared
  capacity and sequential order, registered-send cancellation, receiver close
  and drop recovery, exact pending-send rejection during worker shutdown, and
  the split between direct external work and scheduler-origin stats. A known
  KTable node sent directly through the capability proves that this seam does
  not silently reapply scheduler filtering.

This slice adds no periodic oldest-node producer, KTable oldest-node selection,
five-second cutoff, ten-node limit, immediate first query, one-second delay,
producer task, producer shutdown policy, or producer counters. It adds and
consumes no Go oracle row or fixture; the oldest-node producer behavior frozen
as source-only evidence in slice thirty-one remains source-only. It does not
compose the scheduler, find worker, shared target or rotator, discovery
feedback, or ping worker into a runtime or application lifecycle, and it adds
no external DHT traffic, deployment configuration, live deployment, or
production rollout.

This slice supersedes slice twenty-eight only where that slice says every route
receiver necessarily reaches EOF when the scheduler future returns or is
dropped: that remains exact for ping and sample-infohashes, and for find-node
when no external find-input clone survives. It supersedes slice thirty-one only
where that slice excludes a second or multi-producer find-route sender seam and
describes scheduler-produced nodes as the route's only provenance. All worker
query semantics and statistics remain unchanged, and every periodic-producer,
oracle, recursive-composition, runtime, application, deployment, and rollout
exclusion remains in force.

The thirty-fifth slice freezes the periodic oldest-node `find_node` producer
through the Go oracle at `4dfd19bc3aa71e2028e5944cab56568697671126`, the
Rust implementation at `fd89b31ba0c2c2dc772ccdbcd49e6c63cfa1deeb`, and
the strict consumer at `8b888c8751bd7696ebbde4c3630570b51d3bbbb0`:

- `DhtOldestNodeFindProducer::new` consumes the shared `KTable` and one
  `DhtDiscoveredNodeFindInput`, and returns the producer with a sender-free
  `DhtOldestNodeFindProducerStatsHandle`. The public surface has no
  configuration variant: it fixes the oldest-node age at five seconds, query
  limit at ten, and post-batch delay at one second. The consuming `run` future
  starts and spawns no task. Its owned find-input sender delays find-route EOF
  until the producer or its `run` future exits or is dropped;
- the first KTable query is immediate only when neither pre-ready caller
  shutdown nor preclosed find input wins first. The production clock is
  monotonic `Instant::now`; only the private deterministic test seam injects a
  replacement clock. Subtracting the fixed age floors nonpanickingly at the
  oldest representable instant. A winning query branch synchronously calls
  `KTable::get_oldest_nodes(cutoff, Some(10))`, increments the completed-query
  and selected-occurrence counters, and cannot be interrupted after that
  branch has won. The actual KTable gate fixes the strict boundary: a handle
  timestamped exactly at the cutoff is excluded, as is a newer handle, while a
  handle one nanosecond older is selected. Returned order is preserved;
- selected live handles are processed sequentially. Before each snapshot,
  ready shutdown wins, followed by input closure; no KTable membership,
  dropped-state, eligibility, or recentness recheck occurs. Rust projects each
  handle to one immutable `RoutingNode` immediately before constructing that
  item's capacity-waiting send. A mutation before a later handle's snapshot is
  visible, while a mutation after an earlier snapshot cannot change its
  pending value. Go instead sends the same live interface handles returned by
  `GetOldestNodes` and performs no node accessor call in the producer. The
  immutable projection is therefore an explicit Rust delta rather than a
  claim about Go consumer-time state;
- each awaited send uses slice thirty-two's shared find-route capacity
  directly. It bypasses scheduler batching, deduplication, address projection,
  known-node filtering, routing choice, and scheduler counters. A successful
  return means queue commit, not find-worker consumption. One producer's
  sequential commits preserve order, but this slice adds no priority or
  deterministic ordering across the producer, scheduler, or other find-input
  clones. During a blocked send, ready shutdown wins a tie with newly
  available capacity or route closure. An already committed prefix is never
  retracted; the current node and remaining selected suffix are classified as
  unqueued by the terminal cause;
- only after the complete selected batch is queued does Rust construct a fresh,
  cancellation-aware one-second sleep. Delayed polling cannot cause catch-up,
  and the focused clock gates fix both the 999-millisecond pending boundary and
  progress at the following millisecond. Shutdown or input closure during this
  sleep exits with a zero selected suffix. Go instead executes an unconditional
  `time.After(time.Second)` receive after every batch. Its timer timing was not
  executed by the runtime oracle, and when every query returns an empty table
  its context is never checked, so the source loop can continue querying and
  sleeping after cancellation. Those remain source facts, not Rust runtime
  parity claims;
- the exhaustive public exits are
  `DhtOldestNodeFindProducerExit::Shutdown { selected_dropped }` and
  `InputClosed { selected_dropped }`. The count is the exact current selected
  occurrence plus later selected occurrences that were not queued. Receiver
  closure is typed even for an empty table; Go's production lane has no
  corresponding closed-channel lifecycle. A pre-ready Rust shutdown beats
  both a ready query and preclosed input and performs zero queries, while the
  actual Go pre-cancelled row performs one query, calls `In` once, then returns.
  Dropping a pending Rust run detaches nothing and releases its sender, but is
  not a normal terminal return and makes no suffix-accounting promise; and
- `DhtOldestNodeFindProducerStats` exposes five saturating monotonic counters:
  completed `table_queries`, returned `selected` occurrences, committed
  `queued` occurrences, `input_closed_dropped`, and `shutdown_dropped`.
  Snapshots load fields independently with relaxed ordering and are not
  transactional. After normal terminal return,
  `selected == queued.saturating_add(input_closed_dropped).saturating_add(
  shutdown_dropped)`. The exit's `selected_dropped` is the per-run suffix
  applied to the corresponding terminal counter. The shared route still
  attributes no producer provenance: find-worker counters aggregate every
  committed source, and `routed_find_node` remains scheduler-origin only.

The strict child-module consumer denies unknown fields at every fixture level
and freezes the three ordered rows as `SOURCE_ONLY`, `RUNTIME_EXACT`, and
`RUNTIME_EXACT`, with fixture SHA-256
`06e2ac78f73418038c946fdc5f3562654e130623fcf88e907c1c4e07112505cc`.
It consumes every source, oracle, input, output, accessor, event, and optional
field; binds the fixed Rust cutoff, limit, delay, scheduler find capacity 100,
and find-worker concurrency 100; and checks these six embedded Go sources:

- `internal/concurrency/buffered_concurrent_channel.go`:
  `4be882800ec66d0c1709319fe029d61773c3f4a37bdb409e3a2f7d5d415d954c`;
- `internal/dhtcrawler/config.go`:
  `b3cac15378cdca0f21c5f21f37aeb0679815d5bacd16bfa0c3bac2af56db87ef`;
- `internal/dhtcrawler/crawler.go`:
  `ae6ca2484a57231a08351629c21fdc0a875f2272bfd4ad42a4e5386be86500b6`;
- `internal/dhtcrawler/factory.go`:
  `ed34129835773817736d70e74c7c884e5b9197e35741dee922ee9a5d691288a6`;
- `internal/dhtcrawler/find_node.go`:
  `cd5fab8aa078ad40ed82331dbbfd141a38badc018287dd13211d221b230087bb`;
  and
- `internal/protocol/dht/ktable/table.go`:
  `68e3caf4394b2692fd9358224cce2b70ae3d90d920097bd28885b6b3bb77848f`.

The source row retains Go's exact factory, detached-producer, capacity,
concurrency, cutoff, order, cancellation-select, and unconditional-delay
shapes. Its cutoff clock is runtime-bracketed, but its post-batch delay and
forever-empty cancellation behavior are explicitly unobserved. The first
actual Go row proves query-before-cancellation at an unbuffered first send;
the Rust replay instead fixes the pre-ready shutdown delta, all-zero stats,
unchanged table handle, and route EOF. The second actual Go row commits the
same live A and B handles through a capacity-two manual lane, enters the third
`In`, then abandons C and D after cancellation. The real Rust producer replay
uses a positive capacity-two Tokio route and a fixed query clock. A through D
are timestamped six seconds old, while a fifth hard-coded ID/address sentinel
timestamped exactly at `query_now - OLDEST_AGE` is excluded, fixing the strict
five-second boundary and `selected=4`. The replay commits immutable A and B in
order and records the private pre-snapshot indices exactly as `[0, 1, 2]`: A
and B were snapshotted and queued, C was snapshotted and pending on the full
route, and D was never snapshotted. It then returns
`Shutdown { selected_dropped: 2 }` with complete terminal stats
`table_queries=1`, `selected=4`, `queued=2`,
`input_closed_dropped=0`, and `shutdown_dropped=2`.

The consumer also freezes eight deliberate Rust deltas: pre-ready shutdown
before the first query; positive Tokio capacity in place of Go's unbuffered
lane; biased query, snapshot, send, and delay boundaries; a privately
injectable monotonic clock; cancellation-aware fresh delay without catch-up;
typed empty-table input closure; one immutable `RoutingNode` per live-handle
snapshot; and an owned, taskless run future without detached work. Go's
production capacity and concurrency values are surrounding factory evidence,
not producer-owned configuration, while its live-handle identity and zero
accessor counts are Go-only metadata rather than Rust accessor parity.

This slice adds no public producer configuration, clock, delay, or hook; no
periodic old-node ping producer, bootstrap or reseed injection,
sample-infohashes producer or worker, info-hash triage, get-peers, scrape,
metainfo request, torrent or source persistence, retry, restart, or backoff.
It does not compose the scheduler, ping or find worker, target rotator,
producer, or recursive discovery into one supervisor, `DhtRuntime`,
application, or process lifecycle. It exposes no producer counters through
Prometheus or other external metrics and adds no health, logging, application
or deployment configuration, external DHT traffic, live deployment, or
production rollout.

This slice supersedes slice thirty-two only where that slice excludes this
periodic producer, KTable oldest-node selection, five-second cutoff, ten-node
limit, immediate query, one-second cadence, shutdown policy, counters, and
oracle consumption. Slice thirty-two's shared-capacity, EOF-extension, direct
bypass, and cross-source-order contracts remain unchanged, and its exclusion
of a producer task remains exact because this producer owns only a caller-
polled future. It supersedes slice thirty-one only where the Rust producer is
absent and all Go producer facts are source-only: the two actual Go method rows
now have strict runtime bindings, while Go timer execution and forever-empty
cancellation remain source-only. It supersedes slices twenty-nine and thirty
only for this periodic oldest-node find-route producer. Periodic ping
maintenance, sample-infohashes production, bootstrap work, broader recursive
composition, runtime, application, deployment, and rollout boundaries remain
in force.

The thirty-sixth slice, landed by
`e9510862353fcac3c75b0059e67b1dffcbb9b6db`, adds the first owned partial
crawler maintenance composition over the already-frozen scheduler, shared
target, ping and `find_node` workers, recursive discovery, shared find input,
and periodic oldest-node producer:

- `DhtCrawlerMaintenanceSupervisor::new(discovery, client, table)` consumes the
  unique `DhtDiscoveryReceiver` and borrows the `DhtRuntimeClient` and `KTable`,
  cloning the latter two handles internally. The receiver's new crate-private
  weak-sender seam upgrades only while at least one strong producer for that
  exact discovery channel remains. Storing the weak sender does not itself
  delay queue EOF. A successful upgrade is moved into the `find_node` worker
  and intentionally extends scheduler-ingress EOF for recursive discovery;
- construction is typed and starts no task. If the exact discovery sender can
  no longer be recovered,
  `DhtCrawlerMaintenanceStartError::DiscoveryClosed` retains the supplied
  receiver. If initial target entropy fails,
  `DhtCrawlerMaintenanceStartError::TargetEntropy` retains both the receiver
  and exact `DhtCrawlerTargetError` source. `into_discovery` recovers the unique
  receiver from either variant. Target initialization occurs during
  construction, before `run`'s shutdown preflight, while no scheduler, worker,
  producer, or rotator child has started;
- after target initialization, the fixed wiring constructs the default
  discovered-node scheduler, clones its existing shared find-route input for
  the oldest-node producer, and then consumes each unique route receiver. The
  sample-infohashes receiver is explicitly closed and dropped, so this
  composition can route scheduler work only to ping or `find_node`; it creates
  no sample worker or producer and
  increments no successful `routed_sample_infohashes` count. The ping worker
  receives the ping route plus cloned weak client and table handles. The
  `find_node` worker receives the find route, the same client and table, the
  exact recovered discovery sender, and a clone of the initialized shared
  target. The target rotator retains the unique writer, while the periodic
  oldest-node producer receives the table and the scheduler's shared find
  input. All component capacities, concurrency limits, table behavior, query
  semantics, target cadence, producer cadence, and per-component terminal
  accounting remain those frozen by their earlier slices;
- `DhtCrawlerMaintenanceStatsHandle` is a cloneable sender-free bundle. Its
  `discovery` handle observes exact channel-global counters, including any
  runtime responder that owns an original sender and every recursive sender,
  rather than supervisor-local provenance. `scheduler` remains scheduler-only
  and its `routed_find_node` excludes direct oldest-producer commits. `ping`
  and `oldest_find` are local to those children, while `find_node` aggregates
  every node consumed after
  either the scheduler or producer commits to their shared route. The bundle
  adds no target counters, transactional aggregate snapshot, source labels, or
  external metrics export;
- consuming `run` single-polls caller shutdown before invoking any child
  factory or spawning any child task. A ready shutdown returns
  `DhtCrawlerMaintenanceSupervisorExit::ShutdownBeforeStart`, performs no
  scheduler, worker, producer, or rotation work, and drops the constructed
  components. This is not a claim of zero entropy: the initial target was
  already obtained by `new`. Once the preflight remains pending, exactly five
  tasks are spawned into one owned `JoinSet`: scheduler, ping worker,
  `find_node` worker, oldest-node producer, and target rotator;
- the five tasks wait on clones of one internal watch receiver. Caller
  shutdown is the first biased outer branch ahead of the next observed child
  join. When caller shutdown wins, including a ready tie with a child result,
  it remains the primary `DhtCrawlerMaintenanceSupervisorExit::Shutdown`
  cause. A concrete normal child result that was already complete is still
  retained during cleanup. When a child result wins first,
  `DhtCrawlerMaintenanceSupervisorExit::Failed { first, children }` identifies
  that first observed child through one of the exact `Scheduler`, `Ping`,
  `FindNode`, `OldestFind`, or `Target` identities. No deterministic priority
  is promised among simultaneously ready children.
  Both paths broadcast shutdown once and then fully join every remaining
  child. Signalling all children before awaiting any one of them breaks the
  intentional discovery-sender and find-input EOF cycles without imposing a
  fragile sequential cleanup order;
- `DhtCrawlerMaintenanceChildExits` is a fixed complete record of the exact
  scheduler, ping, `find_node`, oldest-producer, and target-rotator results.
  Cleanup does not coerce an already-completed result into fabricated shutdown
  counts. The supervisor resumes the first observed child-panic payload only
  after draining all siblings. Unexpected child cancellation, another join
  failure, a duplicate child result, or a missing fixed child result is an
  invariant panic after cleanup rather than a fabricated typed exit. Dropping
  a started `run` future drops its `JoinSet`, aborting all owned child tasks
  instead of detaching them; that cancellation is not a normal terminal return
  and provides no five-child exit record; and
- the recovered discovery sender keeps scheduler ingress open even after the
  original producer, including a runtime responder, disappears, and the oldest
  producer's find-input sender keeps the find route open while the producer
  remains live. Those cycles are deliberate within this composition and are
  released on common shutdown, first-child failure cleanup, or drop. They do
  not establish
  ownership or liveness observation of the UDP runtime.

This composition adds no new Go oracle or fixture. It binds the existing
strict component evidence at these exact fixture SHA-256 values:

- shared sought target:
  `683162fe0da0c9fe8f39b80fffaaa3aae4f98683a0c1579b521eeb69f9aa1ea4`;
- discovered-node scheduler:
  `ae6d867378a227284aa0cd93e9120d70afbec1c5e3b19a9f64e09edace4190e0`;
- ping worker:
  `26d403becff0caeb0a27ec9027a366d51e19cdb7129ff05715cf24a6d2e1b040`;
- `find_node` worker:
  `e126ad26fd342b14ae0416b3610d991f927dbe9381ac11609ebeba96d67870b7`;
  and
- periodic oldest-node producer:
  `06e2ac78f73418038c946fdc5f3562654e130623fcf88e907c1c4e07112505cc`.

Those fixtures and their strict Rust consumers remain authoritative only for
their bounded component contracts. The Go lifecycle evidence in
`internal/dhtcrawler/crawler.go` at
`ae6ca2484a57231a08351629c21fdc0a875f2272bfd4ad42a4e5386be86500b6`
and `internal/dhtcrawler/factory.go` at
`ed34129835773817736d70e74c7c884e5b9197e35741dee922ee9a5d691288a6`
shows a materially broader crawler: the factory detaches `go c.start()`,
`start` detaches its worker goroutines, waits only for `stopped`, and cancels
their shared context on return without joining those goroutines. Rust's fixed
five-task ownership, biased preflight, complete joins, panic propagation, and
drop-abort behavior are deliberate lifecycle hardening, not a claim of exact
Go composition parity. The sought-target source row already freezes the
relevant Go start and factory shapes; no new runtime row measures or proves the
combined Go lifecycle.

`DhtCrawlerMaintenanceSupervisor` deliberately does not own or observe
`DhtRuntime`. Its cloned `DhtRuntimeClient` is weak and cannot keep the UDP
socket or runtime task alive. Runtime termination is not automatically a
maintenance-child exit, while maintenance failure does not stop the runtime;
an outer owner must propagate the desired joint lifecycle through `run`'s
shutdown future and the runtime's consuming shutdown or wait API. This slice
adds no runtime-plus-maintenance supervisor, application or process worker,
active flag, health signal, restart, retry, or backoff policy.

This slice also adds no sample-infohashes worker or producer, periodic old-node
ping producer, bootstrap or reseed injection, info-hash triage, get-peers,
scrape, metainfo request, torrent or source persistence, database integration,
Prometheus registration, external logging or health integration, application
or deployment configuration, external DHT traffic, live deployment, or
production rollout. Closing the sample route is a deliberate partial boundary,
not evidence for Go's full crawler topology or higher pipeline.

This slice supersedes slice thirty-five only where that slice excludes
composition of the scheduler, ping and `find_node` workers, target rotator,
oldest-node producer, and recursive discovery in one supervisor. All producer
semantics and exclusions unrelated to that composition remain unchanged. It
supersedes slice thirty-two only where that slice excludes a composed shared
find-input producer lifecycle, while preserving its shared-capacity, direct-
bypass, EOF-extension, and cross-source-order contracts. It supersedes slice
thirty-one only where that slice says recursive offers are not composed back
through the scheduler and no recursive crawler feedback loop exists. It
supersedes slice thirty only where the target rotator and `find_node` consumer
were not jointly owned, slice twenty-nine only where the ping worker was not
jointly owned, and slice twenty-eight only where the scheduler had no crawler
maintenance supervisor. Every remaining sample, bootstrap, higher-pipeline,
runtime, application, deployment, and rollout exclusion stays in force.

The thirty-seventh slice, landed by
`e709e9153a1584fce9ab2ff2b3cdaac9c13e18d7`, adds only a shared producer
capability for the existing bounded ping route:

- `DhtDiscoveredNodeScheduler::ping_input(&self)` lazily clones the
  scheduler's existing ping-route sender and returns a public, cloneable,
  `Send + Sync` `DhtDiscoveredNodePingInput`. Merely constructing the scheduler
  creates no external ping handle. The capability adds no task, queue, or
  capacity: scheduler-origin and directly supplied nodes share the one
  configured ping queue, whose default capacity remains ten;
- `DhtDiscoveredNodePingInput::send(&self, node)` asynchronously waits for that
  shared capacity and returns
  `Result<(), DhtDiscoveredNodePingInputClosed>`. A successful return means the
  node was committed to the queue, not that the ping worker dequeued it,
  admitted its query, or sent a datagram. Receiver closure returns the exact
  uncommitted `RoutingNode`, recoverable through
  `DhtDiscoveredNodePingInputClosed::into_node`. The crate-private `closed`
  waiter observes route closure without consuming capacity and is reserved for
  a future periodic old-node ping producer;
- sequential awaited sends by one producer preserve program order. Competing
  send or reserve futures enter Tokio's FIFO waiter queue in runtime
  registration order. This seam assigns no source priority and promises no
  deterministic cross-producer or scheduler-versus-direct source order;
- each pending `send` future owns one node outside the bounded queue. The seam
  does not bound how many such futures callers may create, so the queue's
  capacity is not a whole-composition retention bound. Dropping a pending
  future commits nothing, drops its future-owned node, and loses its waiter
  position; a caller that needs to retry must retain a separate copy. Once a
  send succeeds, cancellation cannot retract the queued node;
- without a requested ping-input handle, scheduler return or drop retains the
  prior drain-then-EOF behavior for all three routes. A live ping-input handle
  or clone intentionally keeps only the ping route open after the
  scheduler-owned sender is gone; dropping the final external clone then
  permits drain-then-EOF. Explicitly closing or dropping the unique ping-route
  receiver wakes registered and later sends with the typed closed error. The
  owned ping worker's shutdown closes that receiver before draining already
  queued work: a node from a pending direct send is returned as uncommitted and
  is absent from the worker's queued-drop accounting, while a previously
  committed item remains worker-owned and is drained and counted under slice
  twenty-nine;
- direct ping-input sends bypass scheduler discovery batching, address
  deduplication, address projection, KTable known-address filtering, and route
  selection. They also bypass every scheduler counter. In particular,
  `routed_ping` continues to count only scheduler-origin commits. The ping
  worker consumes one source-free `RoutingNode` stream, so its dequeued, query,
  table-command, mismatch, and terminal counters aggregate committed work from
  every source without producer attribution; and
- focused Rust gates preserve the lazy default EOF, clone-extended ping-only
  EOF, shared scheduler-versus-direct capacity, sequential order, registered-
  send cancellation, receiver close and drop recovery, exact pending-send
  rejection during ping-worker shutdown, public `Send + Sync`, and the split
  between direct work and scheduler-origin counters. A known KTable node sent
  directly through the capability reaches the real ping worker, proving that
  this seam does not silently reapply scheduler filtering.

This slice adds no periodic old-node ping producer, KTable oldest-node
selection, dropped-handle or recentness recheck, fifteen-minute threshold,
producer cadence, producer task, producer shutdown policy, or producer
counters. It adds and consumes no Go oracle row or fixture; slice twenty-nine's
dropped and recent live-handle rows remain deferred producer evidence rather
than behavior of this state-free input seam. It adds no configuration or
source-provenance field to the scheduler, route, ping worker, or stats.

The partial `DhtCrawlerMaintenanceSupervisor` from slice thirty-six does not
request `ping_input`, does not construct an old-node ping producer, and remains
the same fixed five-child composition. Its scheduler-owned ping sender and ping
worker therefore retain their existing supervisor shutdown and EOF behavior.
This seam is not wired into `DhtRuntime`, an application worker, or a process
lifecycle, and it adds no sample-infohashes work, bootstrap or reseed work,
higher crawler pipeline, retry or restart policy, external metrics, health or
logging integration, deployment configuration, external DHT traffic, live
deployment, or production rollout.

This slice supersedes slice twenty-eight only where that slice says the ping
route necessarily reaches EOF when the scheduler returns or is dropped: that
remains exact when no ping-input clone survives. It supersedes slice
twenty-nine only where scheduler-origin routing was the ping worker's sole
possible queue provenance and a second ping-route producer capability was
absent. All ping query, table, lifecycle, concurrency, and statistics semantics
remain unchanged. It parallels but does not supersede slice thirty-two's
independent find-input contract, and it does not supersede slice thirty-five's
periodic oldest-node find producer or slice thirty-six's fixed supervisor
wiring. Every periodic ping-producer, sample, bootstrap, higher-pipeline,
runtime, application, deployment, and rollout exclusion remains in force.

The thirty-eighth slice freezes the periodic oldest-node ping producer through
the Go oracle at `2df7b3f10b4a85e85f5353e317be9b13d5ff3179`, the Rust
implementation at `b0fee76a3fc9633444526876844549503840ccc2`, and the strict
consumer at `a6e4c78d9b2b260e83c1d18090f219046283fd4d`:

- `DhtOldestNodePingProducer::new` consumes the shared `KTable` and one
  `DhtDiscoveredNodePingInput`, and returns the owned producer with a
  sender-free `DhtOldestNodePingProducerStatsHandle`. Its public surface has
  no configuration variant: the leading query delay is fixed at ten seconds,
  the old-peer threshold at fifteen minutes, and the oldest-node query is
  uncapped. The consuming `run` future starts and spawns no task. Its live
  ping-input sender delays ping-route EOF until the producer or its polled run
  future exits or is dropped;
- every loop constructs a fresh, cancellation-aware ten-second Tokio sleep
  before querying. Ready shutdown is biased first, input closure second, and
  delay completion last. A winning shutdown or closure performs no query.
  Focused paused-time gates fix the 9,999-millisecond pending boundary and
  progress at the following millisecond. Delayed polling produces one query
  and then a fresh full delay rather than catch-up ticks;
- after delay completion, Rust reads its monotonic clock, floors subtraction
  nonpanickingly at the oldest representable `Instant`, and synchronously calls
  `KTable::get_oldest_nodes(cutoff, None)`. The completed-query and returned-
  occurrence counters advance only after that call returns. The actual table
  gate fixes the strict query boundary: a last response strictly before the
  cutoff is selected, while a response exactly at or after the cutoff is not.
  Rust returns equal-time handles in ID tie-break order; Go sorts by `Time`
  while leaving equal-time order unspecified;
- returned live handles are processed sequentially. For each occurrence,
  ready shutdown wins, then input closure, then acquisition of one permit from
  slice thirty-seven's existing shared ping capacity. A pending reservation
  retains the live handle but neither rechecks nor snapshots it, so address,
  dropped-state, and response-time mutations while capacity is blocked remain
  visible after reservation. Cancelling the pending run releases its waiter
  and sender without consuming capacity or committing a node;
- once a permit is acquired, no asynchronous cancellation point remains for
  that occurrence. Rust first checks the retained handle's dropped state. A
  dropped handle is counted and the unused permit releases its capacity.
  Otherwise Rust computes a fresh fifteen-minute cutoff and skips a response
  strictly newer than it; an exact-cutoff response remains eligible. An
  eligible handle is then projected to one immutable `RoutingNode` and
  committed synchronously through the permit. Closing the receiver after
  permit acquisition cannot revoke that authority. The returned Tokio sender
  is dropped during delivery, so the consumed permit does not independently
  extend route EOF;
- this post-capacity guard and snapshot are a deliberate architectural delta.
  Go's producer sends each same live `ktable.Node` without calling any node
  accessor; `runPing` later checks `Dropped` and strict recentness only after
  the buffered lane has dequeued the node and acquired worker concurrency.
  Rust's state-free ping worker cannot retain that live handle, so the checks
  move to the producer after route reservation but before immutable queue
  commit. This is not a claim that route capacity is the same instant as Go's
  consumer semaphore. Subsequent address mutations cannot change the queued
  Rust node;
- producer commits bypass scheduler batching, address deduplication and
  projection, known-node filtering, routing choice, and every scheduler
  counter. `routed_ping` remains scheduler-origin only, while the ping worker
  continues to aggregate every committed source without provenance. One
  producer preserves selected order, but no priority or deterministic order is
  promised against scheduler commits or other ping-input clones; and
- the exhaustive public exits are
  `DhtOldestNodePingProducerExit::Shutdown { selected_dropped }` and
  `InputClosed { selected_dropped }`. The count is the exact current selected
  occurrence plus its later selected suffix that was neither classified nor
  committed. A committed prefix and dropped or recent decisions are not
  retracted. Receiver closure is typed, including during the leading delay or
  a blocked reservation. Dropping a run future detaches nothing and releases
  its sender and any unused permit, but is not a normal terminal return and
  carries no terminal-conservation promise.

`DhtOldestNodePingProducerStats` exposes seven saturating monotonic counters:
completed `table_queries`, returned `selected` occurrences,
`dropped_skipped`, `recent_skipped`, committed `queued` occurrences,
`input_closed_dropped`, and `shutdown_dropped`. Snapshots load each field
independently with relaxed ordering and are not transactional. After normal
terminal return,
`selected == dropped_skipped.saturating_add(recent_skipped).saturating_add(
queued).saturating_add(input_closed_dropped).saturating_add(
shutdown_dropped)`. The exit's `selected_dropped` is the per-run suffix added
to the corresponding terminal counter; focused gates saturate all seven
counters without wrapping.

The strict child-module consumer denies unknown fields at every fixture level
and freezes the exact ordered row IDs
`production_source_factory_and_lifecycle_contract`,
`already_cancelled_returns_before_initial_timer_and_query`, and
`first_timer_ordered_prefix_then_cancel_at_blocked_third_send` with
classifications `SOURCE_ONLY`, `RUNTIME_EXACT`, and `RUNTIME_EXACT`. The
fixture SHA-256 is
`d300e4606f9811f402af6d835748d09dbc59434f733a28079ac0df5e2f99ae5a`.
It consumes every source, oracle, input, result, accessor, event, and optional
field; binds Rust's fixed delay and threshold, scheduler ping capacity ten,
and ping-worker concurrency ten; and checks these eight embedded Go sources:

- `internal/concurrency/buffered_concurrent_channel.go`:
  `4be882800ec66d0c1709319fe029d61773c3f4a37bdb409e3a2f7d5d415d954c`;
- `internal/dhtcrawler/config.go`:
  `b3cac15378cdca0f21c5f21f37aeb0679815d5bacd16bfa0c3bac2af56db87ef`;
- `internal/dhtcrawler/crawler.go`:
  `ae6ca2484a57231a08351629c21fdc0a875f2272bfd4ad42a4e5386be86500b6`;
- `internal/dhtcrawler/factory.go`:
  `ed34129835773817736d70e74c7c884e5b9197e35741dee922ee9a5d691288a6`;
- `internal/dhtcrawler/ping.go`:
  `45561d97a79060e6b96bc81f7d83491195e4ff60fbdc9460d9973675547804a2`;
- `internal/protocol/dht/ktable/node.go`:
  `93ed9a76a7cd0f50ee3ad255c6e77a8d19e5fe17081edc6238c5efab4983b3c3`;
- `internal/protocol/dht/ktable/query.go`:
  `103ec27a7904bdbbbd91f3ea1dae1f4d6ea3b3d6652757a6ab8ddbf598a7060e`;
  and
- `internal/protocol/dht/ktable/table.go`:
  `68e3caf4394b2692fd9358224cce2b70ae3d90d920097bd28885b6b3bb77848f`.

The source row freezes Go's leading `time.After`, cancellation-aware leading
and per-node selects, strict unbounded oldest-node query, returned order, zero
producer accessor calls, detached and unjoined lifecycle, production lane and
worker bounds, and the later `runPing` dropped and strict-recent guards after
semaphore acquisition. The factory's real ten-second timer, equal-ready Go
select outcomes, equal-time table order, and exact callback scheduling remain
source evidence rather than runtime observations. An empty Go query returns to
a fresh cancellation-aware leading select.

The pre-cancelled actual Go row returns before its positive sixty-second timer,
performs no query or lane call, and records only start and return. Its Rust
replay deliberately uses biased pre-ready shutdown, reads no clock, commits no
work, returns `Shutdown { selected_dropped: 0 }`, and leaves all counters zero.
The ordered-prefix Go row observes a shortened positive ten-millisecond timer,
one runtime-bracketed unbounded query, exact A-through-D return order, A and B
as the same live interface handles, entry into the third `In` call, and
cancellation that abandons C and D; every producer node accessor count is zero.

The corresponding Rust replay uses an injected leading-delay gate and a
positive capacity-two Tokio route. A through D are timestamped one through
four nanoseconds before the fixed query cutoff, while a hard-coded fifth
sentinel exactly at the cutoff is excluded. Rust therefore records one query,
four selected occurrences, reserves and immutably commits A then B, and blocks
before the post-reserve callback for C; the observed post-reserve indices are
exactly `[0, 1]` and the producer clock is read once for the query and once for
each committed node. Biased shutdown then returns
`Shutdown { selected_dropped: 2 }`, drains only A and B in order, and finishes
with `table_queries=1`, `selected=4`, `queued=2`, `shutdown_dropped=2`, and all
other outcome counters zero.

The consumer explicitly classifies nine facts as Go-only metadata: fixture
tokens; runtime-row interval values; runtime-bracketed cutoff and waited-delay
booleans; lane `In` call counts; same-interface delivery identity; producer
accessor counts; event logs; and the post-semaphore consumer-guard position.
It freezes twelve deliberate Rust deltas: biased ready-event ordering; owned,
taskless lifecycle; fresh monotonic delay without catch-up; Go limit zero to
Rust `None`; nonpanicking cutoff flooring; deterministic ID tie-breaks;
positive Tokio capacity with typed closure; relocation of dropped and recent
guards to post-reservation producer code; one immutable post-capacity snapshot;
no recheck or snapshot for a still-blocked occurrence; typed terminal exits;
and seven saturating component-local counters with terminal conservation.

This slice adds no public producer configuration, clock, delay, hook, route
capacity, concurrency limit, or source-provenance field. It does not change
ping query, response-ID, KTable Put or Drop, query ownership, worker shutdown,
or worker statistics semantics from slice twenty-nine. It creates no producer
task and adds no retry, restart, or backoff. It adds no sample-infohashes,
bootstrap or reseed work, higher crawler pipeline, persistence, Prometheus
registration, logging or health integration, application or deployment
configuration, external DHT traffic, live deployment, or production rollout.

In particular, the fixed five-child `DhtCrawlerMaintenanceSupervisor` from
slice thirty-six still does not request the shared ping input or construct this
producer. This slice does not own or observe `DhtRuntime`, join the producer to
that supervisor, or establish a runtime, application, or process lifecycle.

This slice supersedes slice thirty-seven only where that slice reserves the
ping-input lifecycle hook for a future producer and excludes oldest-node
selection, the post-capacity live-handle guard and snapshot, fixed cadence,
typed exits, counters, and oracle consumption. Slice thirty-seven's shared
capacity, direct-bypass, EOF-extension, waiter, public-send, and cross-source-
order contracts remain unchanged; its new crate-private reservation permit is
not a public API expansion. It supersedes slice twenty-nine only where the Go
dropped and recent live-handle behavior remained deferred producer evidence:
Rust now makes the equivalent decisions in the producer rather than adding
live state to the ping worker. All ping-worker contracts remain unchanged. It
supersedes slice thirty-six only where that slice globally excludes the
existence of a periodic old-node ping producer; the supervisor's exact fixed
five-child wiring and lack of a ping-input EOF cycle remain authoritative.
Every remaining sample, bootstrap, higher-pipeline, runtime, application,
deployment, and rollout boundary remains in force.
