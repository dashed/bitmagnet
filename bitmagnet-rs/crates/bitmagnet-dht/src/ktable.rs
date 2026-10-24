use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use crate::{
    Id20, KTableCore, KTableHash, KTableHashLookup, KTableHashPeer, KTableReverseInfo, RoutingNode,
    RoutingPutResult, NODE_TABLE_CAPACITY,
};

const SAMPLE_HASH_LIMIT: usize = 20;
const SAMPLE_TOTAL_TARGET: usize = 40;
const EMPTY_SAMPLE_PENALTY: Duration = Duration::from_secs(5 * 60);

/// Injectable monotonic clock for Go-compatible temporal KTable operations.
///
/// `now` must be fast, nonblocking, non-panicking, and must not re-enter any
/// [`KTable`] or [`KTableNodeHandle`] that shares this clock. Calls occur while
/// internal synchronization is held. Violating this contract can deadlock or
/// deliberately poison the affected state so later access fails closed.
pub trait KTableClock: Send + Sync {
    fn now(&self) -> Instant;
}

/// The production monotonic clock, which satisfies [`KTableClock`]'s safety
/// contract.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemKTableClock;

impl KTableClock for SystemKTableClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// One node's remembered support for BEP-51 `sample_infohashes`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KTableBep51Support {
    #[default]
    Unknown,
    Yes,
    No,
}

/// Go-shaped temporal node operations applied in slice order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KTableNodeOption {
    Responded,
    Bep51Support(bool),
    SampleInfoHashesResponse {
        discovered_num: i64,
        total_num: i64,
        next_sample_at: Instant,
    },
}

/// A void command for one atomic [`KTable::batch_command`] call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KTableCommand {
    PutNode {
        node: RoutingNode,
        options: Vec<KTableNodeOption>,
    },
    DropNode {
        id: Id20,
    },
    DropAddr {
        addr: SocketAddr,
    },
    PutHash {
        id: Id20,
        peers: Vec<KTableHashPeer>,
    },
}

/// Go's hash-or-live-nodes result without an invalid nil-hash state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KTableLookup {
    Found(KTableHash),
    ClosestNodes(Vec<KTableNodeHandle>),
}

/// Deterministically normalized `SampleHashesAndNodes` output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KTableSampleHashesAndNodes {
    pub hashes: Vec<KTableHash>,
    pub nodes: Vec<KTableNodeHandle>,
    pub total_hashes: usize,
}

#[derive(Debug)]
struct KTableNodeState {
    node: RoutingNode,
    last_responded_at: Option<Instant>,
    dropped: bool,
    bep51_support: KTableBep51Support,
    sampled_num: i64,
    last_discovered_num: i64,
    total_num: i64,
    next_sample_infohashes_at: Option<Instant>,
}

/// A generation-specific live node handle.
///
/// Clones observe duplicate address and option updates to the same stored node.
/// Dropping that node marks every clone dropped. Re-adding the same ID creates
/// a distinct handle, leaving the prior generation dropped, matching retained
/// Go `Node` interface values.
#[derive(Clone)]
pub struct KTableNodeHandle {
    state: Arc<RwLock<KTableNodeState>>,
    clock: Arc<dyn KTableClock>,
}

impl fmt::Debug for KTableNodeHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.read();
        formatter
            .debug_struct("KTableNodeHandle")
            .field("node", &state.node)
            .field("last_responded_at", &state.last_responded_at)
            .field("dropped", &state.dropped)
            .field("bep51_support", &state.bep51_support)
            .field("sampled_num", &state.sampled_num)
            .field("last_discovered_num", &state.last_discovered_num)
            .field("total_num", &state.total_num)
            .field(
                "next_sample_infohashes_at",
                &state.next_sample_infohashes_at,
            )
            .finish_non_exhaustive()
    }
}

impl PartialEq for KTableNodeHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
}

impl Eq for KTableNodeHandle {}

impl KTableNodeHandle {
    fn new(node: RoutingNode, clock: Arc<dyn KTableClock>) -> Self {
        Self {
            state: Arc::new(RwLock::new(KTableNodeState {
                node,
                last_responded_at: None,
                dropped: false,
                bep51_support: KTableBep51Support::Unknown,
                sampled_num: 0,
                last_discovered_num: 0,
                total_num: 0,
                next_sample_infohashes_at: None,
            })),
            clock,
        }
    }

    fn read(&self) -> RwLockReadGuard<'_, KTableNodeState> {
        self.state.read().expect("KTable node state lock poisoned")
    }

    fn write(&self) -> RwLockWriteGuard<'_, KTableNodeState> {
        self.state.write().expect("KTable node state lock poisoned")
    }

    #[must_use]
    pub fn id(&self) -> Id20 {
        self.read().node.id
    }

    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.read().node.addr
    }

    #[must_use]
    pub fn routing_node(&self) -> RoutingNode {
        self.read().node
    }

    #[must_use]
    pub fn last_responded_at(&self) -> Option<Instant> {
        self.read().last_responded_at
    }

    #[must_use]
    pub fn dropped(&self) -> bool {
        self.read().dropped
    }

    #[must_use]
    pub fn bep51_support(&self) -> KTableBep51Support {
        self.read().bep51_support
    }

    #[must_use]
    pub fn sampled_num(&self) -> i64 {
        self.read().sampled_num
    }

    #[must_use]
    pub fn last_discovered_num(&self) -> i64 {
        self.read().last_discovered_num
    }

    #[must_use]
    pub fn total_num(&self) -> i64 {
        self.read().total_num
    }

    #[must_use]
    pub fn next_sample_infohashes_at(&self) -> Option<Instant> {
        self.read().next_sample_infohashes_at
    }

    /// Evaluate Go's strict BEP-51 predicate using this handle's table clock.
    ///
    /// Dropped state is deliberately not part of the predicate. A retained old
    /// generation remains observable, while table queries enumerate only the
    /// current generation.
    #[must_use]
    pub fn is_sample_infohashes_candidate(&self) -> bool {
        self.is_sample_infohashes_candidate_at(self.clock.now())
    }

    fn is_sample_infohashes_candidate_at(&self, now: Instant) -> bool {
        let state = self.read();
        state.bep51_support != KTableBep51Support::No
            && state
                .next_sample_infohashes_at
                .is_none_or(|next| next < now)
            && state.last_responded_at.is_none_or(|responded| {
                now.checked_duration_since(responded)
                    .is_some_and(|elapsed| elapsed > Duration::from_secs(5))
            })
    }

    fn update_node(&self, node: RoutingNode) {
        self.write().node = node;
    }

    fn apply_options(&self, options: &[KTableNodeOption], clock: &dyn KTableClock) {
        let mut state = self.write();
        for option in options {
            match *option {
                KTableNodeOption::Responded => state.last_responded_at = Some(clock.now()),
                KTableNodeOption::Bep51Support(supported) => {
                    state.bep51_support = if supported {
                        KTableBep51Support::Yes
                    } else {
                        KTableBep51Support::No
                    };
                }
                KTableNodeOption::SampleInfoHashesResponse {
                    discovered_num,
                    total_num,
                    next_sample_at,
                } => {
                    state.sampled_num = state.sampled_num.wrapping_add(discovered_num);
                    state.last_discovered_num = discovered_num;
                    state.total_num = state.total_num.wrapping_add(total_num);
                    state.next_sample_infohashes_at = Some(if discovered_num == 0 {
                        saturating_add_five_minutes(next_sample_at.max(clock.now()))
                    } else {
                        next_sample_at
                    });
                }
            }
        }
    }

    fn mark_dropped(&self) {
        self.write().dropped = true;
    }
}

fn saturating_add_five_minutes(value: Instant) -> Instant {
    if let Some(result) = value.checked_add(EMPTY_SAMPLE_PENALTY) {
        return result;
    }

    let mut low_nanos = 0_u64;
    let mut high_nanos = 5_u64 * 60 * 1_000_000_000;
    while low_nanos < high_nanos {
        let midpoint = low_nanos + (high_nanos - low_nanos).div_ceil(2);
        if value.checked_add(Duration::from_nanos(midpoint)).is_some() {
            low_nanos = midpoint;
        } else {
            high_nanos = midpoint - 1;
        }
    }
    value
        .checked_add(Duration::from_nanos(low_nanos))
        .unwrap_or(value)
}

struct KTableState {
    core: KTableCore,
    node_handles: HashMap<Id20, KTableNodeHandle>,
}

impl KTableState {
    fn new(origin: Id20) -> Self {
        Self {
            core: KTableCore::new(origin),
            node_handles: HashMap::with_capacity(NODE_TABLE_CAPACITY),
        }
    }

    fn put_node(
        &mut self,
        node: RoutingNode,
        options: &[KTableNodeOption],
        clock: &Arc<dyn KTableClock>,
    ) -> RoutingPutResult {
        let result = self.core.put_node(node);
        match result {
            RoutingPutResult::Rejected => {}
            RoutingPutResult::Accepted => {
                if let Some(stored) = self.core.node(node.id) {
                    let handle = KTableNodeHandle::new(stored, Arc::clone(clock));
                    handle.apply_options(options, clock.as_ref());
                    self.node_handles.insert(node.id, handle);
                }
            }
            RoutingPutResult::AlreadyExists => {
                if let (Some(stored), Some(handle)) =
                    (self.core.node(node.id), self.node_handles.get(&node.id))
                {
                    handle.update_node(stored);
                    handle.apply_options(options, clock.as_ref());
                }
            }
        }
        result
    }

    fn drop_node(&mut self, id: Id20) -> bool {
        if !self.core.drop_node(id) {
            return false;
        }
        self.remove_live_handle(id);
        true
    }

    fn drop_addr(&mut self, addr: SocketAddr) -> bool {
        let Some(id) = self.core.node_id_for_addr(addr) else {
            return false;
        };
        if !self.core.drop_addr(addr) {
            return false;
        }
        self.remove_live_handle(id);
        true
    }

    fn remove_live_handle(&mut self, id: Id20) {
        if let Some(handle) = self.node_handles.remove(&id) {
            handle.mark_dropped();
        }
    }

    fn apply_command(&mut self, command: &KTableCommand, clock: &Arc<dyn KTableClock>) {
        match command {
            KTableCommand::PutNode { node, options } => {
                self.put_node(*node, options, clock);
            }
            KTableCommand::DropNode { id } => {
                self.drop_node(*id);
            }
            KTableCommand::DropAddr { addr } => {
                self.drop_addr(*addr);
            }
            KTableCommand::PutHash { id, peers } => {
                self.core.put_hash(*id, peers);
            }
        }
    }
}

/// Shared, synchronized KTable facade with live temporal node handles.
///
/// Every public table operation acquires one short state lock. Batch commands
/// retain one write lock for their whole void sequence; no asynchronous work
/// occurs while any lock is held.
#[derive(Clone)]
pub struct KTable {
    state: Arc<RwLock<KTableState>>,
    clock: Arc<dyn KTableClock>,
}

impl KTable {
    #[must_use]
    pub fn new(origin: Id20) -> Self {
        Self::with_clock(origin, Arc::new(SystemKTableClock))
    }

    #[must_use]
    /// Construct a shared table with an injected clock.
    ///
    /// The clock must be monotonic, fast, nonblocking, non-panicking, and
    /// non-reentrant into this table, its clones, or handles sharing the clock.
    /// Clock calls occur while internal locks are held; a panic poisons affected
    /// state and all subsequent access fails closed.
    pub fn with_clock(origin: Id20, clock: Arc<dyn KTableClock>) -> Self {
        Self {
            state: Arc::new(RwLock::new(KTableState::new(origin))),
            clock,
        }
    }

    fn read(&self) -> RwLockReadGuard<'_, KTableState> {
        self.state.read().expect("KTable state lock poisoned")
    }

    fn write(&self) -> RwLockWriteGuard<'_, KTableState> {
        self.state.write().expect("KTable state lock poisoned")
    }

    #[must_use]
    pub fn origin(&self) -> Id20 {
        self.read().core.origin()
    }

    #[must_use]
    pub fn node_count(&self) -> usize {
        self.read().core.node_count()
    }

    #[must_use]
    pub fn hash_count(&self) -> usize {
        self.read().core.hash_count()
    }

    #[must_use]
    pub fn reverse_address_count(&self) -> usize {
        self.read().core.reverse_address_count()
    }

    #[must_use]
    pub fn reverse_info(&self, addr: SocketAddr) -> Option<KTableReverseInfo> {
        self.read().core.reverse_info(addr)
    }

    #[must_use]
    pub fn node_handle(&self, id: Id20) -> Option<KTableNodeHandle> {
        self.read().node_handles.get(&id).cloned()
    }

    pub fn put_node(&self, node: RoutingNode) -> RoutingPutResult {
        self.put_node_with_options(node, &[])
    }

    pub fn put_node_with_options(
        &self,
        node: RoutingNode,
        options: &[KTableNodeOption],
    ) -> RoutingPutResult {
        self.write().put_node(node, options, &self.clock)
    }

    pub fn drop_node(&self, id: Id20) -> bool {
        self.write().drop_node(id)
    }

    /// Drop the node currently indexed for the address's IP and scope. The
    /// supplied port and IPv6 flowinfo do not participate in reverse identity.
    pub fn drop_addr(&self, addr: SocketAddr) -> bool {
        self.write().drop_addr(addr)
    }

    pub fn put_hash(&self, id: Id20, peers: &[KTableHashPeer]) -> RoutingPutResult {
        self.write().core.put_hash(id, peers)
    }

    /// Execute every command under one write lock and discard individual
    /// results, matching Go's void `BatchCommand` surface.
    pub fn batch_command(&self, commands: &[KTableCommand]) {
        let mut state = self.write();
        for command in commands {
            state.apply_command(command, &self.clock);
        }
    }

    #[must_use]
    pub fn hash(&self, id: Id20) -> Option<KTableHash> {
        self.read().core.hash(id)
    }

    #[must_use]
    pub fn closest_nodes(&self, id: Id20) -> Vec<KTableNodeHandle> {
        let state = self.read();
        state
            .core
            .closest_nodes(id)
            .into_iter()
            .filter_map(|node| state.node_handles.get(&node.id).cloned())
            .collect()
    }

    #[must_use]
    pub fn get_hash_or_closest_nodes(&self, id: Id20) -> KTableLookup {
        let state = self.read();
        match state.core.get_hash_or_closest_nodes(id) {
            KTableHashLookup::Found(hash) => KTableLookup::Found(hash),
            KTableHashLookup::ClosestNodes(nodes) => KTableLookup::ClosestNodes(
                nodes
                    .into_iter()
                    .filter_map(|node| state.node_handles.get(&node.id).cloned())
                    .collect(),
            ),
        }
    }

    #[must_use]
    pub fn filter_known_addrs(&self, addrs: &[SocketAddr]) -> Vec<SocketAddr> {
        self.read().core.filter_known_addrs(addrs)
    }

    /// Nodes whose last response is strictly before `cutoff`, oldest first.
    /// `None` leaves the result uncapped.
    #[must_use]
    pub fn get_oldest_nodes(
        &self,
        cutoff: Instant,
        limit: Option<NonZeroUsize>,
    ) -> Vec<KTableNodeHandle> {
        let state = self.read();
        let mut nodes = state
            .node_handles
            .values()
            .filter(|handle| {
                handle
                    .last_responded_at()
                    .is_none_or(|responded| responded < cutoff)
            })
            .cloned()
            .collect::<Vec<_>>();
        nodes.sort_by_key(|handle| (handle.last_responded_at(), handle.id()));
        if let Some(limit) = limit {
            nodes.truncate(limit.get());
        }
        nodes
    }

    /// Return eligible BEP-51 nodes using one clock read per visited candidate.
    #[must_use]
    pub fn get_nodes_for_sample_infohashes(&self, limit: NonZeroUsize) -> Vec<KTableNodeHandle> {
        let state = self.read();
        let mut ordered = state.node_handles.values().cloned().collect::<Vec<_>>();
        ordered.sort_by_key(KTableNodeHandle::id);
        let mut candidates = Vec::with_capacity(limit.get());
        for handle in ordered {
            if handle.is_sample_infohashes_candidate_at(self.clock.now()) {
                candidates.push(handle);
                if candidates.len() == limit.get() {
                    break;
                }
            }
        }
        candidates
    }

    /// Apply Go's 20-hash then `40 - selected_hashes`-node sampling policy.
    ///
    /// Go chooses map prefixes. Rust sorts hashes and current live nodes by ID
    /// before taking the same cardinalities, yielding a stable valid subset.
    #[must_use]
    pub fn sample_hashes_and_nodes(&self) -> KTableSampleHashesAndNodes {
        let state = self.read();
        let total_hashes = state.core.hash_count();
        let mut hashes = state.core.hashes_by_id();
        hashes.truncate(SAMPLE_HASH_LIMIT);
        let node_limit = SAMPLE_TOTAL_TARGET - hashes.len();
        let mut nodes = state.node_handles.values().cloned().collect::<Vec<_>>();
        nodes.sort_by_key(KTableNodeHandle::id);
        nodes.truncate(node_limit);
        KTableSampleHashesAndNodes {
            hashes,
            nodes,
            total_hashes,
        }
    }

    #[cfg(test)]
    fn assert_invariants(&self) {
        let state = self.read();
        assert_eq!(state.core.node_count(), state.node_handles.len());
        for (id, handle) in &state.node_handles {
            assert_eq!(*id, handle.id());
            assert_eq!(state.core.node(*id), Some(handle.routing_node()));
            assert!(!handle.dropped());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Barrier, Mutex};
    use std::thread;
    use std::time::Duration;

    use super::*;

    struct ScriptedClock {
        values: Mutex<VecDeque<Instant>>,
        calls: AtomicUsize,
    }

    impl ScriptedClock {
        fn new(values: impl IntoIterator<Item = Instant>) -> Self {
            Self {
                values: Mutex::new(values.into_iter().collect()),
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl KTableClock for ScriptedClock {
        fn now(&self) -> Instant {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.values
                .lock()
                .expect("scripted clock lock poisoned")
                .pop_front()
                .unwrap_or_else(Instant::now)
        }
    }

    struct PanicClock;

    impl KTableClock for PanicClock {
        fn now(&self) -> Instant {
            panic!("scripted KTable clock panic")
        }
    }

    fn assert_panics<T>(action: impl FnOnce() -> T) {
        assert!(catch_unwind(AssertUnwindSafe(action)).is_err());
    }

    fn id(last: u8) -> Id20 {
        let mut bytes = [0; 20];
        bytes[19] = last;
        Id20::from_slice(&bytes).unwrap()
    }

    fn node(last: u8) -> RoutingNode {
        RoutingNode {
            id: id(last),
            addr: format!("192.0.2.{last}:{last}").parse().unwrap(),
        }
    }

    #[test]
    fn accepted_option_clock_panic_poison_fails_closed_for_all_table_clones() {
        let table = KTable::with_clock(Id20::ZERO, Arc::new(PanicClock));
        let cloned = table.clone();
        assert_panics(|| {
            table.put_node_with_options(node(1), &[KTableNodeOption::Responded]);
        });
        assert_panics(|| {
            let _ = table.node_count();
        });
        assert_panics(|| {
            let _ = table.node_handle(id(1));
        });
        assert_panics(|| {
            let _ = cloned.origin();
        });
    }

    #[test]
    fn duplicate_option_clock_panic_poisons_table_and_captured_handle() {
        let table = KTable::with_clock(Id20::ZERO, Arc::new(PanicClock));
        assert_eq!(table.put_node(node(1)), RoutingPutResult::Accepted);
        let captured = table.node_handle(id(1)).unwrap();
        assert_panics(|| {
            table.put_node_with_options(
                RoutingNode {
                    id: id(1),
                    addr: "192.0.2.1:101".parse().unwrap(),
                },
                &[KTableNodeOption::Responded],
            );
        });
        assert_panics(|| {
            let _ = table.node_count();
        });
        assert_panics(|| {
            let _ = captured.addr();
        });
    }

    fn assert_every_table_surface_is_poisoned(table: &KTable) {
        let addr = "192.0.2.1:1".parse().unwrap();
        assert_panics(|| {
            let _ = table.origin();
        });
        assert_panics(|| {
            let _ = table.node_count();
        });
        assert_panics(|| {
            let _ = table.hash_count();
        });
        assert_panics(|| {
            let _ = table.reverse_address_count();
        });
        assert_panics(|| {
            let _ = table.reverse_info(addr);
        });
        assert_panics(|| {
            let _ = table.node_handle(id(1));
        });
        assert_panics(|| {
            table.put_node(node(3));
        });
        assert_panics(|| {
            table.put_node_with_options(node(3), &[]);
        });
        assert_panics(|| {
            table.drop_node(id(1));
        });
        assert_panics(|| {
            table.drop_addr(addr);
        });
        assert_panics(|| {
            table.put_hash(id(1), &[]);
        });
        assert_panics(|| {
            table.batch_command(&[]);
        });
        assert_panics(|| {
            let _ = table.hash(id(1));
        });
        assert_panics(|| {
            let _ = table.closest_nodes(id(1));
        });
        assert_panics(|| {
            let _ = table.get_hash_or_closest_nodes(id(1));
        });
        assert_panics(|| {
            let _ = table.filter_known_addrs(&[addr]);
        });
        assert_panics(|| {
            let _ = table.get_oldest_nodes(Instant::now(), None);
        });
        assert_panics(|| {
            let _ = table.get_nodes_for_sample_infohashes(NonZeroUsize::new(1).unwrap());
        });
        assert_panics(|| {
            let _ = table.sample_hashes_and_nodes();
        });
    }

    #[test]
    fn batch_clock_panic_after_prefix_makes_every_table_surface_unusable() {
        let table = KTable::with_clock(Id20::ZERO, Arc::new(PanicClock));
        assert_panics(|| {
            table.batch_command(&[
                KTableCommand::PutNode {
                    node: node(1),
                    options: vec![],
                },
                KTableCommand::PutHash {
                    id: id(1),
                    peers: vec![],
                },
                KTableCommand::PutNode {
                    node: node(2),
                    options: vec![KTableNodeOption::Responded],
                },
            ]);
        });
        assert_every_table_surface_is_poisoned(&table);
        assert_every_table_surface_is_poisoned(&table.clone());
    }

    #[test]
    fn handles_follow_one_generation_through_shared_clones_and_readd() {
        let table = KTable::new(Id20::ZERO);
        let cloned_table = table.clone();
        assert_eq!(table.put_node(node(1)), RoutingPutResult::Accepted);
        let old = table.node_handle(id(1)).unwrap();
        assert_eq!(
            cloned_table.put_node_with_options(
                RoutingNode {
                    id: id(1),
                    addr: "192.0.2.1:101".parse().unwrap(),
                },
                &[KTableNodeOption::Bep51Support(true)],
            ),
            RoutingPutResult::AlreadyExists
        );
        assert_eq!(old.addr(), "192.0.2.1:101".parse().unwrap());
        assert_eq!(old.bep51_support(), KTableBep51Support::Yes);
        assert!(table.drop_node(id(1)));
        assert!(old.dropped());
        assert_eq!(cloned_table.put_node(node(1)), RoutingPutResult::Accepted);
        let new = table.node_handle(id(1)).unwrap();
        assert_ne!(old, new);
        assert!(old.dropped());
        assert!(!new.dropped());
        table.assert_invariants();
    }

    #[test]
    fn option_clock_calls_follow_order_and_rejected_puts_consume_none() {
        let anchor = Instant::now();
        let responded = anchor + Duration::from_secs(1);
        let empty_applied = anchor + Duration::from_secs(2);
        let clock = Arc::new(ScriptedClock::new([responded, empty_applied]));
        let table = KTable::with_clock(Id20::ZERO, clock.clone());
        assert_eq!(
            table.put_node_with_options(
                node(1),
                &[
                    KTableNodeOption::SampleInfoHashesResponse {
                        discovered_num: 1,
                        total_num: 10,
                        next_sample_at: anchor,
                    },
                    KTableNodeOption::Responded,
                    KTableNodeOption::SampleInfoHashesResponse {
                        discovered_num: 0,
                        total_num: 20,
                        next_sample_at: anchor,
                    },
                ],
            ),
            RoutingPutResult::Accepted
        );
        let handle = table.node_handle(id(1)).unwrap();
        assert_eq!(handle.last_responded_at(), Some(responded));
        assert_eq!(
            handle.next_sample_infohashes_at(),
            Some(empty_applied + EMPTY_SAMPLE_PENALTY)
        );
        assert_eq!(clock.calls(), 2);
        assert_eq!(
            table.put_node_with_options(
                RoutingNode {
                    id: Id20::ZERO,
                    addr: "192.0.2.99:99".parse().unwrap(),
                },
                &[
                    KTableNodeOption::Responded,
                    KTableNodeOption::SampleInfoHashesResponse {
                        discovered_num: 0,
                        total_num: 1,
                        next_sample_at: anchor,
                    },
                ],
            ),
            RoutingPutResult::Rejected
        );
        assert_eq!(clock.calls(), 2);
    }

    #[test]
    fn candidates_use_strict_boundaries_and_one_clock_read_per_visited_node() {
        let now = Instant::now();
        let clock = Arc::new(ScriptedClock::new([now, now, now]));
        let table = KTable::with_clock(Id20::ZERO, clock.clone());
        assert_eq!(
            table.put_node_with_options(node(1), &[KTableNodeOption::Bep51Support(false)]),
            RoutingPutResult::Accepted
        );
        assert_eq!(table.put_node(node(2)), RoutingPutResult::Accepted);
        assert_eq!(table.put_node(node(3)), RoutingPutResult::Accepted);
        let candidates = table.get_nodes_for_sample_infohashes(NonZeroUsize::new(2).unwrap());
        assert_eq!(
            candidates
                .iter()
                .map(KTableNodeHandle::id)
                .collect::<Vec<_>>(),
            vec![id(2), id(3)]
        );
        assert_eq!(clock.calls(), 3);

        let handle = table.node_handle(id(2)).unwrap();
        {
            let mut state = handle.write();
            state.last_responded_at = Some(now - Duration::from_secs(5));
            state.next_sample_infohashes_at = Some(now - Duration::from_nanos(1));
        }
        assert!(!handle.is_sample_infohashes_candidate_at(now));
        {
            let mut state = handle.write();
            state.last_responded_at = Some(now - Duration::from_secs(5) - Duration::from_nanos(1));
            state.next_sample_infohashes_at = Some(now);
        }
        assert!(!handle.is_sample_infohashes_candidate_at(now));
        handle.write().next_sample_infohashes_at = Some(now - Duration::from_nanos(1));
        assert!(handle.is_sample_infohashes_candidate_at(now));
    }

    #[test]
    fn signed_counters_wrap_like_deployed_64_bit_go_ints() {
        let table = KTable::new(Id20::ZERO);
        let now = Instant::now();
        assert_eq!(
            table.put_node_with_options(
                node(1),
                &[
                    KTableNodeOption::SampleInfoHashesResponse {
                        discovered_num: i64::MAX,
                        total_num: i64::MIN,
                        next_sample_at: now,
                    },
                    KTableNodeOption::SampleInfoHashesResponse {
                        discovered_num: 1,
                        total_num: -1,
                        next_sample_at: now,
                    },
                ],
            ),
            RoutingPutResult::Accepted
        );
        let handle = table.node_handle(id(1)).unwrap();
        assert_eq!(handle.sampled_num(), i64::MIN);
        assert_eq!(handle.last_discovered_num(), 1);
        assert_eq!(handle.total_num(), i64::MAX);
    }

    fn upper_instant_bound_from(value: Instant) -> Instant {
        let mut low_seconds = 0_u64;
        let mut high_seconds = u64::MAX;
        while low_seconds < high_seconds {
            let midpoint = low_seconds + (high_seconds - low_seconds).div_ceil(2);
            if value.checked_add(Duration::from_secs(midpoint)).is_some() {
                low_seconds = midpoint;
            } else {
                high_seconds = midpoint - 1;
            }
        }
        value
            .checked_add(Duration::from_secs(low_seconds))
            .unwrap_or(value)
    }

    #[test]
    fn empty_sample_penalty_saturates_without_panicking_near_instant_limit() {
        let near_limit = upper_instant_bound_from(Instant::now());
        assert!(near_limit.checked_add(EMPTY_SAMPLE_PENALTY).is_none());
        let saturated = saturating_add_five_minutes(near_limit);
        assert!(saturated >= near_limit);
        assert!(saturated.checked_add(Duration::from_nanos(1)).is_none());

        let clock = Arc::new(ScriptedClock::new([near_limit]));
        let table = KTable::with_clock(Id20::ZERO, clock);
        assert_eq!(
            table.put_node_with_options(
                node(1),
                &[KTableNodeOption::SampleInfoHashesResponse {
                    discovered_num: 0,
                    total_num: 1,
                    next_sample_at: near_limit,
                }],
            ),
            RoutingPutResult::Accepted
        );
        assert_eq!(
            table
                .node_handle(id(1))
                .unwrap()
                .next_sample_infohashes_at(),
            Some(saturated)
        );
    }

    struct BarrierClock {
        entered: Barrier,
        release: Barrier,
        value: Instant,
    }

    impl KTableClock for BarrierClock {
        fn now(&self) -> Instant {
            self.entered.wait();
            self.release.wait();
            self.value
        }
    }

    #[test]
    fn table_observers_cannot_see_a_partial_batch() {
        let clock = Arc::new(BarrierClock {
            entered: Barrier::new(2),
            release: Barrier::new(2),
            value: Instant::now(),
        });
        let table = KTable::with_clock(Id20::ZERO, clock.clone());
        let writer = {
            let table = table.clone();
            thread::spawn(move || {
                table.batch_command(&[
                    KTableCommand::PutNode {
                        node: node(1),
                        options: vec![KTableNodeOption::Responded],
                    },
                    KTableCommand::PutNode {
                        node: node(2),
                        options: vec![],
                    },
                ]);
            })
        };
        clock.entered.wait();
        let (sender, receiver) = std::sync::mpsc::channel();
        let observer = {
            let table = table.clone();
            thread::spawn(move || sender.send(table.node_count()).unwrap())
        };
        assert!(receiver.recv_timeout(Duration::from_millis(50)).is_err());
        clock.release.wait();
        assert_eq!(receiver.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
        writer.join().unwrap();
        observer.join().unwrap();
    }

    #[test]
    fn facade_and_sampling_use_live_handles_and_exact_cardinality_policy() {
        let table = KTable::new(Id20::ZERO);
        for last in 1..=45 {
            assert_eq!(table.put_node(node(last)), RoutingPutResult::Accepted);
        }
        for last in 1..=25 {
            assert_eq!(table.put_hash(id(last), &[]), RoutingPutResult::Accepted);
        }
        let sample = table.sample_hashes_and_nodes();
        assert_eq!(sample.total_hashes, 25);
        assert_eq!(sample.hashes.len(), 20);
        assert_eq!(sample.nodes.len(), 20);
        assert!(sample.hashes.windows(2).all(|pair| pair[0].id < pair[1].id));
        assert!(sample
            .nodes
            .windows(2)
            .all(|pair| pair[0].id() < pair[1].id()));
        assert_eq!(table.hash_count(), 25);
        assert_eq!(table.reverse_address_count(), 0);
        assert_eq!(table.closest_nodes(id(1))[0].id(), id(1));
        assert!(matches!(
            table.get_hash_or_closest_nodes(id(1)),
            KTableLookup::Found(_)
        ));
        table.assert_invariants();
    }
}
