use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};

use crate::{Id20, NodeTable, RoutingNode, RoutingPutResult, RoutingTree};

pub const HASH_TABLE_CAPACITY: usize = 80;

/// One peer address stored for an info hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KTableHashPeer {
    pub addr: SocketAddr,
}

/// A current info-hash entry. Peer order is deterministic here even though
/// the Go map exposed by `Hash.Peers` has no iteration-order contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KTableHash {
    pub id: Id20,
    pub peers: Vec<KTableHashPeer>,
}

/// Go's `GetHashOrClosestNodes` result without an invalid nil-hash state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KTableHashLookup {
    Found(KTableHash),
    ClosestNodes(Vec<RoutingNode>),
}

/// A deterministic projection of one shared reverse-address entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KTableReverseInfo {
    /// `None` includes Go's all-zero-ID sentinel, even if a zero-ID node
    /// caused the entry to be created by a duplicate update.
    pub peer_id: Option<Id20>,
    pub hashes: Vec<Id20>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ReverseAddress {
    V4(Ipv4Addr),
    V6 { ip: Ipv6Addr, scope_id: u32 },
}

impl ReverseAddress {
    fn from_socket_addr(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(addr) => Self::V4(*addr.ip()),
            SocketAddr::V6(addr) => Self::V6 {
                ip: *addr.ip(),
                scope_id: addr.scope_id(),
            },
        }
    }
}

#[derive(Debug, Default)]
struct ReverseInfo {
    // Go uses the all-zero ID as the absence sentinel.
    peer_id: Id20,
    hashes: HashSet<Id20>,
}

#[derive(Debug)]
struct HashEntry {
    id: Id20,
    peers: HashMap<ReverseAddress, KTableHashPeer>,
}

/// The current-state node and hash keyspaces plus Go's shared reverse-address
/// index.
///
/// This intentionally preserves the production implementation's observable
/// reverse-map behavior. A newly accepted node is not indexed until a
/// duplicate put updates it. Reverse identity ignores ports, while changing a
/// node's full address or dropping a node removes the entire shared entry,
/// including hash associations. Those surprising rules are parity, not a
/// recommended general-purpose index design.
#[derive(Debug)]
pub struct KTableCore {
    origin: Id20,
    nodes: NodeTable,
    hash_routing: RoutingTree,
    hashes: HashMap<Id20, HashEntry>,
    reverse: HashMap<ReverseAddress, ReverseInfo>,
}

impl KTableCore {
    #[must_use]
    pub fn new(origin: Id20) -> Self {
        Self {
            origin,
            nodes: NodeTable::new(origin),
            hash_routing: RoutingTree::new(origin, HASH_TABLE_CAPACITY, true),
            hashes: HashMap::with_capacity(HASH_TABLE_CAPACITY),
            reverse: HashMap::new(),
        }
    }

    #[must_use]
    pub const fn origin(&self) -> Id20 {
        self.origin
    }

    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.count()
    }

    #[must_use]
    pub fn hash_count(&self) -> usize {
        self.hashes.len()
    }

    #[must_use]
    pub fn reverse_address_count(&self) -> usize {
        self.reverse.len()
    }

    #[must_use]
    pub fn reverse_info(&self, addr: SocketAddr) -> Option<KTableReverseInfo> {
        self.reverse
            .get(&ReverseAddress::from_socket_addr(addr))
            .map(|info| {
                let mut hashes = info.hashes.iter().copied().collect::<Vec<_>>();
                hashes.sort_unstable();
                KTableReverseInfo {
                    peer_id: (info.peer_id != Id20::ZERO).then_some(info.peer_id),
                    hashes,
                }
            })
    }

    #[must_use]
    pub fn node(&self, id: Id20) -> Option<RoutingNode> {
        self.nodes
            .closest(id)
            .into_iter()
            .next()
            .filter(|node| node.id == id)
    }

    pub fn put_node(&mut self, node: RoutingNode) -> RoutingPutResult {
        let node = RoutingNode {
            addr: normalize_socket_addr(node.addr),
            ..node
        };
        let previous = self.node(node.id);
        let result = self.nodes.put(node);
        if result != RoutingPutResult::AlreadyExists {
            // Go's accepted-node factory deliberately does not populate the
            // reverse map. Rejected puts likewise have no side effect.
            return result;
        }

        let previous = previous.expect("an already-existing routing ID has a node payload");
        if previous.addr != node.addr {
            self.reverse
                .remove(&ReverseAddress::from_socket_addr(previous.addr));
        }
        self.reverse
            .entry(ReverseAddress::from_socket_addr(node.addr))
            .or_default()
            .peer_id = node.id;
        result
    }

    pub fn drop_node(&mut self, id: Id20) -> bool {
        let Some(node) = self.node(id) else {
            return false;
        };
        if !self.nodes.drop(id) {
            return false;
        }
        self.reverse
            .remove(&ReverseAddress::from_socket_addr(node.addr));
        true
    }

    /// Drop the node currently indexed for the address's IP and scope. The
    /// supplied port and IPv6 flowinfo do not participate in reverse identity.
    pub fn drop_addr(&mut self, addr: SocketAddr) -> bool {
        let key = ReverseAddress::from_socket_addr(addr);
        let Some(peer_id) = self.reverse.get(&key).map(|info| info.peer_id) else {
            return false;
        };
        if peer_id == Id20::ZERO {
            return false;
        }
        self.drop_node(peer_id)
    }

    #[must_use]
    pub(crate) fn node_id_for_addr(&self, addr: SocketAddr) -> Option<Id20> {
        let peer_id = self
            .reverse
            .get(&ReverseAddress::from_socket_addr(addr))?
            .peer_id;
        (peer_id != Id20::ZERO).then_some(peer_id)
    }

    pub(crate) fn hashes_by_id(&self) -> Vec<KTableHash> {
        let mut ids = self.hashes.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        ids.into_iter().filter_map(|id| self.hash(id)).collect()
    }

    pub fn put_hash(&mut self, id: Id20, peers: &[KTableHashPeer]) -> RoutingPutResult {
        let result = self.hash_routing.put(id);
        match result {
            RoutingPutResult::Rejected => return result,
            RoutingPutResult::Accepted => {
                self.hashes.insert(
                    id,
                    HashEntry {
                        id,
                        peers: HashMap::new(),
                    },
                );
            }
            RoutingPutResult::AlreadyExists => {}
        }

        let entry = self
            .hashes
            .get_mut(&id)
            .expect("accepted and existing hash routing IDs have payloads");
        for peer in peers {
            let peer = KTableHashPeer {
                addr: normalize_socket_addr(peer.addr),
            };
            let key = ReverseAddress::from_socket_addr(peer.addr);
            // IP-only identity makes the last port in this update win while
            // updates accumulate addresses not present in the new input.
            entry.peers.insert(key, peer);
            self.reverse.entry(key).or_default().hashes.insert(id);
        }
        result
    }

    #[must_use]
    pub fn hash(&self, id: Id20) -> Option<KTableHash> {
        self.hashes.get(&id).map(|entry| {
            let mut peers = entry.peers.values().copied().collect::<Vec<_>>();
            peers.sort_by_key(|peer| peer.addr.to_string());
            KTableHash {
                id: entry.id,
                peers,
            }
        })
    }

    #[must_use]
    pub fn closest_nodes(&self, id: Id20) -> Vec<RoutingNode> {
        self.nodes.closest(id)
    }

    #[must_use]
    pub fn get_hash_or_closest_nodes(&self, id: Id20) -> KTableHashLookup {
        match self.hash(id) {
            Some(hash) => KTableHashLookup::Found(hash),
            None => KTableHashLookup::ClosestNodes(self.closest_nodes(id)),
        }
    }

    /// Preserve the order, duplicates, and full socket representation of each
    /// unknown input. Only IP family/address and IPv6 scope determine whether
    /// the shared reverse map knows an input; its port and flowinfo are ignored.
    #[must_use]
    pub fn filter_known_addrs(&self, addrs: &[SocketAddr]) -> Vec<SocketAddr> {
        addrs
            .iter()
            .copied()
            .filter(|addr| {
                !self
                    .reverse
                    .contains_key(&ReverseAddress::from_socket_addr(*addr))
            })
            .collect()
    }

    #[cfg(test)]
    fn assert_invariants(&self) {
        assert_eq!(self.hash_routing.count(), self.hashes.len());
        for (id, hash) in &self.hashes {
            assert_eq!(*id, hash.id);
            assert!(self.hash_routing.contains(*id));
            for (key, peer) in &hash.peers {
                assert_eq!(*key, ReverseAddress::from_socket_addr(peer.addr));
            }
        }
    }
}

fn normalize_socket_addr(addr: SocketAddr) -> SocketAddr {
    match addr {
        SocketAddr::V4(addr) => SocketAddr::V4(addr),
        SocketAddr::V6(addr) => SocketAddr::V6(SocketAddrV6::new(
            *addr.ip(),
            addr.port(),
            0,
            addr.scope_id(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use super::*;

    fn id(last: u8) -> Id20 {
        let mut bytes = [0; 20];
        bytes[19] = last;
        Id20::from_slice(&bytes).unwrap()
    }

    #[test]
    fn flowinfo_is_ignored_but_mapped_and_scoped_identities_remain_distinct() {
        let mut core = KTableCore::new(Id20::ZERO);
        let native = SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), 1, 77, 7));
        let mapped = SocketAddr::V6(SocketAddrV6::new(
            Ipv4Addr::new(192, 0, 2, 1).to_ipv6_mapped(),
            2,
            88,
            0,
        ));
        assert_eq!(
            core.put_hash(
                id(1),
                &[
                    KTableHashPeer { addr: native },
                    KTableHashPeer { addr: mapped },
                ],
            ),
            RoutingPutResult::Accepted
        );
        let native_alias = SocketAddr::V6(SocketAddrV6::new(
            "fe80::1".parse().unwrap(),
            65535,
            u32::MAX,
            7,
        ));
        let plain_v4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 2);
        assert!(core.filter_known_addrs(&[native_alias]).is_empty());
        assert_eq!(core.filter_known_addrs(&[plain_v4]), vec![plain_v4]);
        let peers = core.hash(id(1)).unwrap().peers;
        assert!(peers.contains(&KTableHashPeer {
            addr: normalize_socket_addr(native),
        }));
        core.assert_invariants();
    }

    #[test]
    fn rejected_hash_put_cannot_mutate_reverse_or_payload_state() {
        let mut core = KTableCore::new(Id20::ZERO);
        assert_eq!(
            core.put_hash(
                Id20::ZERO,
                &[KTableHashPeer {
                    addr: "192.0.2.1:1".parse().unwrap(),
                }],
            ),
            RoutingPutResult::Rejected
        );
        assert_eq!(core.hash_count(), 0);
        assert_eq!(core.reverse_address_count(), 0);
        core.assert_invariants();
    }
}
