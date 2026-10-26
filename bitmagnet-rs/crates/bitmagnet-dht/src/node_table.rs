use std::collections::HashMap;
use std::net::{SocketAddr, SocketAddrV6};

use crate::{Id20, RoutingPutResult, RoutingTree};

pub const NODE_TABLE_CAPACITY: usize = 80;
pub const NODE_TABLE_CLOSEST_LIMIT: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoutingNode {
    pub id: Id20,
    pub addr: SocketAddr,
}

/// Deterministic current-state node keyspace over the production routing tree.
///
/// This deliberately owns only ID-to-address payloads. Go's shared reverse
/// address map, response clocks, BEP-51 options, and table synchronization are
/// separate contracts and are not represented here.
#[derive(Debug)]
pub struct NodeTable {
    origin: Id20,
    routing: RoutingTree,
    nodes: HashMap<Id20, RoutingNode>,
}

impl NodeTable {
    #[must_use]
    pub fn new(origin: Id20) -> Self {
        Self {
            origin,
            routing: RoutingTree::new(origin, NODE_TABLE_CAPACITY, true),
            nodes: HashMap::with_capacity(NODE_TABLE_CAPACITY),
        }
    }

    #[must_use]
    pub const fn origin(&self) -> Id20 {
        self.origin
    }

    #[must_use]
    pub fn count(&self) -> usize {
        self.nodes.len()
    }

    /// Insert a new node or update the address of an existing ID.
    ///
    /// Duplicate detection occurs inside the routing tree before capacity, so
    /// an existing node's address is updated even when its bucket is full.
    pub fn put(&mut self, node: RoutingNode) -> RoutingPutResult {
        let result = self.routing.put(node.id);
        match result {
            RoutingPutResult::Accepted | RoutingPutResult::AlreadyExists => {
                self.nodes.insert(
                    node.id,
                    RoutingNode {
                        addr: normalize_addr(node.addr),
                        ..node
                    },
                );
            }
            RoutingPutResult::Rejected => {}
        }
        result
    }

    pub fn drop(&mut self, id: Id20) -> bool {
        if !self.routing.drop(id) {
            return false;
        }
        let removed = self.nodes.remove(&id);
        debug_assert!(removed.is_some(), "routing and payload state diverged");
        true
    }

    /// Return the exact target as a singleton when present; otherwise return
    /// at most eight nodes using the production tree's exact traversal order.
    #[must_use]
    pub fn closest(&self, id: Id20) -> Vec<RoutingNode> {
        if let Some(node) = self.nodes.get(&id) {
            return vec![*node];
        }

        self.routing
            .closest(id, NODE_TABLE_CLOSEST_LIMIT)
            .into_iter()
            .map(|node_id| {
                *self
                    .nodes
                    .get(&node_id)
                    .expect("routing and payload state must remain synchronized")
            })
            .collect()
    }

    #[cfg(test)]
    fn assert_invariants(&self) {
        assert_eq!(self.nodes.len(), self.routing.count());
        assert!(!self.nodes.contains_key(&self.origin));
        for (id, node) in &self.nodes {
            assert_eq!(*id, node.id);
            assert!(self.routing.contains(*id));
            if let SocketAddr::V6(addr) = node.addr {
                assert_eq!(addr.flowinfo(), 0);
            }
            assert_eq!(self.closest(*id), vec![*node]);
        }
    }
}

fn normalize_addr(addr: SocketAddr) -> SocketAddr {
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
    use super::*;

    fn id(value: u16) -> Id20 {
        let mut bytes = [0_u8; 20];
        bytes[18..].copy_from_slice(&value.to_be_bytes());
        Id20::from_slice(&bytes).unwrap()
    }

    #[test]
    fn duplicate_updates_at_capacity_and_drop_reopens_the_bucket() {
        let mut table = NodeTable::new(Id20::ZERO);
        for value in 1..=NODE_TABLE_CAPACITY as u16 {
            let mut bytes = *id(value).as_bytes();
            bytes[0] = 0xc0;
            assert_eq!(
                table.put(RoutingNode {
                    id: Id20::from_slice(&bytes).unwrap(),
                    addr: format!("192.0.2.1:{value}").parse().unwrap(),
                }),
                RoutingPutResult::Accepted
            );
            table.assert_invariants();
        }

        let mut first_bytes = *id(1).as_bytes();
        first_bytes[0] = 0xc0;
        let first = Id20::from_slice(&first_bytes).unwrap();
        let replacement = "[fe80::1%7]:0".parse().unwrap();
        assert_eq!(
            table.put(RoutingNode {
                id: first,
                addr: replacement,
            }),
            RoutingPutResult::AlreadyExists
        );
        assert_eq!(table.closest(first)[0].addr, replacement);
        table.assert_invariants();

        let rejected = Id20::from_hex("ffffffffffffffffffffffffffffffffffffffff").unwrap();
        assert_eq!(
            table.put(RoutingNode {
                id: rejected,
                addr: "198.51.100.1:81".parse().unwrap(),
            }),
            RoutingPutResult::Rejected
        );
        assert!(table.drop(first));
        assert_eq!(
            table.put(RoutingNode {
                id: rejected,
                addr: "198.51.100.1:81".parse().unwrap(),
            }),
            RoutingPutResult::Accepted
        );
        table.assert_invariants();
    }

    #[test]
    fn identical_endpoints_remain_independent_without_a_reverse_map() {
        let mut table = NodeTable::new(Id20::ZERO);
        let addr = "[::ffff:192.0.2.9]:6881".parse().unwrap();
        for node_id in [id(1), id(2)] {
            assert_eq!(
                table.put(RoutingNode { id: node_id, addr }),
                RoutingPutResult::Accepted
            );
        }
        assert!(table.drop(id(1)));
        assert_eq!(table.closest(id(2)), vec![RoutingNode { id: id(2), addr }]);
        table.assert_invariants();
    }

    #[test]
    fn ipv6_flowinfo_is_normalized_on_new_and_duplicate_puts() {
        let mut table = NodeTable::new(Id20::ZERO);
        let node_id = id(1);
        let initial = SocketAddr::V6(SocketAddrV6::new("2001:db8::1".parse().unwrap(), 1, 7, 3));
        assert_eq!(
            table.put(RoutingNode {
                id: node_id,
                addr: initial,
            }),
            RoutingPutResult::Accepted
        );
        assert_eq!(
            table.closest(node_id),
            vec![RoutingNode {
                id: node_id,
                addr: SocketAddr::V6(SocketAddrV6::new("2001:db8::1".parse().unwrap(), 1, 0, 3,)),
            }]
        );

        let replacement = SocketAddr::V6(SocketAddrV6::new(
            "2001:db8::2".parse().unwrap(),
            2,
            u32::MAX,
            4,
        ));
        assert_eq!(
            table.put(RoutingNode {
                id: node_id,
                addr: replacement,
            }),
            RoutingPutResult::AlreadyExists
        );
        assert_eq!(
            table.closest(node_id),
            vec![RoutingNode {
                id: node_id,
                addr: SocketAddr::V6(SocketAddrV6::new("2001:db8::2".parse().unwrap(), 2, 0, 4,)),
            }]
        );
        table.assert_invariants();
    }
}
