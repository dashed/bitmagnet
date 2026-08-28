use crate::Id20;

pub const ROUTING_ID_BITS: usize = 160;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoutingPutResult {
    Rejected,
    Accepted,
    AlreadyExists,
}

/// The pure 160-bit binary tree used by the Go Kademlia table.
///
/// Storage is an empty/leaf/branch trie over `id XOR origin`. Admission is a
/// separate leading-zero bucket rule, matching the production Go tree rather
/// than imposing a conventional fixed-capacity trie node.
#[derive(Debug)]
pub struct RoutingTree {
    origin: Id20,
    bucket_capacity: usize,
    splitting_enabled: bool,
    root: TrieNode,
    bucket_counts: [usize; ROUTING_ID_BITS],
}

impl RoutingTree {
    #[must_use]
    pub fn new(origin: Id20, bucket_capacity: usize, splitting_enabled: bool) -> Self {
        Self {
            origin,
            bucket_capacity,
            splitting_enabled,
            root: TrieNode::Empty,
            bucket_counts: [0; ROUTING_ID_BITS],
        }
    }

    #[must_use]
    pub const fn bits(&self) -> usize {
        ROUTING_ID_BITS
    }

    #[must_use]
    pub fn count(&self) -> usize {
        self.root.count()
    }

    #[must_use]
    pub fn contains(&self, id: Id20) -> bool {
        self.root.contains(xor(id, self.origin), 0)
    }

    pub fn put(&mut self, id: Id20) -> RoutingPutResult {
        if id == self.origin {
            return RoutingPutResult::Rejected;
        }

        let distance = xor(id, self.origin);
        if self.root.contains(distance, 0) {
            return RoutingPutResult::AlreadyExists;
        }

        let bucket = leading_zeros(distance);
        if self.bucket_counts[bucket] >= self.bucket_capacity
            && (!self.splitting_enabled
                || self.root.count_closer_than_subpath(distance, 0) >= self.bucket_capacity)
        {
            return RoutingPutResult::Rejected;
        }

        self.root.insert_unique(distance, 0);
        self.bucket_counts[bucket] += 1;
        RoutingPutResult::Accepted
    }

    pub fn drop(&mut self, id: Id20) -> bool {
        let distance = xor(id, self.origin);
        if !self.root.remove(distance, 0) {
            return false;
        }
        let bucket = leading_zeros(distance);
        self.bucket_counts[bucket] -= 1;
        true
    }

    #[must_use]
    pub fn closest(&self, id: Id20, limit: usize) -> Vec<Id20> {
        let target = xor(id, self.origin);
        self.root
            .closest_to_subpath(target, 0, limit)
            .into_iter()
            .map(|distance| xor(distance, self.origin))
            .collect()
    }

    #[cfg(test)]
    fn assert_invariants(&self) {
        use std::collections::BTreeSet;

        let mut leaves = Vec::new();
        let cached_count = self.root.assert_invariants(0, &mut leaves);
        assert_eq!(cached_count, self.count());
        assert_eq!(leaves.len(), self.count());
        assert_eq!(
            leaves.iter().copied().collect::<BTreeSet<_>>().len(),
            leaves.len()
        );

        let mut buckets = [0_usize; ROUTING_ID_BITS];
        for distance in leaves {
            assert_ne!(distance, Id20::ZERO);
            buckets[leading_zeros(distance)] += 1;
        }
        assert_eq!(buckets, self.bucket_counts);
    }
}

#[derive(Debug)]
enum TrieNode {
    Empty,
    Leaf(Id20),
    Branch {
        children: [Box<TrieNode>; 2],
        counts: [usize; 2],
    },
}

impl TrieNode {
    fn contains(&self, id: Id20, depth: usize) -> bool {
        match self {
            Self::Empty => false,
            Self::Leaf(existing) => *existing == id,
            Self::Branch { children, .. } => {
                let branch = usize::from(bit(id, depth));
                children[branch].contains(id, depth + 1)
            }
        }
    }

    fn insert_unique(&mut self, id: Id20, depth: usize) {
        match self {
            Self::Empty => *self = Self::Leaf(id),
            Self::Leaf(existing) => {
                let previous = *existing;
                debug_assert_ne!(previous, id);
                *self = Self::Branch {
                    children: [Box::new(Self::Empty), Box::new(Self::Empty)],
                    counts: [0, 0],
                };
                self.insert_unique(previous, depth);
                self.insert_unique(id, depth);
            }
            Self::Branch { children, counts } => {
                let branch = usize::from(bit(id, depth));
                children[branch].insert_unique(id, depth + 1);
                counts[branch] = children[branch].count();
            }
        }
    }

    fn remove(&mut self, id: Id20, depth: usize) -> bool {
        match self {
            Self::Empty => false,
            Self::Leaf(existing) if *existing == id => {
                *self = Self::Empty;
                true
            }
            Self::Leaf(_) => false,
            Self::Branch { children, counts } => {
                let branch = usize::from(bit(id, depth));
                if !children[branch].remove(id, depth + 1) {
                    return false;
                }
                counts[branch] = children[branch].count();
                let total = counts[0] + counts[1];
                if total == 0 {
                    *self = Self::Empty;
                } else if total == 1 {
                    *self = Self::Leaf(
                        self.only_id()
                            .expect("a one-item branch has exactly one leaf"),
                    );
                }
                true
            }
        }
    }

    fn only_id(&self) -> Option<Id20> {
        match self {
            Self::Empty => None,
            Self::Leaf(id) => Some(*id),
            Self::Branch { children, counts } => {
                debug_assert_eq!(counts[0] + counts[1], 1);
                children[usize::from(counts[0] == 0)].only_id()
            }
        }
    }

    fn count(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Leaf(_) => 1,
            Self::Branch { counts, .. } => counts[0] + counts[1],
        }
    }

    /// Mirrors Go's recursive predicate exactly. In particular, the leaf case
    /// is intentionally not an ordinary numeric `distance < candidate` test.
    fn count_closer_than_subpath(&self, path: Id20, depth: usize) -> usize {
        match self {
            Self::Empty => 0,
            Self::Leaf(id) => {
                for index in depth..ROUTING_ID_BITS {
                    if bit(path, index) && !bit(*id, index) {
                        return 0;
                    }
                }
                1
            }
            Self::Branch { children, counts } => {
                let remaining = ROUTING_ID_BITS - depth;
                if remaining == 0 {
                    return 0;
                }
                let branch = usize::from(bit(path, depth));
                if remaining == 1 {
                    return if branch == 0 {
                        counts[0]
                    } else {
                        counts[0] + counts[1]
                    };
                }
                if branch == 0 {
                    children[0].count_closer_than_subpath(path, depth + 1)
                } else {
                    counts[0] + children[1].count_closer_than_subpath(path, depth + 1)
                }
            }
        }
    }

    fn closest_to_subpath(&self, path: Id20, depth: usize, limit: usize) -> Vec<Id20> {
        if limit == 0 {
            return Vec::new();
        }
        match self {
            Self::Empty => Vec::new(),
            Self::Leaf(id) => vec![*id],
            Self::Branch { children, .. } => {
                let branch = if depth == ROUTING_ID_BITS {
                    0
                } else {
                    usize::from(bit(path, depth))
                };
                let next_depth = (depth + 1).min(ROUTING_ID_BITS);
                let mut result = children[branch].closest_to_subpath(path, next_depth, limit);
                if result.len() < limit {
                    result.extend(children[1 - branch].closest_without_path(limit - result.len()));
                }
                result
            }
        }
    }

    fn closest_without_path(&self, limit: usize) -> Vec<Id20> {
        if limit == 0 {
            return Vec::new();
        }
        match self {
            Self::Empty => Vec::new(),
            Self::Leaf(id) => vec![*id],
            Self::Branch { children, .. } => {
                let mut result = children[0].closest_without_path(limit);
                if result.len() < limit {
                    result.extend(children[1].closest_without_path(limit - result.len()));
                }
                result
            }
        }
    }

    #[cfg(test)]
    fn assert_invariants(&self, depth: usize, leaves: &mut Vec<Id20>) -> usize {
        match self {
            Self::Empty => 0,
            Self::Leaf(id) => {
                assert!(depth <= ROUTING_ID_BITS);
                leaves.push(*id);
                1
            }
            Self::Branch { children, counts } => {
                assert!(depth < ROUTING_ID_BITS);
                let actual = [
                    children[0].assert_invariants(depth + 1, leaves),
                    children[1].assert_invariants(depth + 1, leaves),
                ];
                assert_eq!(*counts, actual, "cached subtree count at depth {depth}");
                assert!(actual[0] + actual[1] >= 2, "empty or singleton branch");
                actual[0] + actual[1]
            }
        }
    }
}

fn xor(left: Id20, right: Id20) -> Id20 {
    let mut result = [0_u8; 20];
    for (output, (left, right)) in result
        .iter_mut()
        .zip(left.as_bytes().iter().zip(right.as_bytes()))
    {
        *output = left ^ right;
    }
    Id20::from_slice(&result).expect("XOR preserves the 20-byte ID width")
}

fn bit(id: Id20, index: usize) -> bool {
    id.as_bytes()[index / 8] & (1 << (7 - index % 8)) != 0
}

fn leading_zeros(id: Id20) -> usize {
    id.as_bytes()
        .iter()
        .map(|byte| byte.leading_zeros() as usize)
        .take_while(|zeros| *zeros == 8)
        .sum::<usize>()
        + id.as_bytes()
            .iter()
            .find(|byte| **byte != 0)
            .map_or(0, |byte| byte.leading_zeros() as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> Id20 {
        Id20::from_hex(value).unwrap()
    }

    #[test]
    fn leaf_split_predicate_preserves_the_go_asymmetry() {
        let zero = Id20::ZERO;
        let eight = id("8000000000000000000000000000000000000000");
        let twelve = id("c000000000000000000000000000000000000000");

        let mut forward = RoutingTree::new(zero, 1, true);
        assert_eq!(forward.put(eight), RoutingPutResult::Accepted);
        assert_eq!(forward.put(twelve), RoutingPutResult::Accepted);

        let mut reverse = RoutingTree::new(zero, 1, true);
        assert_eq!(reverse.put(twelve), RoutingPutResult::Accepted);
        assert_eq!(reverse.put(eight), RoutingPutResult::Rejected);
    }

    #[test]
    fn dropping_to_one_item_compacts_without_changing_closest() {
        let mut tree = RoutingTree::new(Id20::ZERO, 80, true);
        let near = id("0000000000000000000000000000000000000001");
        let middle = id("4000000000000000000000000000000000000000");
        let far = id("8000000000000000000000000000000000000000");
        assert_eq!(tree.put(near), RoutingPutResult::Accepted);
        tree.assert_invariants();
        assert_eq!(tree.put(middle), RoutingPutResult::Accepted);
        tree.assert_invariants();
        assert_eq!(tree.put(far), RoutingPutResult::Accepted);
        tree.assert_invariants();
        assert!(tree.drop(middle));
        tree.assert_invariants();
        assert!(tree.drop(far));
        tree.assert_invariants();
        assert_eq!(tree.closest(Id20::ZERO, 80), vec![near]);
        assert!(tree.drop(near));
        tree.assert_invariants();
        assert_eq!(tree.count(), 0);
    }

    #[test]
    fn cached_counts_and_bucket_histogram_hold_across_rejections_and_reinsertions() {
        let mut tree = RoutingTree::new(Id20::ZERO, 4, true);
        let ids = [
            id("0000000000000000000000000000000000000002"),
            id("0000000000000000000000000000000000000003"),
            id("0000000000000000000000000000000000000004"),
            id("4000000000000000000000000000000000000000"),
            id("8000000000000000000000000000000000000000"),
            id("c000000000000000000000000000000000000000"),
        ];
        for candidate in ids {
            let _ = tree.put(candidate);
            tree.assert_invariants();
            let _ = tree.put(candidate);
            tree.assert_invariants();
        }
        for candidate in ids.into_iter().rev() {
            assert!(tree.drop(candidate));
            tree.assert_invariants();
            assert!(!tree.drop(candidate));
            tree.assert_invariants();
            let _ = tree.put(candidate);
            tree.assert_invariants();
        }
    }

    #[test]
    fn closest_preserves_go_sibling_traversal_without_preallocation_from_limit() {
        let target = id("5555555555555555555555555555555555555555");
        let values = [
            id("0000000000000000000000000000000000000001"),
            id("4000000000000000000000000000000000000000"),
            id("8000000000000000000000000000000000000000"),
            id("ffffffffffffffffffffffffffffffffffffffff"),
            id("5555555555555555555555555555555555555554"),
        ];
        let mut tree = RoutingTree::new(Id20::ZERO, 80, true);
        for value in values.into_iter().rev() {
            assert_eq!(tree.put(value), RoutingPutResult::Accepted);
        }
        assert_eq!(
            tree.closest(target, usize::MAX),
            vec![values[4], values[1], values[0], values[2], values[3]]
        );
        tree.assert_invariants();
    }
}
