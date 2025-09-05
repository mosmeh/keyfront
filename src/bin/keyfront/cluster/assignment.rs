use crate::cluster::{NodeRole, Topology};
use keyfront::cluster::{NodeName, Slot};
use std::{
    cmp::Ordering,
    collections::{
        BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, binary_heap::PeekMut, btree_map,
    },
};

pub fn assign_unassigned_slots(topology: &Topology) -> HashMap<NodeName, HashSet<Slot>> {
    #[derive(PartialEq, Eq)]
    struct Node<'a> {
        name: &'a NodeName,
        slots: HashSet<Slot>,
        target_slots: usize,
    }

    impl PartialOrd for Node<'_> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for Node<'_> {
        fn cmp(&self, other: &Self) -> Ordering {
            self.target_slots
                .cmp(&other.target_slots)
                .reverse() // nodes with fewer slots will be popped first
                .then_with(|| self.name.cmp(other.name)) // break ties
        }
    }

    let mut nodes: BTreeMap<&NodeName, Node> = topology
        .nodes_with_role(NodeRole::Data)
        .map(|name| {
            (
                name,
                Node {
                    name,
                    slots: HashSet::new(),
                    target_slots: 0,
                },
            )
        })
        .collect();
    if nodes.is_empty() {
        return HashMap::new();
    }

    let mut unassigned_slots: Vec<Slot> = Vec::new();
    for (slot, node_name) in topology.slots().iter() {
        if let Some(node_name) = node_name {
            let node = nodes.get_mut(node_name).unwrap();
            assert!(node.slots.insert(slot));
            node.target_slots += 1;
        } else {
            unassigned_slots.push(slot);
        }
    }
    if unassigned_slots.is_empty() {
        return HashMap::new();
    }

    // Distribute unassigned slots so that nodes will have as equal number
    // of slots as possible.
    let mut node_heap: BinaryHeap<&mut Node> = nodes.values_mut().collect();
    for _ in 0..unassigned_slots.len() {
        node_heap.peek_mut().unwrap().target_slots += 1;
    }

    // Remove nodes that will not receive any new slots
    nodes.retain(|_, node| node.target_slots != node.slots.len());

    let mut assignments: HashMap<NodeName, HashSet<Slot>> = HashMap::new();
    for slot in unassigned_slots {
        let mut entry = if let Some(node) = nodes.values().find(|node| {
            if slot.prev().is_some_and(|prev| node.slots.contains(&prev)) {
                return true;
            }
            slot.next().is_some_and(|next| node.slots.contains(&next))
        }) {
            // If possible, assign the slot to a node that has an adjacent slot
            let btree_map::Entry::Occupied(entry) = nodes.entry(node.name) else {
                unreachable!()
            };
            entry
        } else {
            // Otherwise, assign to an arbitrary node
            nodes.first_entry().unwrap()
        };

        let node = entry.get_mut();
        assert!(
            assignments
                .entry(node.name.clone())
                .or_default()
                .insert(slot)
        );
        assert!(node.slots.insert(slot));
        if node.slots.len() == node.target_slots {
            entry.remove();
        }
    }
    assert!(nodes.is_empty());

    assignments
}

pub fn rebalance_slots(topology: &Topology) -> HashMap<NodeName, HashSet<Slot>> {
    struct NodeWithSlots<'a> {
        name: &'a NodeName,
        slots: Vec<Slot>,
    }

    impl PartialEq for NodeWithSlots<'_> {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name && self.slots.len() == other.slots.len()
        }
    }

    impl Eq for NodeWithSlots<'_> {}

    impl PartialOrd for NodeWithSlots<'_> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for NodeWithSlots<'_> {
        fn cmp(&self, other: &Self) -> Ordering {
            self.slots
                .len()
                .cmp(&other.slots.len())
                .reverse() // sorted by descending order of number of slots
                .then_with(|| self.name.cmp(other.name)) // break ties
        }
    }

    struct SourceNode<'a> {
        name: &'a NodeName,
        slots: BTreeSet<Slot>,
        target_slots: usize,
    }

    impl SourceNode<'_> {
        fn balance(&self) -> usize {
            self.slots.len() - self.target_slots
        }
    }

    impl PartialEq for SourceNode<'_> {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name && self.balance() == other.balance()
        }
    }

    impl Eq for SourceNode<'_> {}

    impl PartialOrd for SourceNode<'_> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for SourceNode<'_> {
        fn cmp(&self, other: &Self) -> Ordering {
            self.balance()
                .cmp(&other.balance()) // nodes with more balance will be popped first
                .then_with(|| self.name.cmp(other.name)) // break ties
        }
    }

    struct DestinationNode<'a> {
        name: &'a NodeName,
        slots: HashSet<Slot>,
        target_slots: usize,
    }

    impl DestinationNode<'_> {
        fn balance(&self) -> usize {
            self.target_slots - self.slots.len()
        }
    }

    impl PartialEq for DestinationNode<'_> {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name && self.balance() == other.balance()
        }
    }

    impl Eq for DestinationNode<'_> {}

    impl PartialOrd for DestinationNode<'_> {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for DestinationNode<'_> {
        fn cmp(&self, other: &Self) -> Ordering {
            self.balance()
                .cmp(&other.balance()) // nodes with more balance will be popped first
                .then_with(|| self.name.cmp(other.name)) // break ties
        }
    }

    let mut num_assigned_slots = 0;
    let mut node_slots: HashMap<&NodeName, Vec<Slot>> = topology
        .nodes_with_role(NodeRole::Data)
        .map(|name| (name, Vec::new()))
        .collect();
    for (slot, node) in topology.slots().iter() {
        if let Some(node) = node {
            num_assigned_slots += 1;
            node_slots.get_mut(node).unwrap().push(slot);
        }
    }

    let mut nodes: Vec<NodeWithSlots> = node_slots
        .into_iter()
        .map(|(name, slots)| NodeWithSlots { name, slots })
        .collect();
    nodes.sort_unstable();

    // After rebalancing, `nodes_with_extra_slot` nodes will have
    // `slots_per_node + 1` slots, and the rest will have `slots_per_node` slots.
    let slots_per_node = num_assigned_slots / nodes.len();
    let nodes_with_extra_slot = num_assigned_slots % nodes.len();

    // Nodes with slot surplus
    let mut src_nodes: BinaryHeap<SourceNode> = BinaryHeap::new();
    // Nodes with slot deficit
    let mut dst_nodes: BinaryHeap<DestinationNode> = BinaryHeap::new();
    for (i, node) in nodes.into_iter().enumerate() {
        let target_slots = if i < nodes_with_extra_slot {
            // Assign the extra slots to the nodes that already have more slots
            // to minimize slot movements.
            slots_per_node + 1
        } else {
            slots_per_node
        };
        match node.slots.len().cmp(&target_slots) {
            Ordering::Equal => {
                // Already balanced
            }
            Ordering::Greater => src_nodes.push(SourceNode {
                name: node.name,
                slots: node.slots.into_iter().collect(),
                target_slots,
            }),
            Ordering::Less => dst_nodes.push(DestinationNode {
                name: node.name,
                slots: node.slots.into_iter().collect(),
                target_slots,
            }),
        }
    }

    let mut assignments: HashMap<NodeName, HashSet<Slot>> = HashMap::new();
    while let Some(mut src_node) = src_nodes.peek_mut() {
        let mut dst_node = dst_nodes.peek_mut().unwrap();
        let num_slots_to_move = src_node.balance().min(dst_node.balance());

        let assignment = assignments.entry(dst_node.name.clone()).or_default();
        match src_node.slots.len().cmp(&num_slots_to_move) {
            Ordering::Less => unreachable!(),
            Ordering::Equal => {
                for slot in std::mem::take(&mut src_node.slots) {
                    assert!(assignment.insert(slot));
                    assert!(dst_node.slots.insert(slot));
                }
            }
            Ordering::Greater => {
                let mut remaining = num_slots_to_move;

                // If possible, move slots that are adjacent to slots on the destination node
                let mut preferred_slots: Vec<Slot> = src_node
                    .slots
                    .iter()
                    .filter(|slot| {
                        if slot
                            .prev()
                            .is_some_and(|prev| dst_node.slots.contains(&prev))
                        {
                            return true;
                        }
                        slot.next()
                            .is_some_and(|next| dst_node.slots.contains(&next))
                    })
                    .copied()
                    .collect();
                while remaining > 0
                    && let Some(slot) = preferred_slots.pop()
                {
                    assert!(assignment.insert(slot));
                    assert!(src_node.slots.remove(&slot));
                    assert!(dst_node.slots.insert(slot));

                    if let Some(prev) = slot.prev()
                        && src_node.slots.contains(&prev)
                    {
                        preferred_slots.push(prev);
                    }
                    if let Some(next) = slot.next()
                        && src_node.slots.contains(&next)
                    {
                        preferred_slots.push(next);
                    }

                    remaining -= 1;
                }

                // If there are no more preferred slots, move arbitrary slots
                for _ in 0..remaining {
                    let slot = src_node.slots.pop_last().unwrap();
                    assert!(assignment.insert(slot));
                    assert!(dst_node.slots.insert(slot));
                }
            }
        }

        if src_node.balance() == 0 {
            PeekMut::pop(src_node);
        }
        if dst_node.balance() == 0 {
            PeekMut::pop(dst_node);
        }
    }
    assert!(dst_nodes.is_empty());

    assignments
}
