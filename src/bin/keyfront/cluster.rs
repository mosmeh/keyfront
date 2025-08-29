use anyhow::Context;
use keyfront::cluster::{NodeName, Slot, SlotMap};
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, RwLock, RwLockReadGuard},
};
use tracing::info;

pub struct Cluster {
    this_node: NodeName,
    topology: Arc<RwLock<Topology>>,
}

impl Cluster {
    pub fn connect(addr_to_announce: Option<SocketAddr>) -> anyhow::Result<Self> {
        let addr_to_announce = addr_to_announce.unwrap_or_else(|| {
            // This node is the only node in the cluster, so no one actually
            // cares about this address. Just use a placeholder.
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))
        });
        Self::new_single_node(addr_to_announce)
    }

    fn new_single_node(addr_to_announce: SocketAddr) -> anyhow::Result<Self> {
        info!("Running as a single-node cluster");
        let this_node =
            NodeName::generate_random().context("Failed to generate a random node name")?;
        let topology = Arc::new(RwLock::new(Topology {
            node_addrs: HashMap::from([(this_node.clone(), addr_to_announce)]),
            slots: SlotMap::filled(Some(this_node.clone())),
        }));
        Ok(Self {
            this_node,
            topology,
        })
    }

    pub fn this_node(&self) -> &NodeName {
        &self.this_node
    }

    pub fn topology(&self) -> RwLockReadGuard<'_, Topology> {
        self.topology.read().unwrap()
    }
}

pub struct Topology {
    node_addrs: HashMap<NodeName, SocketAddr>,
    slots: SlotMap<Option<NodeName>>,
}

impl Topology {
    pub fn node_addrs(&self) -> &HashMap<NodeName, SocketAddr> {
        &self.node_addrs
    }

    pub fn slot(&self, slot: Slot) -> Option<&NodeName> {
        self.slots[slot].as_ref()
    }

    pub fn slots(&self) -> &SlotMap<Option<NodeName>> {
        &self.slots
    }
}
