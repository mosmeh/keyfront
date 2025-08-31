use crate::{
    TaskGroup,
    cluster::{Lease, Request, Topology, assignment},
};
use anyhow::{Context, anyhow, bail, ensure};
use bstr::ByteSlice;
use etcd_client::{
    Compare, CompareOp, EventType, GetOptions, KeyValue, LeaderKey, LeaderResponse, ObserveStream,
    ResignOptions, Txn, TxnOp, TxnResponse, WatchResponse, WatchStream,
};
use futures_util::{FutureExt, Stream, StreamExt};
use keyfront::cluster::{NodeName, Slot};
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Poll, ready},
};
use tokio::{
    pin, select,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::DropGuard;
use tracing::{error, info, warn};

pub const NODE_BASE_PATH: &[u8] = b"nodes/";
const LEADER_PATH: &[u8] = b"leader";
const SLOTS_PATH: &[u8] = b"slots";

enum State {
    Leader(Leader),
    Follower(Follower),
    Stopped,
}

pub struct StateMachine {
    pub etcd: etcd_client::Client,
    pub root: Vec<u8>,
    pub this_node: NodeName,
    pub topology: Arc<RwLock<Topology>>,
    pub lease: Lease,
    pub lease_keep_alive_interval: tokio::time::Interval,
    pub watch_stream: WatchStream,
    pub request_rx: mpsc::UnboundedReceiver<Request>,
    pub task_group: TaskGroup,
    #[expect(dead_code)]
    pub shutdown_drop_guard: DropGuard,
}

impl StateMachine {
    pub async fn init(self) -> anyhow::Result<Self> {
        let response = self
            .etcd
            .kv_client()
            .get(self.root.clone(), Some(GetOptions::new().with_prefix()))
            .await?;
        let events = response.kvs().iter().map(|kv| (EventType::Put, kv));
        self.handle_events(events)?;
        Ok(self)
    }

    pub async fn run(mut self) {
        let mut state = State::Follower(Follower::default());
        loop {
            let result = match &mut state {
                State::Leader(leader) => leader.run(&mut self).await,
                State::Follower(follower) => follower.run(&mut self).await,
                State::Stopped => break,
            };
            match result {
                Ok(next_state) => state = next_state,
                Err(e) => {
                    error!("{e:#}");
                    break;
                }
            }
        }

        info!("Revoking etcd lease");
        if let Err(e) = self.etcd.lease_revoke(self.lease.id()).await {
            error!("Failed to revoke lease: {e}");
        }
    }

    fn handle_watch_response(
        &self,
        response: Result<Option<WatchResponse>, etcd_client::Error>,
    ) -> anyhow::Result<WatchResponse> {
        let response = response
            .context("Failed to receive watch response")?
            .context("Watch stream ended unexpectedly")?;
        if response.events().is_empty() {
            return Ok(response);
        }
        let events = response
            .events()
            .iter()
            .filter_map(|event| event.kv().map(|kv| (event.event_type(), kv)));
        self.handle_events(events)?;
        Ok(response)
    }

    fn handle_events<'a>(
        &self,
        events: impl IntoIterator<Item = (EventType, &'a KeyValue)>,
    ) -> anyhow::Result<()> {
        fn parse_node_addr(x: &[u8]) -> anyhow::Result<SocketAddr> {
            str::from_utf8(x)?.parse().map_err(Into::into)
        }

        let mut topology = self.topology.write().unwrap();
        let Topology { node_addrs, slots } = &mut *topology;

        for (event_type, kv) in events {
            let Some(key) = kv.key().strip_prefix(self.root.as_slice()) else {
                continue;
            };
            if let Some(name) = key.strip_prefix(NODE_BASE_PATH) {
                let Some(name) = NodeName::from_hex(name) else {
                    warn!("Invalid node name in key {}", kv.key().as_bstr());
                    continue;
                };
                match event_type {
                    EventType::Put => {
                        let new_addr = match parse_node_addr(kv.value()) {
                            Ok(addr) => addr,
                            Err(e) => {
                                warn!("Invalid node address in key {}: {}", kv.key().as_bstr(), e);
                                continue;
                            }
                        };
                        match node_addrs.entry(name.clone()) {
                            Entry::Occupied(mut entry) => {
                                let addr = entry.get_mut();
                                if *addr != new_addr {
                                    info!(
                                        "Address of node {name} changed from {addr} to {new_addr}"
                                    );
                                    *addr = new_addr;
                                }
                            }
                            Entry::Vacant(entry) => {
                                info!("Node {name} at {new_addr} joined the cluster");
                                entry.insert(new_addr);
                            }
                        }
                    }
                    EventType::Delete => {
                        ensure!(
                            name != self.this_node,
                            "This node was removed from the cluster"
                        );
                        if node_addrs.remove(&name).is_some() {
                            info!("Node {name} left the cluster");
                        }
                    }
                }
            } else if key == SLOTS_PATH && event_type == EventType::Put {
                *slots = serde_json::from_slice(kv.value()).context("Failed to parse slot map")?;
            }
        }

        // Remove assignments to nodes that have left the cluster.
        // This will avoid -MOVED redirection to non-existent nodes.
        // The leader will assign these slots to the remaining nodes.
        for (_, maybe_node) in slots.iter_mut() {
            if let Some(node) = maybe_node
                && !node_addrs.contains_key(node)
            {
                *maybe_node = None;
            }
        }

        Ok(())
    }

    fn prefix_with_root(&self, key: &[u8]) -> Vec<u8> {
        let mut k = self.root.clone();
        k.extend_from_slice(key);
        k
    }
}

struct Leader {
    leader_key: LeaderKey,
}

impl Leader {
    async fn run(&self, ctx: &mut StateMachine) -> anyhow::Result<State> {
        pin! {
            let cancelled = ctx.task_group.cancelled();
        }
        loop {
            ensure!(ctx.lease.is_alive(), "Lease expired");

            let assignments = assignment::assign_unassigned_slots(&ctx.topology.read().unwrap());
            if !assignments.is_empty() {
                info!("Assigning unassigned slots");

                // Propagate the error even if it is LeaderError::NonFatal,
                // because not being able to assign unassigned slots is
                // a critical problem.
                self.assign_slots(ctx, &assignments).await?;
            }

            select! {
                () = &mut cancelled => {
                    self.resign(ctx).await?;
                    return Ok(State::Stopped);
                }
                _ = ctx.lease_keep_alive_interval.tick() => ctx.lease.keep_alive().await?,
                result = ctx.watch_stream.message() => {
                    let response = ctx.handle_watch_response(result)?;
                    for event in response.events() {
                        let Some(kv) = event.kv() else {
                            continue;
                        };
                        if kv.key() == self.leader_key.key()
                            && event.event_type() == EventType::Delete
                        {
                            bail!("Lost leadership because leader key was deleted");
                        }
                    }
                }
                Some(request) = ctx.request_rx.recv() => {
                    if let Some(next_state) = self.handle_request(ctx, request).await? {
                        return Ok(next_state);
                    }
                }
            }
        }
    }

    async fn handle_request(
        &self,
        ctx: &StateMachine,
        request: Request,
    ) -> anyhow::Result<Option<State>> {
        fn handle_result<T: Clone>(
            result: Result<T, LeaderError>,
            response_tx: oneshot::Sender<anyhow::Result<T>>,
        ) -> anyhow::Result<Result<T, ()>> {
            match result {
                Ok(value) => {
                    let _ = response_tx.send(Ok(value.clone()));
                    Ok(Ok(value))
                }
                Err(LeaderError::NonFatal(e)) => {
                    let _ = response_tx.send(Err(e));
                    Ok(Err(()))
                }
                Err(e) => Err(e.into()),
            }
        }

        ensure!(ctx.lease.is_alive(), "Lease expired");
        match request {
            Request::AssignSlots {
                slots,
                node,
                response_tx,
            } => {
                let result = self
                    .assign_slots(ctx, &HashMap::from([(node, slots)]))
                    .await;
                let _ = handle_result(result, response_tx)?;
            }
            Request::RebalanceSlots(response_tx) => {
                let result = self.rebalance_slots(ctx).await;
                let _ = handle_result(result, response_tx)?;
            }
            Request::ResignLeader(response_tx) => {
                let result = handle_result(self.resign(ctx).await, response_tx)?;
                if result.is_ok() {
                    return Ok(Some(State::Follower(Follower::default())));
                }
            }
        }
        Ok(None)
    }

    async fn assign_slots(
        &self,
        ctx: &StateMachine,
        assignments: &HashMap<NodeName, HashSet<Slot>>,
    ) -> Result<(), LeaderError> {
        if assignments.is_empty() {
            return Ok(());
        }

        let mut unique_slots = HashSet::new();
        let mut num_changed_slots = 0;
        let mut num_changed_nodes = 0;
        let new_slots = {
            let topology = ctx.topology.read().unwrap();
            let mut new_slots = topology.slots.clone();
            for (node, slots) in assignments {
                if !topology.node_addrs.contains_key(node) {
                    return Err(anyhow!("Node {node} not found in cluster").into());
                }
                let mut changed_node = false;
                for slot in slots {
                    if !unique_slots.insert(slot) {
                        return Err(anyhow!("Slot {slot} specified multiple times").into());
                    }
                    let prev = new_slots[*slot].replace(node.clone());
                    if prev.as_ref() != Some(node) {
                        num_changed_slots += 1;
                        changed_node = true;
                    }
                }
                if changed_node {
                    num_changed_nodes += 1;
                }
            }
            new_slots
        };
        if num_changed_slots == 0 {
            return Ok(());
        }
        info!("Reassigning {num_changed_slots} slots to {num_changed_nodes} nodes");

        let serialized_slots =
            serde_json::to_vec(&new_slots).context("Failed to serialize slot map")?;
        let put = TxnOp::put(ctx.prefix_with_root(SLOTS_PATH), serialized_slots, None);
        self.txn_if_leader(ctx, [put]).await?;
        ctx.topology.write().unwrap().slots = new_slots;
        Ok(())
    }

    async fn rebalance_slots(&self, ctx: &StateMachine) -> Result<(), LeaderError> {
        let assignments = assignment::rebalance_slots(&ctx.topology.read().unwrap());
        if assignments.is_empty() {
            info!("Attempted to rebalance slots, but all slots are already balanced");
            return Ok(());
        }
        info!("Rebalancing slots");
        self.assign_slots(ctx, &assignments).await
    }

    async fn resign(&self, ctx: &StateMachine) -> Result<(), LeaderError> {
        info!("Resigning from leader");
        let result = ctx
            .etcd
            .election_client()
            .resign(Some(
                ResignOptions::new().with_leader(self.leader_key.clone()),
            ))
            .await;
        match result {
            Ok(_) => {
                info!("Successfully resigned from leader");
                Ok(())
            }
            Err(e) => {
                error!("Failed to resign from leader: {e}");
                Err(e.into())
            }
        }
    }

    /// Performs a transaction only if this node is still the leader.
    async fn txn_if_leader(
        &self,
        ctx: &StateMachine,
        ops: impl Into<Vec<TxnOp>>,
    ) -> Result<TxnResponse, LeaderError> {
        let txn = Txn::new()
            .when([Compare::create_revision(
                self.leader_key.key(),
                CompareOp::Equal,
                self.leader_key.rev(),
            )])
            .and_then(ops);
        let response = ctx.etcd.kv_client().txn(txn).await?;
        if response.succeeded() {
            Ok(response)
        } else {
            Err(LeaderError::LostLeadership)
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum LeaderError {
    #[error(transparent)]
    NonFatal(#[from] anyhow::Error),

    #[error("Lost leadership")]
    LostLeadership,
}

impl From<etcd_client::Error> for LeaderError {
    fn from(e: etcd_client::Error) -> Self {
        Self::NonFatal(e.into())
    }
}

#[derive(Default)]
struct Follower {
    leader_kv: Option<KeyValue>,
}

impl Follower {
    async fn run(&mut self, ctx: &mut StateMachine) -> anyhow::Result<State> {
        let leader_path = ctx.prefix_with_root(LEADER_PATH);

        let mut election_client = ctx.etcd.election_client();
        pin! {
            let observe = election_client.observe(leader_path.clone());
        }
        let mut observe = Observe::new(observe);

        let mut election_client = ctx.etcd.election_client();
        if let Ok(response) = election_client.leader(leader_path.clone()).await {
            self.handle_leader_response(ctx, response);
        }

        pin! {
            let cancelled = ctx.task_group.cancelled();
        }
        loop {
            ensure!(ctx.lease.is_alive(), "Lease expired");
            select! {
                () = &mut cancelled => return Ok(State::Stopped),
                _ = ctx.lease_keep_alive_interval.tick() => ctx.lease.keep_alive().await?,
                result = ctx.watch_stream.message() => {
                    let response = ctx.handle_watch_response(result)?;
                    for event in response.events() {
                        let Some(kv) = event.kv() else {
                            continue;
                        };
                        if let Some(leader_kv) = &self.leader_kv
                            && kv.key() == leader_kv.key()
                            && event.event_type() == EventType::Delete
                            && kv.mod_revision() > leader_kv.mod_revision()
                        {
                            info!("Leader with key {} stepped down", kv.key().as_bstr());
                            self.leader_kv = None;
                        }
                    }
                }
                result = election_client.campaign(
                    leader_path.clone(),
                    ctx.this_node.to_hex(),
                    ctx.lease.id(),
                ), if self.leader_kv.is_none() => {
                    let mut response = match result {
                        Ok(response) => response,
                        Err(e) => {
                            warn!("Failed to campaign for leader: {e}");
                            continue;
                        }
                    };
                    let leader_key = response
                        .take_leader()
                        .context("Campaign response missing leader key")?;
                    info!("Became leader with key {}", leader_key.key().as_bstr());
                    return Ok(State::Leader(Leader { leader_key }));
                }
                response = observe.next() => {
                    let response = response
                        .context("Leader observation stream ended unexpectedly")?
                        .context("Failed to observe leader")?;
                    self.handle_leader_response(ctx, response);
                }
                Some(request) = ctx.request_rx.recv() => {
                    match request {
                        Request::AssignSlots { response_tx, .. }
                        | Request::RebalanceSlots(response_tx)
                        | Request::ResignLeader(response_tx) => {
                            let _ = response_tx.send(Err(anyhow::Error::msg("I'm not a leader")));
                        }
                    }
                }
            }
        }
    }

    fn handle_leader_response(&mut self, ctx: &StateMachine, mut response: LeaderResponse) {
        let Some(kv) = response.take_kv() else {
            return;
        };
        let Some(leader_node) = NodeName::from_hex(kv.value()) else {
            error!(
                "Invalid node name in leader key: {}. Ignoring this leader.",
                kv.value().as_bstr()
            );
            return;
        };
        if leader_node == ctx.this_node {
            return;
        }
        if let Some(leader_kv) = &self.leader_kv {
            if kv.mod_revision() <= leader_kv.mod_revision() {
                // Stale information
                return;
            }
            if kv.key() == leader_kv.key() {
                // We already know this leader
                return;
            }
        }
        info!(
            "Elected {} as leader with key {}",
            leader_node,
            kv.key().as_bstr()
        );
        self.leader_kv = Some(kv);
    }
}

#[expect(clippy::large_enum_variant)]
enum Observe<'a, F> {
    Init(Pin<&'a mut F>),
    Stream(ObserveStream),
}

impl<'a, F> Observe<'a, F>
where
    F: Future<Output = Result<ObserveStream, etcd_client::Error>>,
{
    fn new(future: Pin<&'a mut F>) -> Self {
        Self::Init(future)
    }
}

impl<F> Stream for Observe<'_, F>
where
    F: Future<Output = Result<ObserveStream, etcd_client::Error>>,
{
    type Item = Result<LeaderResponse, etcd_client::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this {
            Self::Init(future) => match ready!(future.poll_unpin(cx)) {
                Ok(mut stream) => {
                    let result = stream.poll_next_unpin(cx);
                    *this = Self::Stream(stream);
                    result
                }
                Err(e) => Poll::Ready(Some(Err(e))),
            },
            Self::Stream(stream) => stream.poll_next_unpin(cx),
        }
    }
}
