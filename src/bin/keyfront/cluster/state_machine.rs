use crate::{
    TaskGroup,
    cluster::{Lease, Request, Topology},
};
use anyhow::{Context, bail, ensure};
use bstr::ByteSlice;
use etcd_client::{
    EventType, GetOptions, KeyValue, LeaderKey, LeaderResponse, ObserveStream, ResignOptions,
    WatchResponse, WatchStream,
};
use futures_util::{FutureExt, Stream, StreamExt};
use keyfront::cluster::NodeName;
use std::{
    collections::hash_map::Entry,
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
        #[expect(clippy::unnecessary_wraps)]
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
            }
        }

        ensure!(ctx.lease.is_alive(), "Lease expired");
        match request {
            Request::ResignLeader(response_tx) => {
                let result = handle_result(self.resign(ctx).await, response_tx)?;
                if result.is_ok() {
                    return Ok(Some(State::Follower(Follower::default())));
                }
            }
        }
        Ok(None)
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
}

#[derive(Debug, thiserror::Error)]
enum LeaderError {
    #[error(transparent)]
    NonFatal(#[from] anyhow::Error),
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
                        Request::ResignLeader(response_tx) => {
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
