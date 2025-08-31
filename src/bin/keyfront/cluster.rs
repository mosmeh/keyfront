mod assignment;
mod state_machine;

use crate::{
    Config, Shutdown, TaskGroup,
    cluster::state_machine::{NODE_BASE_PATH, StateMachine},
};
use anyhow::{Context, bail};
use etcd_client::{
    Certificate, Compare, CompareOp, ConnectOptions, Identity, LeaseKeepAliveStream, LeaseKeeper,
    PutOptions, TlsOptions, Txn, TxnOp, WatchOptions,
};
use keyfront::cluster::{NodeName, Slot, SlotMap};
use std::{
    collections::{HashMap, HashSet},
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, RwLock, RwLockReadGuard},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tracing::info;

pub struct Cluster {
    this_node: NodeName,
    topology: Arc<RwLock<Topology>>,
    request_tx: Option<mpsc::UnboundedSender<Request>>,
}

impl Cluster {
    pub async fn connect(
        config: &Config,
        addr_to_announce: Option<SocketAddr>,
        task_group: &TaskGroup,
        shutdown: &Shutdown,
    ) -> anyhow::Result<Self> {
        if config.meta.is_empty() {
            let addr_to_announce = addr_to_announce.unwrap_or_else(|| {
                // This node is the only node in the cluster, so no one actually
                // cares about this address. Just use a placeholder.
                SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))
            });
            Self::new_single_node(addr_to_announce)
        } else {
            let addr_to_announce = addr_to_announce
                .context("No TCP bind address specified, and no announce address provided")?;
            Self::new_multi_node(config, addr_to_announce, task_group, shutdown).await
        }
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
            request_tx: None,
        })
    }

    async fn new_multi_node(
        config: &Config,
        addr_to_announce: SocketAddr,
        task_group: &TaskGroup,
        shutdown: &Shutdown,
    ) -> anyhow::Result<Self> {
        info!("Running in a multi-node cluster mode");

        let mut root = config.meta_root.clone().into_bytes();
        if !root.ends_with(b"/") {
            root.push(b'/');
        }

        let lease_ttl = config
            .etcd_lease_ttl
            .get()
            .try_into()
            .with_context(|| format!("Invalid etcd lease TTL {}", config.etcd_lease_ttl))?;

        let mut etcd = {
            let mut options = ConnectOptions::new()
                .with_timeout(config.etcd_timeout())
                .with_connect_timeout(config.etcd_timeout())
                .with_require_leader(true);

            if config.etcd_tls_enabled() {
                let (Some(cert_file), Some(key_file)) =
                    (&config.etcd_tls_cert_file, &config.etcd_tls_key_file)
                else {
                    bail!(
                        "etcd-tls-cert-file and etcd-tls-key-file must be specified when etcd TLS is enabled"
                    );
                };

                let mut tls_options = TlsOptions::new();

                let cert = std::fs::read(cert_file).with_context(|| {
                    format!("Failed to read etcd TLS cert file {}", cert_file.display())
                })?;
                let key = std::fs::read(key_file).with_context(|| {
                    format!("Failed to read etcd TLS key file {}", key_file.display())
                })?;
                tls_options = tls_options.identity(Identity::from_pem(cert, key));

                if let Some(ca_cert_file) = &config.etcd_tls_ca_cert_file {
                    let ca_cert = std::fs::read(ca_cert_file).with_context(|| {
                        format!(
                            "Failed to read etcd TLS CA cert file {}",
                            ca_cert_file.display()
                        )
                    })?;
                    tls_options = tls_options.ca_certificate(Certificate::from_pem(ca_cert));
                }

                options = options.with_tls(tls_options);
            }

            info!("Connecting to etcd at {:?}", config.meta);
            etcd_client::Client::connect(&config.meta, Some(options)).await?
        };

        let lease = Lease::new(&mut etcd, lease_ttl).await?;
        info!("Acquired etcd lease with ID {}", lease.id());
        let mut lease_keep_alive_interval =
            tokio::time::interval(config.etcd_lease_keep_alive_interval());
        lease_keep_alive_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let this_node =
            register_node(&mut etcd, root.clone(), addr_to_announce, lease.id()).await?;
        info!("My node name is {this_node}");

        let topology = Arc::new(RwLock::new(Topology {
            node_addrs: HashMap::from([(this_node.clone(), addr_to_announce)]),
            slots: SlotMap::default(),
        }));

        let (_, watch_stream) = etcd
            .watch(root.clone(), Some(WatchOptions::new().with_prefix()))
            .await?;

        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let state_machine = StateMachine {
            etcd,
            root,
            this_node: this_node.clone(),
            topology: topology.clone(),
            lease,
            lease_keep_alive_interval,
            watch_stream,
            request_rx,
            task_group: task_group.clone(),
            shutdown_drop_guard: shutdown.drop_guard(),
        }
        .init()
        .await?;
        task_group.spawn(async move { state_machine.run().await });

        Ok(Self {
            this_node,
            topology,
            request_tx: Some(request_tx),
        })
    }

    pub fn this_node(&self) -> &NodeName {
        &self.this_node
    }

    pub fn topology(&self) -> RwLockReadGuard<'_, Topology> {
        self.topology.read().unwrap()
    }

    pub async fn assign_slots(&self, slots: HashSet<Slot>, node: NodeName) -> anyhow::Result<()> {
        self.request(|response_tx| Request::AssignSlots {
            slots,
            node,
            response_tx,
        })
        .await?
    }

    pub async fn rebalance_slots(&self) -> anyhow::Result<()> {
        self.request(Request::RebalanceSlots).await?
    }

    pub async fn resign_leader(&self) -> anyhow::Result<()> {
        self.request(Request::ResignLeader).await?
    }

    async fn request<T, F>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce(oneshot::Sender<T>) -> Request,
    {
        let (response_tx, response_rx) = oneshot::channel();
        let request = f(response_tx);
        self.request_tx
            .as_ref()
            .context("Not available in a single-node cluster")?
            .send(request)
            .context("Failed to send request to cluster state machine")?;
        response_rx
            .await
            .context("Failed to receive response from cluster state machine")
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

async fn register_node(
    client: &mut etcd_client::Client,
    root: Vec<u8>,
    addr_to_announce: SocketAddr,
    lease_id: i64,
) -> anyhow::Result<NodeName> {
    loop {
        let name = NodeName::generate_random().context("Failed to generate a random node name")?;
        let mut key = root.clone();
        key.extend_from_slice(NODE_BASE_PATH);
        key.extend_from_slice(&name.to_hex());
        let txn = Txn::new()
            .when([Compare::create_revision(key.clone(), CompareOp::Equal, 0)])
            .and_then([TxnOp::put(
                key,
                addr_to_announce.to_string(),
                Some(PutOptions::new().with_lease(lease_id)),
            )]);
        if client.txn(txn).await?.succeeded() {
            return Ok(name);
        }
        // The node name conflicted. Try again with a new name.
    }
}

enum Request {
    AssignSlots {
        slots: HashSet<Slot>,
        node: NodeName,
        response_tx: oneshot::Sender<anyhow::Result<()>>,
    },
    RebalanceSlots(oneshot::Sender<anyhow::Result<()>>),
    ResignLeader(oneshot::Sender<anyhow::Result<()>>),
}

struct Lease {
    alive_until: Instant,
    keeper: LeaseKeeper,
    stream: LeaseKeepAliveStream,
}

impl Lease {
    async fn new(etcd: &mut etcd_client::Client, ttl: i64) -> Result<Self, etcd_client::Error> {
        let granted_at = Instant::now();
        let grant_response = etcd.lease_grant(ttl, None).await?;
        let (keeper, stream) = etcd.lease_keep_alive(grant_response.id()).await?;
        let ttl = grant_response.ttl().try_into().unwrap_or(0);
        let alive_until = granted_at + Duration::from_secs(ttl);
        Ok(Self {
            alive_until,
            keeper,
            stream,
        })
    }

    fn id(&self) -> i64 {
        self.keeper.id()
    }

    fn is_alive(&self) -> bool {
        // Instant is monotonically nondecreasing
        Instant::now() < self.alive_until
    }

    async fn keep_alive(&mut self) -> anyhow::Result<()> {
        let refreshed_at = Instant::now();
        self.keeper
            .keep_alive()
            .await
            .context("Failed to keep alive lease")?;
        let response = self
            .stream
            .message()
            .await
            .context("Failed to receive lease keep-alive response")?
            .context("Lease keep-alive response stream was closed unexpectedly")?;
        let ttl = response.ttl().try_into().unwrap_or(0);
        self.alive_until = refreshed_at + Duration::from_secs(ttl);
        Ok(())
    }
}
