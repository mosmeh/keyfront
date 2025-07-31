mod backend;
mod client;

use crate::{
    backend::{Backend, MemoryBackend, RedisBackend},
    client::Client,
};
use anyhow::{bail, ensure};
use bstr::ByteSlice;
use clap::{ArgMatches, CommandFactory, FromArgMatches, Parser, parser::ValueSource};
use keyfront::{
    NonZeroMemorySize,
    cluster::{CLUSTER_SLOTS, Node},
    net::{Address, Listener, ListenerKind, Socket},
};
use serde::Deserialize;
use std::{
    backtrace::Backtrace,
    fs::OpenOptions,
    net::{Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    panic::PanicHookInfo,
    path::PathBuf,
    sync::{
        Arc, RwLock,
        atomic::{self, AtomicU64, AtomicUsize},
    },
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    select,
    signal::unix::{SignalKind, signal},
    sync::{Semaphore, TryAcquireError},
};
use tokio_util::{
    sync::{CancellationToken, WaitForCancellationFuture},
    task::TaskTracker,
};
use tracing::{error, info, warn};

#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

macro_rules! config {
    ($(
        $(#[$meta:meta])*
        $field:ident: $ty:ty,
    )*) => {
        #[derive(Clone, Parser, Deserialize)]
        #[clap(version)]
        #[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
        struct Config {
            $(
                $(#[$meta])*
                #[clap(long)]
                $field: $ty,
            )*
        }

        impl Config {
            fn merge(arg_matches: ArgMatches, config_from_args: Self, config_from_file: Self) -> Self {
                Self {
                    $(
                        $field: if arg_matches
                            .value_source(stringify!($field))
                            .is_some_and(|s| s != ValueSource::DefaultValue)
                        {
                            config_from_args.$field
                        } else {
                            config_from_file.$field
                        },
                    )*
                }
            }
        }
    };
}

config! {
    /// Listen for incoming connections on these addresses.
    /// This can be either an IP address and port, or a Unix socket path.
    #[clap(default_values_t = defaults::bind())]
    bind: Vec<Address>,

    /// `listen()` backlog.
    #[clap(default_value_t = defaults::backlog())]
    backlog: u32,

    /// Equivalent to `maxclients` in Redis/Valkey.
    #[clap(default_value_t = defaults::max_clients())]
    max_clients: NonZeroUsize,

    /// Equivalent to `proto-max-bulk-len` in Redis/Valkey.
    #[clap(default_value_t = defaults::proto_max_bulk_len())]
    proto_max_bulk_len: NonZeroMemorySize,

    /// IP address and port to announce to clients and other nodes in the cluster.
    ///
    /// Defaults to the first IP address and port in the `bind` list,
    announce: Option<SocketAddr>,

    /// Run in single-node mode.
    single_node: bool,

    /// Address of the backend Redis/Valkey server.
    /// This can be either an IP address and port, or a Unix socket path.
    ///
    /// If not specified, the server will run with in-memory backend.
    backend: Option<Address>,

    /// Timeout for the backend connection in milliseconds.
    #[clap(default_value_t = defaults::backend_timeout())]
    backend_timeout: NonZeroU64,

    /// Interval in milliseconds to ping the backend server.
    #[clap(default_value_t = defaults::ping_interval())]
    ping_interval: NonZeroU64,

    /// Number of worker threads.
    #[clap(default_value_t = defaults::worker_threads())]
    worker_threads: usize,

    /// Path to the log file.
    ///
    /// If not specified, logs are written to stdout.
    log_file: Option<PathBuf>,

    /// Path to the configuration file.
    ///
    /// This file is in TOML format and provides default values for
    /// the command line arguments.
    /// Command line arguments will override values from this file.
    config_file: Option<PathBuf>,
}

mod defaults {
    use keyfront::{NonZeroMemorySize, net::Address};
    use std::{
        net::Ipv4Addr,
        num::{NonZeroU64, NonZeroUsize},
    };

    pub fn bind() -> Vec<Address> {
        vec![Address::Tcp((Ipv4Addr::LOCALHOST, 6379).into())]
    }

    pub fn backlog() -> u32 {
        511
    }

    pub fn max_clients() -> NonZeroUsize {
        NonZeroUsize::new(10000).unwrap()
    }

    pub fn proto_max_bulk_len() -> NonZeroMemorySize {
        NonZeroMemorySize::new(512 * 1024 * 1024).unwrap()
    }

    pub fn backend_timeout() -> NonZeroU64 {
        NonZeroU64::new(10000).unwrap()
    }

    pub fn ping_interval() -> NonZeroU64 {
        NonZeroU64::new(1000).unwrap()
    }

    pub fn worker_threads() -> usize {
        0
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: defaults::bind(),
            backlog: defaults::backlog(),
            max_clients: defaults::max_clients(),
            proto_max_bulk_len: defaults::proto_max_bulk_len(),
            announce: None,
            single_node: false,
            backend: None,
            backend_timeout: defaults::backend_timeout(),
            ping_interval: defaults::ping_interval(),
            worker_threads: defaults::worker_threads(),
            log_file: None,
            config_file: None,
        }
    }
}

impl Config {
    fn backend_timeout(&self) -> Duration {
        Duration::from_millis(self.backend_timeout.get())
    }

    fn ping_interval(&self) -> Duration {
        Duration::from_millis(self.ping_interval.get())
    }

    fn load() -> anyhow::Result<Self> {
        let arg_matches = Self::command().get_matches();
        let config_from_args = match Self::from_arg_matches(&arg_matches) {
            Ok(config) => config,
            Err(e) => e.format(&mut Self::command()).exit(),
        };
        let config = if let Some(config_file) = &config_from_args.config_file {
            let config_from_file: Self = toml::from_str(&std::fs::read_to_string(config_file)?)?;
            ensure!(
                config_from_file.config_file.is_none(),
                "The configuration file cannot specify config-file",
            );
            Self::merge(arg_matches, config_from_args, config_from_file)
        } else {
            config_from_args
        };
        Ok(config)
    }
}

fn panic_hook(info: &PanicHookInfo) {
    let payload = info.payload();
    let payload = if let Some(s) = payload.downcast_ref::<&str>() {
        Some(*s)
    } else {
        payload.downcast_ref().map(String::as_str)
    };

    let location = info.location().map(ToString::to_string);
    let backtrace = Backtrace::capture();

    error!(
        panic.payload = payload,
        panic.location = location,
        panic.backtrace = ?backtrace,
        "A panic occurred",
    );
}

fn main() -> anyhow::Result<()> {
    let config = Config::load()?;

    let subscriber = tracing_subscriber::fmt().with_env_filter(
        tracing_subscriber::EnvFilter::builder()
            .with_default_directive(tracing::Level::INFO.into())
            .from_env()?,
    );
    if let Some(log_file) = &config.log_file {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_file)?;
        subscriber.with_writer(file).with_ansi(false).init();
        std::panic::set_hook(Box::new(panic_hook));
    } else {
        subscriber.init();
    }

    info!("Starting server");
    let mut runtime = if config.worker_threads > 0 {
        tokio::runtime::Builder::new_multi_thread()
    } else {
        tokio::runtime::Builder::new_current_thread()
    };
    if config.worker_threads > 0 {
        runtime.worker_threads(config.worker_threads);
    }
    let result = runtime
        .enable_io()
        .enable_time()
        .build()
        .map_err(Into::into)
        .and_then(|runtime| runtime.block_on(run(config)));
    match result {
        Ok(()) => {}
        Err(e) => error!("Server encountered an error: {:?}", e),
    }
    info!("Server stopped");

    Ok(())
}

async fn run(config: Config) -> anyhow::Result<()> {
    let shutdown = Shutdown::default();
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            select! {
                _ = sigint.recv() => info!("Received SIGINT"),
                _ = sigterm.recv() => info!("Received SIGTERM"),
            }
            shutdown.request();
        }
    });

    let tracker = TaskTracker::new();
    let result = run_server(config.clone(), &tracker, shutdown.clone()).await;
    info!("Shutting down server");
    shutdown.request();
    tracker.close();
    tracker.wait().await;

    for addr in &config.bind {
        if let Address::Unix(path) = addr
            && let Err(e) = std::fs::remove_file(path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                "Failed to remove Unix socket file '{}': {}",
                path.display(),
                e
            );
        }
    }

    result
}

async fn run_server(
    config: Config,
    tracker: &TaskTracker,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    if let Some(addr) = &config.backend {
        info!("Initializing Redis backend");
        let backend = RedisBackend::connect(addr, &config, tracker, shutdown.clone()).await?;
        Server::run(config, backend, tracker, shutdown).await
    } else {
        info!("Initializing in-memory backend");
        let backend = MemoryBackend::default();
        Server::run(config, backend, tracker, shutdown).await
    }
}

struct Server<B> {
    config: Config,
    bound_addrs: Vec<Address>,
    backend: B,
    slots: RwLock<Box<[Option<Node>; CLUSTER_SLOTS]>>,
    this_node: Node,
    next_client_id: AtomicU64,
    clients_sem: Arc<Semaphore>,
    stats: Statistics,
    shutdown: Shutdown,
}

impl<B: Backend> Server<B> {
    async fn run(
        config: Config,
        backend: B,
        tracker: &TaskTracker,
        shutdown: Shutdown,
    ) -> anyhow::Result<()> {
        let mut sockets = Vec::with_capacity(config.bind.len());
        let mut bound_addrs = Vec::with_capacity(config.bind.len());
        for bind in &config.bind {
            let (socket, addr) = Socket::bind(bind.clone())?;
            sockets.push(socket);
            bound_addrs.push(addr);
        }

        let node_name = Node::generate_random_name();
        info!("My node name is {}", node_name.as_bstr());

        let announced_addr = if let Some(addr) = config.announce {
            addr
        } else if let Some(addr) = bound_addrs.iter().find_map(|addr| match addr {
            Address::Tcp(addr) => Some(*addr),
            Address::Unix(_) => None,
        }) {
            addr
        } else if config.single_node {
            // No one actually cares about this address, so just use a dummy address.
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))
        } else {
            bail!("No TCP bind address specified, and no announce address provided");
        };
        let this_node = Node::new(node_name, announced_addr);

        let assigned_node = config.single_node.then(|| {
            info!("Running in single-node mode. All slots will be assigned to this node.");
            this_node.clone()
        });
        let slots = RwLock::new(vec![assigned_node; CLUSTER_SLOTS].try_into().unwrap());

        let clients_sem = Arc::new(Semaphore::new(config.max_clients.get()));

        let mut listeners = Vec::with_capacity(sockets.len());
        for (socket, addr) in sockets.into_iter().zip(bound_addrs.iter()) {
            listeners.push(socket.listen(config.backlog)?);
            info!("Listening on {addr}");
        }

        let server = Arc::new(Self {
            config,
            bound_addrs,
            backend,
            slots,
            this_node,
            next_client_id: AtomicU64::new(1),
            clients_sem: clients_sem.clone(),
            stats: Statistics::default(),
            shutdown: shutdown.clone(),
        });

        for listener in listeners {
            let server = server.clone();
            let shutdown = shutdown.clone();
            match listener {
                ListenerKind::Tcp(listener) => {
                    tracker.spawn(run_accept_loop(listener, server, tracker.clone(), shutdown));
                }
                ListenerKind::Unix(listener) => {
                    tracker.spawn(run_accept_loop(listener, server, tracker.clone(), shutdown));
                }
            }
        }

        shutdown.requested().await;
        clients_sem.close();
        Ok(())
    }
}

#[derive(Default)]
struct Statistics {
    connections: AtomicUsize,
    rejected_connections: AtomicUsize,
    commands: AtomicUsize,
}

#[derive(Clone, Default)]
struct Shutdown(CancellationToken);

impl Shutdown {
    fn request(&self) {
        self.0.cancel();
    }

    fn requested(&self) -> WaitForCancellationFuture<'_> {
        self.0.cancelled()
    }
}

async fn run_accept_loop<L, B>(
    mut listener: L,
    server: Arc<Server<B>>,
    tracker: TaskTracker,
    shutdown: Shutdown,
) where
    L: Listener,
    B: Backend,
{
    loop {
        select! {
            () = shutdown.requested() => break,
            result = listener.accept() => {
                let (reader, mut writer) = match result {
                    Ok(stream) => stream,
                    Err(e) => {
                        warn!("Failed to accept connection: {e}");
                        continue;
                    },
                };

                let permit = match server.clients_sem.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(TryAcquireError::Closed) => break,
                    Err(TryAcquireError::NoPermits) => {
                        let _ = writer.write_all(b"-ERR max number of clients + cluster connections reached\r\n").await;
                        server.stats.rejected_connections.fetch_add(1, atomic::Ordering::Relaxed);
                        continue;
                    }
                };

                tracker.spawn({
                    let server = server.clone();
                    async move {
                        let _permit = permit;
                        Client::run(&server, reader, writer).await;
                    }
                });

                server.stats.connections.fetch_add(1, atomic::Ordering::Relaxed);
            }
        }
    }
}
