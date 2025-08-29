mod backend;
mod client;
mod cluster;

use crate::{
    backend::{Backend, MemoryBackend, RedisBackend},
    client::Client,
    cluster::Cluster,
};
use anyhow::ensure;
use clap::{ArgMatches, CommandFactory, FromArgMatches, Parser, parser::ValueSource};
use keyfront::{
    NonZeroMemorySize,
    net::{Address, IntoSplit, Listener, ListenerKind, Socket, TlsListener},
};
use serde::Deserialize;
use std::{
    backtrace::Backtrace,
    fs::OpenOptions,
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    panic::PanicHookInfo,
    path::PathBuf,
    process::ExitCode,
    sync::{
        Arc,
        atomic::{self, AtomicU64, AtomicUsize},
    },
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    pin, select,
    signal::unix::{SignalKind, signal},
    sync::{Semaphore, TryAcquireError},
};
use tokio_rustls::rustls::{
    self, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    server::WebPkiClientVerifier,
};
use tokio_util::{
    sync::{CancellationToken, DropGuard},
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
        #[derive(Clone, Debug, Parser, Deserialize)]
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

    /// Enable TLS for client connections.
    tls: bool,

    /// Path to the TLS certificate file.
    tls_cert_file: Option<PathBuf>,

    /// Path to the TLS private key file.
    tls_key_file: Option<PathBuf>,

    /// Path to the TLS CA certificate file.
    tls_ca_cert_file: Option<PathBuf>,

    /// Whether to verify client certificates.
    #[clap(default_value_t = defaults::tls_auth_clients())]
    // `bool` is referred to as `std::primitive::bool` here to disable
    // the default `action(ArgAction::SetTrue)`.
    tls_auth_clients: std::primitive::bool,

    /// IP address and port to announce to clients and other nodes in the cluster.
    ///
    /// Defaults to the first IP address and port in the `bind` list,
    announce: Option<SocketAddr>,

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

    pub fn tls_auth_clients() -> bool {
        true
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
            tls: false,
            tls_cert_file: None,
            tls_key_file: None,
            tls_ca_cert_file: None,
            tls_auth_clients: false,
            announce: None,
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
        payload = payload,
        location = location,
        backtrace = ?backtrace,
        "A panic occurred",
    );
}

fn main() -> anyhow::Result<ExitCode> {
    let config = Config::load()?;

    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing::Level::INFO.into())
        .from_env()?;
    let subscriber = tracing_subscriber::fmt()
        .with_target(false)
        .with_timer(tracing_subscriber::fmt::time::ChronoLocal::rfc_3339())
        .with_file(true)
        .with_line_number(true)
        .with_env_filter(env_filter);
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
    info!("Version: {}", env!("CARGO_PKG_VERSION"));
    info!("{config:?}");

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
        .and_then(|runtime| runtime.block_on(run(config.clone())));
    let exit_code = match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            if config.log_file.is_some() {
                error!(
                    backtrace = ?e.backtrace(),
                    "Server encountered an error: {e:#}"
                );
            } else {
                error!("Server encountered an error: {e:?}");
            }
            ExitCode::FAILURE
        }
    };

    info!("Server stopped");

    Ok(exit_code)
}

async fn run(config: Config) -> anyhow::Result<()> {
    if config.tls {
        ensure!(
            config.tls_cert_file.is_some() && config.tls_key_file.is_some(),
            "tls-cert-file and tls-key-file must be specified when TLS is enabled"
        );
        if config.tls_auth_clients {
            ensure!(
                config.tls_ca_cert_file.is_some(),
                "tls-ca-cert-file must be specified when tls-auth-clients is enabled"
            );
        }
    }

    ensure!(
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .is_ok(),
        "Failed to install the default crypto provider for rustls"
    );

    let client_tasks = TaskGroup::default();
    let background_tasks = TaskGroup::default();
    // To trigger the shutdown, cancel client tasks first.
    // Once all clients are disconnected, background tasks will be cancelled.
    // This avoids cancelling essential background tasks while we are still
    // serving clients.
    let shutdown = Shutdown(client_tasks.cancel_token.clone());

    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let _shutdown_drop_guard = shutdown.drop_guard();
            select! {
                _ = sigint.recv() => info!("Received SIGINT"),
                _ = sigterm.recv() => info!("Received SIGTERM"),
            }
        }
    });

    let result = run_server(
        config.clone(),
        client_tasks.clone(),
        background_tasks.clone(),
        shutdown,
    )
    .await;
    info!("Shutting down server");
    client_tasks.cancel();

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

    client_tasks.wait_for_completion().await;
    info!("All clients disconnected");
    background_tasks.cancel();
    background_tasks.wait_for_completion().await;
    result
}

async fn run_server(
    config: Config,
    client_tasks: TaskGroup,
    background_tasks: TaskGroup,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    if let Some(addr) = &config.backend {
        info!("Initializing Redis backend");
        let backend = RedisBackend::connect(addr, &config, background_tasks, &shutdown).await?;
        Server::run(config, backend, client_tasks, shutdown).await
    } else {
        info!("Initializing in-memory backend");
        let backend = MemoryBackend::default();
        Server::run(config, backend, client_tasks, shutdown).await
    }
}

struct Server<B> {
    config: Config,
    backend: B,
    cluster: Cluster,
    bound_addrs: Vec<Address>,
    next_client_id: AtomicU64,
    clients_sem: Arc<Semaphore>,
    client_tasks: TaskGroup,
    stats: Statistics,
    shutdown: Shutdown,
}

impl<B: Backend> Server<B> {
    async fn run(
        config: Config,
        backend: B,
        client_tasks: TaskGroup,
        shutdown: Shutdown,
    ) -> anyhow::Result<()> {
        let tls_config = if config.tls {
            let certs = CertificateDer::pem_file_iter(config.tls_cert_file.as_ref().unwrap())?
                .collect::<Result<Vec<_>, _>>()?;
            let key = PrivateKeyDer::from_pem_file(config.tls_key_file.as_ref().unwrap())?;
            let server_config = ServerConfig::builder();
            let server_config = if config.tls_auth_clients {
                let ca_certs =
                    CertificateDer::pem_file_iter(config.tls_ca_cert_file.as_ref().unwrap())?;
                let mut cert_store = RootCertStore::empty();
                for cert in ca_certs {
                    cert_store.add(cert?)?;
                }
                let verifier = WebPkiClientVerifier::builder(Arc::new(cert_store)).build()?;
                server_config.with_client_cert_verifier(verifier)
            } else {
                server_config.with_no_client_auth()
            };
            let server_config = server_config.with_single_cert(certs, key)?;
            Some(Arc::new(server_config))
        } else {
            None
        };

        let clients_sem = Arc::new(Semaphore::new(config.max_clients.get()));

        let mut sockets = Vec::with_capacity(config.bind.len());
        let mut bound_addrs = Vec::with_capacity(config.bind.len());
        for bind in &config.bind {
            let (socket, addr) = Socket::bind(bind.clone())?;
            sockets.push(socket);
            bound_addrs.push(addr);
        }

        let addr_to_announce = config.announce.or_else(|| {
            bound_addrs.iter().find_map(|addr| match addr {
                Address::Tcp(addr) => Some(*addr),
                Address::Unix(_) => None,
            })
        });
        let cluster = Cluster::connect(addr_to_announce)?;

        let mut listeners = Vec::with_capacity(sockets.len());
        for (socket, addr) in sockets.into_iter().zip(bound_addrs.iter()) {
            listeners.push(socket.listen(config.backlog)?);
            info!("Listening on {addr}");
        }

        let server = Arc::new(Self {
            config,
            backend,
            cluster,
            bound_addrs,
            next_client_id: AtomicU64::new(1),
            clients_sem: clients_sem.clone(),
            client_tasks: client_tasks.clone(),
            stats: Statistics::default(),
            shutdown,
        });

        for listener in listeners {
            let server = server.clone();
            if let Some(tls_config) = &tls_config {
                let tls_config = tls_config.clone();
                match listener {
                    ListenerKind::Tcp(listener) => client_tasks.spawn(run_accept_loop(
                        TlsListener::new(listener, tls_config),
                        server,
                        client_tasks.clone(),
                    )),
                    ListenerKind::Unix(listener) => client_tasks.spawn(run_accept_loop(
                        TlsListener::new(listener, tls_config),
                        server,
                        client_tasks.clone(),
                    )),
                }
            } else {
                match listener {
                    ListenerKind::Tcp(listener) => {
                        client_tasks.spawn(run_accept_loop(listener, server, client_tasks.clone()));
                    }
                    ListenerKind::Unix(listener) => {
                        client_tasks.spawn(run_accept_loop(listener, server, client_tasks.clone()));
                    }
                }
            }
        }

        client_tasks.cancelled().await;
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
struct TaskGroup {
    tracker: TaskTracker,
    cancel_token: CancellationToken,
}

impl TaskGroup {
    fn spawn<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.tracker.spawn(f);
    }

    fn cancel(&self) {
        self.cancel_token.cancel();
        self.tracker.close();
    }

    async fn cancelled(&self) {
        self.cancel_token.cancelled().await;
    }

    async fn wait_for_completion(&self) {
        self.tracker.wait().await;
    }
}

#[derive(Clone)]
struct Shutdown(CancellationToken);

impl Shutdown {
    fn request(&self) {
        self.0.cancel();
    }

    fn drop_guard(&self) -> DropGuard {
        self.0.clone().drop_guard()
    }
}

async fn run_accept_loop<L, B>(listener: L, server: Arc<Server<B>>, task_group: TaskGroup)
where
    L: Listener,
    L::Stream: IntoSplit,
    B: Backend,
{
    pin! {
        let cancelled = task_group.cancelled();
    }
    loop {
        select! {
            () = &mut cancelled => break,
            result = listener.accept() => {
                let mut stream = match result {
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
                        let _ = stream.write_all(b"-ERR max number of clients + cluster connections reached\r\n").await;
                        server.stats.rejected_connections.fetch_add(1, atomic::Ordering::Relaxed);
                        continue;
                    }
                };

                task_group.spawn({
                    let server = server.clone();
                    async move {
                        let _permit = permit;
                        Client::run(&server, stream).await;
                    }
                });

                server.stats.connections.fetch_add(1, atomic::Ordering::Relaxed);
            }
        }
    }
}
