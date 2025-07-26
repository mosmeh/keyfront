use crate::{Address, Config, Shutdown};
use anyhow::{anyhow, bail, ensure};
use bstr::ByteSlice;
use bytes::{Buf, BytesMut};
use futures::StreamExt;
use keyfront::{
    cluster::{CLUSTER_SLOTS, Slot},
    query,
    reply::InfoReply,
    resp::{ProtocolError, ReadResp, ReplyDecoder},
    string::parse_int,
    write_query,
};
use std::{collections::VecDeque, str::FromStr, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::{TcpStream, UnixStream},
    select,
    sync::{mpsc, oneshot},
};
use tokio_util::{codec::FramedRead, task::TaskTracker, time::FutureExt};
use tracing::{error, info};

include!(concat!(env!("OUT_DIR"), "/version.rs"));

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("disconnected")]
    Disconnected,

    #[error("timed out")]
    Timeout,
}

pub struct Backend {
    query_tx: mpsc::UnboundedSender<Query>,
    timeout: Duration,
    version: String,
}

impl Backend {
    pub async fn connect(
        addr: &Address,
        config: &Config,
        tracker: &TaskTracker,
        shutdown: Shutdown,
    ) -> anyhow::Result<Self> {
        let timeout = config.backend_timeout();

        let (query_tx, query_rx) = mpsc::unbounded_channel();
        {
            let shutdown = shutdown.clone();
            info!("Connecting to backend at {addr}");
            match addr {
                Address::Tcp(addr) => {
                    let backend = TcpStream::connect(addr).timeout(timeout).await??;
                    backend.set_nodelay(true)?;
                    let (reader, writer) = backend.into_split();
                    tracker.spawn(run_multiplexer(reader, writer, query_rx, shutdown));
                }
                Address::Unix(path) => {
                    let backend = UnixStream::connect(path).timeout(timeout).await??;
                    let (reader, writer) = backend.into_split();
                    tracker.spawn(run_multiplexer(reader, writer, query_rx, shutdown));
                }
            }
        }

        let info_reply = send(&query_tx, None, query!("INFO"), timeout).await?;
        let Some(info) = InfoReply::from_bytes(info_reply) else {
            bail!("Failed to parse INFO reply")
        };

        let Some(version) = info
            .section(InfoReply::SERVER)
            .and_then(|section| section.get("redis_version"))
        else {
            bail!("Missing redis_version in INFO reply")
        };
        info!("Backend version: Redis {}", version.as_bstr());
        let Ok(version) = String::from_utf8(version.to_vec()) else {
            bail!("Invalid characters in version")
        };
        let Some(parsed_version) = parse_version(&version) else {
            bail!("Invalid version format")
        };
        let compatible_version = parse_version(COMPATIBLE_VERSION).unwrap();
        ensure!(
            parsed_version <= compatible_version,
            "Incompatible backend version: {version} (expected <= {COMPATIBLE_VERSION})"
        );

        tokio::spawn(run_health_checker(
            query_tx.clone(),
            config.ping_interval(),
            timeout,
            shutdown,
        ));

        let backend = Self {
            query_tx,
            timeout,
            version,
        };

        let cluster_enabled: String = backend.config_get("cluster-enabled").await?;
        ensure!(cluster_enabled == "no");

        let databases: usize = backend.config_get("databases").await?;
        ensure!(databases >= CLUSTER_SLOTS);

        let role = backend.role().await?;
        ensure!(role == "master");

        let Some(keyspace) = info.section(InfoReply::KEYSPACE) else {
            bail!("Missing keyspace section in INFO reply")
        };
        let mut non_empty_dbs = 0;
        let mut total_keys = 0;
        for (db_id, stats) in keyspace.to_database_stats() {
            ensure!(
                stats.keys == 0 || db_id < CLUSTER_SLOTS,
                "Databases other than 0-{max_slot} should be empty, but db{db_id} has {keys} keys",
                max_slot = Slot::MAX,
                keys = stats.keys
            );
            if stats.keys > 0 {
                non_empty_dbs += 1;
            }
            total_keys += stats.keys;
        }
        info!(
            "Backend has {non_empty_dbs} / {databases} non-empty databases with {total_keys} keys in total"
        );

        ensure!(
            backend
                .send(None, query!("CLIENT", "SETNAME", "keyfront"))
                .await?
                .read_ok()
        );
        for (k, v) in [
            ("lib-name", "keyfront"),
            ("lib-ver", env!("CARGO_PKG_VERSION")),
        ] {
            ensure!(
                backend
                    .send(None, query!("CLIENT", "SETINFO", k, v))
                    .await?
                    .read_ok(),
            );
        }

        Ok(backend)
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub async fn send(
        &self,
        db_id: Option<u32>,
        bytes: BytesMut,
    ) -> Result<BytesMut, BackendError> {
        send(&self.query_tx, db_id, bytes, self.timeout).await
    }

    async fn config_get<T>(&self, key: &str) -> anyhow::Result<T>
    where
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        let mut reply = self.send(None, query!("CONFIG", "GET", key)).await?;
        match reply.read_array() {
            Some(0) => bail!("Config key not found: {key}"),
            Some(2) => {}
            _ => bail!("Unexpected array length in CONFIG GET reply"),
        }
        if reply
            .read_bulk()
            .is_none_or(|s| s.as_ref() != key.as_bytes())
        {
            bail!("Unexpected CONFIG GET reply key")
        }
        let Some(value) = reply.read_bulk() else {
            bail!("Missing value in CONFIG GET reply")
        };
        str::from_utf8(&value)
            .map_err(|e| anyhow!("Invalid characters in CONFIG GET reply: {e}"))?
            .parse()
            .map_err(|e| anyhow!("Failed to parse CONFIG GET reply: {e}"))
    }

    async fn role(&self) -> anyhow::Result<BytesMut> {
        let mut reply = self.send(None, query!("ROLE")).await?;
        if reply.read_array().is_none_or(|n| n == 0) {
            bail!("Unexpected array length in ROLE reply")
        }
        let Some(role) = reply.read_bulk() else {
            bail!("Missing an element in ROLE reply")
        };
        Ok(role)
    }
}

async fn send(
    query_tx: &mpsc::UnboundedSender<Query>,
    db_id: Option<u32>,
    bytes: BytesMut,
    timeout: Duration,
) -> Result<BytesMut, BackendError> {
    let (reply_tx, reply_rx) = oneshot::channel();
    query_tx
        .send(Query {
            db_id,
            bytes,
            reply_tx,
        })
        .map_err(|_| BackendError::Disconnected)?;
    let result = reply_rx.timeout(timeout).await;
    match result {
        Ok(Ok(reply)) => Ok(reply),
        Ok(Err(_)) => Err(BackendError::Disconnected),
        Err(_) => Err(BackendError::Timeout),
    }
}

fn parse_version(version: &str) -> Option<[u8; 3]> {
    version
        .splitn(3, '.')
        .filter_map(|s| parse_int(s.as_bytes()))
        .collect::<Vec<_>>()
        .try_into()
        .ok()
}

struct Query {
    db_id: Option<u32>,
    bytes: BytesMut,
    reply_tx: oneshot::Sender<BytesMut>,
}

enum PendingReply {
    Relay(oneshot::Sender<BytesMut>),
    ExpectOk,
}

async fn run_multiplexer<R, W>(
    reader: R,
    mut writer: W,
    mut query_rx: mpsc::UnboundedReceiver<Query>,
    shutdown: Shutdown,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut stream = FramedRead::new(reader, ReplyDecoder::default());
    let mut query_buf = BytesMut::new();
    let mut selected_db = 0;
    let mut reply_queue = VecDeque::new();
    let mut itoa_buf = itoa::Buffer::new();
    loop {
        select! {
            () = shutdown.requested() => break,
            result = writer.write_buf(&mut query_buf), if query_buf.has_remaining() => {
                if let Err(e) = result {
                    error!("Failed to write to backend: {e}");
                    break;
                }
            },
            result = stream.next() => {
                let mut reply = match result {
                    Some(Ok(reply)) => reply,
                    None => {
                        error!("Connection closed by backend");
                        break;
                    },
                    Some(Err(ProtocolError::Io(e))) => {
                        error!("Failed to read reply from backend: {e}");
                        break;
                    },
                    Some(Err(e)) => {
                        error!("Failed to parse reply from backend: {e}");
                        break;
                    },
                };
                let Some(pending_reply) = reply_queue.pop_front() else {
                    error!("Unexpected reply from backend");
                    break;
                };
                match pending_reply {
                    PendingReply::Relay(tx) => {
                        let _ = tx.send(reply);
                    },
                    PendingReply::ExpectOk => {
                        if !reply.read_ok() {
                            error!("Expected OK reply, got: {:?}", reply.as_bstr());
                            break;
                        }
                    },
                }
            },
            Some(query) = query_rx.recv() => {
                if let &Some(db_id) = &query.db_id
                    && selected_db != db_id
                {
                    write_query!(query_buf, "SELECT", itoa_buf.format(db_id));
                    reply_queue.push_back(PendingReply::ExpectOk);
                    selected_db = db_id;
                }
                query_buf.unsplit(query.bytes);
                reply_queue.push_back(PendingReply::Relay(query.reply_tx));
            },
        }
    }
    shutdown.request();
}

async fn run_health_checker(
    query_tx: mpsc::UnboundedSender<Query>,
    ping_interval: Duration,
    timeout: Duration,
    shutdown: Shutdown,
) {
    let ping = query!("PING");
    let mut interval = tokio::time::interval(ping_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        select! {
            () = shutdown.requested() => return,
            _ = interval.tick() => {
                let result = send(&query_tx, None, ping.clone(), timeout).await;
                let mut reply = match result {
                    Ok(reply) => reply,
                    Err(e) => {
                        error!("Failed to ping backend: {e}");
                        break;
                    },
                };
                if reply.read_simple().is_none_or(|s| s.as_ref() != b"PONG") {
                    error!("Unexpected ping reply");
                    break;
                }
            }
        }
    }
    shutdown.request();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_version() {
        assert_eq!(parse_version("7.2.5"), Some([7, 2, 5]));
        assert_eq!(parse_version("255.255.255"), Some([255, 255, 255]));
        assert!(parse_version("7.2.256").is_none());
        assert!(parse_version("7.2").is_none());
        assert!(parse_version("7.2.1.0").is_none());
        assert!(parse_version("1.-2.-3").is_none());
        assert!(parse_version("1.2.3-rc4").is_none());
        assert!(parse_version("").is_none());
    }
}
