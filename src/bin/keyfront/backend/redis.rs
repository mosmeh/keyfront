mod commands;

use crate::{
    Address, Config, Shutdown, TaskGroup,
    backend::{Backend, BackendError, COMPATIBLE_VERSION, SyncMode},
    client::{Client, CommandError},
};
use anyhow::{Context, bail, ensure};
use bstr::ByteSlice;
use bytes::{Buf, Bytes, BytesMut};
use futures_util::StreamExt;
use keyfront::{
    ByteBuf,
    cluster::{CLUSTER_SLOTS, Slot},
    commands::{Command, CommandId},
    net::IntoSplit,
    query,
    reply::{Info, KeyspaceStats, ScanReply},
    resp::{ProtocolError, ReadResp, ReplyDecoder, WriteResp},
    string::parse_int,
    write_query,
};
use std::{collections::VecDeque, str::FromStr, time::Duration};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, UnixStream},
    pin, select,
    sync::{mpsc, oneshot},
};
use tokio_util::{codec::FramedRead, sync::DropGuard, time::FutureExt};
use tracing::{error, info};

pub struct RedisBackend {
    query_tx: mpsc::UnboundedSender<Query>,
    timeout: Duration,
    version: String,
}

impl RedisBackend {
    pub async fn connect(
        addr: &Address,
        config: &Config,
        task_group: TaskGroup,
        shutdown: &Shutdown,
    ) -> anyhow::Result<Self> {
        let timeout = config.backend_timeout();

        let (query_tx, query_rx) = mpsc::unbounded_channel();
        {
            let task_group = task_group.clone();
            let shutdown_drop_guard = shutdown.drop_guard();
            info!("Connecting to backend at {addr}");
            match addr {
                Address::Tcp(addr) => {
                    let stream = TcpStream::connect(addr).timeout(timeout).await??;
                    stream.set_nodelay(true)?;
                    task_group.spawn(run_multiplexer(
                        stream,
                        query_rx,
                        task_group.clone(),
                        shutdown_drop_guard,
                    ));
                }
                Address::Unix(path) => {
                    let stream = UnixStream::connect(path).timeout(timeout).await??;
                    task_group.spawn(run_multiplexer(
                        stream,
                        query_rx,
                        task_group.clone(),
                        shutdown_drop_guard,
                    ));
                }
            }
        }

        let info_reply = send(&query_tx, None, query!("INFO"), timeout).await?;
        let info = Info::from_bytes(info_reply).context("Failed to parse INFO reply")?;

        let version = info
            .section(Info::SERVER)
            .and_then(|section| section.get("redis_version"))
            .context("Missing redis_version in INFO reply")?;
        info!("Backend version: Redis {}", version.as_bstr());
        let version =
            String::from_utf8(version.to_vec()).context("Invalid characters in version")?;
        let parsed_version = parse_version(&version).context("Invalid version format")?;
        let compatible_version = parse_version(COMPATIBLE_VERSION).unwrap();
        ensure!(
            parsed_version <= compatible_version,
            "Incompatible backend version: {version} (expected <= {COMPATIBLE_VERSION})"
        );

        tokio::spawn(run_health_checker(
            query_tx.clone(),
            config.ping_interval(),
            timeout,
            task_group,
            shutdown.drop_guard(),
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

        let keyspace = info
            .section(Info::KEYSPACE)
            .context("Missing keyspace section in INFO reply")?;
        let mut non_empty_dbs = 0usize;
        let mut total_keys = 0;
        for (db_id, stats) in keyspace.to_keyspace_stats() {
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
                .raw_query(None, query!("CLIENT", "SETNAME", "keyfront"))
                .await?
                .read_ok()
        );
        for (k, v) in [
            ("lib-name", "keyfront"),
            ("lib-ver", env!("CARGO_PKG_VERSION")),
        ] {
            ensure!(
                backend
                    .raw_query(None, query!("CLIENT", "SETINFO", k, v))
                    .await?
                    .read_ok(),
            );
        }

        Ok(backend)
    }
}

impl RedisBackend {
    async fn config_get<T>(&self, key: &str) -> anyhow::Result<T>
    where
        T: FromStr,
        T::Err: std::error::Error + Send + Sync + 'static,
    {
        let mut reply = self.raw_query(None, query!("CONFIG", "GET", key)).await?;
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
        let value = reply
            .read_bulk()
            .context("Missing value in CONFIG GET reply")?;
        str::from_utf8(&value)
            .context("Invalid characters in CONFIG GET reply")?
            .parse()
            .context("Failed to parse CONFIG GET reply")
    }

    async fn role(&self) -> anyhow::Result<BytesMut> {
        let mut reply = self.raw_query(None, query!("ROLE")).await?;
        if reply.read_array().is_none_or(|n| n == 0) {
            bail!("Unexpected array length in ROLE reply")
        }
        reply
            .read_bulk()
            .context("Missing an element in ROLE reply")
    }
}

impl Backend for RedisBackend {
    fn version(&self) -> &str {
        &self.version
    }

    async fn handle_command(
        &self,
        client: &mut Client<'_, Self>,
        query: BytesMut,
        command: &Command,
        args: &mut [ByteBuf],
    ) -> Result<(), CommandError> {
        match command.id {
            CommandId::Copy => self.copy(client, args).await,
            CommandId::Sort => self.sort(client, query, args).await,
            CommandId::SortRo => self.sort_ro(client, query, args).await,
            CommandId::Scan => self.scan(client, args).await,
            CommandId::Lmpop => self.lmpop(client, query, args).await,
            CommandId::Sintercard => self.sintercard(client, query, args).await,
            CommandId::Zinter
            | CommandId::Zintercard
            | CommandId::Zunion
            | CommandId::Zdiff
            | CommandId::Zmpop => self.zset_multi_key(client, query, args).await,
            CommandId::Zinterstore | CommandId::Zunionstore | CommandId::Zdiffstore => {
                self.zset_multi_key_store(client, query, args).await
            }
            CommandId::Eval
            | CommandId::EvalRo
            | CommandId::Evalsha
            | CommandId::EvalshaRo
            | CommandId::Fcall
            | CommandId::FcallRo => self.eval(client, query, args).await,
            CommandId::Xread => self.xread(client, query, args).await,
            CommandId::Xreadgroup => self.xreadgroup(client, query, args).await,
            CommandId::Georadius => self.georadius(client, query, args).await,
            CommandId::Georadiusbymember => self.georadiusbymember(client, query, args).await,
            CommandId::Debug => self.debug(client, query, args).await,
            CommandId::Asking | CommandId::Migrate | CommandId::Failover => {
                // Unimplemented features of cluster
                Err(CommandError::Unimplemented)
            }
            CommandId::Acl(_) => Err(CommandError::Unimplemented),
            CommandId::Keys | CommandId::Randomkey => {
                // These commands return keys in the currently selected database.
                // As we use databases to emulate slots, simply relaying them
                // will only return a subset of keys.
                Err(CommandError::Unimplemented)
            }
            CommandId::Auth
            | CommandId::Blmove
            | CommandId::Blmpop
            | CommandId::Blpop
            | CommandId::Brpop
            | CommandId::Brpoplpush
            | CommandId::Bzmpop
            | CommandId::Bzpopmax
            | CommandId::Bzpopmin
            | CommandId::Wait
            | CommandId::Waitaof
            | CommandId::Monitor
            | CommandId::Subscribe
            | CommandId::Psubscribe
            | CommandId::Ssubscribe
            | CommandId::Multi
            | CommandId::Watch
            | CommandId::Sync
            | CommandId::Psync => {
                // These commands put the connection into a special state
                // (blocking, transaction, subscription, etc.)
                // As we use a single connection to the backend,
                // simply relaying them will affect the entire server.
                Err(CommandError::Unimplemented)
            }
            _ => {
                let slot = command
                    .key_spec
                    .as_ref()
                    .map(|spec| client.compute_slot(spec.extract_keys(args)))
                    .transpose()?;
                client.append_reply(self.raw_query(slot, query).await?);
                Ok(())
            }
        }
    }

    async fn clear(&self, sync: Option<SyncMode>) -> Result<(), BackendError> {
        let query = match sync {
            Some(SyncMode::Async) => query!("FLUSHALL", "ASYNC"),
            Some(SyncMode::Sync) => query!("FLUSHALL", "SYNC"),
            None => query!("FLUSHALL"),
        };
        if self.raw_query(None, query).await?.read_ok() {
            Ok(())
        } else {
            Err(BackendError::UnexpectedReply)
        }
    }

    async fn info<T>(&self, sections: &[T]) -> Result<Info, BackendError>
    where
        T: AsRef<[u8]> + Sync,
    {
        let mut query = BytesMut::new();
        query.write_array(sections.len() + 1);
        query.write_bulk(b"INFO");
        for section in sections {
            query.write_bulk(section.as_ref());
        }
        let reply = self.raw_query(None, query).await?;

        let mut info = Info::from_bytes(reply).ok_or(BackendError::UnexpectedReply)?;
        if let Some(section) = info.section_mut(Info::KEYSPACE) {
            let stats = KeyspaceStats::aggregate(
                section
                    .to_keyspace_stats()
                    .filter_map(|(db_id, stats)| (db_id < CLUSTER_SLOTS).then_some(stats)),
            );
            section.clear();
            if stats.keys > 0 || stats.expires > 0 {
                section.insert("db0", stats.to_bytes());
            }
        }
        Ok(info)
    }

    async fn count_keys(&self, slot: Slot) -> Result<usize, BackendError> {
        let mut reply = self.raw_query(slot, query!("DBSIZE")).await?;
        reply.read_integer().ok_or(BackendError::UnexpectedReply)
    }

    async fn dump_keys(
        &self,
        slot: Slot,
        count: usize,
        out: &mut BytesMut,
    ) -> Result<(), BackendError> {
        let mut remaining = count;
        let mut cursor = Bytes::from_static(b"0");
        let mut keys_to_reply = BytesMut::new();
        let mut num_keys_in_reply = 0;
        let mut itoa_buf = itoa::Buffer::new();
        while remaining > 0 {
            let query = query!("SCAN", cursor, "COUNT", itoa_buf.format(remaining));
            let scan_reply = self.raw_query(slot, query).await?;
            let mut scan_reply =
                ScanReply::from_bytes(scan_reply).ok_or(BackendError::UnexpectedReply)?;

            if scan_reply.num_keys > remaining {
                for _ in 0..remaining {
                    let key = scan_reply
                        .keys
                        .read_bulk()
                        .ok_or(BackendError::UnexpectedReply)?;
                    keys_to_reply.write_bulk(key);
                }
                num_keys_in_reply += remaining;
                break;
            }
            keys_to_reply.unsplit(scan_reply.keys);
            num_keys_in_reply += scan_reply.num_keys;

            if scan_reply.cursor == b"0".as_ref() {
                break;
            }

            remaining -= scan_reply.num_keys;
            cursor = scan_reply.cursor.freeze();
        }

        out.write_array(num_keys_in_reply);
        out.unsplit(keys_to_reply);
        Ok(())
    }

    async fn slot_stats(
        &self,
    ) -> Result<impl Iterator<Item = (Slot, KeyspaceStats)>, BackendError> {
        let mut info = self.info(&[Info::KEYSPACE]).await?;
        let section = info
            .section_mut(Info::KEYSPACE)
            .ok_or(BackendError::UnexpectedReply)?;
        Ok(std::mem::take(section)
            .into_keyspace_stats()
            .filter_map(|(db_id, stats)| {
                let slot = Slot::new(db_id.try_into().ok()?)?;
                Some((slot, stats))
            }))
    }

    async fn raw_query<T: Into<Option<Slot>>>(
        &self,
        slot: T,
        bytes: BytesMut,
    ) -> Result<BytesMut, BackendError> {
        send(&self.query_tx, slot.into(), bytes, self.timeout).await
    }
}

async fn send(
    query_tx: &mpsc::UnboundedSender<Query>,
    slot: Option<Slot>,
    bytes: BytesMut,
    timeout: Duration,
) -> Result<BytesMut, BackendError> {
    let (reply_tx, reply_rx) = oneshot::channel();
    query_tx
        .send(Query {
            slot,
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
    slot: Option<Slot>,
    bytes: BytesMut,
    reply_tx: oneshot::Sender<BytesMut>,
}

enum PendingReply {
    Relay(oneshot::Sender<BytesMut>),
    ExpectOk,
}

async fn run_multiplexer<T: IntoSplit>(
    stream: T,
    mut query_rx: mpsc::UnboundedReceiver<Query>,
    task_group: TaskGroup,
    _shutdown_drop_guard: DropGuard,
) {
    let (reader, mut writer) = stream.into_split();
    let mut stream = FramedRead::new(reader, ReplyDecoder::default());
    let mut query_buf = BytesMut::new();
    let mut selected_db = 0;
    let mut reply_queue = VecDeque::new();
    let mut itoa_buf = itoa::Buffer::new();
    pin! {
        let cancelled = task_group.cancelled();
    }
    loop {
        select! {
            () = &mut cancelled => break,
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
                if let &Some(slot) = &query.slot
                    && selected_db != slot.get()
                {
                    write_query!(query_buf, "SELECT", itoa_buf.format(slot.get()));
                    reply_queue.push_back(PendingReply::ExpectOk);
                    selected_db = slot.get();
                }
                query_buf.unsplit(query.bytes);
                reply_queue.push_back(PendingReply::Relay(query.reply_tx));
            },
        }
    }
}

async fn run_health_checker(
    query_tx: mpsc::UnboundedSender<Query>,
    ping_interval: Duration,
    timeout: Duration,
    task_group: TaskGroup,
    _shutdown_drop_guard: DropGuard,
) {
    let ping = query!("PING");
    let mut interval = tokio::time::interval(ping_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    pin! {
        let cancelled = task_group.cancelled();
    }
    loop {
        select! {
            () = &mut cancelled => break,
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
