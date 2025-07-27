use crate::client::{Client, CommandError, ConnectionError};
use bstr::ByteSlice;
use bytes::BytesMut;
use keyfront::{
    ByteBuf,
    net::Address,
    query,
    reply::{InfoReply, KeyspaceStats},
    resp::WriteResp,
};
use std::{net::SocketAddr, sync::atomic};
use tracing::{debug, info};

#[expect(clippy::unnecessary_wraps)]
impl Client<'_> {
    // SHUTDOWN [NOSAVE | SAVE] [NOW] [FORCE] [ABORT]
    pub(super) fn shutdown(&self, args: &[ByteBuf]) -> Result<(), CommandError> {
        const OPTS: &[&[u8]] = &[b"NOSAVE", b"SAVE", b"NOW", b"FORCE", b"ABORT"];

        for opt in args {
            if !OPTS.iter().any(|o| opt.eq_ignore_ascii_case(o)) {
                return Err(CommandError::Syntax);
            }
        }

        info!("User requested shutdown");
        self.server.shutdown.request();
        Ok(())
    }

    // INFO [section [section ...]]
    pub(super) async fn info(&mut self, query: BytesMut) -> Result<(), CommandError> {
        let info_reply = self.send_query(None, query).await?;
        let mut info = InfoReply::from_bytes(info_reply)
            .ok_or(CommandError::Connection(ConnectionError::UnexpectedReply))?;
        let mut itoa_buf = itoa::Buffer::new();
        let config = &self.server.config;

        if let Some(section) = info.section_mut(InfoReply::SERVER) {
            let mut tcp_addr = None;
            let mut unix_addr = None;
            for addr in &self.server.bound_addrs {
                match addr {
                    Address::Tcp(addr) if tcp_addr.is_none() => tcp_addr = Some(addr),
                    Address::Unix(path) if unix_addr.is_none() => unix_addr = Some(path),
                    _ => {}
                }
            }

            section.replace(
                "tcp_port",
                itoa_buf.format(tcp_addr.map_or(0, SocketAddr::port)),
            );
            section.replace("redis_mode", "cluster");
            section.replace("server_mode", "cluster");

            section.retain(|k, _| !k.starts_with(b"listener"));
            if let Some(addr) = tcp_addr {
                section.insert(
                    "listener0",
                    format!("name=tcp,bind={},port={}", addr.ip(), addr.port()),
                );
            }
            if let Some(path) = unix_addr {
                section.insert("listener1", format!("name=unix,bind={}", path.display()));
            }
        }

        if let Some(section) = info.section_mut(InfoReply::CLIENTS) {
            let max_clients = config.max_clients.get();
            section.replace(
                "connected_clients",
                itoa_buf.format(max_clients - self.server.clients_sem.available_permits()),
            );
            section.replace("maxclients", itoa_buf.format(max_clients));
        }

        if let Some(section) = info.section_mut(InfoReply::STATS) {
            let stats = &self.server.stats;
            section.replace(
                "total_connections_received",
                itoa_buf.format(stats.connections.load(atomic::Ordering::Relaxed)),
            );
            section.replace(
                "total_commands_processed",
                itoa_buf.format(stats.commands.load(atomic::Ordering::Relaxed)),
            );
            section.replace(
                "rejected_connections",
                itoa_buf.format(stats.rejected_connections.load(atomic::Ordering::Relaxed)),
            );
        }

        if let Some(section) = info.section_mut(InfoReply::CLUSTER) {
            section.replace("cluster_enabled", "1");
        }

        if let Some(section) = info.section_mut(InfoReply::KEYSPACE) {
            let stats = KeyspaceStats::aggregate(section.to_slot_stats().map(|(_, stats)| stats));
            section.clear();
            if stats.keys > 0 || stats.expires > 0 {
                section.insert(
                    "db0",
                    format!(
                        "keys={},expires={},avg_ttl={}",
                        stats.keys, stats.expires, stats.avg_ttl
                    ),
                );
            }
        }

        self.reply.write_bulk(info.to_bytes());
        Ok(())
    }

    pub(super) async fn dbsize(&mut self) -> Result<(), CommandError> {
        fn compute_num_keys(info_reply: BytesMut) -> Option<usize> {
            let info = InfoReply::from_bytes(info_reply)?;
            let keyspace = info.section(InfoReply::KEYSPACE)?;
            let stats = KeyspaceStats::aggregate(keyspace.to_slot_stats().map(|(_, stats)| stats));
            Some(stats.keys)
        }

        let info_reply = self.send_query(None, query!("INFO", "keyspace")).await?;
        let num_keys = compute_num_keys(info_reply)
            .ok_or(CommandError::Connection(ConnectionError::UnexpectedReply))?;
        self.reply.write_integer(num_keys);
        Ok(())
    }

    // FLUSHDB [ASYNC | SYNC]
    pub(super) async fn flushdb(&mut self, args: &[ByteBuf]) -> Result<(), CommandError> {
        let query = match args {
            [opt] if opt.eq_ignore_ascii_case(b"ASYNC") | opt.eq_ignore_ascii_case(b"SYNC") => {
                query!("FLUSHALL", opt)
            }
            [] => query!("FLUSHALL"),
            _ => return Err(CommandError::Syntax),
        };
        self.reply.unsplit(self.send_query(None, query).await?);
        Ok(())
    }

    pub(super) fn swapdb(&mut self) -> Result<(), CommandError> {
        self.reply
            .write_error("-ERR SWAPDB is not allowed in cluster mode");
        Ok(())
    }

    pub(super) fn replicaof(&mut self) -> Result<(), CommandError> {
        self.reply
            .write_error("-ERR REPLICAOF not allowed in cluster mode.");
        Ok(())
    }

    pub(super) async fn debug(
        &mut self,
        query: BytesMut,
        args: &[ByteBuf],
    ) -> Result<(), CommandError> {
        match args {
            [subcommand, message] if subcommand.eq_ignore_ascii_case(b"LOG") => {
                debug!("DEBUG LOG: {}", message.as_bstr());
            }
            _ => {}
        }
        self.reply.unsplit(self.send_query(None, query).await?);
        Ok(())
    }
}
