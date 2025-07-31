use crate::{
    backend::{Backend, SyncMode},
    client::{Client, CommandError},
};
use keyfront::{ByteBuf, net::Address, reply::Info, resp::WriteResp};
use std::{net::SocketAddr, sync::atomic};
use tracing::info;

#[expect(clippy::unnecessary_wraps)]
impl<B: Backend> Client<'_, B> {
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
    pub(super) async fn info(&mut self, args: &[ByteBuf]) -> Result<(), CommandError> {
        let mut info = self.server.backend.info(args).await?;
        let mut itoa_buf = itoa::Buffer::new();

        if let Some(section) = info.section_mut(Info::SERVER) {
            let mut tcp_addr = None;
            let mut unix_addr = None;
            for addr in &self.server.bound_addrs {
                match addr {
                    Address::Tcp(addr) if tcp_addr.is_none() => tcp_addr = Some(addr),
                    Address::Unix(path) if unix_addr.is_none() => unix_addr = Some(path),
                    _ => {}
                }
            }

            section.insert(
                "tcp_port",
                itoa_buf.format(tcp_addr.map_or(0, SocketAddr::port)),
            );
            section.insert("redis_mode", "cluster");
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

        if let Some(section) = info.section_mut(Info::CLIENTS) {
            let max_clients = self.server.config.max_clients.get();
            section.insert(
                "connected_clients",
                itoa_buf.format(max_clients - self.server.clients_sem.available_permits()),
            );
            section.insert("maxclients", itoa_buf.format(max_clients));
        }

        if let Some(section) = info.section_mut(Info::STATS) {
            let stats = &self.server.stats;
            section.insert(
                "total_connections_received",
                itoa_buf.format(stats.connections.load(atomic::Ordering::Relaxed)),
            );
            section.insert(
                "total_commands_processed",
                itoa_buf.format(stats.commands.load(atomic::Ordering::Relaxed)),
            );
            section.insert(
                "rejected_connections",
                itoa_buf.format(stats.rejected_connections.load(atomic::Ordering::Relaxed)),
            );
        }

        if let Some(section) = info.section_mut(Info::CLUSTER) {
            section.insert("cluster_enabled", "1");
        }

        self.reply.write_bulk(info.to_bytes());
        Ok(())
    }

    pub(super) async fn dbsize(&mut self) -> Result<(), CommandError> {
        let mut num_keys = 0usize;
        for (_, stats) in self.server.backend.slot_stats().await? {
            num_keys = num_keys.saturating_add(stats.keys);
        }
        self.reply.write_integer(num_keys);
        Ok(())
    }

    // FLUSHALL [ASYNC | SYNC]
    pub(super) async fn flushall(&mut self, args: &[ByteBuf]) -> Result<(), CommandError> {
        let sync = match args {
            [] => None,
            [opt] if opt.eq_ignore_ascii_case(b"ASYNC") => Some(SyncMode::Async),
            [opt] if opt.eq_ignore_ascii_case(b"SYNC") => Some(SyncMode::Sync),
            _ => return Err(CommandError::Syntax),
        };
        self.server.backend.clear(sync).await?;
        self.reply.write_ok();
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
}
