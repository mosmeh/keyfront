mod cluster;
mod connection;
mod server;

use crate::{
    Server,
    backend::{Backend, BackendError},
};
use bstr::ByteSlice;
use bytes::{Buf, BytesMut};
use futures_util::StreamExt;
use keyfront::{
    ByteBuf,
    cluster::Slot,
    commands::{COMMANDS, Command, CommandId, CommandName},
    net::IntoSplit,
    reply::Section,
    resp::{ProtocolError, QueryDecoder, WriteResp},
};
use std::sync::atomic;
use tokio::{io::AsyncWriteExt, pin, select};
use tokio_util::codec::FramedRead;
use tracing::error;

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("communication with backend failed: {0}")]
    Backend(#[from] BackendError),

    #[error("user requested closing connection")]
    Quit,
}

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error(transparent)]
    Connection(#[from] ConnectionError),

    #[error("-ERR wrong number of arguments")]
    WrongArity,

    #[error("-ERR syntax error")]
    Syntax,

    #[error("-ERR value is not an integer or out of range")]
    InvalidInteger,

    #[error("-CROSSSLOT Keys in request don't hash to the same slot")]
    CrossSlot,

    #[error("-MOVED {} {}", slot, addr.as_bstr())]
    Moved { slot: Slot, addr: ByteBuf },

    #[error("-CLUSTERDOWN Hash slot not served")]
    HashSlotNotServed,

    #[error("-ERR unimplemented command or option")]
    Unimplemented,
}

impl From<BackendError> for CommandError {
    fn from(e: BackendError) -> Self {
        Self::Connection(ConnectionError::Backend(e))
    }
}

pub struct Client<'a, B> {
    server: &'a Server<B>,
    id: u64,
    name: Option<ByteBuf>,
    reply: BytesMut,
}

impl<'a, B: Backend> Client<'a, B> {
    pub async fn run<S: IntoSplit>(server: &'a Server<B>, stream: S) {
        let (reader, mut writer) = stream.into_split();
        let decoder = QueryDecoder::new(server.config.proto_max_bulk_len);
        let mut stream = FramedRead::new(reader, decoder);
        let mut client = Self {
            server,
            id: server
                .next_client_id
                .fetch_add(1, atomic::Ordering::Relaxed),
            name: None,
            reply: BytesMut::new(),
        };
        pin! {
            let cancelled = server.client_tasks.cancelled();
        }
        loop {
            select! {
                () = &mut cancelled => break,
                result = stream.next() => {
                    let query = match result {
                        Some(Ok(query)) => query,
                        Some(Err(ProtocolError::Io(_))) | None => break,
                        Some(Err(e)) => {
                            write!(client.reply, "-ERR Protocol error: {e}");
                            break;
                        }
                    };
                    let args = stream.decoder_mut().args_mut();
                    match client.handle_query(query, args).await {
                        Ok(()) => {}
                        Err(ConnectionError::Quit) => break,
                        Err(e) => {
                            write!(client.reply, "-ERR {e}");
                            break;
                        }
                    }
                }
                result = writer.write_buf(&mut client.reply), if client.reply.has_remaining() => {
                    if result.is_err() {
                        return;
                    }
                }
            }
        }
        let _ = writer.write_all_buf(&mut client.reply).await;
    }

    async fn handle_query(
        &mut self,
        query: BytesMut,
        args: &mut [ByteBuf],
    ) -> Result<(), ConnectionError> {
        fn append_args_to_error(args: &[ByteBuf], out: &mut Vec<u8>) {
            out.extend_from_slice(b"', with args beginning with: ");
            for arg in args {
                out.push(b'\'');
                out.extend_from_slice(&arg[..arg.len().min(128)]);
                out.extend_from_slice(b"' ");
                if out.len() >= 128 {
                    break;
                }
            }
        }

        let [command_name, args @ ..] = args else {
            return Ok(());
        };
        let command_name = CommandName::new(command_name);
        let Some(command) = COMMANDS.get(&command_name) else {
            let mut err = b"-ERR unknown command '".to_vec();
            err.extend_from_slice(command_name.truncated(128).as_bytes());
            append_args_to_error(args, &mut err);
            self.reply.write_error(err);
            return Ok(());
        };

        let (command, args) = match args {
            [subcommand_name, args @ ..] if !command.subcommands.is_empty() => {
                let subcommand_name = CommandName::new(subcommand_name);
                let Some(subcommand) = command.subcommands.get(&subcommand_name) else {
                    let mut err = b"-ERR unknown subcommand '".to_vec();
                    err.extend_from_slice(subcommand_name.truncated(128).as_bytes());
                    err.extend_from_slice(b"'. Try ");
                    err.extend_from_slice(command.full_name.to_ascii_uppercase().as_bytes());
                    err.extend_from_slice(b" HELP.");
                    self.reply.write_error(err);
                    return Ok(());
                };
                (subcommand, args)
            }
            _ => (command, args),
        };

        let result = self.try_handle_command(query, command, args).await;
        match result {
            Ok(()) => {}
            Err(CommandError::Connection(e)) => return Err(e),
            Err(CommandError::Unimplemented) => {
                let mut err = b"-ERR unimplemented command or option: command '".to_vec();
                err.extend_from_slice(command.full_name.as_bytes());
                append_args_to_error(args, &mut err);
                self.reply.write_error(err);
            }
            Err(CommandError::WrongArity) => write!(
                self.reply,
                "-ERR wrong number of arguments for '{}' command",
                command.full_name
            ),
            Err(e) => self.reply.write_error(e.to_string()),
        }
        Ok(())
    }

    async fn try_handle_command(
        &mut self,
        query: BytesMut,
        command: &Command,
        args: &mut [ByteBuf],
    ) -> Result<(), CommandError> {
        if !command.arity.matches(args.len()) {
            return Err(CommandError::WrongArity);
        }

        let result = match command.id {
            CommandId::Shutdown => self.shutdown(args),
            CommandId::Info => self.info(args).await,
            CommandId::Dbsize => self.dbsize().await,
            CommandId::Flushall | CommandId::Flushdb => self.flushall(args).await,
            CommandId::Swapdb => self.swapdb(),
            CommandId::Replicaof | CommandId::Slaveof => self.replicaof(),
            CommandId::Quit => self.quit(),
            CommandId::Reset => self.reset(),
            CommandId::Hello => self.hello(args),
            CommandId::Select => self.select(args),
            CommandId::Client(c) => self.client(c, args),
            CommandId::Cluster(c) => self.cluster(c, args).await,
            CommandId::Readonly => self.readonly(),
            CommandId::Readwrite => self.readwrite(),
            CommandId::Move => self.r#move(),
            CommandId::Keyfront => self.keyfront(args).await,
            _ => {
                self.server
                    .backend
                    .handle_command(self, query, command, args)
                    .await
            }
        };

        self.server
            .stats
            .commands
            .fetch_add(1, atomic::Ordering::Relaxed);

        result
    }

    async fn keyfront(&mut self, args: &[ByteBuf]) -> Result<(), CommandError> {
        match args {
            [subcommand] if subcommand.eq_ignore_ascii_case(b"HELP") => {
                const LINES: &[&str] = &[
                    "KEYFRONT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "INFO",
                    "    Return information about the server.",
                    "RELAY <command> [<arg> ...]",
                    "    Pass through a command to the backend server without intercepting it.",
                    "HELP",
                    "    Print this help.",
                ];
                self.reply.write_array(LINES.len());
                for line in LINES {
                    self.reply.write_simple(line);
                }
                Ok(())
            }
            [subcommand] if subcommand.eq_ignore_ascii_case(b"INFO") => {
                let mut info = Section::default();
                info.insert("version", env!("CARGO_PKG_VERSION"));
                self.reply.write_bulk(info.to_bytes());
                Ok(())
            }
            [subcommand, args @ ..]
                if subcommand.eq_ignore_ascii_case(b"RELAY") && !args.is_empty() =>
            {
                let mut query = BytesMut::new();
                query.write_array(args.len());
                for arg in args {
                    query.write_bulk(arg);
                }
                self.append_reply(self.server.backend.raw_query(None, query).await?);
                Ok(())
            }
            _ => Err(CommandError::Syntax),
        }
    }
}

impl<B> Client<'_, B> {
    pub fn reply_mut(&mut self) -> &mut BytesMut {
        &mut self.reply
    }

    pub fn append_reply(&mut self, bytes: BytesMut) {
        self.reply.unsplit(bytes);
    }

    /// Computes the slot for the given keys.
    ///
    /// # Errors
    ///
    /// Returns an error if the keys do not hash to the same slot,
    /// or if the slot is not served by the current node.
    ///
    /// # Panics
    ///
    /// Panics if `keys` is empty.
    pub fn compute_slot<T, I>(&self, keys: I) -> Result<Slot, CommandError>
    where
        T: AsRef<[u8]>,
        I: IntoIterator<Item = T>,
    {
        let Some(slot) = Slot::from_keys(keys) else {
            return Err(CommandError::CrossSlot);
        };
        let cluster = &self.server.cluster;
        let topology = cluster.topology();
        if let Some(node_name) = topology.slot(slot) {
            if node_name == cluster.this_node() {
                return Ok(slot);
            }
            let addr = *topology.node_addrs().get(node_name).unwrap();
            drop(topology);
            return Err(CommandError::Moved {
                slot,
                addr: format!("{}:{}", addr.ip(), addr.port()).into(),
            });
        }
        Err(CommandError::HashSlotNotServed)
    }

    #[expect(clippy::unnecessary_wraps)]
    fn r#move(&mut self) -> Result<(), CommandError> {
        self.reply
            .write_error("-ERR MOVE is not allowed in cluster mode");
        Ok(())
    }
}
