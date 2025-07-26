mod cluster;
mod connection;
mod generic;
mod server;

use crate::{Server, backend::BackendError};
use bstr::ByteSlice;
use bytes::{Buf, BytesMut};
use futures::StreamExt;
use keyfront::{
    ByteBuf,
    cluster::Slot,
    commands::{COMMANDS, Command, CommandId, CommandName},
    reply::Section,
    resp::{ProtocolError, QueryDecoder, WriteResp},
};
use std::sync::atomic;
use tokio::{io::AsyncWriteExt, net::TcpStream, select};
use tokio_util::codec::FramedRead;
use tracing::error;

#[derive(Debug, thiserror::Error)]
enum ConnectionError {
    #[error("communication with backend failed: {0}")]
    Backend(#[from] BackendError),

    #[error("unexpected reply from backend")]
    UnexpectedReply,

    #[error("user requested closing connection")]
    Quit,
}

#[derive(Debug, thiserror::Error)]
enum CommandError {
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

pub struct Client<'a> {
    server: &'a Server,
    id: u64,
    name: Option<ByteBuf>,
    reply: BytesMut,
}

impl<'a> Client<'a> {
    pub async fn run(server: &'a Server, stream: TcpStream) {
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
        loop {
            select! {
                () = server.shutdown.requested() => break,
                result = stream.next() => {
                    let query = match result {
                        Some(Ok(query)) => query,
                        Some(Err(ProtocolError::Io(_))) | None => break,
                        Some(Err(e)) => {
                            write!(client.reply, "-ERR Protocol error: {e}");
                            break;
                        }
                    };
                    let args = stream.decoder().args();
                    match client.relay_or_intercept_command(query, args).await {
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

    async fn relay_or_intercept_command(
        &mut self,
        query: BytesMut,
        args: &[ByteBuf],
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
                return Ok(());
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
        args: &[ByteBuf],
    ) -> Result<(), CommandError> {
        if !command.arity.matches(args.len()) {
            return Err(CommandError::WrongArity);
        }

        let result = match command.id {
            CommandId::Copy => self.copy(args).await,
            CommandId::Move => self.r#move(),
            CommandId::Sort => self.sort(query, args).await,
            CommandId::SortRo => self.sort_ro(query, args).await,
            CommandId::Scan => self.scan(args).await,
            CommandId::Lmpop => self.lmpop(query, args).await,
            CommandId::Sintercard => self.sintercard(query, args).await,
            CommandId::Zinter
            | CommandId::Zintercard
            | CommandId::Zunion
            | CommandId::Zdiff
            | CommandId::Zmpop => self.zset_multi_key(query, args).await,
            CommandId::Zinterstore | CommandId::Zunionstore | CommandId::Zdiffstore => {
                self.zset_multi_key_store(query, args).await
            }
            CommandId::Eval
            | CommandId::EvalRo
            | CommandId::Evalsha
            | CommandId::EvalshaRo
            | CommandId::Fcall
            | CommandId::FcallRo => self.eval(query, args).await,
            CommandId::Xread => self.xread(query, args).await,
            CommandId::Xreadgroup => self.xreadgroup(query, args).await,
            CommandId::Georadius => self.georadius(query, args).await,
            CommandId::Georadiusbymember => self.georadiusbymember(query, args).await,
            CommandId::Shutdown => self.shutdown(args),
            CommandId::Info => self.info(query).await,
            CommandId::Dbsize => self.dbsize().await,
            CommandId::Flushdb => self.flushdb(args).await,
            CommandId::Swapdb => self.swapdb(),
            CommandId::Replicaof | CommandId::Slaveof => self.replicaof(),
            CommandId::Debug => self.debug(query, args).await,
            CommandId::Quit => self.quit(),
            CommandId::Reset => self.reset(),
            CommandId::Hello => self.hello(args),
            CommandId::Select => self.select(args),
            CommandId::Client(c) => self.client(c, args),
            CommandId::Cluster(c) => self.cluster(c, args).await,
            CommandId::Readonly => self.readonly(),
            CommandId::Readwrite => self.readwrite(),
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
            CommandId::Keyfront => self.keyfront(args).await,
            _ => {
                let slot = command
                    .key_spec
                    .as_ref()
                    .map(|spec| self.compute_slot(spec.extract_keys(args)))
                    .transpose()?;
                self.reply.unsplit(self.send_query(slot, query).await?);
                Ok(())
            }
        };

        self.server
            .stats
            .commands
            .fetch_add(1, atomic::Ordering::Relaxed);

        result
    }

    async fn send_query<T: Into<Option<Slot>>>(
        &self,
        slot: T,
        bytes: BytesMut,
    ) -> Result<BytesMut, ConnectionError> {
        let db_id = slot.into().map(|s| u32::from(s.get()));
        self.server
            .backend
            .as_ref()
            .ok_or(ConnectionError::Backend(BackendError::Disconnected))?
            .send(db_id, bytes)
            .await
            .map_err(Into::into)
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
    fn compute_slot<T, I>(&self, keys: I) -> Result<Slot, CommandError>
    where
        T: AsRef<[u8]>,
        I: IntoIterator<Item = T>,
    {
        let slot = Slot::from_keys(keys).ok_or(CommandError::CrossSlot)?;
        let node = &self.server.slots.read().unwrap()[slot.index()];
        match node {
            Some(node) if node == &self.server.this_node => {}
            Some(node) => {
                return Err(CommandError::Moved {
                    slot,
                    addr: format!("{}:{}", node.addr().ip(), node.addr().port()).into(),
                });
            }
            None => return Err(CommandError::HashSlotNotServed),
        }
        Ok(slot)
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
                self.reply.unsplit(self.send_query(None, query).await?);
                Ok(())
            }
            _ => Err(CommandError::Syntax),
        }
    }
}
