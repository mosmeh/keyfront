mod memory;
mod redis;

pub use memory::MemoryBackend;
pub use redis::RedisBackend;

use crate::client::{Client, CommandError};
use bytes::BytesMut;
use keyfront::{
    ByteBuf,
    cluster::Slot,
    commands::Command,
    reply::{Info, KeyspaceStats},
};
use std::future::ready;

include!(concat!(env!("OUT_DIR"), "/version.rs"));

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("disconnected")]
    Disconnected,

    #[error("timed out")]
    Timeout,

    #[error("unexpected reply")]
    UnexpectedReply,
}

pub trait Backend: Send + Sync + 'static {
    /// Returns the version of the backend in the format of `version` in
    /// HELLO command reply.
    fn version(&self) -> &str;

    /// Handles a command from the client, returning the result.
    fn handle_command(
        &self,
        client: &mut Client<'_, Self>,
        query: BytesMut,
        command: &Command,
        args: &mut [ByteBuf],
    ) -> impl Future<Output = Result<(), CommandError>> + Send
    where
        Self: Sized;

    /// Removes all keys.
    fn clear(
        &self,
        sync: Option<SyncMode>,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Returns INFO reply for the specified sections.
    ///
    /// If `sections` is empty, returns the default set of sections.
    fn info<T>(&self, sections: &[T]) -> impl Future<Output = Result<Info, BackendError>> + Send
    where
        T: AsRef<[u8]> + Sync;

    /// Returns the number of keys in `slot`.
    fn count_keys(&self, slot: Slot) -> impl Future<Output = Result<usize, BackendError>> + Send;

    /// Dumps at most `count` keys in `slot` into `out` in RESP array format.
    /// The order of keys is arbitrary.
    fn dump_keys(
        &self,
        slot: Slot,
        count: usize,
        out: &mut BytesMut,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Returns an iterator over all slots and their key statistics.
    fn slot_stats(
        &self,
    ) -> impl Future<Output = Result<impl Iterator<Item = (Slot, KeyspaceStats)>, BackendError>> + Send;

    /// Handles a raw query, returning the raw reply.
    #[expect(unused_variables)]
    fn raw_query<T>(
        &self,
        slot: T,
        bytes: BytesMut,
    ) -> impl Future<Output = Result<BytesMut, BackendError>> + Send
    where
        T: Into<Option<Slot>> + Send,
    {
        ready(Ok(BytesMut::from(
            "-ERR This backend does not support raw queries\r\n",
        )))
    }
}

#[derive(Clone, Copy)]
pub enum SyncMode {
    Async,
    Sync,
}
