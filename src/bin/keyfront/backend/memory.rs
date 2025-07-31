use crate::{
    backend::{Backend, BackendError, COMPATIBLE_VERSION, SyncMode},
    client::{Client, CommandError},
};
use bstr::ByteSlice;
use bytes::BytesMut;
use keyfront::{
    ByteBuf,
    cluster::{CLUSTER_SLOTS, Slot},
    commands::{Command, CommandId},
    reply::{Info, KeyspaceStats},
    resp::WriteResp,
    string::parse_int,
};
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    mem,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};
use tracing::debug;

type Map = HashMap<ByteBuf, ByteBuf>;

pub struct MemoryBackend(Box<[RwLock<Map>; CLUSTER_SLOTS]>);

impl Default for MemoryBackend {
    fn default() -> Self {
        Self(
            (0..CLUSTER_SLOTS)
                .map(|_| RwLock::default())
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        )
    }
}

impl Backend for MemoryBackend {
    fn version(&self) -> &str {
        COMPATIBLE_VERSION
    }

    async fn handle_command(
        &self,
        client: &mut Client<'_, Self>,
        _query: BytesMut,
        command: &Command,
        args: &mut [ByteBuf],
    ) -> Result<(), CommandError> {
        match command.id {
            CommandId::Ping => self.ping(client, args),
            CommandId::Echo => self.echo(client, args),
            CommandId::Exists => self.exists(client, args),
            CommandId::Get => self.get(client, args),
            CommandId::Mget => self.mget(client, args),
            CommandId::Getset => self.getset(client, args),
            CommandId::Getdel => self.getdel(client, args),
            CommandId::Strlen => self.strlen(client, args),
            CommandId::Set => self.set(client, args),
            CommandId::Setnx => self.setnx(client, args),
            CommandId::Mset => self.mset(client, args),
            CommandId::Msetnx => self.msetnx(client, args),
            CommandId::Del => self.del(client, args),
            CommandId::Incr => self.incr(client, args),
            CommandId::Incrby => self.incrby(client, args),
            CommandId::Decr => self.decr(client, args),
            CommandId::Decrby => self.decrby(client, args),
            CommandId::Debug => self.debug(client, args),
            _ => Err(CommandError::Unimplemented),
        }
    }

    async fn clear(&self, sync: Option<SyncMode>) -> Result<(), BackendError> {
        let maps: Vec<Map> = self
            .write_all()
            .into_iter()
            .map(|mut map| mem::take(&mut *map))
            .collect();
        match sync {
            Some(SyncMode::Async) | None => {
                tokio::task::spawn_blocking(move || drop(maps));
            }
            Some(SyncMode::Sync) => drop(maps),
        }
        Ok(())
    }

    async fn info<T>(&self, sections: &[T]) -> Result<Info, BackendError>
    where
        T: AsRef<[u8]> + Sync,
    {
        const ALL_SECTIONS: &[&[u8]] = &[
            Info::SERVER,
            Info::CLIENTS,
            Info::STATS,
            Info::CLUSTER,
            Info::KEYSPACE,
        ];

        let mut info = Info::default();
        let mut all = sections.is_empty();
        let mut present = HashSet::new();
        for section in sections {
            let section = section.as_ref();
            if section.eq_ignore_ascii_case(b"default")
                || section.eq_ignore_ascii_case(b"all")
                || section.eq_ignore_ascii_case(b"everything")
            {
                all = true;
            } else {
                present.insert(section.to_ascii_lowercase());
            }
        }
        for section in ALL_SECTIONS {
            if all || present.contains(*section) {
                info.insert_section(section);
            }
        }
        if let Some(section) = info.section_mut(Info::KEYSPACE) {
            let stats = KeyspaceStats::aggregate(self.slot_stats().await?.map(|(_, stats)| stats));
            if stats.keys > 0 || stats.expires > 0 {
                section.insert("db0", stats.to_bytes());
            }
        }
        Ok(info)
    }

    async fn count_keys(&self, slot: Slot) -> Result<usize, BackendError> {
        Ok(self.read(slot).len())
    }

    async fn dump_keys(
        &self,
        slot: Slot,
        count: usize,
        out: &mut BytesMut,
    ) -> Result<(), BackendError> {
        let map = self.read(slot);
        let count = count.min(map.len());
        out.write_array(count);
        for key in map.keys().take(count) {
            out.write_bulk(key);
        }
        Ok(())
    }

    async fn slot_stats(
        &self,
    ) -> Result<impl Iterator<Item = (Slot, KeyspaceStats)>, BackendError> {
        let iter = self.read_all().into_iter().enumerate().map(|(slot, map)| {
            let slot = Slot::new(slot as u16).unwrap();
            let stats = KeyspaceStats {
                keys: map.len(),
                // Expiration is not supported in this backend.
                expires: 0,
                avg_ttl: 0,
            };
            (slot, stats)
        });
        Ok(iter)
    }
}

#[expect(clippy::unused_self, clippy::unnecessary_wraps)]
impl MemoryBackend {
    // PING [message]
    fn ping(&self, client: &mut Client<'_, Self>, args: &[ByteBuf]) -> Result<(), CommandError> {
        match args {
            [] => client.reply_mut().write_simple("PONG"),
            [message] => client.reply_mut().write_bulk(message),
            _ => return Err(CommandError::WrongArity),
        }
        Ok(())
    }

    // ECHO message
    fn echo(&self, client: &mut Client<'_, Self>, args: &[ByteBuf]) -> Result<(), CommandError> {
        let [message] = args else { unreachable!() };
        client.reply_mut().write_bulk(message);
        Ok(())
    }

    // EXISTS key [key ...]
    fn exists(&self, client: &mut Client<'_, Self>, args: &[ByteBuf]) -> Result<(), CommandError> {
        let slot = client.compute_slot(args)?;
        let map = self.read(slot);
        let mut num_exists = 0usize;
        for key in args {
            if map.contains_key(key) {
                num_exists += 1;
            }
        }
        drop(map);
        client.reply_mut().write_integer(num_exists);
        Ok(())
    }

    // GET key
    fn get(&self, client: &mut Client<'_, Self>, args: &[ByteBuf]) -> Result<(), CommandError> {
        let [key] = args else { unreachable!() };
        let slot = client.compute_slot([key])?;
        let map = self.read(slot);
        if let Some(value) = map.get(key) {
            client.reply_mut().write_bulk(value);
        } else {
            drop(map);
            client.reply_mut().write_null();
        }
        Ok(())
    }

    // MGET key [key ...]
    fn mget(&self, client: &mut Client<'_, Self>, args: &[ByteBuf]) -> Result<(), CommandError> {
        let slot = client.compute_slot(args)?;
        client.reply_mut().write_array(args.len());
        let map = self.read(slot);
        for key in args {
            if let Some(value) = map.get(key) {
                client.reply_mut().write_bulk(value);
            } else {
                client.reply_mut().write_null();
            }
        }
        Ok(())
    }

    // GETSET key value
    fn getset(
        &self,
        client: &mut Client<'_, Self>,
        args: &mut [ByteBuf],
    ) -> Result<(), CommandError> {
        let [key, value] = args else { unreachable!() };
        let slot = client.compute_slot([&key])?;
        let old_value = self.write(slot).insert(mem::take(key), mem::take(value));
        if let Some(old_value) = old_value {
            client.reply_mut().write_bulk(old_value);
        } else {
            client.reply_mut().write_null();
        }
        Ok(())
    }

    // GETDEL key
    fn getdel(&self, client: &mut Client<'_, Self>, args: &[ByteBuf]) -> Result<(), CommandError> {
        let [key] = args else { unreachable!() };
        let slot = client.compute_slot([key])?;
        let value = self.write(slot).remove(key);
        if let Some(value) = value {
            client.reply_mut().write_bulk(value);
        } else {
            client.reply_mut().write_null();
        }
        Ok(())
    }

    // STRLEN key
    fn strlen(&self, client: &mut Client<'_, Self>, args: &[ByteBuf]) -> Result<(), CommandError> {
        let [key] = args else { unreachable!() };
        let slot = client.compute_slot([key])?;
        let len = self.read(slot).get(key).map_or(0, |value| value.len());
        client.reply_mut().write_integer(len);
        Ok(())
    }

    // SET key value [NX | XX] [GET]
    fn set(&self, client: &mut Client<'_, Self>, args: &mut [ByteBuf]) -> Result<(), CommandError> {
        let [key, value, opts @ ..] = args else {
            unreachable!()
        };
        let mut nx = false;
        let mut xx = false;
        let mut get = false;
        for opt in opts {
            if !xx && opt.eq_ignore_ascii_case(b"NX") {
                nx = true;
            } else if !nx && opt.eq_ignore_ascii_case(b"XX") {
                xx = true;
            } else if opt.eq_ignore_ascii_case(b"GET") {
                get = true;
            } else {
                return Err(CommandError::Syntax);
            }
        }
        let key = mem::take(key);
        let value = mem::take(value);
        let slot = client.compute_slot([&key])?;
        let mut map = self.write(slot);
        match map.entry(key) {
            Entry::Occupied(mut entry) => {
                if get {
                    client.reply_mut().write_bulk(entry.get());
                    if !nx {
                        entry.insert(value);
                    }
                } else if nx {
                    drop(map);
                    client.reply_mut().write_null();
                } else {
                    entry.insert(value);
                    drop(map);
                    client.reply_mut().write_ok();
                }
            }
            Entry::Vacant(entry) => {
                if !xx {
                    entry.insert(value);
                }
                drop(map);
                if get || xx {
                    client.reply_mut().write_null();
                } else {
                    client.reply_mut().write_ok();
                }
            }
        }
        Ok(())
    }

    // SETNX key value
    fn setnx(
        &self,
        client: &mut Client<'_, Self>,
        args: &mut [ByteBuf],
    ) -> Result<(), CommandError> {
        let [key, value] = args else { unreachable!() };
        let slot = client.compute_slot([&key])?;
        let mut map = self.write(slot);
        match map.entry(mem::take(key)) {
            Entry::Occupied(_) => {
                drop(map);
                client.reply_mut().write_integer(0);
            }
            Entry::Vacant(entry) => {
                entry.insert(mem::take(value));
                drop(map);
                client.reply_mut().write_integer(1);
            }
        }
        Ok(())
    }

    // MSET key value [key value ...]
    fn mset(
        &self,
        client: &mut Client<'_, Self>,
        args: &mut [ByteBuf],
    ) -> Result<(), CommandError> {
        if !args.len().is_multiple_of(2) {
            return Err(CommandError::WrongArity);
        }
        let slot = client.compute_slot(args.iter().step_by(2))?;
        let mut map = self.write(slot);
        for kv in args.chunks_exact_mut(2) {
            let [key, value] = kv else { unreachable!() };
            map.insert(mem::take(key), mem::take(value));
        }
        drop(map);
        client.reply_mut().write_ok();
        Ok(())
    }

    // MSETNX key value [key value ...]
    fn msetnx(
        &self,
        client: &mut Client<'_, Self>,
        args: &mut [ByteBuf],
    ) -> Result<(), CommandError> {
        if !args.len().is_multiple_of(2) {
            return Err(CommandError::WrongArity);
        }
        let slot = client.compute_slot(args.iter().step_by(2))?;
        let mut map = self.write(slot);
        for key in args.iter().step_by(2) {
            if map.contains_key(key) {
                drop(map);
                client.reply_mut().write_integer(0);
                return Ok(());
            }
        }
        for kv in args.chunks_exact_mut(2) {
            let [key, value] = kv else { unreachable!() };
            map.insert(mem::take(key), mem::take(value));
        }
        drop(map);
        client.reply_mut().write_integer(1);
        Ok(())
    }

    // DEL key [key ...]
    fn del(&self, client: &mut Client<'_, Self>, args: &[ByteBuf]) -> Result<(), CommandError> {
        let slot = client.compute_slot(args)?;
        let mut num_deleted = 0usize;
        let mut map = self.write(slot);
        for key in args {
            if map.remove(key).is_some() {
                num_deleted += 1;
            }
        }
        drop(map);
        client.reply_mut().write_integer(num_deleted);
        Ok(())
    }

    // INCR key
    fn incr(
        &self,
        client: &mut Client<'_, Self>,
        args: &mut [ByteBuf],
    ) -> Result<(), CommandError> {
        let [key] = args else { unreachable!() };
        self.incr_decr(client, key, 1)
    }

    // INCRBY key increment
    fn incrby(
        &self,
        client: &mut Client<'_, Self>,
        args: &mut [ByteBuf],
    ) -> Result<(), CommandError> {
        let [key, increment] = args else {
            unreachable!()
        };
        let increment = parse_int(increment).ok_or(CommandError::InvalidInteger)?;
        self.incr_decr(client, key, increment)
    }

    // DECR key
    fn decr(
        &self,
        client: &mut Client<'_, Self>,
        args: &mut [ByteBuf],
    ) -> Result<(), CommandError> {
        let [key] = args else { unreachable!() };
        self.incr_decr(client, key, -1)
    }

    // DECRBY key decrement
    fn decrby(
        &self,
        client: &mut Client<'_, Self>,
        args: &mut [ByteBuf],
    ) -> Result<(), CommandError> {
        let [key, decrement] = args else {
            unreachable!()
        };
        let decrement: i64 = parse_int(decrement).ok_or(CommandError::InvalidInteger)?;
        let Some(increment) = decrement.checked_neg() else {
            client
                .reply_mut()
                .write_error("-ERR decrement would overflow");
            return Ok(());
        };
        self.incr_decr(client, key, increment)
    }

    fn incr_decr(
        &self,
        client: &mut Client<'_, Self>,
        key: &mut ByteBuf,
        delta: i64,
    ) -> Result<(), CommandError> {
        let slot = client.compute_slot([&key])?;
        let mut map = self.write(slot);
        let mut itoa_buf = itoa::Buffer::new();
        let new_value = match map.entry(mem::take(key)) {
            Entry::Occupied(mut entry) => {
                let value: i64 = parse_int(entry.get()).ok_or(CommandError::InvalidInteger)?;
                let Some(new_value) = value.checked_add(delta) else {
                    drop(map);
                    client
                        .reply_mut()
                        .write_error("-ERR increment or decrement would overflow");
                    return Ok(());
                };
                entry.insert(itoa_buf.format(new_value).into());
                new_value
            }
            Entry::Vacant(entry) => {
                entry.insert(itoa_buf.format(delta).into());
                delta
            }
        };
        drop(map);
        client.reply_mut().write_integer(new_value);
        Ok(())
    }

    fn debug(&self, client: &mut Client<'_, Self>, args: &[ByteBuf]) -> Result<(), CommandError> {
        match args {
            [subcommand, ..] if subcommand.eq_ignore_ascii_case(b"OOM") => {
                // The document of `Vec::with_capacity` says "Panics if the new capacity exceeds `isize::MAX` bytes."
                let _ = Vec::<u8>::with_capacity(isize::MAX.cast_unsigned());
                client.reply_mut().write_ok();
            }
            [subcommand, message] if subcommand.eq_ignore_ascii_case(b"LOG") => {
                debug!("DEBUG LOG: {}", message.as_bstr());
                client.reply_mut().write_ok();
            }
            [subcommand, count, opts @ ..] if subcommand.eq_ignore_ascii_case(b"POPULATE") => {
                const VALUE_PREFIX: &[u8] = b"value:";
                const BUF_LEN: usize = 127;

                let count: usize = parse_int(count).ok_or(CommandError::InvalidInteger)?;
                let mut opts = opts;
                let key_prefix = if let [prefix, rest @ ..] = opts {
                    opts = rest;
                    let mut prefix = prefix.to_vec();
                    prefix.push(b':');
                    prefix.truncate(BUF_LEN);
                    prefix
                } else {
                    b"key:".to_vec()
                };
                let value_len = if let [size, rest @ ..] = opts {
                    opts = rest;
                    let size = parse_int(size).ok_or(CommandError::InvalidInteger)?;
                    Some(size)
                } else {
                    None
                };
                if !opts.is_empty() {
                    return Err(CommandError::Syntax);
                }

                let mut key = [0u8; BUF_LEN];
                key[..key_prefix.len()].copy_from_slice(&key_prefix);

                let mut value = [0u8; BUF_LEN];
                value[..VALUE_PREFIX.len()].copy_from_slice(VALUE_PREFIX);

                let mut locked_maps = self.write_all();
                for map in &mut locked_maps {
                    if let Some(additional) = (count / CLUSTER_SLOTS).checked_sub(map.len())
                        && map.try_reserve(additional).is_err()
                    {
                        client.reply_mut().write_error("-ERR OOM");
                        return Ok(());
                    }
                }

                let mut itoa_buf = itoa::Buffer::new();
                for i in 0..count {
                    let index_bytes = itoa_buf.format(i).as_bytes();

                    let key_end = (key_prefix.len() + index_bytes.len()).min(BUF_LEN);
                    if let Some(index_len) = key_end.checked_sub(key_prefix.len()) {
                        key[key_prefix.len()..][..index_len]
                            .copy_from_slice(&index_bytes[..index_len]);
                    }
                    let key = ByteBuf::from(&key[..key_end]);

                    let map = &mut locked_maps[Slot::from_key(&key).index()];
                    let Entry::Vacant(entry) = map.entry(key) else {
                        continue;
                    };

                    let value_end = VALUE_PREFIX.len() + index_bytes.len();
                    value[VALUE_PREFIX.len()..value_end].copy_from_slice(index_bytes);
                    let value = match value_len {
                        Some(n) if n <= BUF_LEN => value[..n].into(),
                        Some(n) => {
                            let mut v = Vec::with_capacity(n);
                            v.extend_from_slice(&value);
                            v.resize(n, 0);
                            v.into()
                        }
                        None => value[..value_end].into(),
                    };
                    entry.insert(value);
                }
                drop(locked_maps);

                client.reply_mut().write_ok();
            }
            _ => return Err(CommandError::Syntax),
        }
        Ok(())
    }

    fn read(&self, slot: Slot) -> RwLockReadGuard<'_, Map> {
        self.0[slot.index()].read().unwrap()
    }

    fn read_all(&self) -> Vec<RwLockReadGuard<'_, Map>> {
        self.0.iter().map(|lock| lock.read().unwrap()).collect()
    }

    fn write(&self, slot: Slot) -> RwLockWriteGuard<'_, Map> {
        self.0[slot.index()].write().unwrap()
    }

    fn write_all(&self) -> Vec<RwLockWriteGuard<'_, Map>> {
        self.0.iter().map(|lock| lock.write().unwrap()).collect()
    }
}
