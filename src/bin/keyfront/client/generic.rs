use crate::client::{Client, CommandError, ConnectionError};
use bstr::ByteSlice;
use bytes::BytesMut;
use keyfront::{
    ByteBuf,
    cluster::{CLUSTER_SLOT_MASK_BITS, Slot},
    query,
    reply::ScanReply,
    resp::WriteResp,
    string::parse_int,
};
use std::cmp::Ordering;

#[expect(clippy::unnecessary_wraps)]
impl Client<'_> {
    // COPY source destination [DB destination-db] [REPLACE]
    pub(super) async fn copy(&mut self, args: &[ByteBuf]) -> Result<(), CommandError> {
        let [source, destination, opts @ ..] = args else {
            unreachable!()
        };

        let slot = self.compute_slot([source, destination])?;

        let mut opts = opts;
        let mut replace = false;
        loop {
            match opts {
                [] => break,
                [opt, db_id, rest @ ..] if opt.eq_ignore_ascii_case(b"DB") => {
                    let db_id: i32 = parse_int(db_id).ok_or(CommandError::InvalidInteger)?;
                    if db_id != 0 {
                        self.reply.write_error("-ERR DB index is out of range");
                        return Ok(());
                    }
                    opts = rest;
                }
                [opt, rest @ ..] if opt.eq_ignore_ascii_case(b"REPLACE") => {
                    replace = true;
                    opts = rest;
                }
                _ => return Err(CommandError::Syntax),
            }
        }

        let query = if replace {
            query!("COPY", source, destination, "REPLACE")
        } else {
            query!("COPY", source, destination)
        };
        self.reply.unsplit(self.send_query(slot, query).await?);
        Ok(())
    }

    pub(super) fn r#move(&mut self) -> Result<(), CommandError> {
        self.reply
            .write_error("-ERR MOVE is not allowed in cluster mode");
        Ok(())
    }

    // SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC | DESC] [ALPHA] [STORE destination]
    pub(super) async fn sort(
        &mut self,
        query: BytesMut,
        args: &[ByteBuf],
    ) -> Result<(), CommandError> {
        let [key, opts @ ..] = args else {
            unreachable!()
        };
        let mut opts = opts;
        let mut store_key = None;
        loop {
            match opts {
                [] => break,
                [opt, _pattern, rest @ ..] if opt.eq_ignore_ascii_case(b"BY") => opts = rest,
                [opt, _ofset, _count, rest @ ..] if opt.eq_ignore_ascii_case(b"LIMIT") => {
                    opts = rest;
                }
                [opt, _pattern, rest @ ..] if opt.eq_ignore_ascii_case(b"GET") => opts = rest,
                [opt, destination, rest @ ..] if opt.eq_ignore_ascii_case(b"STORE") => {
                    store_key = Some(destination);
                    opts = rest;
                }
                [_, rest @ ..] => opts = rest,
            }
        }
        let slot = if let Some(store_key) = store_key {
            self.compute_slot([key, store_key])?
        } else {
            self.compute_slot([key])?
        };
        self.reply.unsplit(self.send_query(slot, query).await?);
        Ok(())
    }

    // SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC | DESC] [ALPHA]
    pub(super) async fn sort_ro(
        &mut self,
        query: BytesMut,
        args: &[ByteBuf],
    ) -> Result<(), CommandError> {
        let [key, ..] = args else { unreachable!() };
        let slot = self.compute_slot([key])?;
        self.reply.unsplit(self.send_query(slot, query).await?);
        Ok(())
    }

    // SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
    pub(super) async fn scan(&mut self, args: &[ByteBuf]) -> Result<(), CommandError> {
        const DEFAULT_SCAN_COMMAND_COUNT: usize = 10;

        let [cursor, opts @ ..] = args else {
            unreachable!()
        };
        let Some(cursor) = parse_int::<u64>(cursor) else {
            self.reply.write_error("-ERR invalid cursor");
            return Ok(());
        };
        let mut opts = opts;
        let mut pattern = None;
        let mut count: Option<usize> = None;
        let mut ty = None;
        loop {
            match opts {
                [] => break,
                [opt, value, rest @ ..] if opt.eq_ignore_ascii_case(b"MATCH") => {
                    // "*" matches all keys, so it's equivalent to no pattern
                    if value.as_ref() != b"*" {
                        pattern = Some(value);
                    }
                    opts = rest;
                }
                [opt, value, rest @ ..] if opt.eq_ignore_ascii_case(b"COUNT") => {
                    let c = match parse_int::<i64>(value) {
                        Some(c @ 1..) => c,
                        Some(_) => return Err(CommandError::Syntax),
                        None => return Err(CommandError::InvalidInteger),
                    };
                    count = Some(c.try_into().map_err(|_| CommandError::InvalidInteger)?);
                    opts = rest;
                }
                [opt, value, rest @ ..] if opt.eq_ignore_ascii_case(b"TYPE") => {
                    const OBJ_TYPE_NAMES: &[&[u8]] =
                        &[b"string", b"list", b"set", b"zset", b"hash", b"stream"];

                    if !OBJ_TYPE_NAMES.iter().any(|x| x.eq_ignore_ascii_case(value)) {
                        write!(self.reply, "-ERR unknown type name '{}'", value.as_bstr());
                        return Ok(());
                    }
                    ty = Some(value);
                    opts = rest;
                }
                _ => return Err(CommandError::Syntax),
            }
        }

        let mut scan = Scan {
            pattern,
            ty,
            remaining: count.unwrap_or(DEFAULT_SCAN_COMMAND_COUNT),
            keys_to_reply: BytesMut::new(),
            num_keys_in_reply: 0,
        };
        let next_cursor = scan.scan(self, cursor).await?;

        self.reply.write_array(2);
        self.reply
            .write_bulk(itoa::Buffer::new().format(next_cursor));
        self.reply.write_array(scan.num_keys_in_reply);
        self.reply.unsplit(scan.keys_to_reply);
        Ok(())
    }
}

struct Scan<'a> {
    pattern: Option<&'a ByteBuf>,
    ty: Option<&'a ByteBuf>,
    remaining: usize,
    keys_to_reply: BytesMut,
    num_keys_in_reply: usize,
}

impl Scan<'_> {
    /// Scans keys until there is no more keys to scan or `remaining` is exhausted.
    ///
    /// Returns the next cursor.
    async fn scan(&mut self, client: &Client<'_>, cursor: u64) -> Result<u64, ConnectionError> {
        let mut slot = Slot::new(cursor as u16 & Slot::MAX).unwrap();
        let mut cursor_in_slot = cursor >> CLUSTER_SLOT_MASK_BITS;

        if let Some(slot_from_pattern) = self.pattern.and_then(Slot::from_pattern) {
            cursor_in_slot = match slot.cmp(&slot_from_pattern) {
                Ordering::Less => {
                    // Fast-forward to the slot
                    slot = slot_from_pattern;
                    self.scan_slot_to_end(client, slot, 0).await?
                }
                Ordering::Equal => {
                    // The cursor is at the correct slot
                    self.scan_slot_to_end(client, slot, cursor_in_slot).await?
                }
                Ordering::Greater => {
                    // The cursor is already past the slot
                    return Ok(0);
                }
            };
            if cursor_in_slot == 0 {
                // Scanning this slot is done, so is the whole scan
                return Ok(0);
            }
        } else {
            while self.remaining > 0 {
                cursor_in_slot = self.scan_slot_to_end(client, slot, cursor_in_slot).await?;
                if cursor_in_slot != 0 {
                    continue;
                }
                match Slot::new(slot.get() + 1) {
                    Some(next_slot) => slot = next_slot,
                    None => {
                        // Scanning the last slot is done, so is the whole scan
                        return Ok(0);
                    }
                }
            }
        }

        let next_cursor = cursor_in_slot
            .checked_shl(CLUSTER_SLOT_MASK_BITS as u32)
            .ok_or(ConnectionError::UnexpectedReply)?
            | u64::from(slot.get());
        Ok(next_cursor)
    }

    /// Scans keys in the `slot` until there is no more keys to scan or `remaining` is exhausted.
    ///
    /// Returns the next cursor.
    async fn scan_slot_to_end(
        &mut self,
        client: &Client<'_>,
        slot: Slot,
        mut cursor: u64,
    ) -> Result<u64, ConnectionError> {
        let mut itoa_buf = itoa::Buffer::new();
        while self.remaining > 0 {
            let mut query = BytesMut::new();
            query.write_array(
                4 + 2 * (usize::from(self.pattern.is_some()) + usize::from(self.ty.is_some())),
            );
            query.write_bulk("SCAN");
            query.write_bulk(itoa_buf.format(cursor));
            query.write_bulk("COUNT");
            query.write_bulk(itoa_buf.format(self.remaining));
            if let Some(pattern) = self.pattern {
                query.write_bulk("MATCH");
                query.write_bulk(pattern);
            }
            if let Some(ty) = self.ty {
                query.write_bulk("TYPE");
                query.write_bulk(ty);
            }

            let scan_reply = client.send_query(slot, query).await?;
            let scan_reply =
                ScanReply::from_bytes(scan_reply).ok_or(ConnectionError::UnexpectedReply)?;

            self.keys_to_reply.unsplit(scan_reply.keys);
            self.num_keys_in_reply += scan_reply.num_keys;

            // It's OK to return more keys than requested, but we stop
            // scanning as soon as we reach the requested number.
            self.remaining = self.remaining.saturating_sub(scan_reply.num_keys);

            cursor = parse_int(&scan_reply.cursor).ok_or(ConnectionError::UnexpectedReply)?;
            if cursor == 0 {
                break;
            }
        }
        Ok(cursor)
    }
}

impl Client<'_> {
    // LMPOP numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
    pub(super) async fn lmpop(
        &mut self,
        query: BytesMut,
        args: &[ByteBuf],
    ) -> Result<(), CommandError> {
        self.generic_multi_key(query, args, None).await
    }

    // SINTERCARD numkeys key [key ...] [LIMIT limit]
    pub(super) async fn sintercard(
        &mut self,
        query: BytesMut,
        args: &[ByteBuf],
    ) -> Result<(), CommandError> {
        self.generic_multi_key(query, args, None).await
    }

    // ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]
    // ZINTERCARD numkeys key [key ...] [LIMIT limit]
    // ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]
    // ZDIFF numkeys key [key ...] [WITHSCORES]
    // ZMPOP numkeys key [key ...] <MIN | MAX> [COUNT count]
    pub(super) async fn zset_multi_key(
        &mut self,
        query: BytesMut,
        args: &[ByteBuf],
    ) -> Result<(), CommandError> {
        self.generic_multi_key(query, args, None).await
    }

    // ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN | MAX>]
    // ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE <SUM | MIN | MAX>]
    // ZDIFFSTORE destination numkeys key [key ...]
    pub(super) async fn zset_multi_key_store(
        &mut self,
        query: BytesMut,
        args: &[ByteBuf],
    ) -> Result<(), CommandError> {
        let [destination, opts @ ..] = args else {
            unreachable!()
        };
        self.generic_multi_key(query, opts, destination).await
    }

    // EVAL script numkeys [key [key ...]] [arg [arg ...]]
    // EVAL_RO script numkeys [key [key ...]] [arg [arg ...]]
    // EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
    // EVALSHA_RO sha1 numkeys [key [key ...]] [arg [arg ...]]
    // FCALL function numkeys [key [key ...]] [arg [arg ...]]
    // FCALL_RO function numkeys [key [key ...]] [arg [arg ...]]
    pub(super) async fn eval(
        &mut self,
        query: BytesMut,
        args: &[ByteBuf],
    ) -> Result<(), CommandError> {
        let [_, opts @ ..] = args else { unreachable!() };
        self.generic_multi_key(query, opts, None).await
    }

    // COMMAND numkeys [key [key ...]] [arg [arg ...]]
    async fn generic_multi_key(
        &mut self,
        query: BytesMut,
        args: &[ByteBuf],
        additional_key: impl Into<Option<&ByteBuf>>,
    ) -> Result<(), CommandError> {
        let [num_keys, opts @ ..] = args else {
            unreachable!()
        };
        let num_keys: i64 = parse_int(num_keys).ok_or(CommandError::InvalidInteger)?;
        let keys = match num_keys.try_into() {
            Ok(n) if n <= opts.len() => &opts[..n],
            Err(_) if num_keys < 0 => {
                self.reply
                    .write_error("-ERR Number of keys can't be negative");
                return Ok(());
            }
            _ => {
                self.reply
                    .write_error("-ERR Number of keys can't be greater than number of args");
                return Ok(());
            }
        };
        let slot = if keys.is_empty() {
            None
        } else {
            Some(self.compute_slot(keys.iter().chain(additional_key.into().into_iter()))?)
        };
        self.reply.unsplit(self.send_query(slot, query).await?);
        Ok(())
    }

    // XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
    pub(super) async fn xread(
        &mut self,
        query: BytesMut,
        args: &[ByteBuf],
    ) -> Result<(), CommandError> {
        let mut opts = args;
        loop {
            match opts {
                [opt, _count, rest @ ..] if opt.eq_ignore_ascii_case(b"COUNT") => opts = rest,
                [opt, ..] if opt.eq_ignore_ascii_case(b"BLOCK") => {
                    return Err(CommandError::Unimplemented);
                }
                [opt, rest @ ..] if opt.eq_ignore_ascii_case(b"STREAMS") && !rest.is_empty() => {
                    if !rest.len().is_multiple_of(2) {
                        self.reply.write_error("-ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.");
                        return Ok(());
                    }
                    opts = rest;
                    break;
                }
                _ => return Err(CommandError::Syntax),
            }
        }
        let slot = self.compute_slot(opts.iter().take(opts.len() / 2))?;
        self.reply.unsplit(self.send_query(slot, query).await?);
        Ok(())
    }

    // XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
    pub(super) async fn xreadgroup(
        &mut self,
        query: BytesMut,
        args: &[ByteBuf],
    ) -> Result<(), CommandError> {
        let mut opts = args;
        let mut has_group = false;
        loop {
            match opts {
                [opt, _group, _consumer, rest @ ..] if opt.eq_ignore_ascii_case(b"GROUP") => {
                    has_group = true;
                    opts = rest;
                }
                [opt, _count, rest @ ..] if opt.eq_ignore_ascii_case(b"COUNT") => opts = rest,
                [opt, ..] if opt.eq_ignore_ascii_case(b"BLOCK") => {
                    return Err(CommandError::Unimplemented);
                }
                [opt, rest @ ..] if opt.eq_ignore_ascii_case(b"NOACK") => opts = rest,
                [opt, rest @ ..] if opt.eq_ignore_ascii_case(b"STREAMS") && !rest.is_empty() => {
                    if !rest.len().is_multiple_of(2) {
                        self.reply.write_error("-ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.");
                        return Ok(());
                    }
                    opts = rest;
                    break;
                }
                _ => return Err(CommandError::Syntax),
            }
        }
        if !has_group {
            self.reply
                .write_error("-ERR Missing GROUP option for XREADGROUP");
            return Ok(());
        }
        let slot = self.compute_slot(opts.iter().take(opts.len() / 2))?;
        self.reply.unsplit(self.send_query(slot, query).await?);
        Ok(())
    }

    // GEORADIUS key longitude latitude radius <M | KM | FT | MI> [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC] [STORE key | STOREDIST key]
    pub(super) async fn georadius(
        &mut self,
        query: BytesMut,
        args: &[ByteBuf],
    ) -> Result<(), CommandError> {
        let [key, _longitude, _latitude, _radius, _unit, opts @ ..] = args else {
            unreachable!()
        };
        self.generic_georadius(query, key, opts).await
    }

    // GEORADIUSBYMEMBER key member radius <M | KM | FT | MI> [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC] [STORE key | STOREDIST key]
    pub(super) async fn georadiusbymember(
        &mut self,
        query: BytesMut,
        args: &[ByteBuf],
    ) -> Result<(), CommandError> {
        let [key, _member, _radius, _unit, opts @ ..] = args else {
            unreachable!()
        };
        self.generic_georadius(query, key, opts).await
    }

    async fn generic_georadius(
        &mut self,
        query: BytesMut,
        key: &ByteBuf,
        mut opts: &[ByteBuf],
    ) -> Result<(), CommandError> {
        let mut store_key = None;
        loop {
            match opts {
                [] => break,
                [opt, k, rest @ ..]
                    if opt.eq_ignore_ascii_case(b"STORE")
                        || opt.eq_ignore_ascii_case(b"STOREDIST") =>
                {
                    store_key = Some(k);
                    opts = rest;
                }
                [_, rest @ ..] => opts = rest,
            }
        }
        let slot = if let Some(store_key) = store_key {
            self.compute_slot([key, store_key])?
        } else {
            self.compute_slot([key])?
        };
        self.reply.unsplit(self.send_query(slot, query).await?);
        Ok(())
    }
}
