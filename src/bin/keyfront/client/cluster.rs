use crate::client::{Client, CommandError, ConnectionError};
use bstr::ByteSlice;
use bytes::{Bytes, BytesMut};
use keyfront::{
    ByteBuf,
    cluster::{CLUSTER_SLOTS, Node, Slot},
    commands::ClusterCommand,
    query,
    reply::{InfoReply, ScanReply, Section},
    resp::{ReadResp, WriteResp},
    string::parse_int,
};
use std::ops::Range;

#[derive(Debug, thiserror::Error)]
enum ClusterError {
    #[error(transparent)]
    Command(#[from] CommandError),

    #[error("-ERR Invalid or out of range slot")]
    InvalidSlot,

    #[error("-ERR start slot number {start} is greater than end slot number {end}")]
    StartGreaterThanEnd { start: Slot, end: Slot },

    #[error("-ERR Slot {0} specified multiple times")]
    SlotSpecifiedMultipleTimes(u16),

    #[error("-ERR Slot {0} is already busy")]
    SlotAlreadyBusy(u16),

    #[error("-ERR Slot {0} is already unassigned")]
    SlotAlreadyUnassigned(u16),
}

impl From<ConnectionError> for ClusterError {
    fn from(e: ConnectionError) -> Self {
        Self::Command(e.into())
    }
}

#[expect(clippy::unnecessary_wraps)]
impl Client<'_> {
    pub(super) async fn cluster(
        &mut self,
        command: ClusterCommand,
        args: &[ByteBuf],
    ) -> Result<(), CommandError> {
        let result = match command {
            ClusterCommand::Info => self.cluster_info(),
            ClusterCommand::Myid => self.cluster_myid(),
            ClusterCommand::Nodes => self.cluster_nodes(),
            ClusterCommand::Slots => self.cluster_slots(),
            ClusterCommand::SlotStats => self.cluster_slot_stats(args).await,
            ClusterCommand::Addslots => self.cluster_addslots(args),
            ClusterCommand::Delslots => self.cluster_delslots(args),
            ClusterCommand::Addslotsrange => self.cluster_addslotsrange(args),
            ClusterCommand::Delslotsrange => self.cluster_delslotsrange(args),
            ClusterCommand::Keyslot => self.cluster_keyslot(args),
            ClusterCommand::Countkeysinslot => self.cluster_countkeysinslot(args).await,
            ClusterCommand::Getkeysinslot => self.cluster_getkeysinslot(args).await,
            _ => return Err(CommandError::Unimplemented),
        };
        match result {
            Ok(()) => {}
            Err(ClusterError::Command(e)) => return Err(e),
            Err(e) => self.reply.write_error(e.to_string()),
        }
        Ok(())
    }

    fn cluster_info(&mut self) -> Result<(), ClusterError> {
        let slots_assigned = self
            .server
            .slots
            .read()
            .unwrap()
            .iter()
            .filter(|slot| slot.is_some())
            .count();
        let mut itoa_buf = itoa::Buffer::new();
        let slots_assigned = itoa_buf.format(slots_assigned);
        let mut info = Section::default();
        info.insert("cluster_state", "ok");
        info.insert("cluster_slots_assigned", slots_assigned);
        info.insert("cluster_slots_ok", slots_assigned);
        info.insert("cluster_slots_pfail", "0");
        info.insert("cluster_slots_fail", "0");
        info.insert("cluster_known_nodes", "1");
        info.insert("cluster_size", "1");
        info.insert("cluster_current_epoch", "0");
        info.insert("cluster_my_epoch", "0");
        self.reply.write_bulk(info.to_bytes());
        Ok(())
    }

    fn cluster_myid(&mut self) -> Result<(), ClusterError> {
        self.reply.write_bulk(self.server.this_node.name());
        Ok(())
    }

    fn cluster_nodes(&mut self) -> Result<(), ClusterError> {
        let this_node = &self.server.this_node;
        self.reply.write_bulk(format!(
            "{} {}:{}@0 myself,master - 0 0 0 connected\n",
            this_node.name().as_bstr(),
            this_node.addr().ip(),
            this_node.addr().port()
        ));
        Ok(())
    }

    fn cluster_slots(&mut self) -> Result<(), ClusterError> {
        fn write_node(buf: &mut BytesMut, node: &Node, slot_range: Range<u16>) {
            buf.write_array(3);
            buf.write_integer(slot_range.start);
            buf.write_integer(slot_range.end - 1);
            buf.write_array(4);
            buf.write_bulk(node.addr().ip().to_string());
            buf.write_integer(node.addr().port());
            buf.write_bulk(node.name());
            buf.write_array(0);
        }

        let mut buf = BytesMut::new();
        let mut num_items = 0;
        let mut current = None;
        {
            let slots = self.server.slots.read().unwrap();
            for (slot, node) in slots.iter().enumerate() {
                if node.as_ref() == current.as_ref().map(|(_, n)| n) {
                    continue;
                }
                let slot = slot as u16;
                if let Some((start_slot, node)) = current {
                    write_node(&mut buf, &node, start_slot..slot);
                    num_items += 1;
                }
                current = node.as_ref().map(|x| (slot, x.clone()));
            }
        }
        if let Some((start_slot, node)) = current {
            write_node(&mut buf, &node, start_slot..CLUSTER_SLOTS as u16);
            num_items += 1;
        }
        self.reply.write_array(num_items);
        self.reply.unsplit(buf);
        Ok(())
    }

    // CLUSTER SLOT-STATS <SLOTSRANGE start-slot end-slot | ORDERBY metric [LIMIT limit] [ASC | DESC]>
    async fn cluster_slot_stats(&mut self, args: &[ByteBuf]) -> Result<(), ClusterError> {
        fn to_reply(key_counts: impl IntoIterator<Item = (Slot, usize)>) -> BytesMut {
            let mut stats_to_reply = BytesMut::new();
            let mut num_slots_in_reply = 0;
            for (slot, key_count) in key_counts {
                stats_to_reply.write_array(2);
                stats_to_reply.write_integer(slot.get());
                stats_to_reply.write_array(2);
                stats_to_reply.write_bulk("key-count");
                stats_to_reply.write_integer(key_count);
                num_slots_in_reply += 1;
            }
            let mut reply = BytesMut::new();
            reply.write_array(num_slots_in_reply);
            reply.unsplit(stats_to_reply);
            reply
        }

        match args {
            [subcommand, start, end] if subcommand.eq_ignore_ascii_case(b"SLOTSRANGE") => {
                let start = parse_int(start)
                    .and_then(Slot::new)
                    .ok_or(ClusterError::InvalidSlot)?;
                let end = parse_int(end)
                    .and_then(Slot::new)
                    .ok_or(ClusterError::InvalidSlot)?;
                if start > end {
                    return Err(ClusterError::StartGreaterThanEnd { start, end });
                }
                let key_counts = self
                    .slot_key_counts()
                    .await?
                    .into_iter()
                    .filter(|&(slot, _)| slot >= start && slot <= end);
                self.reply.unsplit(to_reply(key_counts));
                Ok(())
            }
            [subcommand, metric, opts @ ..] if subcommand.eq_ignore_ascii_case(b"ORDERBY") => {
                if !metric.eq_ignore_ascii_case(b"key-count") {
                    self.reply
                        .write_error("-ERR Unrecognized sort metric for ORDERBY.");
                    return Ok(());
                }

                let mut opts = opts;
                let mut limit = CLUSTER_SLOTS;
                let mut num_limits = 0usize;
                let mut asc = false;
                let mut num_asc_desc = 0usize;
                loop {
                    match opts {
                        [] => break,
                        [opt, value, rest @ ..] if opt.eq_ignore_ascii_case(b"LIMIT") => {
                            let value: i64 =
                                parse_int(value).ok_or(CommandError::InvalidInteger)?;
                            if value < 1 || value > CLUSTER_SLOTS as i64 {
                                write!(
                                    self.reply,
                                    "Limit has to lie in between 1 and {CLUSTER_SLOTS} (maximum number of slots)."
                                );
                                return Ok(());
                            }
                            limit = value as usize;
                            num_limits += 1;
                            opts = rest;
                        }
                        [opt, rest @ ..] if opt.eq_ignore_ascii_case(b"ASC") => {
                            asc = true;
                            num_asc_desc += 1;
                            opts = rest;
                        }
                        [opt, rest @ ..] if opt.eq_ignore_ascii_case(b"DESC") => {
                            asc = false;
                            num_asc_desc += 1;
                            opts = rest;
                        }
                        _ => return Err(ClusterError::Command(CommandError::Syntax)),
                    }
                    if num_limits > 1 || num_asc_desc > 1 {
                        self.reply
                            .write_error("-ERR Multiple filters of the same type are disallowed.");
                        return Ok(());
                    }
                }

                let mut key_counts = self.slot_key_counts().await?;
                key_counts.sort_unstable_by(|(a_slot, a_count), (b_slot, b_count)| {
                    if asc {
                        a_count.cmp(b_count)
                    } else {
                        b_count.cmp(a_count)
                    }
                    .then_with(|| a_slot.cmp(b_slot))
                });
                let key_counts = key_counts.into_iter().take(limit);
                self.reply.unsplit(to_reply(key_counts));
                Ok(())
            }
            _ => Err(CommandError::Syntax.into()),
        }
    }

    async fn slot_key_counts(&self) -> Result<Vec<(Slot, usize)>, ConnectionError> {
        fn to_key_counts(info_reply: BytesMut) -> Option<Box<[usize; CLUSTER_SLOTS]>> {
            let info = InfoReply::from_bytes(info_reply)?;
            let keyspace = info.section(InfoReply::KEYSPACE)?;
            let mut key_counts: Box<[_; CLUSTER_SLOTS]> = vec![0; CLUSTER_SLOTS]
                .into_boxed_slice()
                .try_into()
                .unwrap();
            for (slot, stats) in keyspace.to_slot_stats() {
                key_counts[slot.index()] = stats.keys;
            }
            Some(key_counts)
        }

        let info_reply = self.send_query(None, query!("INFO", "keyspace")).await?;
        let key_counts = to_key_counts(info_reply).ok_or(ConnectionError::UnexpectedReply)?;
        let key_counts = self
            .server
            .slots
            .read()
            .unwrap()
            .iter()
            .enumerate()
            .filter(|&(_, node)| node.as_ref() == Some(&self.server.this_node))
            .map(|(slot, _)| (Slot::new(slot as u16).unwrap(), key_counts[slot]))
            .collect();
        Ok(key_counts)
    }

    // CLUSTER ADDSLOTS slot [slot ...]
    fn cluster_addslots(&mut self, args: &[ByteBuf]) -> Result<(), ClusterError> {
        self.cluster_update_slots(args, true)
    }

    // CLUSTER DELSLOTS slot [slot ...]
    fn cluster_delslots(&mut self, args: &[ByteBuf]) -> Result<(), ClusterError> {
        self.cluster_update_slots(args, false)
    }

    fn cluster_update_slots(&mut self, args: &[ByteBuf], add: bool) -> Result<(), ClusterError> {
        let mut is_slot_updated = [false; CLUSTER_SLOTS];
        for arg in args {
            let slot = parse_int(arg)
                .and_then(Slot::new)
                .ok_or(ClusterError::InvalidSlot)?;
            if std::mem::replace(&mut is_slot_updated[slot.index()], true) {
                return Err(ClusterError::SlotSpecifiedMultipleTimes(slot.get()));
            }
        }
        self.update_slots(&is_slot_updated, add)?;
        self.reply.write_ok();
        Ok(())
    }

    // CLUSTER ADDSLOTSRANGE start-slot end-slot [start-slot end-slot ...]
    fn cluster_addslotsrange(&mut self, args: &[ByteBuf]) -> Result<(), ClusterError> {
        self.cluster_update_slots_range(args, true)
    }

    // CLUSTER DELSLOTSRANGE start-slot end-slot [start-slot end-slot ...]
    fn cluster_delslotsrange(&mut self, args: &[ByteBuf]) -> Result<(), ClusterError> {
        self.cluster_update_slots_range(args, false)
    }

    fn cluster_update_slots_range(
        &mut self,
        args: &[ByteBuf],
        add: bool,
    ) -> Result<(), ClusterError> {
        if !args.len().is_multiple_of(2) {
            return Err(CommandError::WrongArity.into());
        }
        let mut updated_slots = [false; CLUSTER_SLOTS];
        for slots in args.chunks_exact(2) {
            let [start, end] = slots else {
                unreachable!();
            };
            let start = parse_int(start)
                .and_then(Slot::new)
                .ok_or(ClusterError::InvalidSlot)?;
            let end = parse_int(end)
                .and_then(Slot::new)
                .ok_or(ClusterError::InvalidSlot)?;
            if start > end {
                return Err(ClusterError::StartGreaterThanEnd { start, end });
            }
            for slot in start.get()..=end.get() {
                if std::mem::replace(&mut updated_slots[usize::from(slot)], true) {
                    return Err(ClusterError::SlotSpecifiedMultipleTimes(slot));
                }
            }
        }
        self.update_slots(&updated_slots, add)?;
        self.reply.write_ok();
        Ok(())
    }

    fn update_slots(
        &self,
        updated_slots: &[bool; CLUSTER_SLOTS],
        add: bool,
    ) -> Result<(), ClusterError> {
        let mut slots = self.server.slots.write().unwrap();
        for (slot, update) in updated_slots.iter().enumerate() {
            if !*update {
                continue;
            }
            if add && slots[slot].is_some() {
                return Err(ClusterError::SlotAlreadyBusy(slot as u16));
            } else if !add && slots[slot].is_none() {
                return Err(ClusterError::SlotAlreadyUnassigned(slot as u16));
            }
        }
        for (slot, update) in updated_slots.iter().enumerate() {
            if *update {
                slots[slot] = add.then(|| self.server.this_node.clone());
            }
        }
        Ok(())
    }

    // CLUSTER KEYSLOT key
    fn cluster_keyslot(&mut self, args: &[ByteBuf]) -> Result<(), ClusterError> {
        let [key] = args else { unreachable!() };
        self.reply.write_integer(Slot::from_key(key).get());
        Ok(())
    }

    // CLUSTER COUNTKEYSINSLOT slot
    async fn cluster_countkeysinslot(&mut self, args: &[ByteBuf]) -> Result<(), ClusterError> {
        let [slot] = args else { unreachable!() };
        let slot = parse_int(slot)
            .and_then(Slot::new)
            .ok_or(ClusterError::InvalidSlot)?;
        self.reply
            .unsplit(self.send_query(slot, query!("DBSIZE")).await?);
        Ok(())
    }

    // CLUSTER GETKEYSINSLOT slot count
    async fn cluster_getkeysinslot(&mut self, args: &[ByteBuf]) -> Result<(), ClusterError> {
        async fn reply_keys_in_slot(
            client: &mut Client<'_>,
            slot: Slot,
            count: usize,
        ) -> Result<(), ConnectionError> {
            let mut remaining = count;
            let mut cursor = Bytes::from_static(b"0");
            let mut keys_to_reply = BytesMut::new();
            let mut num_keys_in_reply = 0;
            let mut itoa_buf = itoa::Buffer::new();
            while remaining > 0 {
                let query = query!("SCAN", cursor, "COUNT", itoa_buf.format(remaining));
                let scan_reply = client.send_query(slot, query).await?;
                let mut scan_reply =
                    ScanReply::from_bytes(scan_reply).ok_or(ConnectionError::UnexpectedReply)?;

                if scan_reply.num_keys > remaining {
                    for _ in 0..remaining {
                        let key = scan_reply
                            .keys
                            .read_bulk()
                            .ok_or(ConnectionError::UnexpectedReply)?;
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
            client.reply.write_array(num_keys_in_reply);
            client.reply.unsplit(keys_to_reply);
            Ok(())
        }

        let [slot, count] = args else { unreachable!() };
        let (Some(slot), Some(count)) = (parse_int::<i64>(slot), parse_int::<i64>(count)) else {
            return Err(CommandError::InvalidInteger.into());
        };
        let (Some(slot), Ok(count)) = (slot.try_into().ok().and_then(Slot::new), count.try_into())
        else {
            self.reply
                .write_error("-ERR Invalid slot or number of keys");
            return Ok(());
        };

        reply_keys_in_slot(self, slot, count)
            .await
            .map_err(ClusterError::from)
    }

    pub(super) fn readonly(&mut self) -> Result<(), CommandError> {
        self.reply.write_ok();
        Ok(())
    }

    pub(super) fn readwrite(&mut self) -> Result<(), CommandError> {
        self.reply.write_ok();
        Ok(())
    }
}
