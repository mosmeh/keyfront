use crate::{
    backend::{Backend, BackendError},
    client::{Client, CommandError},
};
use bytes::BytesMut;
use keyfront::{
    ByteBuf,
    cluster::{CLUSTER_SLOTS, Slot, SlotMap},
    commands::ClusterCommand,
    reply::InfoSection,
    resp::WriteResp,
    string::parse_int,
};
use std::collections::HashSet;

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error(transparent)]
    Command(#[from] CommandError),

    #[error("-ERR Invalid or out of range slot")]
    InvalidSlot,

    #[error("-ERR start slot number {start} is greater than end slot number {end}")]
    StartGreaterThanEnd { start: Slot, end: Slot },

    #[error("-ERR Slot {0} specified multiple times")]
    SlotSpecifiedMultipleTimes(u16),
}

impl From<BackendError> for ClusterError {
    fn from(e: BackendError) -> Self {
        Self::Command(CommandError::Connection(e.into()))
    }
}

#[expect(clippy::unnecessary_wraps)]
impl<B: Backend> Client<'_, B> {
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
        let topology = self.server.cluster.topology();
        let mut slots_assigned = 0usize;
        let mut nodes_with_slots = HashSet::new();
        for node_name in topology.slots().iter().filter_map(|(_, n)| n.as_ref()) {
            slots_assigned += 1;
            nodes_with_slots.insert(node_name);
        }
        let slots_ok = slots_assigned;
        let known_nodes = topology.node_addrs().len();
        let size = nodes_with_slots.len();
        drop(topology);

        let mut itoa_buf = itoa::Buffer::new();
        let mut info = InfoSection::default();
        info.insert("cluster_state", "ok");
        info.insert("cluster_slots_assigned", itoa_buf.format(slots_assigned));
        info.insert("cluster_slots_ok", itoa_buf.format(slots_ok));
        info.insert("cluster_slots_pfail", "0");
        info.insert("cluster_slots_fail", "0");
        info.insert("cluster_known_nodes", itoa_buf.format(known_nodes));
        info.insert("cluster_size", itoa_buf.format(size));
        info.insert("cluster_current_epoch", "0");
        info.insert("cluster_my_epoch", "0");
        self.reply.write_bulk(info.to_bytes());
        Ok(())
    }

    fn cluster_myid(&mut self) -> Result<(), ClusterError> {
        let this_node = self.server.cluster.this_node();
        self.reply.write_bulk(this_node.to_hex());
        Ok(())
    }

    fn cluster_nodes(&mut self) -> Result<(), ClusterError> {
        let cluster = &self.server.cluster;
        let mut itoa_buf = itoa::Buffer::new();
        let mut reply = Vec::new();
        {
            let topology = cluster.topology();
            for (name, addr) in topology.node_addrs() {
                reply.extend_from_slice(
                    format!("{} {}:{}@0 ", name, addr.ip(), addr.port()).as_bytes(),
                );
                if name == cluster.this_node() {
                    reply.extend_from_slice(b"myself,");
                }
                reply.extend_from_slice(b"master - 0 0 0 connected");
                let slot_ranges = topology
                    .slots()
                    .assigned_ranges()
                    .filter_map(|(start, end, n)| (n == name).then_some((start, end)));
                for (start, end) in slot_ranges {
                    reply.push(b' ');
                    reply.extend_from_slice(itoa_buf.format(start.get()).as_bytes());
                    if start != end {
                        reply.push(b'-');
                        reply.extend_from_slice(itoa_buf.format(end.get()).as_bytes());
                    }
                }
                reply.push(b'\n');
            }
        }
        self.reply.write_bulk(reply);
        Ok(())
    }

    fn cluster_slots(&mut self) -> Result<(), ClusterError> {
        let mut buf = BytesMut::new();
        let mut num_items = 0;
        {
            let topology = self.server.cluster.topology();
            for (start, end, node_name) in topology.slots().assigned_ranges() {
                let addr = topology.node_addrs().get(node_name).unwrap();
                buf.write_array(3);
                buf.write_integer(start.get());
                buf.write_integer(end.get());
                buf.write_array(4);
                buf.write_bulk(addr.ip().to_string());
                buf.write_integer(addr.port());
                buf.write_bulk(node_name.to_hex());
                buf.write_array(0);
                num_items += 1;
            }
        }
        self.reply.write_array(num_items);
        self.append_reply(buf);
        Ok(())
    }

    // CLUSTER SLOT-STATS <SLOTSRANGE start-slot end-slot | ORDERBY metric [LIMIT limit] [ASC | DESC]>
    async fn cluster_slot_stats(&mut self, args: &[ByteBuf]) -> Result<(), ClusterError> {
        fn write_reply(key_counts: impl IntoIterator<Item = (Slot, usize)>, out: &mut BytesMut) {
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
            out.write_array(num_slots_in_reply);
            out.unsplit(stats_to_reply);
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
                write_reply(key_counts, &mut self.reply);
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
                write_reply(key_counts, &mut self.reply);
                Ok(())
            }
            _ => Err(CommandError::Syntax.into()),
        }
    }

    async fn slot_key_counts(&self) -> Result<Vec<(Slot, usize)>, BackendError> {
        let mut key_counts = SlotMap::filled(0);
        for (slot, stats) in self.server.backend.slot_stats().await? {
            key_counts[slot] = stats.keys;
        }
        let cluster = &self.server.cluster;
        let key_counts = cluster
            .topology()
            .slots()
            .iter()
            .filter(|&(_, node_name)| node_name.as_ref() == Some(cluster.this_node()))
            .map(|(slot, _)| (slot, key_counts[slot]))
            .collect();
        Ok(key_counts)
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
            .write_integer(self.server.backend.count_keys(slot).await?);
        Ok(())
    }

    // CLUSTER GETKEYSINSLOT slot count
    async fn cluster_getkeysinslot(&mut self, args: &[ByteBuf]) -> Result<(), ClusterError> {
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
        self.server
            .backend
            .dump_keys(slot, count, &mut self.reply)
            .await
            .map_err(Into::into)
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
