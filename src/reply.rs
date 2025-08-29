use crate::{ByteBuf, resp::ReadResp, string::parse_int};
use bstr::ByteSlice;
use bytes::{BufMut, BytesMut};
use std::num::Saturating;

const CRLF: &[u8] = b"\r\n";

#[derive(Default)]
pub struct Info(Vec<(ByteBuf, InfoSection)>);

impl Info {
    pub const SERVER: &'static [u8] = b"server";
    pub const CLIENTS: &'static [u8] = b"clients";
    pub const STATS: &'static [u8] = b"stats";
    pub const CLUSTER: &'static [u8] = b"cluster";
    pub const KEYSPACE: &'static [u8] = b"keyspace";

    pub fn from_bytes(mut reply: BytesMut) -> Option<Self> {
        let bytes = reply.read_bulk()?;
        let mut sections = Vec::new();
        let mut section = None;
        for line in bytes.split_str(CRLF) {
            if line.is_empty() {
                continue;
            }
            if let Some(name) = line.strip_prefix(b"# ") {
                if let Some(section) = section.replace((name.into(), InfoSection(Vec::new()))) {
                    sections.push(section);
                }
                continue;
            }
            let Some((_, section)) = section.as_mut() else {
                continue;
            };
            let Some((key, value)) = line.split_once_str(b":") else {
                continue;
            };
            section.0.push((key.into(), value.into()));
        }
        if let Some(section) = section {
            sections.push(section);
        }
        Some(Self(sections))
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut bytes = BytesMut::new();
        for (name, section) in &self.0 {
            if !bytes.is_empty() {
                bytes.put_slice(CRLF);
            }
            bytes.put_slice(b"# ");
            bytes.put_slice(name);
            bytes.put_slice(CRLF);
            bytes.unsplit(section.to_bytes());
        }
        bytes
    }

    pub fn section<T: AsRef<[u8]>>(&self, name: T) -> Option<&InfoSection> {
        let needle = name.as_ref();
        self.0
            .iter()
            .find_map(|(n, section)| n.eq_ignore_ascii_case(needle).then_some(section))
    }

    pub fn section_mut<T: AsRef<[u8]>>(&mut self, name: T) -> Option<&mut InfoSection> {
        let needle = name.as_ref();
        self.0
            .iter_mut()
            .find_map(|(n, section)| n.eq_ignore_ascii_case(needle).then_some(section))
    }

    pub fn insert_section<T: AsRef<[u8]>>(&mut self, name: T) -> Option<InfoSection> {
        let needle = name.as_ref();
        if let Some((_, section)) = self
            .0
            .iter_mut()
            .find(|(n, _)| n.eq_ignore_ascii_case(needle))
        {
            Some(std::mem::take(section))
        } else {
            self.0.push((needle.into(), InfoSection::default()));
            None
        }
    }
}

#[derive(Default)]
pub struct InfoSection(Vec<(ByteBuf, ByteBuf)>);

impl InfoSection {
    pub fn to_bytes(&self) -> BytesMut {
        let mut bytes = BytesMut::new();
        for (key, value) in &self.0 {
            bytes.put_slice(key.as_ref());
            bytes.put_u8(b':');
            bytes.put_slice(value.as_ref());
            bytes.put_slice(CRLF);
        }
        bytes
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<&[u8]> {
        let key = key.as_ref();
        self.0
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.as_ref())
    }

    pub fn insert<K, V>(&mut self, key: K, value: V) -> Option<ByteBuf>
    where
        K: Into<ByteBuf>,
        V: Into<ByteBuf>,
    {
        let key = key.into();
        let value = value.into();
        let item = self
            .0
            .iter_mut()
            .find(|(k, _)| k.eq_ignore_ascii_case(key.as_ref()));
        if let Some((_, v)) = item {
            Some(std::mem::replace(v, value))
        } else {
            self.0.push((key, value));
            None
        }
    }

    /// Replaces the value of a key if it already exists.
    ///
    /// If the key does not exist, it returns `None` without modifying the info.
    /// If the key existed, the value is updated, and the old value is returned.
    pub fn replace<K, V>(&mut self, key: K, value: V) -> Option<ByteBuf>
    where
        K: AsRef<[u8]>,
        V: Into<ByteBuf>,
    {
        let key = key.as_ref();
        self.0
            .iter_mut()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| std::mem::replace(v, value.into()))
    }

    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        self.0.retain(|(k, v)| f(k, v));
    }

    pub fn to_keyspace_stats(&self) -> impl Iterator<Item = (usize, KeyspaceStats)> {
        self.0.iter().filter_map(|(k, v)| to_keyspace_stats(k, v))
    }

    pub fn into_keyspace_stats(self) -> impl Iterator<Item = (usize, KeyspaceStats)> {
        self.0
            .into_iter()
            .filter_map(|(k, v)| to_keyspace_stats(&k, &v))
    }
}

fn to_keyspace_stats(k: &[u8], v: &[u8]) -> Option<(usize, KeyspaceStats)> {
    let db_id = parse_int(k.strip_prefix(b"db")?)?;
    let stats = KeyspaceStats::from_bytes(v)?;
    Some((db_id, stats))
}

pub struct KeyspaceStats {
    pub keys: usize,
    pub expires: usize,
    pub avg_ttl: usize,
}

impl KeyspaceStats {
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let mut parts = bytes.split_str(b",");
        let keys = parts.next()?.strip_prefix(b"keys=")?;
        let expires = parts.next()?.strip_prefix(b"expires=")?;
        let avg_ttl = parts.next()?.strip_prefix(b"avg_ttl=")?;
        if parts.next().is_some() {
            return None;
        }
        Some(Self {
            keys: parse_int(keys)?,
            expires: parse_int(expires)?,
            avg_ttl: parse_int(avg_ttl)?,
        })
    }

    pub fn to_bytes(&self) -> BytesMut {
        let mut itoa_buf = itoa::Buffer::new();
        let mut bytes = BytesMut::new();
        bytes.put_slice(b"keys=");
        bytes.put_slice(itoa_buf.format(self.keys).as_bytes());
        bytes.put_slice(b",expires=");
        bytes.put_slice(itoa_buf.format(self.expires).as_bytes());
        bytes.put_slice(b",avg_ttl=");
        bytes.put_slice(itoa_buf.format(self.avg_ttl).as_bytes());
        bytes
    }

    pub fn aggregate(stats: impl IntoIterator<Item = Self>) -> Self {
        let mut keys = Saturating(0);
        let mut expires = Saturating(0);
        let mut ttl = Saturating(0);
        for stats in stats {
            keys += stats.keys;
            expires += stats.expires;
            ttl += Saturating(stats.avg_ttl) * Saturating(stats.expires);
        }
        Self {
            keys: keys.0,
            expires: expires.0,
            avg_ttl: if expires.0 > 0 { (ttl / expires).0 } else { 0 },
        }
    }
}

pub struct ScanReply {
    pub cursor: BytesMut,
    pub num_keys: usize,
    pub keys: BytesMut,
}

impl ScanReply {
    pub fn from_bytes(mut reply: BytesMut) -> Option<Self> {
        if reply.read_array()? != 2 {
            return None;
        }
        let cursor = reply.read_bulk()?;
        let num_keys = reply.read_array()?;
        Some(Self {
            cursor,
            num_keys,
            keys: reply,
        })
    }
}
