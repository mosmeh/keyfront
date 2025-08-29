use crate::string::hex_digit_to_int;
use bstr::ByteSlice;
use std::ops::{Index, IndexMut};

const CLUSTER_NAMELEN: usize = 40;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeName([u8; CLUSTER_NAMELEN / 2]);

impl NodeName {
    pub fn generate_random() -> Option<Self> {
        let mut bytes = [0; CLUSTER_NAMELEN / 2];
        tokio_rustls::rustls::crypto::CryptoProvider::get_default()?
            .secure_random
            .fill(&mut bytes)
            .ok()
            .map(|()| Self(bytes))
    }

    pub fn to_hex(&self) -> [u8; CLUSTER_NAMELEN] {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut hex = [0u8; CLUSTER_NAMELEN];
        for (digit, &byte) in hex.chunks_exact_mut(2).zip(self.0.iter()) {
            let [high, low] = digit else { unreachable!() };
            *high = DIGITS[(byte >> 4) as usize];
            *low = DIGITS[(byte & 0xf) as usize];
        }
        hex
    }

    pub fn from_hex<T: AsRef<[u8]>>(hex: T) -> Option<Self> {
        let hex = hex.as_ref();
        if hex.len() != CLUSTER_NAMELEN {
            return None;
        }
        let mut bytes = [0u8; CLUSTER_NAMELEN / 2];
        for (byte, digits) in bytes.iter_mut().zip(hex.chunks_exact(2)) {
            let &[high, low] = digits else { unreachable!() };
            if !high.is_ascii_hexdigit() || !low.is_ascii_hexdigit() {
                return None;
            }
            *byte = (hex_digit_to_int(high) << 4) | hex_digit_to_int(low);
        }
        Some(Self(bytes))
    }
}

impl std::fmt::Display for NodeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_hex().as_bstr().fmt(f)
    }
}

impl std::fmt::Debug for NodeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_hex().as_bstr().fmt(f)
    }
}

pub const CLUSTER_SLOT_MASK_BITS: usize = 14;
pub const CLUSTER_SLOTS: usize = 1 << CLUSTER_SLOT_MASK_BITS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Slot(u16);

impl std::fmt::Display for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Slot {
    pub const MAX: u16 = CLUSTER_SLOTS as u16 - 1;

    pub fn new(slot: u16) -> Option<Self> {
        (slot < CLUSTER_SLOTS as u16).then_some(Self(slot))
    }

    pub fn from_key<T: AsRef<[u8]>>(key: T) -> Self {
        let key = key.as_ref();
        let Some(start) = key.find_byte(b'{') else {
            return hash(key); // No '{'
        };
        let s = &key[start + 1..];
        match s.find_byte(b'}') {
            Some(0) | None => hash(key), // Nothing between {} or no '}'
            Some(i) => hash(&s[..i]),
        }
    }

    /// Returns a slot if all `keys` are in the same slot.
    ///
    /// # Panics
    ///
    /// Panics if `keys` is empty.
    pub fn from_keys<T, I>(keys: I) -> Option<Self>
    where
        T: AsRef<[u8]>,
        I: IntoIterator<Item = T>,
    {
        let mut slot = None;
        for key in keys {
            let key_slot = Self::from_key(key);
            match slot {
                None => slot = Some(key_slot),
                Some(s) if s == key_slot => {}
                _ => return None,
            }
        }
        assert!(slot.is_some());
        slot
    }

    /// Returns a slot if the `pattern` matches keys in only one slot.
    ///
    /// This is the case if the pattern is `foo{bar}baz*` for example,
    /// but not if it is `foo*bar{baz}` because `*` can match hash tags.
    pub fn from_pattern<T: AsRef<[u8]>>(pattern: T) -> Option<Self> {
        let pattern = pattern.as_ref();
        let Some(i) = pattern.find_byteset(b"*?[\\{") else {
            // No special characters. The pattern matches a single key.
            return Some(hash(pattern));
        };
        let [b'{', rest @ ..] = &pattern[i..] else {
            // Wildcard, character class, or escape character found.
            return None;
        };
        let Some(i) = rest.find_byteset(b"*?[\\}") else {
            // No closing '}' and no other special characters.
            // The pattern matches a single key.
            return Some(hash(pattern));
        };
        match &rest[i..] {
            [b'}', rest @ ..] if i == 0 => {
                // Empty tag '{}'. If there are no other special characters,
                // the pattern matches a single key.
                rest.find_byteset(b"*?[\\").is_none().then(|| hash(pattern))
            }
            [b'}', ..] => {
                // Non-empty tag '{...}' without special characters.
                Some(hash(&rest[..i]))
            }
            _ => {
                // Special characters after the opening '{'.
                None
            }
        }
    }

    pub fn get(self) -> u16 {
        self.0
    }

    pub fn index(self) -> usize {
        usize::from(self.0)
    }
}

fn hash(key: &[u8]) -> Slot {
    Slot(crc16(key) & Slot::MAX)
}

const CRC16_TAB: [u16; 256] = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a, 0xb16b,
    0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462, 0x3443, 0x0420, 0x1401,
    0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738,
    0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96,
    0x1a71, 0x0a50, 0x3a33, 0x2a12, 0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd,
    0xad2a, 0xbd0b, 0x8d68, 0x9d49, 0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb,
    0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290, 0x22f3, 0x32d2,
    0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405, 0xa7db, 0xb7fa, 0x8799, 0x97b8,
    0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827,
    0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e, 0xed0f, 0xdd6c, 0xcd4d,
    0xbdaa, 0xad8b, 0x9de8, 0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74,
    0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
];

fn crc16(data: &[u8]) -> u16 {
    let mut crc = 0u16;
    for &byte in data {
        crc = (crc << 8) ^ CRC16_TAB[usize::from(((crc >> 8) ^ u16::from(byte)) & 0xff)];
    }
    crc
}

#[derive(Clone)]
pub struct SlotMap<T>(Box<[T; CLUSTER_SLOTS]>);

impl<T: Default> Default for SlotMap<T> {
    fn default() -> Self {
        Self::filled_with(|_| T::default())
    }
}

impl<T> Index<Slot> for SlotMap<T> {
    type Output = T;

    fn index(&self, slot: Slot) -> &Self::Output {
        &self.0[slot.index()]
    }
}

impl<T> IndexMut<Slot> for SlotMap<T> {
    fn index_mut(&mut self, slot: Slot) -> &mut Self::Output {
        &mut self.0[slot.index()]
    }
}

impl<T> SlotMap<T> {
    pub fn filled(value: T) -> Self
    where
        T: Clone,
    {
        Self::filled_with(|_| value.clone())
    }

    pub fn filled_with<F>(mut f: F) -> Self
    where
        F: FnMut(Slot) -> T,
    {
        Self(
            (0..CLUSTER_SLOTS)
                .map(|i| f(Slot::new(i as u16).unwrap()))
                .collect::<Vec<_>>()
                .try_into()
                .unwrap_or_else(|_| unreachable!()),
        )
    }

    pub fn iter(&self) -> impl Iterator<Item = (Slot, &T)> {
        self.0
            .iter()
            .enumerate()
            .map(|(i, x)| (Slot::new(i as u16).unwrap(), x))
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (Slot, &mut T)> {
        self.0
            .iter_mut()
            .enumerate()
            .map(|(i, x)| (Slot::new(i as u16).unwrap(), x))
    }
}

impl<T> SlotMap<Option<T>> {
    pub fn assigned_ranges(&self) -> SlotRanges<'_, T> {
        SlotRanges::new(self.iter())
    }
}

pub struct SlotRanges<'a, T> {
    iter: Box<dyn Iterator<Item = (Slot, &'a Option<T>)> + 'a>,
    range_start: Option<(Slot, &'a T)>,
}

impl<'a, T> SlotRanges<'a, T> {
    pub fn new<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (Slot, &'a Option<T>)> + 'a,
    {
        Self {
            iter: Box::new(iter.into_iter().fuse()),
            range_start: None,
        }
    }
}

impl<'a, T> Iterator for SlotRanges<'a, T>
where
    T: PartialEq,
{
    type Item = (Slot, Slot, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        for (slot, node) in self.iter.by_ref() {
            if node.as_ref() == self.range_start.map(|(_, prev_node)| prev_node) {
                continue;
            }
            let next_range_start = node.as_ref().map(|x| (slot, x));
            if let Some((start, prev_node)) = self.range_start {
                let prev_slot = Slot::new(slot.get() - 1).unwrap();
                let next_value = (start, prev_slot, prev_node);
                self.range_start = next_range_start;
                return Some(next_value);
            }
            self.range_start = next_range_start;
        }
        if let Some((start, node)) = self.range_start.take() {
            return Some((start, Slot::new(Slot::MAX).unwrap(), node));
        }
        None
    }
}
