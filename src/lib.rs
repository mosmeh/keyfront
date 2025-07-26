pub mod cluster;
pub mod commands;
pub mod reply;
pub mod resp;
pub mod string;

mod byte_buf;

pub use byte_buf::ByteBuf;
pub use bytes;
pub use tokio_util;

use serde::Deserialize;
use std::{
    convert::Infallible,
    net::SocketAddr,
    num::{NonZeroUsize, ParseIntError},
    path::PathBuf,
    str::FromStr,
};

#[derive(Debug, Clone)]
pub enum Address {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Tcp(addr) => addr.fmt(f),
            Self::Unix(path) => path.display().fmt(f),
        }
    }
}

impl FromStr for Address {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(if let Ok(addr) = s.parse() {
            Self::Tcp(addr)
        } else {
            Self::Unix(s.into())
        })
    }
}

impl<'de> Deserialize<'de> for Address {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(String::deserialize(deserializer)?.parse().unwrap())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NonZeroMemorySize(NonZeroUsize);

impl NonZeroMemorySize {
    pub fn new(bytes: usize) -> Option<Self> {
        NonZeroUsize::new(bytes).map(Self)
    }

    pub fn count_bytes(self) -> usize {
        self.0.get()
    }
}

impl std::fmt::Display for NonZeroMemorySize {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for NonZeroMemorySize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (s, unit) = match s.find(|c: char| !c.is_ascii_digit()) {
            Some(0) => return Err("size must start with a number".to_owned()),
            Some(i) => s.split_at(i),
            None => (s, ""),
        };

        let n: NonZeroUsize = s.parse().map_err(|e: ParseIntError| e.to_string())?;
        let mul = NonZeroUsize::new(match unit {
            "" => 1,
            u if u.eq_ignore_ascii_case("b") => 1,
            u if u.eq_ignore_ascii_case("k") => 1000,
            u if u.eq_ignore_ascii_case("kb") => 1024,
            u if u.eq_ignore_ascii_case("m") => 1000 * 1000,
            u if u.eq_ignore_ascii_case("mb") => 1024 * 1024,
            u if u.eq_ignore_ascii_case("g") => 1000 * 1000 * 1000,
            u if u.eq_ignore_ascii_case("gb") => 1024 * 1024 * 1024,
            c => return Err(format!("unknown unit '{c}'")),
        })
        .unwrap();

        match n.checked_mul(mul) {
            Some(size) => Ok(Self(size)),
            None => Err("size is too large".to_owned()),
        }
    }
}

impl<'de> Deserialize<'de> for NonZeroMemorySize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

macro_rules! assert_eq_size {
    ($ty1:ty, $ty2:ty) => {
        const _: fn() = || {
            let _ = std::mem::transmute::<$ty1, $ty2>;
        };
    };
}

pub(crate) use assert_eq_size;

macro_rules! static_assert {
    ($x:expr $(,)?) => {
        const _: [(); !{
            const ASSERT: bool = $x;
            ASSERT
        } as usize] = [];
    };
}

pub(crate) use static_assert;
