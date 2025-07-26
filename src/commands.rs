use crate::ByteBuf;
use std::hash::Hash;

pub struct CommandName<'a>(&'a [u8]);

impl PartialEq for CommandName<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(other.0)
    }
}

impl Eq for CommandName<'_> {}

impl Hash for CommandName<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for b in self.0 {
            state.write_u8(b.to_ascii_lowercase());
        }
    }
}

impl phf::PhfHash for CommandName<'_> {
    fn phf_hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl<'a> phf_shared::PhfBorrow<CommandName<'a>> for CommandName<'static> {
    fn borrow(&self) -> &CommandName<'a> {
        self
    }
}

impl<'a> CommandName<'a> {
    pub fn new(name: &'a [u8]) -> Self {
        Self(name)
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0
    }

    #[must_use]
    pub fn truncated(&self, max_len: usize) -> Self {
        Self(if self.0.len() > max_len {
            &self.0[..max_len]
        } else {
            self.0
        })
    }
}

pub struct Command {
    pub id: CommandId,
    pub full_name: &'static str,
    pub arity: Arity,
    pub key_spec: Option<KeySpec>,
    pub subcommands: phf::Map<CommandName<'static>, Command>,
}

pub enum Arity {
    Exactly(usize),
    AtLeast(usize),
}

impl Arity {
    pub const fn matches(&self, n: usize) -> bool {
        match self {
            Self::Exactly(m) => n == *m,
            Self::AtLeast(m) => n >= *m,
        }
    }
}

pub struct KeySpec {
    first: usize,
    last: isize,
    step: usize,
}

impl KeySpec {
    /// Returns an iterator over the keys in the command arguments.
    ///
    /// # Panics
    ///
    /// If the arguments do not follow the key spec.
    pub fn extract_keys<'a>(&self, args: &'a [ByteBuf]) -> impl Iterator<Item = &'a [u8]> {
        let base = if self.last >= 0 {
            self.first
        } else {
            args.len()
        }
        .cast_signed();
        let last = (base + self.last).cast_unsigned();
        args[self.first..=last]
            .iter()
            .step_by(self.step)
            .map(AsRef::as_ref)
    }
}

include!(concat!(env!("OUT_DIR"), "/commands.rs"));
