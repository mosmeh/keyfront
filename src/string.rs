use bstr::ByteSlice;

#[derive(Debug, thiserror::Error)]
pub(crate) enum SplitArgsError {
    #[error("unbalanced quotes")]
    UnbalancedQuotes,
}

enum State {
    Normal,
    InDoubleQuotes,
    InSingleQuotes,
}

pub(crate) fn split_args<T: From<Vec<u8>>>(
    mut line: &[u8],
    out: &mut Vec<T>,
) -> Result<(), SplitArgsError> {
    loop {
        match line.find_not_byteset(b" \n\r\t\x0b\x0c") {
            Some(i) => line = &line[i..],
            None => return Ok(()), // line is empty, or line consists only of space characters
        }
        let mut state = State::Normal;
        let mut current = Vec::new();
        loop {
            match state {
                State::Normal => match line {
                    [b' ' | b'\n' | b'\r' | b'\t', rest @ ..] => {
                        line = rest;
                        break;
                    }
                    [] => break,
                    [b'"', rest @ ..] => {
                        state = State::InDoubleQuotes;
                        line = rest;
                    }
                    [b'\'', rest @ ..] => {
                        state = State::InSingleQuotes;
                        line = rest;
                    }
                    [ch, rest @ ..] => {
                        current.push(*ch);
                        line = rest;
                    }
                },
                State::InDoubleQuotes => match line {
                    [b'\\', b'x', a, b, rest @ ..]
                        if a.is_ascii_hexdigit() && b.is_ascii_hexdigit() =>
                    {
                        current.push(hex_digit_to_int(*a) * 16 + hex_digit_to_int(*b));
                        line = rest;
                    }
                    [b'\\', ch, rest @ ..] => {
                        let byte = match *ch {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'b' => 0x8,
                            b'a' => 0x7,
                            ch => ch,
                        };
                        current.push(byte);
                        line = rest;
                    }
                    [b'"', next, ..] if !is_space(*next) => {
                        return Err(SplitArgsError::UnbalancedQuotes);
                    }
                    [b'"', rest @ ..] => {
                        line = rest;
                        break;
                    }
                    [] => return Err(SplitArgsError::UnbalancedQuotes),
                    [ch, rest @ ..] => {
                        current.push(*ch);
                        line = rest;
                    }
                },
                State::InSingleQuotes => match line {
                    [b'\\', b'\'', rest @ ..] => {
                        current.push(b'\'');
                        line = rest;
                    }
                    [b'\'', next, ..] if !is_space(*next) => {
                        return Err(SplitArgsError::UnbalancedQuotes);
                    }
                    [b'\'', rest @ ..] => {
                        line = rest;
                        break;
                    }
                    [] => return Err(SplitArgsError::UnbalancedQuotes),
                    [ch, rest @ ..] => {
                        current.push(*ch);
                        line = rest;
                    }
                },
            }
        }
        out.push(current.into());
    }
}

// isspace() in C
const fn is_space(ch: u8) -> bool {
    matches!(ch, b' ' | b'\n' | b'\r' | b'\t' | 0xb | 0xc)
}

fn hex_digit_to_int(ch: u8) -> u8 {
    match ch {
        b'0'..=b'9' => ch - b'0',
        b'a'..=b'f' => ch - b'a' + 10,
        b'A'..=b'F' => ch - b'A' + 10,
        _ => unreachable!(),
    }
}

pub fn parse_int<T: Integer>(bytes: &[u8]) -> Option<T> {
    T::from_bytes(bytes)
}

pub trait Integer: private::Sealed {}

mod private {
    pub trait Sealed: Sized {
        fn from_bytes(bytes: &[u8]) -> Option<Self>;
    }
}

macro_rules! impl_from_bytes_unsigned {
    ($ty:ty) => {
        impl Integer for $ty {}

        impl private::Sealed for $ty {
            fn from_bytes(bytes: &[u8]) -> Option<Self> {
                match bytes {
                    [] => return None,
                    b if b.len() > <$ty as itoa::Integer>::MAX_STR_LEN => return None,
                    &[c @ b'0'..=b'9'] => return Some(<$ty>::from(c - b'0')),
                    _ => {}
                }
                let [c @ b'1'..=b'9', rest @ ..] = bytes else {
                    return None;
                };
                let mut value = <$ty>::from(c - b'0');
                for &c in rest {
                    if !c.is_ascii_digit() {
                        return None;
                    }
                    value = value.checked_mul(10)?.checked_add(<$ty>::from(c - b'0'))?;
                }
                Some(value)
            }
        }
    };
}

impl_from_bytes_unsigned!(u8);
impl_from_bytes_unsigned!(u16);
impl_from_bytes_unsigned!(u32);
impl_from_bytes_unsigned!(u64);

macro_rules! impl_from_bytes_delegated {
    ($ty:ty, $delegate:ty) => {
        impl Integer for $ty {}

        impl private::Sealed for $ty {
            fn from_bytes(bytes: &[u8]) -> Option<Self> {
                let value = <$delegate>::from_bytes(bytes)?;
                Some(value as Self)
            }
        }
    };
}

#[cfg(target_pointer_width = "16")]
impl_from_bytes_delegated!(usize, u16);
#[cfg(target_pointer_width = "32")]
impl_from_bytes_delegated!(usize, u32);
#[cfg(target_pointer_width = "64")]
impl_from_bytes_delegated!(usize, u64);

macro_rules! impl_from_bytes_signed {
    ($signed:ty, $unsigned:ty) => {
        impl Integer for $signed {}

        impl private::Sealed for $signed {
            fn from_bytes(bytes: &[u8]) -> Option<Self> {
                let mut s = bytes;
                let mut negative = false;
                match s {
                    [] => return None,
                    b if b.len() > <$signed as itoa::Integer>::MAX_STR_LEN => return None,
                    &[c @ b'0'..=b'9'] => return Some(<$signed>::from(c - b'0')),
                    [b'-', rest @ ..] => {
                        s = rest;
                        negative = true;
                    }
                    _ => {}
                }
                let [c @ b'1'..=b'9', rest @ ..] = s else {
                    return None;
                };
                let mut value = <$unsigned>::from(c - b'0');
                for &c in rest {
                    if !c.is_ascii_digit() {
                        return None;
                    }
                    value = value
                        .checked_mul(10)?
                        .checked_add(<$unsigned>::from(c - b'0'))?;
                }
                if negative {
                    if value > (-(<$signed>::MIN + 1)).cast_unsigned() + 1 {
                        return None; // Overflow
                    }
                    Some(value.wrapping_neg().cast_signed())
                } else {
                    value.try_into().ok()
                }
            }
        }
    };
}

impl_from_bytes_signed!(i16, u16);
impl_from_bytes_signed!(i32, u32);
impl_from_bytes_signed!(i64, u64);
impl_from_bytes_signed!(isize, usize);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_args() {
        fn assert_split_eq(line: &[u8], expected: &[&[u8]]) {
            let mut args = Vec::<Vec<u8>>::new();
            split_args(line, &mut args).unwrap();
            for (arg, &expected_arg) in args.iter().zip(expected) {
                assert_eq!(arg.as_slice(), expected_arg);
            }
        }

        assert_split_eq(
            b"Testing one two three",
            &[b"Testing", b"one", b"two", b"three"],
        );
        assert_split_eq(b"", &[]);
        assert_split_eq(
            b"\"Testing split strings\" 'Another split string'",
            &[b"Testing split strings", b"Another split string"],
        );
        assert_split_eq(b"\"Hello\" ", &[b"Hello"]);
        assert_split_eq(b"\"\\x73\\x75\\x70\\x65\\x72\\x20\\x00\\x73\\x65\\x63\\x72\\x65\\x74\\x20\\x70\\x61\\x73\\x73\\x77\\x6f\\x72\\x64\"",
            &[b"super \x00secret password"]);
        assert_split_eq(b"unquoted", &[b"unquoted"]);
        assert_split_eq(b"empty string \"\"", &[b"empty", b"string", b""]);
    }

    #[test]
    fn parse_i64() {
        parse_invalid::<i64>();
        assert_eq!(parse_int(b"-1"), Some(-1i64));
        assert_eq!(parse_int(b"0"), Some(0i64));
        assert_eq!(parse_int(b"1"), Some(1i64));
        assert_eq!(parse_int(b"99"), Some(99i64));
        assert_eq!(parse_int(b"-99"), Some(-99i64));
        assert_eq!(parse_int(b"-9223372036854775808"), Some(i64::MIN));
        assert!(parse_int::<i64>(b"-9223372036854775809").is_none());
        assert_eq!(parse_int(b"9223372036854775807"), Some(i64::MAX));
        assert!(parse_int::<i64>(b"9223372036854775808").is_none());
        assert!(parse_int::<i64>(b"18446744073709551615").is_none());
    }

    #[test]
    fn parse_i32() {
        parse_invalid::<i32>();
        assert_eq!(parse_int(b"-1"), Some(-1i32));
        assert_eq!(parse_int(b"0"), Some(0i32));
        assert_eq!(parse_int(b"1"), Some(1i32));
        assert_eq!(parse_int(b"99"), Some(99i32));
        assert_eq!(parse_int(b"-99"), Some(-99i32));
        assert_eq!(parse_int(b"-2147483648"), Some(i32::MIN));
        assert!(parse_int::<i32>(b"-2147483649").is_none());
        assert_eq!(parse_int(b"2147483647"), Some(i32::MAX));
        assert!(parse_int::<i32>(b"2147483648").is_none());
    }

    #[test]
    fn parse_u64() {
        parse_invalid::<u64>();
        assert!(parse_int::<u64>(b"-1").is_none());
        assert_eq!(parse_int(b"0"), Some(0u64));
        assert_eq!(parse_int(b"1"), Some(1u64));
        assert_eq!(parse_int(b"99"), Some(99u64));
        assert!(parse_int::<u64>(b"-99").is_none());
        assert!(parse_int::<u64>(b"-9223372036854775808").is_none());
        assert_eq!(parse_int(b"18446744073709551615"), Some(u64::MAX));
        assert!(parse_int::<u64>(b"18446744073709551616").is_none());
    }

    fn parse_invalid<T: Integer>() {
        assert!(parse_int::<T>(b"+1").is_none());
        assert!(parse_int::<T>(b" 1").is_none());
        assert!(parse_int::<T>(b"1 ").is_none());
        assert!(parse_int::<T>(b"01").is_none());
    }
}
