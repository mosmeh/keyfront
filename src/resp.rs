use crate::{
    ByteBuf, NonZeroMemorySize,
    string::{SplitArgsError, parse_int},
};
use bstr::ByteSlice;
use bytes::{Buf, BufMut, BytesMut};
use std::{cmp::Ordering, fmt::Debug, io::Write, num::NonZeroUsize};
use tokio_util::codec::Decoder;

const PROTO_INLINE_MAX_SIZE: usize = 64 * 1024;
const CRLF: &[u8] = b"\r\n";

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("too big inline request")]
    TooBigInlineRequest,

    #[error("too big mbulk count string")]
    TooBigMultibulkCountString,

    #[error("invalid multibulk length")]
    InvalidMultibulkLen,

    #[error("too big bulk count string")]
    TooBigBulkCountString,

    #[error("expected '{}', got '{}'", char::from(*.expected), char::from(*.got))]
    UnexpectedChar { expected: u8, got: u8 },

    #[error("invalid bulk length")]
    InvalidBulkLen,

    #[error("unbalanced quotes in request")]
    UnbalancedQuotes,

    #[error("unknown RESP type: '{}'", char::from(*.0))]
    UnknownType(u8),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub struct QueryDecoder {
    bytes: BytesMut,
    num_args: Option<NonZeroUsize>,
    args: Vec<ByteBuf>,
    proto_max_bulk_len: usize,
}

impl QueryDecoder {
    pub fn new(proto_max_bulk_len: NonZeroMemorySize) -> Self {
        Self {
            bytes: BytesMut::new(),
            num_args: None,
            args: Vec::new(),
            proto_max_bulk_len: proto_max_bulk_len.count_bytes(),
        }
    }
}

impl Decoder for QueryDecoder {
    type Item = BytesMut;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let result = match src.first() {
            None => Ok(None),
            _ if self.num_args.is_some() => {
                assert!(!self.bytes.is_empty());
                self.decode_multibulk(src)
            }
            Some(b'*') => {
                assert!(self.bytes.is_empty());
                self.args.clear();
                self.decode_multibulk(src)
            }
            _ => {
                assert!(self.bytes.is_empty());
                self.args.clear();
                self.decode_inline(src)
            }
        };
        if result.is_err() {
            self.args.clear();
        }
        result
    }
}

impl QueryDecoder {
    pub fn args(&self) -> &[ByteBuf] {
        &self.args
    }

    fn decode_multibulk(&mut self, src: &mut BytesMut) -> Result<Option<BytesMut>, ProtocolError> {
        let num_args = match self.num_args {
            None => {
                let [b'*', rest @ ..] = src.as_ref() else {
                    unreachable!()
                };
                let header = match extract_line(rest) {
                    Some(header) => header,
                    None if src.len() > PROTO_INLINE_MAX_SIZE => {
                        return Err(ProtocolError::TooBigMultibulkCountString);
                    }
                    None => return Ok(None),
                };
                let len: i32 = parse_int(header).ok_or(ProtocolError::InvalidMultibulkLen)?;
                let consumed = src.split_to(1 + header.len() + CRLF.len());
                if len <= 0 {
                    return Ok(Some(BytesMut::new()));
                }
                self.bytes.unsplit(consumed);

                let num_args = NonZeroUsize::new(len as usize).unwrap();
                self.num_args = Some(num_args);
                num_args
            }
            Some(array_len) => array_len,
        };

        while self.args.len() < num_args.get() {
            let Some((ch, rest)) = src.split_first() else {
                return Ok(None);
            };
            let header = match extract_line(rest) {
                Some(header) if *ch == b'$' => header,
                Some(_) => {
                    return Err(ProtocolError::UnexpectedChar {
                        expected: b'$',
                        got: *ch,
                    });
                }
                None if src.len() > PROTO_INLINE_MAX_SIZE => {
                    return Err(ProtocolError::TooBigBulkCountString);
                }
                None => return Ok(None),
            };
            let len = parse_int::<u32>(header).ok_or(ProtocolError::InvalidBulkLen)? as usize;
            if len > self.proto_max_bulk_len {
                return Err(ProtocolError::InvalidBulkLen);
            }
            let header_line_end = 1 + header.len() + CRLF.len();
            let bulk_line_end = header_line_end + len + CRLF.len();
            if bulk_line_end > src.len() {
                src.reserve(bulk_line_end - src.len());
                return Ok(None);
            }
            self.args.push(ByteBuf::new(&src[header_line_end..][..len]));
            self.bytes.unsplit(src.split_to(bulk_line_end));
        }

        self.num_args = None;
        Ok(Some(self.bytes.split()))
    }

    fn decode_inline(&mut self, src: &mut BytesMut) -> Result<Option<BytesMut>, ProtocolError> {
        let end = match src.find_byte(b'\n') {
            Some(end) => end,
            None if src.len() > PROTO_INLINE_MAX_SIZE => {
                return Err(ProtocolError::TooBigInlineRequest);
            }
            None => return Ok(None),
        };
        match crate::string::split_args(&src[..end], &mut self.args) {
            Ok(()) => (),
            Err(SplitArgsError::UnbalancedQuotes) => return Err(ProtocolError::UnbalancedQuotes),
        }
        let consumed = src.split_to(end + 1);
        Ok(Some(if self.args.is_empty() {
            BytesMut::new()
        } else {
            consumed
        }))
    }
}

struct Array {
    len: usize,
    capacity: usize,
}

#[derive(Default)]
pub struct ReplyDecoder {
    array_stack: Vec<Array>,
    array_bytes: BytesMut,
}

impl Decoder for ReplyDecoder {
    type Item = BytesMut;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            let bytes = match src.as_ref() {
                [] => return Ok(None),
                [b'+' | b'-' | b':', ..] => {
                    let Some(line) = extract_line(src) else {
                        return Ok(None);
                    };
                    src.split_to(line.len() + CRLF.len())
                }
                [b'$', rest @ ..] => {
                    let Some(header) = extract_line(rest) else {
                        return Ok(None);
                    };
                    let data_len: i32 = parse_int(header).ok_or(ProtocolError::InvalidBulkLen)?;
                    let header_end = 1 + header.len() + CRLF.len();
                    if data_len < 0 {
                        src.split_to(header_end)
                    } else {
                        let data_end = header_end + data_len as usize + CRLF.len();
                        if data_end > src.len() {
                            src.reserve(data_end - src.len());
                            return Ok(None);
                        }
                        src.split_to(data_end)
                    }
                }
                [b'*', rest @ ..] => {
                    let Some(header) = extract_line(rest) else {
                        return Ok(None);
                    };
                    let array_len: i32 =
                        parse_int(header).ok_or(ProtocolError::InvalidMultibulkLen)?;
                    let bytes = src.split_to(1 + header.len() + CRLF.len());
                    if array_len > 0 {
                        self.array_stack.push(Array {
                            len: 0,
                            capacity: array_len as usize,
                        });
                        self.array_bytes.unsplit(bytes);
                        continue;
                    }
                    bytes
                }
                [c, ..] => return Err(ProtocolError::UnknownType(*c)),
            };
            let Some(mut array) = self.array_stack.last_mut() else {
                return Ok(Some(bytes));
            };
            self.array_bytes.unsplit(bytes);
            loop {
                array.len += 1;
                match array.len.cmp(&array.capacity) {
                    Ordering::Less => break,
                    Ordering::Equal => {}
                    Ordering::Greater => unreachable!(),
                }
                self.array_stack.pop().unwrap();
                let Some(x) = self.array_stack.last_mut() else {
                    return Ok(Some(self.array_bytes.split()));
                };
                array = x;
            }
        }
    }
}

fn extract_line(bytes: &[u8]) -> Option<&[u8]> {
    match bytes.find_byte(b'\r') {
        Some(end) if end + 1 < bytes.len() => Some(&bytes[..end]), // if there is a room for trailing \n
        _ => None,
    }
}

const OK: &[u8] = b"OK";

pub trait ReadResp {
    fn read_simple(&mut self) -> Option<BytesMut>;
    fn read_ok(&mut self) -> bool;
    fn read_bulk(&mut self) -> Option<BytesMut>;
    fn read_array(&mut self) -> Option<usize>;
}

impl ReadResp for BytesMut {
    fn read_simple(&mut self) -> Option<BytesMut> {
        let [b'+', rest @ ..] = self.as_ref() else {
            return None;
        };
        let len = extract_line(rest)?.len();
        self.advance(1);
        let data = self.split_to(len);
        self.advance(CRLF.len());
        Some(data)
    }

    fn read_ok(&mut self) -> bool {
        let [b'+', rest @ ..] = self.as_ref() else {
            return false;
        };
        let Some(OK) = extract_line(rest) else {
            return false;
        };
        self.advance(1 + OK.len() + CRLF.len());
        true
    }

    fn read_bulk(&mut self) -> Option<BytesMut> {
        let [b'$', rest @ ..] = self.as_ref() else {
            return None;
        };
        let header = extract_line(rest)?;
        let data_len: i32 = parse_int(header)?;
        let header_end = 1 + header.len() + CRLF.len();
        if data_len < 0 {
            self.advance(header_end);
            return Some(Self::new());
        }
        let data_len = data_len as usize;
        if header_end + data_len + CRLF.len() > self.len() {
            return None;
        }
        self.advance(header_end);
        let data = self.split_to(data_len);
        self.advance(CRLF.len());
        Some(data)
    }

    fn read_array(&mut self) -> Option<usize> {
        let [b'*', rest @ ..] = self.as_ref() else {
            return None;
        };
        let header = extract_line(rest)?;
        let array_len: i32 = parse_int(header)?;
        self.advance(1 + header.len() + CRLF.len());
        Some(if array_len > 0 { array_len as usize } else { 0 })
    }
}

pub trait WriteResp {
    fn write_simple<B: AsRef<[u8]>>(&mut self, value: B);
    fn write_integer<I: itoa::Integer>(&mut self, value: I);
    fn write_bulk<B: AsRef<[u8]>>(&mut self, data: B);
    fn write_array(&mut self, len: usize);
    fn write_error<B: AsRef<[u8]>>(&mut self, error: B);
    fn write_null(&mut self);
    fn write_fmt(&mut self, args: std::fmt::Arguments);

    fn write_ok(&mut self) {
        self.write_simple(OK);
    }
}

impl WriteResp for BytesMut {
    fn write_simple<B: AsRef<[u8]>>(&mut self, value: B) {
        let value = value.as_ref();
        self.reserve(1 + value.len() + CRLF.len());
        self.put_u8(b'+');
        self.put_slice(value);
        self.put_slice(CRLF);
    }

    fn write_integer<I: itoa::Integer>(&mut self, value: I) {
        write_integer_with_prefix(b':', value, self);
    }

    fn write_bulk<B: AsRef<[u8]>>(&mut self, data: B) {
        let data = data.as_ref();
        let mut itoa_buf = itoa::Buffer::new();
        let len_bytes = itoa_buf.format(data.len()).as_bytes();
        self.reserve(1 + len_bytes.len() + CRLF.len() + data.len() + CRLF.len());
        self.put_u8(b'$');
        self.put_slice(len_bytes);
        self.put_slice(CRLF);
        self.put_slice(data);
        self.put_slice(CRLF);
    }

    fn write_array(&mut self, len: usize) {
        write_integer_with_prefix(b'*', len, self);
    }

    fn write_error<B: AsRef<[u8]>>(&mut self, error: B) {
        let error = error.as_ref();
        assert!(error.starts_with_str("-"));
        self.reserve(error.len() + CRLF.len());
        for &byte in error {
            if byte == b'\r' || byte == b'\n' {
                self.put_u8(b' ');
            } else {
                self.put_u8(byte);
            }
        }
        self.put_slice(CRLF);
    }

    fn write_null(&mut self) {
        const NULL: &[u8] = b"$-1";
        self.reserve(NULL.len() + CRLF.len());
        self.put_slice(NULL);
        self.put_slice(CRLF);
    }

    fn write_fmt(&mut self, args: std::fmt::Arguments) {
        self.writer().write_fmt(args).unwrap();
        self.put_slice(CRLF);
    }
}

fn write_integer_with_prefix<I: itoa::Integer>(prefix: u8, value: I, out: &mut BytesMut) {
    let mut itoa_buf = itoa::Buffer::new();
    let len_bytes = itoa_buf.format(value).as_bytes();
    out.reserve(1 + len_bytes.len() + CRLF.len());
    out.put_u8(prefix);
    out.put_slice(len_bytes);
    out.put_slice(CRLF);
}

#[macro_export]
macro_rules! count {
    () => (0usize);
    ($x:tt $($xs:tt)*) => (1usize + $crate::count!($($xs)*));
}

#[macro_export]
macro_rules! write_query {
    ($buf:expr, $($arg:expr),* $(,)?) => {{
        use $crate::resp::WriteResp;

        $buf.write_array($crate::count!($($arg)*));
        $($buf.write_bulk($arg);)*
    }}
}

#[macro_export]
macro_rules! query {
    ($($arg:expr),* $(,)?) => {{
        let mut query = $crate::bytes::BytesMut::new();
        $crate::write_query!(query, $($arg),*);
        query
    }}
}
