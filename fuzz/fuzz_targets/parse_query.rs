#![no_main]

use keyfront::{
    NonZeroMemorySize, bytes::BytesMut, resp::QueryDecoder, tokio_util::codec::Decoder,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut decoder = QueryDecoder::new(NonZeroMemorySize::new(1024).unwrap());
    let mut bytes = BytesMut::from(data);
    while let Ok(Some(_)) = decoder.decode(&mut bytes) {}
});
