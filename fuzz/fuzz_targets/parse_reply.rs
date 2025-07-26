#![no_main]

use keyfront::{bytes::BytesMut, resp::ReplyDecoder, tokio_util::codec::Decoder};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut decoder = ReplyDecoder::default();
    let mut bytes = BytesMut::from(data);
    while let Ok(Some(_)) = decoder.decode(&mut bytes) {}
});
