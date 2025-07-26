#![no_main]

use keyfront::cluster::Slot;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    Slot::from_key(data);
    Slot::from_pattern(data);
});
