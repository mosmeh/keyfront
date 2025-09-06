use crate::{assert_eq_size, static_assert};

assert_eq_size!(usize, u64);

const MAX_INLINE_LEN: usize = 15;

/// An owned, immutable sequence of bytes that can store up to 15 bytes inline.
///
/// Values larger than 15 bytes are stored on the heap.
#[repr(C)]
pub struct Repr {
    heap_ptr: *mut u8, // pointer to the first byte
    heap_len1: u32,    // length bits 0..31
    heap_len2: u16,    // length bits 32..47
    heap_len3: u8,     // length bits 48..55
    tag: Tag,
}

assert_eq_size!(Repr, [u8; MAX_INLINE_LEN + 1]);

unsafe impl Send for Repr {}
unsafe impl Sync for Repr {}

#[allow(dead_code)]
#[derive(Clone, Copy)]
#[repr(u8)]
enum Tag {
    // We list all the possible tags here so that the compiler can perform
    // niche-filling optimizations.
    InlineLen0 = 0,
    InlineLen1 = 1,
    InlineLen2 = 2,
    InlineLen3 = 3,
    InlineLen4 = 4,
    InlineLen5 = 5,
    InlineLen6 = 6,
    InlineLen7 = 7,
    InlineLen8 = 8,
    InlineLen9 = 9,
    InlineLen10 = 10,
    InlineLen11 = 11,
    InlineLen12 = 12,
    InlineLen13 = 13,
    InlineLen14 = 14,
    InlineLen15 = 15,
    Heap = 16,
}

static_assert!(Tag::Heap as usize > MAX_INLINE_LEN);

impl Repr {
    pub fn from_slice(bytes: &[u8]) -> Self {
        if bytes.len() <= MAX_INLINE_LEN {
            unsafe { Self::new_inline_unchecked(bytes) }
        } else {
            Self::new_heap(bytes.to_vec().into_boxed_slice())
        }
    }

    pub fn from_boxed_slice(bytes: Box<[u8]>) -> Self {
        if bytes.len() <= MAX_INLINE_LEN {
            unsafe { Self::new_inline_unchecked(&bytes) }
        } else {
            Self::new_heap(bytes)
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that `bytes` has a length of <= `MAX_INLINE_LEN`.
    unsafe fn new_inline_unchecked(bytes: &[u8]) -> Self {
        let mut buf = [0; MAX_INLINE_LEN + 1];
        buf[MAX_INLINE_LEN] = bytes.len() as u8;
        buf[..bytes.len()].copy_from_slice(bytes);
        unsafe { std::mem::transmute(buf) }
    }

    fn new_heap(bytes: Box<[u8]>) -> Self {
        let len = bytes.len();
        assert!(len < (1 << (32 + 16 + 8))); // 64 petabytes. Should be enough.
        Self {
            heap_ptr: Box::into_raw(bytes).cast(),
            heap_len1: len as u32,
            heap_len2: (len >> 32) as u16,
            heap_len3: (len >> 48) as u8,
            tag: Tag::Heap,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        let (ptr, len) = if matches!(self.tag, Tag::Heap) {
            (self.heap_ptr.cast_const(), self.heap_len())
        } else {
            (std::ptr::from_ref::<Self>(self).cast(), self.tag as usize)
        };
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }

    fn heap_len(&self) -> usize {
        self.heap_len1 as usize
            | (usize::from(self.heap_len2) << 32)
            | (usize::from(self.heap_len3) << 48)
    }
}

impl Drop for Repr {
    fn drop(&mut self) {
        if matches!(self.tag, Tag::Heap) {
            let _ = unsafe {
                Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                    self.heap_ptr,
                    self.heap_len(),
                ))
            };
        }
    }
}

impl Clone for Repr {
    fn clone(&self) -> Self {
        Self::from_slice(self.as_slice())
    }
}
