//! A rendered viewport frame as the engine holds it, before any transport.

use std::sync::Arc;

/// Latest rendered frame, published by the readback observer and read by every
/// `StreamFrames` subscriber.
#[derive(Debug)]
pub(crate) struct FrameData {
    /// Monotonic per engine run; frame `n` lives in ring slot `n % slot_count`.
    pub sequence: u64,
    pub width: u32,
    pub height: u32,
    /// Tight RGBA8 rows, top-left origin.
    pub rgba8: Vec<u8>,
    /// A LoadStage/ApplyDelta/SetViewport was applied since the previous frame.
    /// Subscribers that skip unchanged frames must still send this one.
    pub after_change: bool,
    /// Highest `ApplyDeltaRequest.edit_id` applied before this frame's render started.
    pub last_edit_id: u64,
    /// Engine main-loop frame time (the frame that requested this readback).
    pub engine_frame_us: u32,
    /// Time from the readback request to the frame being published.
    pub readback_us: u32,
}

/// Shared pointer to a published frame.
pub(crate) type SharedFrame = Arc<FrameData>;

/// wgpu copies rows with a stride rounded up to 256 bytes, and Bevy's readback
/// returns those padded rows as-is. Strips the padding to tight RGBA8 rows.
/// Returns `None` when `data` is not a padded `width` x `height` image.
pub(crate) fn unpadded_rgba8(data: &[u8], width: u32, height: u32) -> Option<Vec<u8>> {
    const COPY_ROW_ALIGNMENT: usize = 256;
    let (width, height) = (width as usize, height as usize);
    let row = width * 4;
    let stride = row.div_ceil(COPY_ROW_ALIGNMENT) * COPY_ROW_ALIGNMENT;
    if data.len() != stride * height {
        return None;
    }
    if stride == row {
        return Some(data.to_vec());
    }
    let mut tight = Vec::with_capacity(row * height);
    for line in data.chunks_exact(stride) {
        tight.extend_from_slice(&line[..row]);
    }
    Some(tight)
}

/// Fast 64-bit hash of a frame's pixels (FNV-1a over 8-byte words, then the tail).
///
/// Used only to decide "same as the last frame I sent" for `skip_unchanged`, so a
/// collision (about 2^-64 for different frames) would merely skip one frame that
/// the editor would have seen as identical anyway.
pub(crate) fn hash_pixels(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    let (words, tail) = bytes.as_chunks::<8>();
    for word in words {
        hash = (hash ^ u64::from_le_bytes(*word)).wrapping_mul(PRIME);
    }
    for &byte in tail {
        hash = (hash ^ u64::from(byte)).wrapping_mul(PRIME);
    }
    (hash ^ bytes.len() as u64).wrapping_mul(PRIME)
}
