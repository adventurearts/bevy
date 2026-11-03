//! Shared-memory frame ring: a file both processes map, holding the last few frames.
//!
//! Layout and the seqlock protocol are specified in `proto/adventure.proto`
//! ("Frame ring"). This module is the writer. The file lives in the OS temp dir as
//! `adv_bridge_ring_<pid>.bin` (generation 0) or `adv_bridge_ring_<pid>_<gen>.bin`
//! after a resize that needed a larger slot.
//!
//! Unsafe code is limited to two things: mapping the file, and reading or writing
//! the atomics and payload at fixed offsets inside that mapping. The offsets are all
//! multiples of 8 (slot capacities are rounded up to 64 bytes), so the `AtomicU64`
//! accesses are aligned.

use std::{
    fs::{self, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::atomic::{fence, AtomicU64, Ordering},
};

use bevy::log::warn;
use memmap2::MmapMut;
use sysinfo::{Pid, ProcessesToUpdate, System};

/// File magic, "ADVR" when read as bytes.
const MAGIC: u32 = 0x5256_4441;
const VERSION: u32 = 1;
const HEADER_BYTES: usize = 64;
const SLOT_HEADER_BYTES: usize = 32;
/// Slot payload capacities are rounded up to this, keeping every slot 8-aligned.
const CAPACITY_ALIGN: u64 = 64;

// Byte offsets inside the file header.
const HEADER_SLOT_COUNT: usize = 8;
const HEADER_SLOT_BYTES: usize = 16;
const HEADER_LATEST: usize = 24;
// Byte offsets inside a slot header (relative to the slot start).
const SLOT_SEQ_BEGIN: usize = 0;
const SLOT_SEQ_END: usize = 8;
const SLOT_WIDTH: usize = 16;
const SLOT_HEIGHT: usize = 20;

/// One mapped ring file. Removes its file when dropped.
pub(crate) struct FrameRing {
    path: PathBuf,
    /// `Option` so the mapping can be unmapped before the file is removed (Windows
    /// refuses to delete a mapped file).
    map: Option<MmapMut>,
    slot_count: u32,
    /// Payload capacity of one slot, a multiple of [`CAPACITY_ALIGN`].
    slot_bytes: u64,
}

impl FrameRing {
    /// Creates a ring with `slot_count` slots that can hold `frame_bytes` each.
    /// `generation` 0 uses the plain name; higher generations get a `_<gen>` suffix
    /// so a recreated ring always has a new path.
    pub(crate) fn create(slot_count: u32, frame_bytes: u64, generation: u32) -> io::Result<Self> {
        let slot_bytes = frame_bytes.div_ceil(CAPACITY_ALIGN) * CAPACITY_ALIGN;
        let path = ring_path(std::process::id(), generation);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        let total =
            HEADER_BYTES as u64 + u64::from(slot_count) * (SLOT_HEADER_BYTES as u64 + slot_bytes);
        file.set_len(total)?;
        // SAFETY: the file was just created by us with `create_new`; the editor only
        // maps it read-only and nothing else truncates it while we hold it.
        let mut map = unsafe { MmapMut::map_mut(&file)? };
        let base = map.as_mut_ptr();
        // SAFETY: the mapping is `total` bytes long and the header occupies its first
        // 64 bytes; no reader can see the file before `Hello` returns its path.
        unsafe {
            write_u32(base, 0, MAGIC);
            write_u32(base, 4, VERSION);
            write_u32(base, HEADER_SLOT_COUNT, slot_count);
            write_u64(base, HEADER_SLOT_BYTES, slot_bytes);
        }
        map.flush()?;
        Ok(Self {
            path,
            map: Some(map),
            slot_count,
            slot_bytes,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn slot_count(&self) -> u32 {
        self.slot_count
    }

    pub(crate) fn slot_bytes(&self) -> u64 {
        self.slot_bytes
    }

    /// Whether a frame of `frame_bytes` fits in one slot.
    pub(crate) fn fits(&self, frame_bytes: u64) -> bool {
        frame_bytes <= self.slot_bytes
    }

    /// Writes frame `sequence` into slot `sequence % slot_count` with the seqlock
    /// protocol: `seq_begin = N`, payload, `seq_end = N`, then publish `latest = N`.
    pub(crate) fn write(
        &mut self,
        sequence: u64,
        width: u32,
        height: u32,
        pixels: &[u8],
    ) -> io::Result<()> {
        if !self.fits(pixels.len() as u64) {
            return Err(io::Error::other("frame does not fit in a ring slot"));
        }
        let slot = (sequence % u64::from(self.slot_count)) as usize;
        let offset = HEADER_BYTES + slot * (SLOT_HEADER_BYTES + self.slot_bytes as usize);
        let map = self
            .map
            .as_mut()
            .ok_or_else(|| io::Error::other("ring is closed"))?;
        let base = map.as_mut_ptr();
        // SAFETY: `offset + SLOT_HEADER_BYTES + pixels.len() <= total file length`
        // (checked by `fits` and by the layout in `create`), and every access is within
        // this slot. The only concurrent accessors are readers in other processes, which
        // use the seqlock to detect overlap. All accesses to the two header words are atomic.
        unsafe {
            atomic_at(base, offset + SLOT_SEQ_BEGIN).store(sequence, Ordering::Relaxed);
            // Keep the payload writes from being reordered before the `seq_begin` store.
            fence(Ordering::Release);
            write_u32(base, offset + SLOT_WIDTH, width);
            write_u32(base, offset + SLOT_HEIGHT, height);
            std::ptr::copy_nonoverlapping(
                pixels.as_ptr(),
                base.add(offset + SLOT_HEADER_BYTES),
                pixels.len(),
            );
            atomic_at(base, offset + SLOT_SEQ_END).store(sequence, Ordering::Release);
            atomic_at(base, HEADER_LATEST).store(sequence, Ordering::Release);
        }
        Ok(())
    }
}

impl Drop for FrameRing {
    fn drop(&mut self) {
        // Unmap first, then remove. Failure (for example a reader still has the file
        // mapped on Windows) is left for the next startup's stale-file cleanup.
        self.map = None;
        if let Err(err) = fs::remove_file(&self.path) {
            warn!(
                "adv_bridge: could not remove frame ring {}: {err}",
                self.path.display()
            );
        }
    }
}

/// `adv_bridge_ring_<pid>.bin` for generation 0, `adv_bridge_ring_<pid>_<gen>.bin` after.
pub(crate) fn ring_path(pid: u32, generation: u32) -> PathBuf {
    let name = if generation == 0 {
        format!("adv_bridge_ring_{pid}.bin")
    } else {
        format!("adv_bridge_ring_{pid}_{generation}.bin")
    };
    std::env::temp_dir().join(name)
}

/// Removes ring files left behind by processes that are no longer running.
/// Called once at plugin build; a crashed engine cannot run its `Drop`.
pub(crate) fn remove_stale_rings() {
    let Ok(entries) = fs::read_dir(std::env::temp_dir()) else {
        return;
    };
    let candidates: Vec<(PathBuf, u32)> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?.to_owned();
            let stem = name
                .strip_prefix("adv_bridge_ring_")?
                .strip_suffix(".bin")?;
            let pid = stem.split('_').next()?.parse::<u32>().ok()?;
            Some((path, pid))
        })
        .filter(|(_, pid)| *pid != std::process::id())
        .collect();
    if candidates.is_empty() {
        return;
    }
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::All, true);
    for (path, pid) in candidates {
        if system.process(Pid::from_u32(pid)).is_none() {
            let _ = fs::remove_file(path);
        }
    }
}

/// An atomic view of 8 bytes inside the mapping.
///
/// # Safety
/// `base + offset` must point to 8 readable and writable bytes that are 8-byte aligned
/// and live as long as the returned reference is used.
unsafe fn atomic_at<'a>(base: *const u8, offset: usize) -> &'a AtomicU64 {
    debug_assert_eq!(
        (base as usize + offset) % 8,
        0,
        "ring atomics must be 8-aligned"
    );
    // SAFETY: upheld by the caller; `AtomicU64` has the same layout as `u64`.
    unsafe { &*base.add(offset).cast::<AtomicU64>() }
}

/// Writes a little-endian `u32` at `offset`.
///
/// # Safety
/// `base + offset .. + 4` must be writable mapped memory.
unsafe fn write_u32(base: *mut u8, offset: usize, value: u32) {
    // SAFETY: upheld by the caller; unaligned write is allowed.
    unsafe {
        base.add(offset)
            .cast::<u32>()
            .write_unaligned(value.to_le())
    }
}

/// Writes a little-endian `u64` at `offset`.
///
/// # Safety
/// `base + offset .. + 8` must be writable mapped memory.
unsafe fn write_u64(base: *mut u8, offset: usize, value: u64) {
    // SAFETY: upheld by the caller; unaligned write is allowed.
    unsafe {
        base.add(offset)
            .cast::<u64>()
            .write_unaligned(value.to_le())
    }
}
