//! GPU readback of the viewport, one request per engine frame, and publication.
//!
//! Each frame, `spawn_readback` (in `Last`) spawns an entity that carries a
//! [`FrameRequest`] and a `Readback` component. Bevy extracts the `Readback` into the
//! render world at the end of the frame, so the request is made exactly once and
//! reflects everything applied this frame. `retire_readbacks` (in `First` of the
//! next frame) removes the `Readback`, so it is not requested again. When the copy
//! finishes, `on_frame_ready` publishes the frame and despawns the entity.
//!
//! That gives exact metadata: `last_edit_id` is the highest `ApplyDelta` edit applied
//! before the frame's extraction, and `after_change` marks the first frame after a
//! LoadStage/ApplyDelta/SetViewport.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bevy::{
    image::Image,
    prelude::*,
    render::{
        gpu_readback::{Readback, ReadbackComplete},
        render_resource::{TextureFormat, TextureUsages},
    },
};
use tokio::sync::watch;

use crate::{
    frame::{unpadded_rgba8, FrameData, SharedFrame},
    ring::FrameRing,
    scene::ViewportTarget,
};

/// Ring slots. Three lets a reader hold one frame while the next is written.
pub(crate) const RING_SLOTS: u32 = 3;
/// Requests older than this never completed (for example the GPU image was not
/// ready) and are dropped so their entities do not pile up.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Ring location and layout as reported by `Hello`. Empty path = no ring.
#[derive(Clone, Debug, Default)]
pub(crate) struct RingInfo {
    pub path: String,
    pub slots: u32,
    pub slot_bytes: u64,
}

/// [`RingInfo`] shared with the gRPC thread.
#[derive(Resource, Clone)]
pub(crate) struct SharedRing(pub Arc<Mutex<RingInfo>>);

/// Owns the ring file and its generation counter.
#[derive(Resource, Default)]
pub(crate) struct RingWriter {
    pub ring: Option<FrameRing>,
    pub generation: u32,
}

/// Publishes frames to the gRPC thread.
#[derive(Resource)]
pub(crate) struct FrameSink {
    pub tx: watch::Sender<Option<SharedFrame>>,
    /// Sequence of the last published frame.
    pub sequence: u64,
    /// Request index of the last published frame; older completions are dropped.
    pub last_index: u64,
}

/// Highest `ApplyDeltaRequest.edit_id` applied so far (0 = none tracked yet).
#[derive(Resource, Default)]
pub(crate) struct EditTracker {
    pub highest_applied: u64,
}

/// Set by any LoadStage/ApplyDelta/SetViewport; consumed by the next readback request.
#[derive(Resource, Default)]
pub(crate) struct FrameChange(pub bool);

/// Counter for readback requests.
#[derive(Resource, Default)]
pub(crate) struct RequestCounter(pub u64);

/// Metadata captured at request time and read back by the observer.
#[derive(Component)]
pub(crate) struct FrameRequest {
    index: u64,
    width: u32,
    height: u32,
    edit_id: u64,
    after_change: bool,
    engine_frame_us: u32,
    started: Instant,
}

/// Creates the viewport target image and returns its handle.
pub(crate) fn create_target(images: &mut Assets<Image>, width: u32, height: u32) -> Handle<Image> {
    // COPY_SRC is required for the GPU readback of the target.
    let mut target = Image::new_target_texture(width, height, TextureFormat::Rgba8UnormSrgb, None);
    target.texture_descriptor.usage |= TextureUsages::COPY_SRC;
    images.add(target)
}

/// Creates the ring for the initial viewport. A failure is logged and the bridge
/// keeps working with inline frames only.
pub(crate) fn create_initial_ring(world: &mut World) {
    let (width, height) = {
        let target = world.resource::<ViewportTarget>();
        (target.width, target.height)
    };
    let frame_bytes = u64::from(width) * u64::from(height) * 4;
    match FrameRing::create(RING_SLOTS, frame_bytes, 0) {
        Ok(ring) => publish_ring(world, Some(ring)),
        Err(err) => bevy::log::warn!("adv_bridge: no shared-memory frame ring: {err}"),
    }
}

/// Makes the ring hold a frame of `width` x `height`. Recreates it under a new path
/// when the current slots are too small; otherwise keeps it.
pub(crate) fn fit_ring(world: &mut World, width: u32, height: u32) {
    let frame_bytes = u64::from(width) * u64::from(height) * 4;
    let fits = world
        .resource::<RingWriter>()
        .ring
        .as_ref()
        .is_some_and(|ring| ring.fits(frame_bytes));
    if fits {
        return;
    }
    // Dropping the old ring removes its file before the new one is created.
    let generation = {
        let mut writer = world.resource_mut::<RingWriter>();
        writer.ring = None;
        writer.generation += 1;
        writer.generation
    };
    match FrameRing::create(RING_SLOTS, frame_bytes, generation) {
        Ok(ring) => publish_ring(world, Some(ring)),
        Err(err) => {
            bevy::log::warn!("adv_bridge: could not resize the frame ring: {err}");
            publish_ring(world, None);
        }
    }
}

/// Stores `ring` and republishes its location for `Hello`.
fn publish_ring(world: &mut World, ring: Option<FrameRing>) {
    let info = match &ring {
        Some(ring) => RingInfo {
            path: ring.path().display().to_string(),
            slots: ring.slot_count(),
            slot_bytes: ring.slot_bytes(),
        },
        None => RingInfo::default(),
    };
    world.resource_mut::<RingWriter>().ring = ring;
    *world
        .resource::<SharedRing>()
        .0
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = info;
}

/// Requests this frame's readback. Runs in `Last` so the request sees every edit
/// applied in `Update` this frame.
pub(crate) fn spawn_readback(
    mut commands: Commands,
    target: Res<ViewportTarget>,
    edits: Res<EditTracker>,
    mut change: ResMut<FrameChange>,
    mut counter: ResMut<RequestCounter>,
    time: Res<Time<Real>>,
) {
    if target.image == Handle::default() {
        return; // setup has not run yet
    }
    counter.0 += 1;
    let engine_frame_us = u32::try_from(time.delta().as_micros()).unwrap_or(u32::MAX);
    commands
        .spawn((
            Name::new("adv_bridge/readback"),
            Readback::texture(target.image.clone()),
            FrameRequest {
                index: counter.0,
                width: target.width,
                height: target.height,
                edit_id: edits.highest_applied,
                after_change: std::mem::take(&mut change.0),
                engine_frame_us,
                started: Instant::now(),
            },
        ))
        .observe(on_frame_ready);
}

/// Removes `Readback` from requests made in an earlier frame, so each request is
/// copied exactly once. Runs in `First`.
pub(crate) fn retire_readbacks(
    mut commands: Commands,
    requests: Query<Entity, (With<FrameRequest>, With<Readback>)>,
) {
    for entity in &requests {
        commands.entity(entity).remove::<Readback>();
    }
}

/// Drops requests whose copy never completed. Runs in `Last`, before new requests.
pub(crate) fn expire_requests(mut commands: Commands, requests: Query<(Entity, &FrameRequest)>) {
    for (entity, request) in &requests {
        if request.started.elapsed() > REQUEST_TIMEOUT {
            commands.entity(entity).despawn();
        }
    }
}

/// Publishes a completed readback: writes the ring slot, then the watch value.
fn on_frame_ready(
    readback: On<ReadbackComplete>,
    requests: Query<&FrameRequest>,
    mut sink: ResMut<FrameSink>,
    mut ring: ResMut<RingWriter>,
    mut commands: Commands,
) {
    let entity = readback.entity;
    commands.entity(entity).despawn();
    let Ok(request) = requests.get(entity) else {
        return;
    };
    // Completions can arrive out of order; never publish an older frame than one already sent.
    if request.index <= sink.last_index {
        return;
    }
    let Some(rgba8) = unpadded_rgba8(&readback.data, request.width, request.height) else {
        return;
    };
    sink.last_index = request.index;
    sink.sequence += 1;
    let sequence = sink.sequence;
    if let Some(ring) = ring.ring.as_mut()
        && let Err(err) = ring.write(sequence, request.width, request.height, &rgba8)
    {
        bevy::log::warn!("adv_bridge: frame {sequence} not written to the ring: {err}");
    }
    let frame = FrameData {
        sequence,
        width: request.width,
        height: request.height,
        rgba8,
        after_change: request.after_change,
        last_edit_id: request.edit_id,
        engine_frame_us: request.engine_frame_us,
        readback_us: u32::try_from(request.started.elapsed().as_micros()).unwrap_or(u32::MAX),
    };
    sink.tx.send_replace(Some(Arc::new(frame)));
}
