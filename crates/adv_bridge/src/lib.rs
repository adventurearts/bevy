//! Prototype editor <-> engine bridge.
//!
//! The editor owns the USD stage and sends this engine flat, already-resolved
//! entities over gRPC (see `proto/adventure.proto`). [`AdvBridgePlugin`] runs a
//! tonic server on its own thread and hands work to Bevy through channels that
//! are drained in the main schedule, so the Bevy main thread never blocks on the
//! network.
//!
//! The engine renders into an offscreen [`Image`] that is read back on the GPU
//! every frame and published on the `StreamFrames` stream.

mod scene;
mod server;

use std::{net::SocketAddr, sync::Arc, sync::Mutex, thread};

use bevy::{
    image::Image,
    log::error,
    prelude::*,
    render::gpu_readback::{Readback, ReadbackComplete},
};
use crossbeam_channel::{Receiver, Sender};
use tokio::sync::watch;

/// Generated protobuf and gRPC types for the `adventure.v1` package.
pub mod proto {
    tonic::include_proto!("adventure.v1");
}

/// Work queued from the gRPC server for the Bevy main thread.
pub(crate) enum Command {
    LoadStage(Vec<proto::FlatEntity>),
    ApplyDelta(Vec<proto::EntityDelta>),
    SetViewport { width: u32, height: u32 },
}

/// Starts the editor bridge: a gRPC server on `addr` and an offscreen viewport
/// of `viewport` pixels.
#[derive(Clone, Debug)]
pub struct AdvBridgePlugin {
    /// Address the gRPC server listens on.
    pub addr: SocketAddr,
    /// Initial offscreen viewport size in pixels (width, height).
    pub viewport: (u32, u32),
}

impl Default for AdvBridgePlugin {
    fn default() -> Self {
        Self {
            addr: SocketAddr::from(([127, 0, 0, 1], 50051)),
            viewport: (960, 540),
        }
    }
}

impl Plugin for AdvBridgePlugin {
    fn build(&self, app: &mut App) {
        let (command_tx, command_rx): (Sender<Command>, Receiver<Command>) =
            crossbeam_channel::unbounded();
        let (frame_tx, frame_rx) = watch::channel(None);

        let (width, height) = self.viewport;
        let viewport = Arc::new(Mutex::new((width, height)));
        let state = server::ServerState {
            commands: command_tx,
            viewport: viewport.clone(),
            frames: frame_rx,
        };

        let addr = self.addr;
        thread::Builder::new()
            .name("adv_bridge-grpc".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        error!("adv_bridge: failed to start tokio runtime: {err}");
                        return;
                    }
                };
                if let Err(err) = runtime.block_on(server::serve(addr, state)) {
                    error!("adv_bridge: gRPC server on {addr} stopped: {err}");
                }
            })
            .expect("failed to spawn adv_bridge gRPC thread");

        app.insert_resource(scene::BridgeCommands(command_rx))
            .insert_resource(scene::FrameSink {
                tx: frame_tx,
                sequence: 0,
            })
            .insert_resource(scene::ViewportTarget {
                image: Handle::default(),
                width,
                height,
            })
            .init_resource::<scene::EntityIndex>()
            .add_systems(Startup, setup)
            .add_systems(Update, scene::apply_commands);
    }
}

/// Creates the offscreen target, the primitive meshes and the default camera
/// and light, then starts the GPU readback of the target.
fn setup(world: &mut World) {
    let (width, height) = {
        let target = world.resource::<scene::ViewportTarget>();
        (target.width, target.height)
    };
    // COPY_SRC is required for the GPU readback of the target.
    let mut target = Image::new_target_texture(
        width,
        height,
        bevy::render::render_resource::TextureFormat::Rgba8UnormSrgb,
        None,
    );
    target.texture_descriptor.usage |= bevy::render::render_resource::TextureUsages::COPY_SRC;
    let image = world.resource_mut::<Assets<Image>>().add(target);
    world.resource_mut::<scene::ViewportTarget>().image = image.clone();

    let primitives = scene::Primitives {
        cube: world.resource_mut::<Assets<Mesh>>().add(Cuboid::default()),
        sphere: world.resource_mut::<Assets<Mesh>>().add(Sphere::default()),
        plane: world.resource_mut::<Assets<Mesh>>().add(Plane3d::default()),
    };
    world.insert_resource(primitives);

    world
        .spawn((Name::new("adv_bridge/readback"), Readback::texture(image)))
        .observe(on_frame_ready);

    scene::spawn_default_camera_and_light(world, true, true);
}

/// Publishes a completed GPU readback of the viewport to the frame stream.
fn on_frame_ready(
    readback: On<ReadbackComplete>,
    mut sink: ResMut<scene::FrameSink>,
    target: Res<scene::ViewportTarget>,
) {
    let expected = target.width as usize * target.height as usize * 4;
    if readback.data.len() != expected {
        // Readback of a previous size, just after a SetViewport.
        return;
    }
    sink.sequence += 1;
    let frame = proto::Frame {
        sequence: sink.sequence,
        width: target.width,
        height: target.height,
        rgba8: readback.data.clone(),
    };
    sink.tx.send_replace(Some(Arc::new(frame)));
}
