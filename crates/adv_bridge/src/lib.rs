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

mod input;
mod scene;
mod server;

use std::{net::SocketAddr, sync::Arc, sync::Mutex, thread};

use bevy::{
    app::AppExit,
    image::Image,
    log::error,
    prelude::*,
    render::gpu_readback::{Readback, ReadbackComplete},
};
use crossbeam_channel::{Receiver, Sender};
use tokio::sync::{broadcast, watch};
use tokio_stream::wrappers::TcpListenerStream;

/// Engine events kept for slow `StreamEvents` subscribers before they start to lag.
const EVENT_BUFFER: usize = 256;

/// Generated protobuf and gRPC types for the `adventure.v1` package.
pub mod proto {
    tonic::include_proto!("adventure.v1");
}

/// Work queued from the gRPC server for the Bevy main thread.
pub(crate) enum Command {
    /// Replaces the stage. The reply is sent once the entities are spawned.
    LoadStage {
        entities: Vec<proto::FlatEntity>,
        reply: Reply,
    },
    /// Applies edits to loaded entities. The reply is sent once they are applied.
    ApplyDelta {
        deltas: Vec<proto::EntityDelta>,
        reply: Reply,
    },
    /// Resizes the offscreen target. The reply is sent once Bevy has applied it.
    SetViewport {
        width: u32,
        height: u32,
        reply: Reply,
    },
    /// Empty path clears the selection. The reply reports whether the path exists.
    SetSelection { path: String, reply: Reply },
}

/// Reply channel for a command: `Ok` once Bevy has applied it, `Err` with a reason.
pub(crate) type Reply = tokio::sync::oneshot::Sender<Result<(), String>>;

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
        let (input_tx, input_rx) = crossbeam_channel::unbounded::<proto::InputEvent>();
        let (event_tx, _) = broadcast::channel::<proto::EngineEvent>(EVENT_BUFFER);
        let (fatal_tx, fatal_rx) = crossbeam_channel::unbounded::<String>();

        let (width, height) = self.viewport;
        let viewport = Arc::new(Mutex::new((width, height)));
        let state = server::ServerState {
            commands: command_tx,
            viewport: viewport.clone(),
            frames: frame_rx,
            inputs: input_tx,
            events: event_tx.clone(),
        };

        // Bind here, not on the server thread, so a port clash is known before the
        // app starts and can stop it (see `exit_if_bind_failed`).
        let addr = self.addr;
        let listener = bind(addr);
        match listener {
            Ok(listener) => {
                thread::Builder::new()
                    .name("adv_bridge-grpc".into())
                    .spawn(move || {
                        let runtime = match tokio::runtime::Builder::new_multi_thread()
                            .enable_all()
                            .build()
                        {
                            Ok(runtime) => runtime,
                            Err(err) => {
                                let _ = fatal_tx.send(format!(
                                    "adv_bridge: failed to start tokio runtime: {err}"
                                ));
                                return;
                            }
                        };
                        let result = runtime.block_on(async move {
                            let listener = tokio::net::TcpListener::from_std(listener)
                                .map_err(|err| err.to_string())?;
                            server::serve(TcpListenerStream::new(listener), state)
                                .await
                                .map_err(|err| err.to_string())
                        });
                        if let Err(err) = result {
                            let _ = fatal_tx
                                .send(format!("adv_bridge: gRPC server on {addr} stopped: {err}"));
                        }
                    })
                    .expect("failed to spawn adv_bridge gRPC thread");
            }
            Err(err) => {
                app.insert_resource(BridgeFailure(format!(
                    "adv_bridge: cannot listen on {addr}: {err} - is another game instance already running?"
                )));
            }
        }

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
            .insert_resource(scene::AppliedViewport(viewport.clone()))
            .init_resource::<scene::EntityIndex>()
            .insert_resource(input::InputQueue(input_rx))
            .insert_resource(input::EventSink(event_tx))
            .init_resource::<input::Interaction>()
            .insert_resource(ServerFatal(fatal_rx))
            .add_systems(Startup, (setup, exit_if_bind_failed))
            .add_systems(
                Update,
                (
                    exit_on_server_failure,
                    scene::apply_commands,
                    input::apply_input,
                    input::draw_selection,
                )
                    .chain(),
            );
    }
}

/// Binds the gRPC port and makes the socket non-blocking for tokio.
fn bind(addr: SocketAddr) -> std::io::Result<std::net::TcpListener> {
    let listener = std::net::TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    Ok(listener)
}

/// Set when the gRPC listener could not bind; the app exits on its first update.
#[derive(Resource)]
struct BridgeFailure(String);

/// Failures reported by the gRPC thread after a successful bind.
#[derive(Resource)]
struct ServerFatal(Receiver<String>);

/// Logs the bind failure and stops the app with a non-zero exit code. Without the
/// gRPC server the editor would run half-dead, so this is not recoverable.
fn exit_if_bind_failed(failure: Option<Res<BridgeFailure>>, mut exit: MessageWriter<AppExit>) {
    if let Some(failure) = failure {
        error!("{}", failure.0);
        exit.write(AppExit::error());
    }
}

/// Stops the app when the gRPC thread dies after binding.
fn exit_on_server_failure(fatal: Res<ServerFatal>, mut exit: MessageWriter<AppExit>) {
    if let Ok(message) = fatal.0.try_recv() {
        error!("{message}");
        exit.write(AppExit::error());
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
    // A readback of the previous size (just after SetViewport) does not match the
    // current target; it is dropped and the next readback is used instead.
    let Some(rgba8) = unpadded_rgba8(&readback.data, target.width, target.height) else {
        return;
    };
    sink.sequence += 1;
    let frame = proto::Frame {
        sequence: sink.sequence,
        width: target.width,
        height: target.height,
        rgba8,
    };
    sink.tx.send_replace(Some(Arc::new(frame)));
}

/// wgpu copies rows with a stride rounded up to 256 bytes, and Bevy's readback
/// returns those padded rows as-is. Strips the padding to tight RGBA8 rows.
/// Returns `None` when `data` is not a padded `width` x `height` image.
fn unpadded_rgba8(data: &[u8], width: u32, height: u32) -> Option<Vec<u8>> {
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
