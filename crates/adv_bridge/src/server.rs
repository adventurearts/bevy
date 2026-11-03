//! gRPC service. Runs on the tokio runtime owned by the bridge thread and never
//! touches Bevy directly: mutations are queued as [`Command`]s and frames are
//! read from a `watch` channel that the Bevy side publishes into.

use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use crossbeam_channel::Sender;
use tokio::{
    sync::{broadcast, mpsc, oneshot, watch},
    time::MissedTickBehavior,
};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{transport::Server, Request, Response, Status, Streaming};

use bevy::log::{info, warn};

use crate::{
    frame::{hash_pixels, FrameData, SharedFrame},
    proto::{
        bridge_server::{Bridge, BridgeServer},
        Ack, ApplyDeltaRequest, EngineEvent, FlatEntity, Frame, HelloReply, HelloRequest,
        InputEvent, LoadStageRequest, SetSelectionRequest, SetViewportRequest, StreamEventsRequest,
        StreamFramesRequest,
    },
    readback::RingInfo,
    Command, Reply,
};

/// Largest viewport edge accepted by `SetViewport`.
const MAX_VIEWPORT_EDGE: u32 = 8192;
/// How long a mutating RPC waits for Bevy to apply the command.
const APPLY_TIMEOUT: Duration = Duration::from_secs(5);
/// `StreamFrames` rate when the client asks for `max_fps = 0`.
const DEFAULT_FPS: u32 = 30;
/// Upper bound on the requested `StreamFrames` rate.
const MAX_FPS: u32 = 120;

/// Everything the gRPC service shares with the rest of the plugin.
pub(crate) struct ServerState {
    pub commands: Sender<Command>,
    /// Viewport size Bevy has applied (written by the Bevy side), reported by `Hello`.
    pub viewport: Arc<Mutex<(u32, u32)>>,
    pub frames: watch::Receiver<Option<SharedFrame>>,
    /// Editor input, drained by the Bevy main loop every frame.
    pub inputs: Sender<InputEvent>,
    /// Engine events; each `StreamEvents` call subscribes to this.
    pub events: broadcast::Sender<EngineEvent>,
    /// Frame ring location, written by the Bevy side and reported by `Hello`.
    pub ring: Arc<Mutex<RingInfo>>,
}

/// Serves the `Bridge` service on `incoming` until the process exits.
pub(crate) async fn serve<I>(incoming: I, state: ServerState) -> Result<(), tonic::transport::Error>
where
    I: Stream<Item = std::io::Result<tokio::net::TcpStream>> + Send,
{
    Server::builder()
        .add_service(BridgeServer::new(BridgeService { state }))
        .serve_with_incoming(incoming)
        .await
}

struct BridgeService {
    state: ServerState,
}

impl BridgeService {
    /// Queues a command for the Bevy main thread and waits until Bevy has applied
    /// it, so a following RPC sees its effect. `build` receives the reply channel.
    ///
    /// On timeout the ack is `ok = false`, but the command stays queued and may
    /// still be applied later; the editor should resync (LoadStage) if it cares.
    async fn submit(&self, build: impl FnOnce(Reply) -> Command) -> Result<Response<Ack>, Status> {
        let (reply, outcome) = oneshot::channel();
        self.state
            .commands
            .send(build(reply))
            .map_err(|_| Status::unavailable("engine main loop is not running"))?;
        match tokio::time::timeout(APPLY_TIMEOUT, outcome).await {
            Ok(Ok(Ok(()))) => Ok(Response::new(ack_ok())),
            Ok(Ok(Err(message))) => Ok(Response::new(Ack { ok: false, message })),
            Ok(Err(_)) => Err(Status::unavailable("engine dropped the request")),
            Err(_) => Ok(Response::new(Ack {
                ok: false,
                message: format!(
                    "timed out after {} s waiting for the engine to apply the command",
                    APPLY_TIMEOUT.as_secs()
                ),
            })),
        }
    }

    fn viewport(&self) -> (u32, u32) {
        *self
            .state
            .viewport
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn ring(&self) -> RingInfo {
        self.state
            .ring
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

fn ack_ok() -> Ack {
    Ack {
        ok: true,
        message: String::new(),
    }
}

/// Converts a published frame to its wire form. With `in_ring` the pixels stay in
/// the shared-memory slot (`sequence % slots`) and `rgba8` is left empty.
fn to_proto(frame: &FrameData, in_ring: bool) -> Frame {
    Frame {
        sequence: frame.sequence,
        width: frame.width,
        height: frame.height,
        rgba8: if in_ring {
            Vec::new()
        } else {
            frame.rgba8.clone()
        },
        in_ring,
        last_edit_id: frame.last_edit_id,
        engine_frame_us: frame.engine_frame_us,
        readback_us: frame.readback_us,
    }
}

#[tonic::async_trait]
impl Bridge for BridgeService {
    async fn hello(&self, request: Request<HelloRequest>) -> Result<Response<HelloReply>, Status> {
        let editor_version = request.into_inner().editor_version;
        info!("adv_bridge: editor connected (version {editor_version:?})");
        let (viewport_width, viewport_height) = self.viewport();
        let ring = self.ring();
        Ok(Response::new(HelloReply {
            engine_version: concat!("adv_bridge ", env!("CARGO_PKG_VERSION")).to_owned(),
            viewport_width,
            viewport_height,
            frame_ring_path: ring.path,
            frame_ring_slots: ring.slots,
            frame_ring_slot_bytes: ring.slot_bytes,
        }))
    }

    async fn load_stage(
        &self,
        request: Request<LoadStageRequest>,
    ) -> Result<Response<Ack>, Status> {
        let entities: Vec<FlatEntity> = request.into_inner().entities;
        self.submit(|reply| Command::LoadStage { entities, reply })
            .await
    }

    async fn apply_delta(
        &self,
        request: Request<ApplyDeltaRequest>,
    ) -> Result<Response<Ack>, Status> {
        let ApplyDeltaRequest { deltas, edit_id } = request.into_inner();
        self.submit(|reply| Command::ApplyDelta {
            deltas,
            edit_id,
            reply,
        })
        .await
    }

    async fn set_viewport(
        &self,
        request: Request<SetViewportRequest>,
    ) -> Result<Response<Ack>, Status> {
        let SetViewportRequest { width, height } = request.into_inner();
        if !(1..=MAX_VIEWPORT_EDGE).contains(&width) || !(1..=MAX_VIEWPORT_EDGE).contains(&height) {
            return Err(Status::invalid_argument(format!(
                "viewport must be 1..={MAX_VIEWPORT_EDGE} pixels per side, got {width}x{height}"
            )));
        }
        // Waits until Bevy has applied the resize, so `Ack.ok` and `Hello` agree with
        // the frames that follow.
        self.submit(|reply| Command::SetViewport {
            width,
            height,
            reply,
        })
        .await
    }

    type StreamFramesStream = Pin<Box<dyn Stream<Item = Result<Frame, Status>> + Send + 'static>>;

    async fn stream_frames(
        &self,
        request: Request<StreamFramesRequest>,
    ) -> Result<Response<Self::StreamFramesStream>, Status> {
        let StreamFramesRequest {
            max_fps,
            prefer_shared_memory,
            skip_unchanged,
        } = request.into_inner();
        let max_fps = match max_fps {
            0 => DEFAULT_FPS,
            fps => fps.min(MAX_FPS),
        };
        let mut frames = self.state.frames.clone();
        let ring = self.state.ring.clone();
        let (tx, rx) = mpsc::channel::<Result<Frame, Status>>(2);

        // One task per subscriber: wait for the next tick, then send the latest frame
        // if it is new. Slow clients therefore see fewer frames, not a backlog.
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / f64::from(max_fps)));
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut last_sent = 0_u64;
            let mut last_hash: Option<u64> = None;
            loop {
                tick.tick().await;
                if frames.changed().await.is_err() {
                    break; // engine side is gone
                }
                let Some(frame) = frames.borrow_and_update().clone() else {
                    continue;
                };
                if frame.sequence == last_sent {
                    continue;
                }
                last_sent = frame.sequence;
                if skip_unchanged {
                    // Identical to what this subscriber got last, and no change was
                    // applied since: skip. A change always sends its first frame.
                    let hash = hash_pixels(&frame.rgba8);
                    if last_hash == Some(hash) && !frame.after_change {
                        continue;
                    }
                    last_hash = Some(hash);
                }
                let ring_ready = !ring
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .path
                    .is_empty();
                let in_ring = prefer_shared_memory && ring_ready;
                if tx.send(Ok(to_proto(&frame, in_ring))).await.is_err() {
                    break; // client hung up
                }
            }
        });

        let stream: Self::StreamFramesStream = Box::pin(ReceiverStream::new(rx));
        Ok(Response::new(stream))
    }

    async fn set_selection(
        &self,
        request: Request<SetSelectionRequest>,
    ) -> Result<Response<Ack>, Status> {
        let path = request.into_inner().path;
        self.submit(|reply| Command::SetSelection { path, reply })
            .await
    }

    async fn send_input(
        &self,
        request: Request<Streaming<InputEvent>>,
    ) -> Result<Response<Ack>, Status> {
        // Unbounded: the Bevy loop drains it every frame, so backlog stays small.
        let mut inbound = request.into_inner();
        while let Some(event) = inbound.message().await? {
            if self.state.inputs.send(event).is_err() {
                return Err(Status::unavailable("engine main loop is not running"));
            }
        }
        Ok(Response::new(ack_ok()))
    }

    type StreamEventsStream =
        Pin<Box<dyn Stream<Item = Result<EngineEvent, Status>> + Send + 'static>>;

    async fn stream_events(
        &self,
        _request: Request<StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        // Each subscriber gets its own task and bounded channel, so a slow client only
        // lags itself; Bevy never waits on it. Logs are not forwarded yet (TODO).
        let mut events = self.state.events.subscribe();
        let (tx, rx) = mpsc::channel::<Result<EngineEvent, Status>>(64);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tx.closed() => break,
                    received = events.recv() => match received {
                        Ok(event) => {
                            if tx.send(Ok(event)).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!("adv_bridge: a StreamEvents subscriber skipped {skipped} events");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    },
                }
            }
        });
        let stream: Self::StreamEventsStream = Box::pin(ReceiverStream::new(rx));
        Ok(Response::new(stream))
    }
}
