//! gRPC service. Runs on the tokio runtime owned by the bridge thread and never
//! touches Bevy directly: mutations are queued as [`Command`]s and frames are
//! read from a `watch` channel that the Bevy side publishes into.

use std::{
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use crossbeam_channel::Sender;
use tokio::{
    sync::{mpsc, watch},
    time::MissedTickBehavior,
};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming, transport::Server};

use bevy::log::info;

use crate::{
    Command,
    proto::{
        Ack, EngineEvent, EntityDelta, FlatEntity, Frame, HelloReply, HelloRequest, InputEvent,
        LoadStageRequest, SetViewportRequest, StreamEventsRequest, StreamFramesRequest,
        bridge_server::{Bridge, BridgeServer},
    },
};

/// Largest viewport edge accepted by `SetViewport`.
const MAX_VIEWPORT_EDGE: u32 = 8192;
/// `StreamFrames` rate when the client asks for `max_fps = 0`.
const DEFAULT_FPS: u32 = 30;
/// Upper bound on the requested `StreamFrames` rate.
const MAX_FPS: u32 = 120;

/// Everything the gRPC service shares with the rest of the plugin.
pub(crate) struct ServerState {
    pub commands: Sender<Command>,
    /// Viewport size as last requested by the editor, reported by `Hello`.
    pub viewport: Arc<Mutex<(u32, u32)>>,
    pub frames: watch::Receiver<Option<Arc<Frame>>>,
}

/// Serves the `Bridge` service on `addr` until the process exits.
pub(crate) async fn serve(addr: SocketAddr, state: ServerState) -> Result<(), tonic::transport::Error> {
    Server::builder()
        .add_service(BridgeServer::new(BridgeService { state }))
        .serve(addr)
        .await
}

struct BridgeService {
    state: ServerState,
}

impl BridgeService {
    /// Queues a command for the Bevy main thread and acknowledges acceptance.
    /// The ack means "queued", not "applied".
    fn enqueue(&self, command: Command) -> Result<Response<Ack>, Status> {
        self.state
            .commands
            .send(command)
            .map_err(|_| Status::unavailable("engine main loop is not running"))?;
        Ok(Response::new(ack_ok()))
    }

    fn viewport(&self) -> (u32, u32) {
        *self
            .state
            .viewport
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn ack_ok() -> Ack {
    Ack {
        ok: true,
        message: String::new(),
    }
}

#[tonic::async_trait]
impl Bridge for BridgeService {
    async fn hello(&self, request: Request<HelloRequest>) -> Result<Response<HelloReply>, Status> {
        let editor_version = request.into_inner().editor_version;
        info!("adv_bridge: editor connected (version {editor_version:?})");
        let (viewport_width, viewport_height) = self.viewport();
        Ok(Response::new(HelloReply {
            engine_version: concat!("adv_bridge ", env!("CARGO_PKG_VERSION")).to_owned(),
            viewport_width,
            viewport_height,
        }))
    }

    async fn load_stage(&self, request: Request<LoadStageRequest>) -> Result<Response<Ack>, Status> {
        let entities: Vec<FlatEntity> = request.into_inner().entities;
        self.enqueue(Command::LoadStage(entities))
    }

    async fn apply_delta(&self, request: Request<crate::proto::ApplyDeltaRequest>) -> Result<Response<Ack>, Status> {
        let deltas: Vec<EntityDelta> = request.into_inner().deltas;
        self.enqueue(Command::ApplyDelta(deltas))
    }

    async fn set_viewport(&self, request: Request<SetViewportRequest>) -> Result<Response<Ack>, Status> {
        let SetViewportRequest { width, height } = request.into_inner();
        if !(1..=MAX_VIEWPORT_EDGE).contains(&width) || !(1..=MAX_VIEWPORT_EDGE).contains(&height) {
            return Err(Status::invalid_argument(format!(
                "viewport must be 1..={MAX_VIEWPORT_EDGE} pixels per side, got {width}x{height}"
            )));
        }
        *self
            .state
            .viewport
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = (width, height);
        self.enqueue(Command::SetViewport { width, height })
    }

    type StreamFramesStream = Pin<Box<dyn Stream<Item = Result<Frame, Status>> + Send + 'static>>;

    async fn stream_frames(
        &self,
        request: Request<StreamFramesRequest>,
    ) -> Result<Response<Self::StreamFramesStream>, Status> {
        let max_fps = match request.into_inner().max_fps {
            0 => DEFAULT_FPS,
            fps => fps.min(MAX_FPS),
        };
        let mut frames = self.state.frames.clone();
        let (tx, rx) = mpsc::channel::<Result<Frame, Status>>(2);

        // One task per subscriber: wait for the next tick, then send the latest
        // frame if it is new. Slow clients therefore see fewer frames, not a backlog.
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / f64::from(max_fps)));
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut last_sent = 0_u64;
            loop {
                tick.tick().await;
                if frames.changed().await.is_err() {
                    break; // engine side is gone
                }
                let latest = frames.borrow_and_update().clone();
                let Some(frame) = latest else { continue };
                if frame.sequence == last_sent {
                    continue;
                }
                last_sent = frame.sequence;
                if tx.send(Ok((*frame).clone())).await.is_err() {
                    break; // client hung up
                }
            }
        });

        let stream: Self::StreamFramesStream = Box::pin(ReceiverStream::new(rx));
        Ok(Response::new(stream))
    }

    async fn send_input(&self, request: Request<Streaming<InputEvent>>) -> Result<Response<Ack>, Status> {
        // TODO(phase 2): forward mouse/keyboard events to the engine input systems.
        let mut inbound = request.into_inner();
        while inbound.message().await?.is_some() {}
        Ok(Response::new(ack_ok()))
    }

    type StreamEventsStream = Pin<Box<dyn Stream<Item = Result<EngineEvent, Status>> + Send + 'static>>;

    async fn stream_events(
        &self,
        _request: Request<StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        // TODO(phase 2): forward engine logs, selection and play state. For now the
        // stream is held open and never yields; the sender lives until the client leaves.
        let (tx, rx) = mpsc::channel::<Result<EngineEvent, Status>>(1);
        tokio::spawn(async move { tx.closed().await });
        let stream: Self::StreamEventsStream = Box::pin(ReceiverStream::new(rx));
        Ok(Response::new(stream))
    }
}
