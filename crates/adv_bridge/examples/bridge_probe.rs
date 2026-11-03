//! Minimal editor-side client for the bridge. Run `headless_bridge` first.
//!
//! ```text
//! cargo run -p adv_bridge --example bridge_probe -- --save out.png
//! ```
//!
//! Calls `Hello`, sends a small `LoadStage`, waits for the 5th frame (so the
//! scene has rendered), optionally saves it as a PNG with pixel statistics, then
//! exercises `ApplyDelta` (recolour and move `/Level/Crate`) and `SetViewport`
//! (640x360) and checks that later frames reflect both.

use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use adv_bridge::proto::{
    bridge_client::BridgeClient, engine_event::Kind as EventKind, input_event::Kind as InputKind,
    ApplyDeltaRequest, Color, EngineEvent, EntityDelta, FlatEntity, Frame, HelloRequest,
    InputEvent, LoadStageRequest, MouseButton, MouseMove, PrimitiveKind, Quat, SetSelectionRequest,
    SetViewportRequest, StreamEventsRequest, StreamFramesRequest, Transform, Vec3,
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt as _};
use tonic::Streaming;

/// Frame index to sample, so the scene has had time to render.
const FRAME_INDEX: usize = 5;
/// Clear colour of the bridge camera's target (Bevy default, sRGB 43,44,47).
const CLEAR_RGB: [u8; 3] = [43, 44, 47];
const CLEAR_TOLERANCE: u8 = 4;
/// Per-frame wait. Also bounds the warm-up wait: the first frames after
/// LoadStage can stay at the clear colour while pipelines compile (debug build).
const FRAME_TIMEOUT: Duration = Duration::from_secs(120);

type FrameStream = Streaming<Frame>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;
    if let Some(stem) = args.interact.as_deref() {
        let passed = run_interact(stem).await?;
        std::process::exit(if passed { 0 } else { 1 });
    }
    if args.shm {
        let passed = run_shm().await?;
        std::process::exit(if passed { 0 } else { 1 });
    }
    if let Some(secs) = args.bench {
        let passed = run_bench(secs).await?;
        std::process::exit(if passed { 0 } else { 1 });
    }
    if args.load_select {
        let passed = run_load_select().await?;
        std::process::exit(if passed { 0 } else { 1 });
    }
    if !args.resize.is_empty() {
        let passed =
            run_resize(&args.resize, args.resize_before_load, args.save.as_deref()).await?;
        std::process::exit(if passed { 0 } else { 1 });
    }
    let save_path = args.save;
    let mut client = BridgeClient::connect(endpoint()).await?;

    let hello = client
        .hello(HelloRequest {
            editor_version: "bridge_probe".into(),
        })
        .await?
        .into_inner();
    println!(
        "hello: engine={} viewport={}x{}",
        hello.engine_version, hello.viewport_width, hello.viewport_height
    );

    let ack = client
        .load_stage(LoadStageRequest {
            entities: vec![
                entity(
                    "/Level",
                    "",
                    PrimitiveKind::PrimitiveNone,
                    [0.0, 0.0, 0.0],
                    [0.0; 3],
                ),
                entity(
                    "/Level/Crate",
                    "/Level",
                    PrimitiveKind::PrimitiveCube,
                    [0.0, 0.0, 0.0],
                    [0.7, 0.4, 0.2],
                ),
                entity(
                    "/Level/Ball",
                    "/Level",
                    PrimitiveKind::PrimitiveSphere,
                    [0.0, 2.0, 0.0],
                    [0.2, 0.5, 0.9],
                ),
                entity(
                    "/Level/Floor",
                    "/Level",
                    PrimitiveKind::PrimitivePlane,
                    [0.0, -1.0, 0.0],
                    [0.3, 0.3, 0.3],
                ),
            ],
        })
        .await?
        .into_inner();
    println!("load_stage: ok={} {}", ack.ok, ack.message);

    let mut frames = client
        .stream_frames(StreamFramesRequest {
            max_fps: 10,
            ..Default::default()
        })
        .await?
        .into_inner();

    let mut frame = next_frame(&mut frames, FRAME_INDEX).await?;
    if Stats::of(&frame).is_flat() {
        let started = tokio::time::Instant::now();
        frame = next_frame_matching(&mut frames, |f| !Stats::of(f).is_flat()).await?;
        println!(
            "scene was still flat at frame {FRAME_INDEX}; first non-flat frame after {:.1}s",
            started.elapsed().as_secs_f32()
        );
    }
    report(
        "initial",
        &frame,
        save_path
            .as_deref()
            .map(|p| suffixed(p, "_initial"))
            .as_deref(),
    );

    // ApplyDelta: recolour the crate to pure red and move it.
    let ack = client
        .apply_delta(ApplyDeltaRequest {
            deltas: vec![EntityDelta {
                path: "/Level/Crate".into(),
                local: Some(transform([1.5, 0.5, 0.0])),
                base_color: Some(rgba([1.0, 0.0, 0.0])),
                removed: false,
            }],
            ..Default::default()
        })
        .await?
        .into_inner();
    println!("apply_delta: ok={} {}", ack.ok, ack.message);
    // Wait for a frame that shows the recoloured crate (the change is not instant).
    let after_delta = next_frame_matching(&mut frames, |f| {
        count_pixels(f, |r, g, b| r > 180 && g < 90 && b < 90) > 0
    })
    .await?;
    report(
        "after apply_delta",
        &after_delta,
        save_path
            .as_deref()
            .map(|p| suffixed(p, "_delta"))
            .as_deref(),
    );
    let red = count_pixels(&after_delta, |r, g, b| r > 180 && g < 90 && b < 90);
    println!("  reddish pixels (r>180,g<90,b<90): {red}");

    // SetViewport: shrink the target to 640x360.
    let ack = client
        .set_viewport(SetViewportRequest {
            width: 640,
            height: 360,
        })
        .await?
        .into_inner();
    println!("set_viewport: ok={} {}", ack.ok, ack.message);
    let resized = next_frame_matching(&mut frames, |f| f.width == 640 && f.height == 360).await?;
    report(
        "after set_viewport",
        &resized,
        save_path
            .as_deref()
            .map(|p| suffixed(p, "_viewport"))
            .as_deref(),
    );

    Ok(())
}

/// Command-line options.
struct Args {
    /// `--save <path>`: write PNGs of the sampled frames.
    save: Option<PathBuf>,
    /// `--interact <stem>`: run the picking/drag/selection checks instead.
    interact: Option<PathBuf>,
    /// `--resize WxH` (repeatable): viewport sizes to exercise in order.
    resize: Vec<(u32, u32)>,
    /// `--resize-before-load`: apply the first `--resize` size before LoadStage.
    resize_before_load: bool,
    /// `--load-select`: LoadStage then SetSelection with no pause, 20 times.
    load_select: bool,
    /// `--shm`: ring pixels vs inline pixels, and the torn-read stress.
    shm: bool,
    /// `--bench <secs>`: inline vs ring frames, throughput and edit latency.
    bench: Option<u64>,
}

/// Parses `--save <path>` and `--interact <stem>`.
fn parse_args() -> Result<Args, Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mut parsed = Args {
        save: None,
        interact: None,
        resize: Vec::new(),
        resize_before_load: false,
        load_select: false,
        shm: false,
        bench: None,
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--save" => {
                parsed.save = Some(PathBuf::from(args.next().ok_or("--save needs a path")?))
            }
            "--interact" => {
                parsed.interact = Some(PathBuf::from(args.next().ok_or("--interact needs a stem")?))
            }
            "--resize" => {
                let spec = args.next().ok_or("--resize needs WxH")?;
                let (w, h) = spec.split_once('x').ok_or("--resize expects WxH")?;
                parsed.resize.push((w.parse()?, h.parse()?));
            }
            "--resize-before-load" => parsed.resize_before_load = true,
            "--load-select" => parsed.load_select = true,
            "--shm" => parsed.shm = true,
            "--bench" => {
                let secs = args.next().ok_or("--bench needs seconds")?;
                parsed.bench = Some(secs.parse()?);
            }
            other => return Err(format!("unknown argument {other:?}").into()),
        }
    }
    Ok(parsed)
}

/// Waits for the `n`th frame (1-based) with a timeout.
async fn next_frame(frames: &mut FrameStream, n: usize) -> Result<Frame, Box<dyn Error>> {
    let mut seen = 0;
    next_frame_matching(frames, |_| {
        seen += 1;
        seen >= n
    })
    .await
}

/// Waits for the first frame accepted by `accept`.
async fn next_frame_matching(
    frames: &mut FrameStream,
    mut accept: impl FnMut(&Frame) -> bool,
) -> Result<Frame, Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + FRAME_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, frames.next()).await {
            Ok(Some(Ok(frame))) => {
                if accept(&frame) {
                    return Ok(frame);
                }
            }
            Ok(Some(Err(status))) => return Err(format!("frame stream error: {status}").into()),
            Ok(None) => return Err("frame stream ended".into()),
            Err(_) => return Err("timed out waiting for a frame".into()),
        }
    }
}

fn report(label: &str, frame: &Frame, save: Option<&std::path::Path>) {
    let stats = Stats::of(frame);
    println!(
        "frame [{label}] seq={} {}x{} bytes={}",
        frame.sequence,
        frame.width,
        frame.height,
        frame.rgba8.len()
    );
    println!("  distinct RGB values: {}", stats.distinct);
    println!(
        "  pixels not near-black (max channel > 16): {:.2}%",
        stats.not_near_black * 100.0
    );
    println!(
        "  pixels differing from the clear colour {CLEAR_RGB:?} (+-{CLEAR_TOLERANCE}): {:.2}%",
        stats.not_clear * 100.0
    );
    if stats.is_flat() {
        println!("  WARNING: frame is a single flat colour, the scene is not rendering");
    }
    if let Some(path) = save {
        match write_png(path, frame) {
            Ok(()) => println!("  saved {}", path.display()),
            Err(err) => println!("  failed to save {}: {err}", path.display()),
        }
    }
}

struct Stats {
    distinct: usize,
    not_near_black: f64,
    not_clear: f64,
}

impl Stats {
    fn of(frame: &Frame) -> Self {
        let pixels = frame.rgba8.len() / 4;
        let mut histogram: HashMap<[u8; 3], usize> = HashMap::new();
        let mut near_black = 0usize;
        let mut clear = 0usize;
        for px in frame.rgba8.as_chunks::<4>().0 {
            let rgb = [px[0], px[1], px[2]];
            *histogram.entry(rgb).or_default() += 1;
            if rgb.iter().max().copied().unwrap_or(0) <= 16 {
                near_black += 1;
            }
            if near_clear(rgb) {
                clear += 1;
            }
        }
        let total = pixels.max(1) as f64;
        Self {
            distinct: histogram.len(),
            not_near_black: 1.0 - near_black as f64 / total,
            not_clear: 1.0 - clear as f64 / total,
        }
    }
}

fn count_pixels(frame: &Frame, pred: impl Fn(u8, u8, u8) -> bool) -> usize {
    frame
        .rgba8
        .as_chunks::<4>()
        .0
        .iter()
        .filter(|px| pred(px[0], px[1], px[2]))
        .count()
}

/// Writes the RGBA8 frame as a PNG.
fn write_png(path: &std::path::Path, frame: &Frame) -> Result<(), Box<dyn Error>> {
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)?;
    }
    let file = std::io::BufWriter::new(std::fs::File::create(path)?);
    let mut encoder = png::Encoder::new(file, frame.width, frame.height);
    encoder.set_color(png::ColorType::Rgba);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder.write_header()?;
    writer.write_image_data(&frame.rgba8)?;
    writer.finish()?;
    Ok(())
}

fn rgba(rgb: [f32; 3]) -> Color {
    Color {
        r: rgb[0],
        g: rgb[1],
        b: rgb[2],
        a: 1.0,
    }
}

fn transform(translation: [f32; 3]) -> Transform {
    Transform {
        translation: Some(Vec3 {
            x: translation[0],
            y: translation[1],
            z: translation[2],
        }),
        rotation: Some(Quat {
            x: 0.0,
            y: 0.0,
            z: 0.0,
            w: 1.0,
        }),
        scale: Some(Vec3 {
            x: 1.0,
            y: 1.0,
            z: 1.0,
        }),
    }
}

fn entity(
    path: &str,
    parent: &str,
    primitive: PrimitiveKind,
    translation: [f32; 3],
    color: [f32; 3],
) -> FlatEntity {
    FlatEntity {
        path: path.into(),
        parent_path: parent.into(),
        type_name: "Xform".into(),
        local: Some(transform(translation)),
        primitive: primitive as i32,
        base_color: Some(rgba(color)),
        light: 0,
        is_camera: false,
    }
}

/// `dir/name.png` + `_tag` -> `dir/name_tag.png`.
fn suffixed(path: &std::path::Path, tag: &str) -> PathBuf {
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("frame");
    path.with_file_name(format!("{stem}{tag}.png"))
}

/// True when every channel is within [`CLEAR_TOLERANCE`] of the clear colour.
/// Tone mapping adds a few levels of noise to the clear colour, so exact
/// equality would miss it.
fn near_clear(rgb: [u8; 3]) -> bool {
    rgb.iter()
        .zip(CLEAR_RGB)
        .all(|(&value, clear)| value.abs_diff(clear) <= CLEAR_TOLERANCE)
}

impl Stats {
    /// A frame that is (almost) all clear colour: the scene is not drawn.
    fn is_flat(&self) -> bool {
        self.not_clear < 0.001
    }
}

// ---- --interact: picking, dragging and selection ----------------------------

/// Green test cube at the origin, found in frames by its colour.
const CUBE_PATH: &str = "/Level/Cube";
const CUBE_RGB: [f32; 3] = [0.2, 0.8, 0.3];
const BALL_PATH: &str = "/Level/Ball";
/// Drag distance in viewport pixels, sent in equal steps.
const DRAG_PIXELS: f32 = 80.0;
const DRAG_STEPS: u32 = 4;
/// Pause between drag steps; longer than the 50 ms event throttle.
const STEP_PAUSE: Duration = Duration::from_millis(60);
const WAIT: Duration = Duration::from_secs(15);

type Client = BridgeClient<tonic::transport::Channel>;

/// Runs the interaction checks and saves PNGs as `<stem>_before/_selected/_moved.png`.
/// Returns whether every check passed.
async fn run_interact(stem: &std::path::Path) -> Result<bool, Box<dyn Error>> {
    let mut client = Client::connect(endpoint()).await?;
    let hello = client
        .hello(HelloRequest {
            editor_version: "bridge_probe --interact".into(),
        })
        .await?
        .into_inner();
    let (cx, cy) = (
        hello.viewport_width as f32 / 2.0,
        hello.viewport_height as f32 / 2.0,
    );
    println!(
        "hello: engine={} viewport={}x{} centre=({cx}, {cy})",
        hello.engine_version, hello.viewport_width, hello.viewport_height
    );

    // Subscribe before any input so no engine event can be missed.
    let mut subscription = client
        .stream_events(StreamEventsRequest {})
        .await?
        .into_inner();
    let (event_tx, mut events) = tokio::sync::mpsc::unbounded_channel::<EngineEvent>();
    tokio::spawn(async move {
        while let Ok(Some(event)) = subscription.message().await {
            if event_tx.send(event).is_err() {
                break;
            }
        }
    });

    let ack = client
        .load_stage(LoadStageRequest {
            entities: vec![
                entity(
                    "/Level",
                    "",
                    PrimitiveKind::PrimitiveNone,
                    [0.0; 3],
                    [0.0; 3],
                ),
                entity(
                    CUBE_PATH,
                    "/Level",
                    PrimitiveKind::PrimitiveCube,
                    [0.0; 3],
                    CUBE_RGB,
                ),
                entity(
                    BALL_PATH,
                    "/Level",
                    PrimitiveKind::PrimitiveSphere,
                    [-2.2, 0.0, 0.0],
                    [0.2, 0.5, 0.9],
                ),
            ],
        })
        .await?
        .into_inner();
    let mut checks = Checks::default();
    checks.check("load_stage accepted", ack.ok, &ack.message);

    let mut frames = client
        .stream_frames(StreamFramesRequest {
            max_fps: 10,
            ..Default::default()
        })
        .await?
        .into_inner();

    // Warm-up: the first frames can stay at the clear colour while pipelines compile.
    let started = tokio::time::Instant::now();
    let before = wait_frame(&mut frames, WAIT, |f| blob(f, is_green).count > 200)
        .await
        .ok_or("cube never became visible")?;
    println!("cube visible after {:.1}s", started.elapsed().as_secs_f32());
    save(stem, "_before", &before)?;
    let cube_before = blob(&before, is_green);
    let centre_offset = bevy::math::ops::hypot(cube_before.cx - cx, cube_before.cy - cy);
    checks.check(
        "cube is near the viewport centre",
        centre_offset < 0.15 * hello.viewport_width as f32,
        format!(
            "centroid ({:.0}, {:.0}), {centre_offset:.0} px from centre, {} px",
            cube_before.cx, cube_before.cy, cube_before.count
        ),
    );
    let outline_before = blob(&before, is_outline).count;

    // Pick: a click at the viewport centre should hit the cube.
    send_inputs(
        &client,
        vec![
            (mouse_move(cx, cy), Duration::ZERO),
            (mouse_button(true, cx, cy), Duration::ZERO),
            (mouse_button(false, cx, cy), Duration::ZERO),
        ],
    )
    .await?;
    let picked = match wait_event(&mut events, |e| {
        matches!(e.kind, Some(EventKind::Picked(_)))
    })
    .await
    {
        Some(EngineEvent {
            kind: Some(EventKind::Picked(pick)),
        }) => pick.path,
        _ => "<no pick event>".to_owned(),
    };
    checks.check(
        "click picks the cube",
        picked == CUBE_PATH,
        format!("picked {picked:?}"),
    );

    // Selection: unknown paths are rejected, a known path draws an outline.
    let rejected = client
        .set_selection(SetSelectionRequest {
            path: "/Level/Missing".into(),
        })
        .await?
        .into_inner();
    checks.check(
        "unknown selection is rejected",
        !rejected.ok,
        &rejected.message,
    );

    let accepted = client
        .set_selection(SetSelectionRequest {
            path: CUBE_PATH.into(),
        })
        .await?
        .into_inner();
    checks.check("selection accepted", accepted.ok, &accepted.message);
    let selected = wait_frame(&mut frames, WAIT, |f| {
        blob(f, is_outline).count > outline_before + 50
    })
    .await;
    match &selected {
        Some(frame) => {
            save(stem, "_selected", frame)?;
            checks.check(
                "selection outline is visible",
                true,
                format!(
                    "outline pixels {outline_before} -> {}",
                    blob(frame, is_outline).count
                ),
            );
        }
        None => checks.check(
            "selection outline is visible",
            false,
            "no frame with an outline",
        ),
    }
    let baseline = selected.unwrap_or(before);
    let baseline_blob = blob(&baseline, is_green);

    // Drag: press on the cube, move +80 px in four steps, release.
    let mut drag_steps = vec![
        (mouse_move(cx, cy), Duration::ZERO),
        (mouse_button(true, cx, cy), STEP_PAUSE),
    ];
    for step in 1..=DRAG_STEPS {
        let x = cx + DRAG_PIXELS * step as f32 / DRAG_STEPS as f32;
        drag_steps.push((mouse_move(x, cy), STEP_PAUSE));
    }
    drag_steps.push((mouse_button(false, cx + DRAG_PIXELS, cy), Duration::ZERO));
    send_inputs(&client, drag_steps).await?;

    let transforms = collect_transform_events(&mut events).await;
    let interim = transforms.iter().filter(|t| !t.r#final).count();
    checks.check(
        "drag emits transform events",
        !transforms.is_empty(),
        format!("{} events ({interim} interim)", transforms.len()),
    );
    let xs: Vec<f32> = transforms.iter().map(translation_x).collect();
    checks.check(
        "interim events are throttled but present",
        interim >= 2,
        format!("{interim} interim events, x = {xs:?}"),
    );
    let last = transforms.last();
    checks.check(
        "last transform event is final for the cube",
        last.is_some_and(|t| t.r#final && t.path == CUBE_PATH),
        format!("last final={:?}", last.map(|t| t.r#final)),
    );
    let final_x = last.map_or(0.0, translation_x);
    checks.check(
        "dragged translation.x grew",
        final_x > 0.05 && xs.windows(2).all(|w| w[1] >= w[0] - 1e-4),
        format!("final translation.x = {final_x:.3} (start 0.0)"),
    );

    let moved = wait_frame(&mut frames, WAIT, |f| {
        let b = blob(f, is_green);
        b.count > 100 && b.cx > baseline_blob.cx + 5.0
    })
    .await;
    match &moved {
        Some(frame) => {
            save(stem, "_moved", frame)?;
            let after = blob(frame, is_green);
            checks.check(
                "cube moved on screen",
                true,
                format!("centroid x {:.0} -> {:.0}", baseline_blob.cx, after.cx),
            );
        }
        None => checks.check(
            "cube moved on screen",
            false,
            "no frame with the cube shifted right",
        ),
    }

    let passed = checks.failed == 0;
    println!(
        "RESULT: {} ({} of {} checks passed)",
        if passed { "PASS" } else { "FAIL" },
        checks.total - checks.failed,
        checks.total
    );
    Ok(passed)
}

#[derive(Default)]
struct Checks {
    total: usize,
    failed: usize,
}

impl Checks {
    fn check(&mut self, name: &str, ok: bool, detail: impl std::fmt::Display) {
        self.total += 1;
        if !ok {
            self.failed += 1;
        }
        println!("{} {name}: {detail}", if ok { "PASS" } else { "FAIL" });
    }
}

fn translation_x(transform: &adv_bridge::proto::TransformChanged) -> f32 {
    transform
        .local
        .as_ref()
        .and_then(|local| local.translation.as_ref())
        .map_or(0.0, |v| v.x)
}

fn mouse_move(x: f32, y: f32) -> InputEvent {
    InputEvent {
        kind: Some(InputKind::MouseMove(MouseMove { x, y })),
    }
}

fn mouse_button(pressed: bool, x: f32, y: f32) -> InputEvent {
    InputEvent {
        kind: Some(InputKind::MouseButton(MouseButton {
            button: 0,
            pressed,
            x,
            y,
        })),
    }
}

/// Streams input events to the engine, pausing after each one, and waits for the ack.
async fn send_inputs(
    client: &Client,
    steps: Vec<(InputEvent, Duration)>,
) -> Result<(), Box<dyn Error>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<InputEvent>(16);
    let mut call_client = client.clone();
    let call = tokio::spawn(async move { call_client.send_input(ReceiverStream::new(rx)).await });
    for (event, pause) in steps {
        tx.send(event)
            .await
            .map_err(|_| "input stream closed early")?;
        tokio::time::sleep(pause).await;
    }
    drop(tx);
    let ack = call.await??.into_inner();
    if !ack.ok {
        return Err(ack.message.into());
    }
    Ok(())
}

/// Next engine event matching `accept`, or `None` after the wait.
async fn wait_event(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<EngineEvent>,
    accept: impl Fn(&EngineEvent) -> bool,
) -> Option<EngineEvent> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, events.recv()).await {
            Ok(Some(event)) if accept(&event) => return Some(event),
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => return None,
        }
    }
}

/// Transform events up to and including the final one of a drag.
async fn collect_transform_events(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<EngineEvent>,
) -> Vec<adv_bridge::proto::TransformChanged> {
    let mut collected = Vec::new();
    while let Some(event) = wait_event(events, |e| {
        matches!(e.kind, Some(EventKind::TransformChanged(_)))
    })
    .await
    {
        if let Some(EventKind::TransformChanged(transform)) = event.kind {
            let is_final = transform.r#final;
            collected.push(transform);
            if is_final {
                break;
            }
        }
    }
    collected
}

/// Next frame accepted by `accept` within `timeout`, or `None`.
async fn wait_frame(
    frames: &mut FrameStream,
    timeout: Duration,
    mut accept: impl FnMut(&Frame) -> bool,
) -> Option<Frame> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, frames.next()).await {
            Ok(Some(Ok(frame))) => {
                if accept(&frame) {
                    return Some(frame);
                }
            }
            Ok(Some(Err(_))) | Ok(None) | Err(_) => return None,
        }
    }
}

/// Count and centroid of the pixels matching `pred`.
struct Blob {
    count: usize,
    cx: f32,
    cy: f32,
}

fn blob(frame: &Frame, pred: impl Fn(u8, u8, u8) -> bool) -> Blob {
    let (mut count, mut sum_x, mut sum_y) = (0usize, 0f64, 0f64);
    let width = frame.width as usize;
    for (index, px) in frame.rgba8.as_chunks::<4>().0.iter().enumerate() {
        if pred(px[0], px[1], px[2]) {
            count += 1;
            sum_x += (index % width) as f64;
            sum_y += (index / width) as f64;
        }
    }
    let divisor = count.max(1) as f64;
    Blob {
        count,
        cx: (sum_x / divisor) as f32,
        cy: (sum_y / divisor) as f32,
    }
}

/// The test cube's colour, lit or in shadow (green clearly dominates).
fn is_green(r: u8, g: u8, b: u8) -> bool {
    let (r, g, b) = (r as i32, g as i32, b as i32);
    g >= 60 && g - r >= 40 && g - b >= 40
}

/// The selection outline colour (sRGB 255, 217, 51), not shaded.
fn is_outline(r: u8, g: u8, b: u8) -> bool {
    r > 200 && g > 150 && b < 120
}

fn save(stem: &std::path::Path, tag: &str, frame: &Frame) -> Result<(), Box<dyn Error>> {
    let path = suffixed(stem, tag);
    write_png(&path, frame)?;
    println!("  saved {}", path.display());
    Ok(())
}

// ---- --resize: viewport size changes must produce real frames of that size --

/// Demo scene used by the resize cases (same entities as the default flow).
fn demo_scene() -> Vec<FlatEntity> {
    vec![
        entity(
            "/Level",
            "",
            PrimitiveKind::PrimitiveNone,
            [0.0; 3],
            [0.0; 3],
        ),
        entity(
            "/Level/Crate",
            "/Level",
            PrimitiveKind::PrimitiveCube,
            [0.0; 3],
            [0.7, 0.4, 0.2],
        ),
        entity(
            "/Level/Ball",
            "/Level",
            PrimitiveKind::PrimitiveSphere,
            [0.0, 2.0, 0.0],
            [0.2, 0.5, 0.9],
        ),
        entity(
            "/Level/Floor",
            "/Level",
            PrimitiveKind::PrimitivePlane,
            [0.0, -1.0, 0.0],
            [0.3, 0.3, 0.3],
        ),
    ]
}

/// Applies each viewport size in order, waits for a real frame of exactly that
/// size, checks `Hello` reports the applied size, and finally shrinks back to
/// 960x540. Returns whether every case passed.
async fn run_resize(
    cases: &[(u32, u32)],
    before_load: bool,
    save: Option<&std::path::Path>,
) -> Result<bool, Box<dyn Error>> {
    let mut client = Client::connect(endpoint()).await?;
    let hello = client
        .hello(HelloRequest {
            editor_version: "bridge_probe --resize".into(),
        })
        .await?
        .into_inner();
    println!(
        "hello: viewport={}x{}",
        hello.viewport_width, hello.viewport_height
    );

    if before_load && let Some(&(width, height)) = cases.first() {
        let ack = client
            .set_viewport(SetViewportRequest { width, height })
            .await?
            .into_inner();
        println!(
            "pre-load set_viewport {width}x{height}: ok={} {}",
            ack.ok, ack.message
        );
    }
    let ack = client
        .load_stage(LoadStageRequest {
            entities: demo_scene(),
        })
        .await?
        .into_inner();
    println!("load_stage: ok={} {}", ack.ok, ack.message);

    let mut frames = client
        .stream_frames(StreamFramesRequest {
            max_fps: 10,
            ..Default::default()
        })
        .await?
        .into_inner();
    let started = tokio::time::Instant::now();
    wait_frame(&mut frames, WAIT, |f| !Stats::of(f).is_flat())
        .await
        .ok_or("scene never rendered before the resize cases")?;
    println!(
        "scene rendered after {:.1}s",
        started.elapsed().as_secs_f32()
    );

    let mut all_passed = true;
    let mut sequence: Vec<(u32, u32)> = cases.to_vec();
    sequence.push((960, 540));
    for (width, height) in sequence {
        let ack = client
            .set_viewport(SetViewportRequest { width, height })
            .await?
            .into_inner();
        println!(
            "--- set_viewport {width}x{height}: ok={} {}",
            ack.ok, ack.message
        );
        if !ack.ok {
            all_passed = false;
            continue;
        }

        let mut seen: Vec<(u32, u32, usize)> = Vec::new();
        let outcome = wait_frame(&mut frames, Duration::from_secs(30), |f| {
            let key = (f.width, f.height, f.rgba8.len());
            if seen.last() != Some(&key) {
                seen.push(key);
            }
            f.width == width
                && f.height == height
                && f.rgba8.len() == (width * height * 4) as usize
                && !Stats::of(f).is_flat()
        })
        .await;
        let reported = client
            .hello(HelloRequest {
                editor_version: "bridge_probe --resize".into(),
            })
            .await?
            .into_inner();
        match outcome {
            Some(frame) => {
                let hello_ok =
                    reported.viewport_width == width && reported.viewport_height == height;
                let passed = hello_ok;
                all_passed &= passed;
                println!(
                    "{} {width}x{height}: frame seq={} {}x{} bytes={}, hello reports {}x{}",
                    if passed { "PASS" } else { "FAIL" },
                    frame.sequence,
                    frame.width,
                    frame.height,
                    frame.rgba8.len(),
                    reported.viewport_width,
                    reported.viewport_height
                );
                if let Some(path) = save {
                    save_frame(path, &format!("_{width}x{height}"), &frame)?;
                }
            }
            None => {
                all_passed = false;
                println!("FAIL {width}x{height}: no real frame of this size within 30 s");
            }
        }
        let distinct: Vec<String> = seen
            .iter()
            .map(|(w, h, len)| format!("{w}x{h}({len}B)"))
            .collect();
        println!("  frame sizes seen while waiting: {distinct:?}");
    }

    println!("RESULT: {}", if all_passed { "PASS" } else { "FAIL" });
    Ok(all_passed)
}

fn save_frame(stem: &std::path::Path, tag: &str, frame: &Frame) -> Result<(), Box<dyn Error>> {
    let path = suffixed(stem, tag);
    write_png(&path, frame)?;
    println!("  saved {}", path.display());
    Ok(())
}

// ---- --load-select: a path that exists after the LoadStage ack must be selectable

/// Stage sizes swept by `--load-select`.
const LOAD_SELECT_ROUNDS: usize = 20;

/// Runs `LoadStage` then `SetSelection` immediately (no pause), 20 times with a
/// different stage size each round, then checks an unknown path is still rejected.
/// Returns whether every check passed.
async fn run_load_select() -> Result<bool, Box<dyn Error>> {
    let mut client = Client::connect(endpoint()).await?;
    let mut round_failures = 0usize;
    let mut failures = 0usize;
    for round in 0..LOAD_SELECT_ROUNDS {
        // 2 + round entities: the root, the crate, and `round` extra spheres.
        let mut entities = vec![
            entity(
                "/Demo",
                "",
                PrimitiveKind::PrimitiveNone,
                [0.0; 3],
                [0.0; 3],
            ),
            entity(
                "/Demo/Crate",
                "/Demo",
                PrimitiveKind::PrimitiveCube,
                [0.0; 3],
                [0.7, 0.4, 0.2],
            ),
        ];
        for extra in 0..round {
            let x = extra as f32 * 1.5 - 2.0;
            entities.push(entity(
                &format!("/Demo/Extra{extra}"),
                "/Demo",
                PrimitiveKind::PrimitiveSphere,
                [x, 1.5, 0.0],
                [0.2, 0.5, 0.9],
            ));
        }
        let entity_count = entities.len();
        let load = client
            .load_stage(LoadStageRequest { entities })
            .await?
            .into_inner();
        let select = client
            .set_selection(SetSelectionRequest {
                path: "/Demo/Crate".into(),
            })
            .await?
            .into_inner();
        let ok = load.ok && select.ok;
        if !ok {
            round_failures += 1;
            failures += 1;
        }
        println!(
            "{} round {:>2}: {entity_count:>2} entities, load ok={} {}, select ok={} {}",
            if ok { "PASS" } else { "FAIL" },
            round + 1,
            load.ok,
            load.message,
            select.ok,
            select.message
        );
    }

    let unknown = client
        .set_selection(SetSelectionRequest {
            path: "/Demo/Missing".into(),
        })
        .await?
        .into_inner();
    let rejected = !unknown.ok;
    if !rejected {
        failures += 1;
    }
    println!(
        "{} unknown path after loads is rejected: ok={} {}",
        if rejected { "PASS" } else { "FAIL" },
        unknown.ok,
        unknown.message
    );

    let passed = failures == 0;
    println!(
        "RESULT: {} ({} of {} load+select rounds ok)",
        if passed { "PASS" } else { "FAIL" },
        LOAD_SELECT_ROUNDS - round_failures,
        LOAD_SELECT_ROUNDS
    );
    Ok(passed)
}

/// Bridge endpoint: `ADV_PROBE_ADDR` (`host:port`), default `127.0.0.1:50051`.
fn endpoint() -> String {
    let addr = std::env::var("ADV_PROBE_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".into());
    format!("http://{addr}")
}

// ---- --shm: ring pixels match inline pixels, and torn reads are detected -----

/// Ring layout constants, mirrored from proto/adventure.proto ("Frame ring").
const RING_HEADER_BYTES: usize = 64;
const RING_SLOT_HEADER_BYTES: usize = 32;
const RING_LATEST_OFFSET: usize = 24;
/// Frame rate of the torn-read stress.
const STRESS_FPS: u32 = 60;
const STRESS_SECS: u64 = 5;

/// Read-only view of the engine's frame ring, read with the seqlock protocol.
struct RingView {
    map: memmap2::Mmap,
    slots: u32,
    slot_bytes: u64,
}

/// Outcome of reading one ring slot.
enum SlotRead {
    /// The copy is consistent: `seq_begin == seq_end == sequence` before and after.
    Ok(Vec<u8>),
    /// The slot was overwritten while it was being copied.
    Torn,
    /// The slot did not hold the requested sequence (already overwritten or not yet written).
    Stale,
}

impl RingView {
    fn open(hello: &adv_bridge::proto::HelloReply) -> Result<Self, Box<dyn Error>> {
        if hello.frame_ring_path.is_empty() {
            return Err("engine reports no frame ring".into());
        }
        let file = std::fs::File::open(&hello.frame_ring_path)?;
        // SAFETY: the engine owns this file and only ever writes it through the seqlock;
        // the probe maps it read-only and validates every copy.
        let map = unsafe { memmap2::Mmap::map(&file)? };
        Ok(Self {
            map,
            slots: hello.frame_ring_slots,
            slot_bytes: hello.frame_ring_slot_bytes,
        })
    }

    fn slot_offset(&self, sequence: u64) -> usize {
        let slot = (sequence % u64::from(self.slots)) as usize;
        RING_HEADER_BYTES + slot * (RING_SLOT_HEADER_BYTES + self.slot_bytes as usize)
    }

    /// Atomic view of the 8 bytes at `offset`. The engine lays these out 8-aligned.
    fn atomic(&self, offset: usize) -> &AtomicU64 {
        debug_assert_eq!(offset % 8, 0);
        // SAFETY: offset is inside the mapping and 8-aligned (page-aligned base, slot
        // capacities are multiples of 64), and AtomicU64 has the layout of u64.
        unsafe { &*self.map.as_ptr().add(offset).cast::<AtomicU64>() }
    }

    /// The sequence of the most recently completed frame (0 = none yet).
    fn latest(&self) -> u64 {
        self.atomic(RING_LATEST_OFFSET).load(Ordering::Acquire)
    }

    /// Reads frame `sequence` (`width` x `height`): check the header, copy the
    /// payload, fence, check the header again.
    fn read(&self, sequence: u64, width: u32, height: u32) -> SlotRead {
        let len = u64::from(width) * u64::from(height) * 4;
        if len > self.slot_bytes {
            return SlotRead::Stale;
        }
        let base = self.slot_offset(sequence);
        if self.atomic(base).load(Ordering::Acquire) != sequence
            || self.atomic(base + 8).load(Ordering::Acquire) != sequence
        {
            return SlotRead::Stale;
        }
        let mut pixels = vec![0_u8; len as usize];
        // SAFETY: base + 32 + len lies inside the slot, which lies inside the mapping
        // (checked by `len <= slot_bytes`). Racing writes are possible by design and
        // are caught by the header re-check below, the usual seqlock reader pattern.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.map.as_ptr().add(base + RING_SLOT_HEADER_BYTES),
                pixels.as_mut_ptr(),
                pixels.len(),
            );
        }
        std::sync::atomic::fence(Ordering::Acquire);
        let begin = self.atomic(base).load(Ordering::Relaxed);
        let end = self.atomic(base + 8).load(Ordering::Relaxed);
        if begin == sequence && end == sequence {
            SlotRead::Ok(pixels)
        } else {
            SlotRead::Torn
        }
    }
}

/// Which slot the torn-read stress reads.
#[derive(Clone, Copy)]
enum StressTarget {
    /// The newest completed frame: the writer never reuses it during the read.
    Latest,
    /// The oldest frame still in the ring: the writer overwrites it on its next frame.
    Oldest,
}

/// Reads the ring at `STRESS_FPS` for `secs` seconds. Returns (ok, torn, stale).
async fn stress_ring(
    ring: &RingView,
    width: u32,
    height: u32,
    target: StressTarget,
    secs: u64,
) -> (u64, u64, u64) {
    let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / f64::from(STRESS_FPS)));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    let (mut ok, mut torn, mut stale) = (0_u64, 0_u64, 0_u64);
    while tokio::time::Instant::now() < deadline {
        tick.tick().await;
        let latest = ring.latest();
        if latest == 0 {
            continue;
        }
        let sequence = match target {
            StressTarget::Latest => latest,
            StressTarget::Oldest => latest.saturating_sub(u64::from(ring.slots) - 1).max(1),
        };
        match ring.read(sequence, width, height) {
            SlotRead::Ok(_) => ok += 1,
            SlotRead::Torn => torn += 1,
            SlotRead::Stale => stale += 1,
        }
    }
    (ok, torn, stale)
}

/// Checks ring pixels against inline pixels for the same static scene, then stresses the ring.
async fn run_shm() -> Result<bool, Box<dyn Error>> {
    let mut client = Client::connect(endpoint()).await?;
    let hello = client
        .hello(HelloRequest {
            editor_version: "bridge_probe --shm".into(),
        })
        .await?
        .into_inner();
    println!(
        "hello: ring={:?} slots={} slot_bytes={}",
        hello.frame_ring_path, hello.frame_ring_slots, hello.frame_ring_slot_bytes
    );
    let ring = RingView::open(&hello)?;

    let ack = client
        .load_stage(LoadStageRequest {
            entities: demo_scene(),
        })
        .await?
        .into_inner();
    println!("load_stage: ok={} {}", ack.ok, ack.message);

    let mut inline = client
        .stream_frames(StreamFramesRequest {
            max_fps: 10,
            ..Default::default()
        })
        .await?
        .into_inner();
    let mut shared = client
        .stream_frames(StreamFramesRequest {
            max_fps: 10,
            prefer_shared_memory: true,
            ..Default::default()
        })
        .await?
        .into_inner();

    // The scene keeps changing for the first seconds after LoadStage (pipelines and
    // lighting settle), so compare only once two consecutive ring frames are identical.
    let (width, height) = (hello.viewport_width, hello.viewport_height);
    let mut previous: Option<Vec<u8>> = None;
    let mut settled_at = None;
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let latest = ring.latest();
        if latest == 0 {
            continue;
        }
        if let SlotRead::Ok(pixels) = ring.read(latest, width, height) {
            let frame = adv_bridge::proto::Frame {
                rgba8: pixels.clone(),
                ..Default::default()
            };
            if previous.as_ref() == Some(&pixels) && !Stats::of(&frame).is_flat() {
                settled_at = Some(latest);
                break;
            }
            previous = Some(pixels);
        }
    }
    println!(
        "scene settled: {}",
        settled_at.map_or("not within 20 s".to_owned(), |seq| format!(
            "identical frames from seq {seq}"
        ))
    );

    let inline_frame = wait_frame(&mut inline, WAIT, |f| {
        !Stats::of(f).is_flat() && settled_at.is_none_or(|seq| f.sequence > seq)
    })
    .await
    .ok_or("inline frames never rendered the scene")?;
    // Read each slot as soon as its frame arrives: the engine reuses a slot after
    // `slots` frames, so a late read is stale. An editor drops such frames, and so
    // does this check (counted as attempts).
    let mut attempts = 0_u64;
    let mut captured: Option<Vec<u8>> = None;
    let ring_frame = wait_frame(&mut shared, WAIT, |f| {
        if !f.in_ring || settled_at.is_some_and(|seq| f.sequence <= seq) {
            return false;
        }
        attempts += 1;
        match ring.read(f.sequence, f.width, f.height) {
            SlotRead::Ok(pixels) => {
                captured = Some(pixels);
                true
            }
            SlotRead::Torn | SlotRead::Stale => false,
        }
    })
    .await
    .ok_or("no in-ring frame could be read consistently")?;
    println!(
        "in-ring frames read before a consistent one: {}",
        attempts - 1
    );
    let (width, height) = (ring_frame.width, ring_frame.height);
    let mut checks = Checks::default();
    checks.check(
        "ring frame has no inline pixels",
        ring_frame.rgba8.is_empty(),
        format!("{} inline bytes", ring_frame.rgba8.len()),
    );

    let from_ring = match captured {
        Some(pixels) => SlotRead::Ok(pixels),
        None => SlotRead::Stale,
    };
    match from_ring {
        SlotRead::Ok(pixels) => {
            let differing = pixels
                .iter()
                .zip(&inline_frame.rgba8)
                .filter(|(a, b)| a != b)
                .count();
            if differing > 0 {
                let (w, h) = (width as usize, height as usize);
                let (mut x0, mut y0, mut x1, mut y1) = (w, h, 0_usize, 0_usize);
                for (index, (a, b)) in pixels
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .zip(inline_frame.rgba8.as_chunks::<4>().0)
                    .enumerate()
                {
                    if a != b {
                        let (x, y) = (index % w, index / w);
                        x0 = x0.min(x);
                        y0 = y0.min(y);
                        x1 = x1.max(x);
                        y1 = y1.max(y);
                    }
                }
                let later = wait_frame(&mut inline, WAIT, |f| {
                    f.sequence > inline_frame.sequence + 10
                })
                .await;
                let consecutive = later.map_or(usize::MAX, |later| {
                    later
                        .rgba8
                        .iter()
                        .zip(&inline_frame.rgba8)
                        .filter(|(a, b)| a != b)
                        .count()
                });
                println!(
                    "  differing pixels span x {x0}..={x1}, y {y0}..={y1}; inline seq {} vs a later inline frame: {consecutive} bytes differ",
                    inline_frame.sequence
                );
            }
            checks.check(
                "ring pixels are byte-identical to inline pixels",
                pixels.len() == inline_frame.rgba8.len() && differing == 0,
                format!(
                    "ring seq {} vs inline seq {}, {} bytes, {differing} differ",
                    ring_frame.sequence,
                    inline_frame.sequence,
                    pixels.len()
                ),
            );
        }
        SlotRead::Torn => checks.check(
            "ring slot read is consistent",
            false,
            "torn on an idle read",
        ),
        SlotRead::Stale => checks.check(
            "ring slot read is consistent",
            false,
            "slot no longer holds the frame",
        ),
    }

    let (ok, torn, stale) =
        stress_ring(&ring, width, height, StressTarget::Latest, STRESS_SECS).await;
    println!(
        "stress latest slot, {STRESS_SECS} s at {STRESS_FPS} fps: ok={ok} torn={torn} stale={stale}"
    );
    checks.check(
        "latest-slot reads are never torn",
        torn == 0,
        format!("torn={torn} of {}", ok + torn + stale),
    );
    let (ok, torn, stale) =
        stress_ring(&ring, width, height, StressTarget::Oldest, STRESS_SECS).await;
    println!(
        "stress oldest slot, {STRESS_SECS} s at {STRESS_FPS} fps: ok={ok} torn={torn} stale={stale} (torn/stale are detections, not failures)"
    );
    checks.check(
        "stress ran and read frames",
        ok + torn + stale > 0,
        format!("ok={ok} torn={torn} stale={stale}"),
    );

    let passed = checks.failed == 0;
    println!(
        "RESULT: {} ({} of {} checks passed)",
        if passed { "PASS" } else { "FAIL" },
        checks.total - checks.failed,
        checks.total
    );
    Ok(passed)
}

// ---- --bench: inline vs ring transport, with edit -> frame latency -----------

/// Interval between edits in `--bench`.
const BENCH_EDIT_INTERVAL: Duration = Duration::from_millis(250);

/// Measured numbers for one transport.
struct BenchResult {
    transport: &'static str,
    secs: f64,
    frames: u64,
    bytes: u64,
    torn: u64,
    latencies_ms: Vec<f64>,
    edits_sent: u64,
    engine_us_mean: f64,
    readback_us_mean: f64,
}

/// Runs the bench for both transports, one after the other.
async fn run_bench(secs: u64) -> Result<bool, Box<dyn Error>> {
    let mut results = Vec::new();
    for shared in [false, true] {
        results.push(bench_once(shared, secs).await?);
    }
    println!();
    println!(
        "{:<8} {:>8} {:>9} {:>6} {:>10} {:>10} {:>7} {:>6} {:>10} {:>12}",
        "transport",
        "frames/s",
        "MB/s",
        "torn",
        "lat mean",
        "lat p95",
        "lat n",
        "edits",
        "engine us",
        "readback us"
    );
    for result in &results {
        let mut sorted = result.latencies_ms.clone();
        sorted.sort_by(f64::total_cmp);
        let mean = if sorted.is_empty() {
            f64::NAN
        } else {
            sorted.iter().sum::<f64>() / sorted.len() as f64
        };
        let p95 = if sorted.is_empty() {
            f64::NAN
        } else {
            sorted[((sorted.len() as f64 * 0.95).ceil() as usize).clamp(1, sorted.len()) - 1]
        };
        println!(
            "{:<8} {:>8.1} {:>9.1} {:>6} {:>8.1}ms {:>8.1}ms {:>7} {:>6} {:>10.0} {:>12.0}",
            result.transport,
            result.frames as f64 / result.secs,
            result.bytes as f64 / result.secs / 1.0e6,
            result.torn,
            mean,
            p95,
            sorted.len(),
            result.edits_sent,
            result.engine_us_mean,
            result.readback_us_mean
        );
    }
    Ok(true)
}

/// One transport: LoadStage, a 250 ms edit loop, and `secs` seconds of frames.
async fn bench_once(shared: bool, secs: u64) -> Result<BenchResult, Box<dyn Error>> {
    let mut client = Client::connect(endpoint()).await?;
    let hello = client
        .hello(HelloRequest {
            editor_version: "bridge_probe --bench".into(),
        })
        .await?
        .into_inner();
    let ring = RingView::open(&hello).ok();
    if shared && ring.is_none() {
        return Err("shared transport needs a frame ring".into());
    }
    client
        .load_stage(LoadStageRequest {
            entities: demo_scene(),
        })
        .await?;
    let mut frames = client
        .stream_frames(StreamFramesRequest {
            max_fps: 120,
            prefer_shared_memory: shared,
            ..Default::default()
        })
        .await?
        .into_inner();
    wait_frame(&mut frames, WAIT, |f| !Stats::of(f).is_flat())
        .await
        .ok_or("scene never rendered")?;

    // Edits every 250 ms, timestamped before the call (the ack waits for apply).
    let sent: Arc<Mutex<BTreeMap<u64, Instant>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let edit_counter = Arc::new(AtomicU64::new(0));
    let counter_for_task = Arc::clone(&edit_counter);
    let edit_sent = Arc::clone(&sent);
    let mut edit_client = client.clone();
    let editor = tokio::spawn(async move {
        let mut tick = tokio::time::interval(BENCH_EDIT_INTERVAL);
        // Start from the wall clock: the engine keeps the highest id it has applied, so
        // ids must keep increasing across bench runs against the same server.
        let mut edit_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_micros() as u64);
        loop {
            tick.tick().await;
            edit_id += 1;
            counter_for_task.store(edit_id, Ordering::Relaxed);
            let x = if edit_id.is_multiple_of(2) { 0.5 } else { -0.5 };
            let at = Instant::now();
            edit_sent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(edit_id, at);
            let request = ApplyDeltaRequest {
                deltas: vec![EntityDelta {
                    path: "/Level/Crate".into(),
                    local: Some(transform([x, 0.5, 0.0])),
                    base_color: None,
                    removed: false,
                }],
                edit_id,
            };
            if edit_client.apply_delta(request).await.is_err() {
                break;
            }
        }
        edit_id
    });

    let transport = if shared { "ring" } else { "inline" };
    let started = Instant::now();
    let deadline = started + Duration::from_secs(secs);
    let (mut count, mut bytes, mut torn) = (0_u64, 0_u64, 0_u64);
    let mut latencies_ms = Vec::new();
    let (mut engine_sum, mut readback_sum) = (0_u64, 0_u64);
    while Instant::now() < deadline {
        let next = match tokio::time::timeout(Duration::from_secs(5), frames.next()).await {
            Ok(Some(Ok(frame))) => frame,
            _ => break,
        };
        let received = Instant::now();
        let frame_bytes = if next.in_ring {
            let Some(ring) = ring.as_ref() else { break };
            match ring.read(next.sequence, next.width, next.height) {
                SlotRead::Ok(pixels) => pixels.len() as u64,
                SlotRead::Torn | SlotRead::Stale => {
                    torn += 1;
                    continue;
                }
            }
        } else {
            next.rgba8.len() as u64
        };
        count += 1;
        bytes += frame_bytes;
        engine_sum += u64::from(next.engine_frame_us);
        readback_sum += u64::from(next.readback_us);
        // Every edit applied at or before this frame's render has now been seen.
        let mut pending = sent.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let seen: Vec<u64> = pending
            .range(..=next.last_edit_id)
            .map(|(id, _)| *id)
            .collect();
        for id in seen {
            if let Some(at) = pending.remove(&id) {
                latencies_ms.push(received.duration_since(at).as_secs_f64() * 1000.0);
            }
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    editor.abort();
    let edits_sent = edit_counter.load(Ordering::Relaxed);
    let frames_done = count.max(1) as f64;
    Ok(BenchResult {
        transport,
        secs: elapsed,
        frames: count,
        bytes,
        torn,
        latencies_ms,
        edits_sent,
        engine_us_mean: engine_sum as f64 / frames_done,
        readback_us_mean: readback_sum as f64 / frames_done,
    })
}
