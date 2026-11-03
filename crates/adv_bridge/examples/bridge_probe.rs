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

use std::{collections::HashMap, error::Error, path::PathBuf, time::Duration};

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
        .stream_frames(StreamFramesRequest { max_fps: 10 })
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
        .stream_frames(StreamFramesRequest { max_fps: 10 })
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
        .stream_frames(StreamFramesRequest { max_fps: 10 })
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
