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
    ApplyDeltaRequest, Color, EntityDelta, FlatEntity, Frame, HelloRequest, LoadStageRequest,
    PrimitiveKind, Quat, SetViewportRequest, StreamFramesRequest, Transform, Vec3,
    bridge_client::BridgeClient,
};
use tokio_stream::StreamExt as _;
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
    let save_path = parse_save_arg()?;
    let mut client = BridgeClient::connect("http://127.0.0.1:50051").await?;

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
                entity("/Level", "", PrimitiveKind::PrimitiveNone, [0.0, 0.0, 0.0], [0.0; 3]),
                entity("/Level/Crate", "/Level", PrimitiveKind::PrimitiveCube, [0.0, 0.0, 0.0], [0.7, 0.4, 0.2]),
                entity("/Level/Ball", "/Level", PrimitiveKind::PrimitiveSphere, [0.0, 2.0, 0.0], [0.2, 0.5, 0.9]),
                entity("/Level/Floor", "/Level", PrimitiveKind::PrimitivePlane, [0.0, -1.0, 0.0], [0.3, 0.3, 0.3]),
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
    report("initial", &frame, save_path.as_deref().map(|p| suffixed(p, "_initial")).as_deref());

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
    report("after apply_delta", &after_delta, save_path.as_deref().map(|p| suffixed(p, "_delta")).as_deref());
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
    report("after set_viewport", &resized, save_path.as_deref().map(|p| suffixed(p, "_viewport")).as_deref());

    Ok(())
}

/// Parses `--save <path>`; returns `None` when the flag is absent.
fn parse_save_arg() -> Result<Option<PathBuf>, Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mut save = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--save" => save = Some(PathBuf::from(args.next().ok_or("--save needs a path")?)),
            other => return Err(format!("unknown argument {other:?}").into()),
        }
    }
    Ok(save)
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
        for px in frame.rgba8.chunks_exact(4) {
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
        .chunks_exact(4)
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
