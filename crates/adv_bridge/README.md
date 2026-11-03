# adv_bridge

Prototype engine side of the Adventure editor bridge. The editor (an egui app)
owns the USD stage, resolves it, and sends this engine flat entities over gRPC.
The wire contract is [`proto/adventure.proto`](proto/adventure.proto).

## What the plugin does

`AdvBridgePlugin { addr, viewport }` (defaults: `127.0.0.1:50051`, `960x540`).
`headless_bridge` takes `ADV_BRIDGE_ADDR=host:port` to run on another port, and
`bridge_probe` takes `ADV_PROBE_ADDR` to match. Use a separate port when an editor
already holds 50051.

If the gRPC port cannot be bound, the plugin logs
`adv_bridge: cannot listen on <addr>: <os error> - is another game instance already running?`
and the app exits with code 1 on its first update. The same happens if the gRPC
thread dies after binding. The app never runs without its bridge. Verified: a second
`headless_bridge` on the same port exits with code 1 in about 2 s. Hosts such as
`adv-proj --editor` should use `fn main() -> AppExit` so the exit code propagates.

- Starts a tonic gRPC server on its own thread with its own tokio runtime. Bevy
  never blocks on the network; requests are queued over a channel and applied in
  an `Update` system.
- Renders the scene into an offscreen `Image` (not a window). The image is read
  back on the GPU every frame with `Readback` and the latest bytes are published
  to `StreamFrames` subscribers as RGBA8, top-left origin.
- Spawns a default camera and directional light so the viewport is never empty.

RPCs:

| RPC | Status |
| --- | --- |
| `Hello` | Returns engine version and the viewport size Bevy has actually applied. |
| `LoadStage` | Despawns all bridged entities, spawns the flat list. Parents first, `ChildOf` links, `Name` = prim path, cube/sphere/plane meshes with `StandardMaterial`, point/directional lights, cameras. |
| `ApplyDelta` | Updates transform and base color by path, or removes by path. Acked once applied. |
| `SetViewport` | Resizes the offscreen target. `Ack` is returned after Bevy applies the size, so `Hello` and the next frames match it. Each side is 1..=8192. |
| `StreamFrames` | Server-streams frames at `max_fps` (default 30, capped at 120). `prefer_shared_memory` sends `in_ring` frames (pixels in the ring file); `skip_unchanged` drops frames identical to the last one sent, except the first frame after a change. |
| `SendInput` | Client stream of mouse events, applied in arrival order (see Viewport input). Keyboard and wheel are accepted and ignored (TODO). |
| `StreamEvents` | Server stream of `PickResult` and `TransformChanged`. Each subscriber gets a broadcast copy; a slow subscriber lags on its own and never blocks Bevy. Log forwarding is TODO. |
| `SetSelection` | Draws an outline around the entity at `path`. `""` clears. Unknown path returns `ok = false`. |

## Viewport input

Coordinates are viewport pixels, top-left origin, in the size of the last
`Frame`. Only the primary button (`button = 0`) is handled.

- **Picking.** A press runs a ray test from the active camera against each
  bridged primitive's local shape (cube, sphere, 1 m plane) and picks the
  nearest. It emits `PickResult { path }` (`""` on a miss). Bevy's mesh picking
  is not used: it is driven by pointer events, which the editor's input does not
  produce. Lights and cameras have no pick shape and cannot be picked.
- **Dragging.** A press on a picked entity starts a drag on the horizontal plane
  through its current height. The grab offset is captured at press, so the
  entity does not jump. Moves update the local translation (world point mapped
  through the parent's transform); rotation and scale are untouched.
  `TransformChanged { final: false }` is sent at most every 50 ms, and
  `final: true` on release, but only if the entity actually moved. The editor
  must not echo these back as `ApplyDelta`.
- **Cancellation.** `LoadStage` cancels a drag. An `ApplyDelta` that names the
  dragged path cancels it too. A drag also ends quietly if its entity disappears.
- **Selection** survives `LoadStage` (the path is kept) and is drawn only while
  an entity with that path exists.

## Editor mode: no window

The engine side of the editor runs without any OS window. The viewport is an
offscreen `Image`, so no surface is needed. Copy this app setup for adv-proj:

```rust
use adv_bridge::AdvBridgePlugin;
use bevy::{
    app::AppExit,
    prelude::*,
    window::{ExitCondition, WindowPlugin},
    winit::WinitSettings,
};

fn main() -> AppExit {
    App::new()
    .add_plugins(DefaultPlugins.set(WindowPlugin {
        primary_window: None,
        exit_condition: ExitCondition::DontExit,
        close_when_requested: false,
        ..default()
    }))
    .insert_resource(WinitSettings::game())
    .add_plugins(AdvBridgePlugin::default())
    .run()
}
```

Why this works: with no window, the loop is driven by winit's unfocused mode
(`reactive_low_power(1/60 s)`), so it ticks at about 60 Hz. Measured idle CPU of
the example is about 15% of one core. `DontExit` keeps the app alive with no
windows.

Earlier, a hidden window hit a Vulkan surface-query panic on the dev box
(`get_physical_device_surface_capabilities: unknown error`). Windowless mode
avoids the surface entirely, and the default backend passes 5 of 5 interaction
runs. If a hidden window is ever needed again, `WGPU_BACKEND=dx12` was the
workaround.

Notes:

- `Ack` means applied. Every mutating RPC (`LoadStage`, `ApplyDelta`, `SetViewport`,
  `SetSelection`) waits until Bevy has applied the command, so a call made right
  after an ack sees its effect. The wait is capped at 5 s; on timeout the ack is
  `ok = false` ("timed out") but the command stays queued and may still apply.
  `SetSelection` of a path that is not in the stage returns `ok = false`.
- proto3 cannot tell an absent `Color.a` from `0.0`, so a color without alpha
  is transparent. The editor should send `a = 1.0`.
- Readback rows are padded to wgpu's 256-byte copy alignment; the engine strips the
  padding, so every `Frame` is tight `width * height * 4` RGBA8 at any width
  (for example 1116 or 801 px).
- One readback is requested per engine frame, so the frame rate is the engine loop
  rate. The example runs the loop at 120 Hz, so `max_fps` up to 120 can be met.
  `max_fps` only throttles what is sent. Each frame is requested exactly once: the
  request entity is spawned in `Last` and its `Readback` is removed in the next frame's
  `First`, before the copy is made.
- `Frame.last_edit_id` is the highest `ApplyDeltaRequest.edit_id` applied before the
  frame's readback was requested (in `Last`, after this frame's `Update`). It is exact
  for that request, not an estimate. `engine_frame_us` is the main-loop frame time of
  that same frame, and `readback_us` is the time from request to publish.
- `after_change` is set on the first frame after any LoadStage, ApplyDelta or
  SetViewport. `skip_unchanged` subscribers still receive that frame.

## Shared-memory frame ring

The engine writes each frame into a ring file in the OS temp dir (`Hello` returns
its path, slot count and slot capacity). The file layout and seqlock protocol are in
`proto/adventure.proto`; `src/ring.rs` is the writer.

- Slot capacity is `width * height * 4` rounded up to 64 bytes, so slots stay 8-aligned.
  Three slots by default.
- A `SetViewport` that needs a larger slot recreates the ring under a new path
  (`adv_bridge_ring_<pid>_<gen>.bin`). Re-`Hello` after `SetViewport`, as the proto says.
- The ring file is removed on a clean exit. A crashed engine leaves its file behind;
  the next startup removes files whose pid is no longer running.
- `StreamFrames.prefer_shared_memory` sends `in_ring = true` with empty `rgba8`. The
  reader takes slot `sequence % slot_count`. Readers must check `seq_begin` and `seq_end`
  against `sequence` before and after the copy, and drop the frame on a mismatch.
- `StreamFrames.skip_unchanged` compares a 64-bit hash of the pixels with the last frame
  sent to that subscriber. The hash is FNV-1a over 8-byte words. It costs about 1.0 ms per
  960x540 frame in the release build (the debug build is about 30x slower). The engine
  logs the figure once at startup. The hash is computed only for subscribers that ask
  for `skip_unchanged`.
- Cameras from the stage all target the viewport image, but only the first one
  is active.

## Layout

- `build.rs` compiles the proto with `protox` (pure Rust, no `protoc`) and
  generates code into `OUT_DIR` with `tonic-prost-build`.
- `src/lib.rs` plugin, `proto` module, startup and schedule wiring.
- `src/server.rs` gRPC service (tokio side).
- `src/scene.rs` Bevy side: command application, entity spawning, index.
- `src/input.rs` viewport input: picking, drag, selection outline, event emit.
- `src/readback.rs` one readback request per frame, edit and change metadata, publish.
- `src/frame.rs` frame data, row unpadding, pixel hash.
- `src/ring.rs` shared-memory frame ring writer (seqlock), stale-file cleanup.

## Running the example

Needs a GPU. No window is created (see "Editor mode: no window").

```text
cargo run -p adv_bridge --example headless_bridge
```

In a second terminal, the probe calls `Hello`, sends a small `LoadStage`, and
waits for one frame:

```text
cargo run -p adv_bridge --example bridge_probe
```

Interaction checks (click pick, selection outline, drag, transform events).
The probe exits 0 on PASS and 1 on FAIL; PNGs go to `<stem>_before/_selected/_moved.png`:

```text
cargo run -p adv_bridge --example bridge_probe -- --interact target/interact

Other probe modes: `--load-select` (20 rounds of LoadStage then SetSelection with
no pause), and `--resize WxH` (repeatable) with optional `--resize-before-load`.

Transport modes (run them against a release build for meaningful numbers):

- `--shm`: waits until the scene settles (two identical consecutive ring frames), then
  checks that a ring frame is byte-identical to an inline frame of the same scene. It
  then runs a 5 s, 60 fps torn-read stress on the newest slot (expect 0 torn) and on
  the oldest slot (torn and stale counts show the detector working).
- `--bench <secs>`: runs inline, then ring, with edits every 250 ms. It reports frames/s,
  payload MB/s, dropped reads, and edit-to-frame latency (mean and p95), measured with
  `edit_id` and `last_edit_id`. Engine frame and readback times come from the frames.

Measured on the dev box (release build, default backend, windowless, 120 Hz loop,
20 s per transport):

| transport | frames/s | MB/s | dropped | latency mean | latency p95 | engine frame | readback |
| --- | --- | --- | --- | --- | --- | --- | --- |
| inline | 103.8 | 215 | 0 | 46.8 ms | 73.0 ms | 9.4 ms | 22.6 ms |
| ring | 65.3 | 135 | 0 | 31.7 ms | 32.5 ms | 8.2 ms | 18.3 ms |

The ring reader is slower on the client in this bench (see the open items in the
crate notes), though it has lower latency and no gRPC payload.
```

Check the work with `cargo check -p adv_bridge --examples`.
