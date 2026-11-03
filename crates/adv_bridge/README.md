# adv_bridge

Prototype engine side of the Adventure editor bridge. The editor (an egui app)
owns the USD stage, resolves it, and sends this engine flat entities over gRPC.
The wire contract is [`proto/adventure.proto`](proto/adventure.proto).

## What the plugin does

`AdvBridgePlugin { addr, viewport }` (defaults: `127.0.0.1:50051`, `960x540`):

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
| `Hello` | Returns engine version and current viewport size. |
| `LoadStage` | Despawns all bridged entities, spawns the flat list. Parents first, `ChildOf` links, `Name` = prim path, cube/sphere/plane meshes with `StandardMaterial`, point/directional lights, cameras. |
| `ApplyDelta` | Updates transform and base color by path, or removes by path. |
| `SetViewport` | Resizes the offscreen target. |
| `StreamFrames` | Server-streams frames at `max_fps` (default 30, capped at 120). |
| `SendInput` | Accepts and drains the stream. TODO: forward to input systems. |
| `StreamEvents` | Holds the stream open, never yields. TODO: logs, picks, play state. |

Notes:

- `Ack` means the command was queued, not that it has been applied yet.
- proto3 cannot tell an absent `Color.a` from `0.0`, so a color without alpha
  is transparent. The editor should send `a = 1.0`.
- Readback runs every frame regardless of `max_fps`; `max_fps` only throttles
  what is sent over gRPC.
- Cameras from the stage all target the viewport image, but only the first one
  is active.

## Layout

- `build.rs` compiles the proto with `protox` (pure Rust, no `protoc`) and
  generates code into `OUT_DIR` with `tonic-prost-build`.
- `src/lib.rs` plugin, `proto` module, readback observer.
- `src/server.rs` gRPC service (tokio side).
- `src/scene.rs` Bevy side: command application, entity spawning, index.

## Running the example

Needs a GPU and a desktop session (the window is created hidden).

```text
cargo run -p adv_bridge --example headless_bridge
```

In a second terminal, the probe calls `Hello`, sends a small `LoadStage`, and
waits for one frame:

```text
cargo run -p adv_bridge --example bridge_probe
```

Check the work with `cargo check -p adv_bridge --examples`.
