//! Runs the engine side of the editor bridge without any OS window.
//!
//! ```text
//! cargo run -p adv_bridge --example headless_bridge
//! ADV_BRIDGE_ADDR=127.0.0.1:50061 cargo run -p adv_bridge --example headless_bridge
//! ```
//!
//! The viewport is an offscreen image, so the editor mode needs no window: no
//! surface is created (a hidden window's Vulkan surface query panicked on the
//! dev box). `ExitCondition::DontExit` keeps the app alive with no windows. Without
//! a focused window winit falls back to `reactive_low_power(1/60 s)`, so the loop
//! ticks at about 60 Hz instead of spinning a core.
//!
//! `ADV_BRIDGE_ADDR` (`host:port`) overrides the default `127.0.0.1:50051`, so a
//! test server can run next to an editor that already holds the default port.
//! Loading a stage is logged by the bridge (`adv_bridge: loading stage`). If the gRPC
//! port is already taken the app logs why and exits with a non-zero code.

use std::time::Duration;

use adv_bridge::AdvBridgePlugin;
use bevy::{
    app::AppExit,
    prelude::*,
    window::{ExitCondition, WindowPlugin},
    winit::{UpdateMode, WinitSettings},
};

fn main() -> AppExit {
    let defaults = AdvBridgePlugin::default();
    let addr = std::env::var("ADV_BRIDGE_ADDR")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(defaults.addr);

    App::new()
        .add_plugins(DefaultPlugins.set(WindowPlugin {
            primary_window: None,
            exit_condition: ExitCondition::DontExit,
            close_when_requested: false,
            ..default()
        }))
        // No window is ever focused, so winit uses the unfocused mode. Set it to
        // 120 Hz so `StreamFrames.max_fps` up to 120 can be met (the frame rate is
        // the engine loop rate). Idle CPU rises with the loop rate; measure it when
        // changing this.
        .insert_resource(WinitSettings {
            focused_mode: UpdateMode::Continuous,
            unfocused_mode: UpdateMode::reactive_low_power(Duration::from_secs_f64(1.0 / 120.0)),
        })
        .add_plugins(AdvBridgePlugin { addr, ..defaults })
        .run()
}
