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

use adv_bridge::AdvBridgePlugin;
use bevy::{
    app::AppExit,
    prelude::*,
    window::{ExitCondition, WindowPlugin},
    winit::WinitSettings,
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
        // Explicit so the editor mode's loop rate does not depend on defaults:
        // no focused window means the unfocused (60 Hz reactive) mode applies.
        .insert_resource(WinitSettings::game())
        .add_plugins(AdvBridgePlugin { addr, ..defaults })
        .run()
}
