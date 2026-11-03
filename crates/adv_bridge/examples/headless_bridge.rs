//! Runs the engine side of the editor bridge with a hidden window.
//!
//! ```text
//! cargo run -p adv_bridge --example headless_bridge
//! ```
//!
//! The viewport is an offscreen image, so the window is only needed to create a
//! GPU device. Loading a stage is logged by the bridge (`adv_bridge: loading stage`).

use adv_bridge::AdvBridgePlugin;
use bevy::{prelude::*, window::WindowPlugin};

fn main() {
    App::new()
        .add_plugins(
            DefaultPlugins.set(WindowPlugin {
                primary_window: Some(Window {
                    title: "adv_bridge (headless)".into(),
                    visible: false,
                    ..default()
                }),
                ..default()
            }),
        )
        .add_plugins(AdvBridgePlugin::default())
        .run();
}
