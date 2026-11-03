//! Bevy side of the bridge: applies queued commands to the world and owns the
//! resources that map editor prim paths to engine entities.

use std::collections::{HashMap, HashSet};

use bevy::{
    camera::{Camera, Camera3d, RenderTarget},
    image::Image,
    log::{info, warn},
    prelude::*,
};
use crossbeam_channel::Receiver;
use tokio::sync::watch;

use crate::{proto, Command};

/// Receives commands from the gRPC thread. Drained once per frame.
#[derive(Resource)]
pub(crate) struct BridgeCommands(pub Receiver<Command>);

/// Publishes rendered frames to the gRPC thread.
#[derive(Resource)]
pub(crate) struct FrameSink {
    pub tx: watch::Sender<Option<std::sync::Arc<proto::Frame>>>,
    pub sequence: u64,
}

/// Viewport size Bevy has actually applied; shared with the gRPC thread for `Hello`.
#[derive(Resource)]
pub(crate) struct AppliedViewport(pub std::sync::Arc<std::sync::Mutex<(u32, u32)>>);

/// The offscreen render target the bridge camera draws into.
#[derive(Resource)]
pub(crate) struct ViewportTarget {
    pub image: Handle<Image>,
    pub width: u32,
    pub height: u32,
}

/// Prim path -> entity for everything spawned from the editor.
#[derive(Resource, Default)]
pub(crate) struct EntityIndex(pub HashMap<String, Entity>);

/// Shared primitive meshes, created once at startup.
#[derive(Resource)]
pub(crate) struct Primitives {
    pub cube: Handle<Mesh>,
    pub sphere: Handle<Mesh>,
    pub plane: Handle<Mesh>,
}

/// Marks every entity owned by the bridge so `LoadStage` can replace them.
#[derive(Component)]
pub(crate) struct Bridged;

/// Primitive shape used for picking and the selection outline. Lights and cameras have none.
#[derive(Component, Clone, Copy)]
pub(crate) struct PickShape(pub proto::PrimitiveKind);

/// Drains pending commands in arrival order. Exclusive because loading a stage
/// needs `World` access to spawn entities and mutate assets.
pub(crate) fn apply_commands(world: &mut World) {
    let pending: Vec<Command> = world.resource::<BridgeCommands>().0.try_iter().collect();
    for command in pending {
        match command {
            Command::LoadStage { entities, reply } => {
                load_stage(world, entities);
                let _ = reply.send(Ok(()));
            }
            Command::ApplyDelta { deltas, reply } => {
                apply_deltas(world, deltas);
                let _ = reply.send(Ok(()));
            }
            Command::SetViewport {
                width,
                height,
                reply,
            } => {
                set_viewport(world, width, height);
                // Ignore a caller that has gone away.
                let _ = reply.send(Ok(()));
            }
            Command::SetSelection { path, reply } => {
                crate::input::set_selection(world, path, reply)
            }
        }
    }
}

/// Replaces every bridged entity with `entities`. Parents are spawned before
/// their children so `ChildOf` can point at a live entity.
fn load_stage(world: &mut World, entities: Vec<proto::FlatEntity>) {
    crate::input::cancel_drag(world);
    despawn_bridged(world);
    info!("adv_bridge: loading stage with {} entities", entities.len());

    let all_paths: HashSet<String> = entities.iter().map(|e| e.path.clone()).collect();
    let mut cameras: Vec<Entity> = Vec::new();
    let mut has_light = false;

    let mut pending = entities;
    // `force` is set when a whole round made no progress (a cycle or a parent
    // that never arrives); the remaining entities are then attached as roots.
    let mut force = false;
    while !pending.is_empty() {
        let mut deferred = Vec::new();
        let mut spawned_any = false;
        for entity in pending {
            let parent = match parent_of(&entity, world, &all_paths) {
                ParentLookup::Found(parent) => Some(parent),
                ParentLookup::Root => None,
                ParentLookup::NotYet if !force => {
                    deferred.push(entity);
                    continue;
                }
                ParentLookup::NotYet => {
                    warn!(
                        "adv_bridge: {} has an unresolvable parent, attaching to root",
                        entity.path
                    );
                    None
                }
                ParentLookup::Missing => {
                    warn!(
                        "adv_bridge: {} names missing parent {:?}",
                        entity.path, entity.parent_path
                    );
                    None
                }
            };

            let id = spawn_entity(world, &entity, parent);
            if entity.is_camera {
                cameras.push(id);
            }
            if proto::LightKind::try_from(entity.light).unwrap_or(proto::LightKind::LightNone)
                != proto::LightKind::LightNone
            {
                has_light = true;
            }
            // A duplicate path keeps the old entity alive (it is still bridged) but
            // deltas will address the newer one.
            if world
                .resource_mut::<EntityIndex>()
                .0
                .insert(entity.path.clone(), id)
                .is_some()
            {
                warn!("adv_bridge: duplicate prim path {}", entity.path);
            }
            spawned_any = true;
        }
        force = !spawned_any;
        pending = deferred;
    }

    // Only the first camera drives the viewport; the rest exist but are inactive.
    for (index, camera) in cameras.iter().enumerate() {
        if let Some(mut camera_component) = world.get_mut::<Camera>(*camera) {
            camera_component.is_active = index == 0;
        }
    }

    spawn_default_camera_and_light(world, cameras.is_empty(), !has_light);
}

enum ParentLookup {
    /// `parent_path` is empty.
    Root,
    /// Parent already spawned.
    Found(Entity),
    /// Parent is in this stage but not spawned yet.
    NotYet,
    /// Parent is not in this stage at all.
    Missing,
}

fn parent_of(
    entity: &proto::FlatEntity,
    world: &World,
    all_paths: &HashSet<String>,
) -> ParentLookup {
    if entity.parent_path.is_empty() {
        return ParentLookup::Root;
    }
    if let Some(parent) = world.resource::<EntityIndex>().0.get(&entity.parent_path) {
        return ParentLookup::Found(*parent);
    }
    if all_paths.contains(&entity.parent_path) {
        ParentLookup::NotYet
    } else {
        ParentLookup::Missing
    }
}

/// Spawns one flat entity with its transform, optional parent, mesh, light and
/// camera components. Returns the new entity.
fn spawn_entity(world: &mut World, entity: &proto::FlatEntity, parent: Option<Entity>) -> Entity {
    let id = world
        .spawn((
            Name::new(entity.path.clone()),
            to_transform(entity.local.as_ref()),
            // Every bridged entity gets Visibility so children never sit under a
            // parent without it (Bevy warns B0004 otherwise).
            Visibility::default(),
            Bridged,
        ))
        .id();
    if let Some(parent) = parent {
        world.entity_mut(id).insert(ChildOf(parent));
    }

    let primitive = proto::PrimitiveKind::try_from(entity.primitive).unwrap_or_default();
    let mesh = match primitive {
        proto::PrimitiveKind::PrimitiveCube => Some(world.resource::<Primitives>().cube.clone()),
        proto::PrimitiveKind::PrimitiveSphere => {
            Some(world.resource::<Primitives>().sphere.clone())
        }
        proto::PrimitiveKind::PrimitivePlane => Some(world.resource::<Primitives>().plane.clone()),
        proto::PrimitiveKind::PrimitiveNone => None,
    };
    if primitive != proto::PrimitiveKind::PrimitiveNone {
        world.entity_mut(id).insert(PickShape(primitive));
    }
    if let Some(mesh) = mesh {
        let material = world
            .resource_mut::<Assets<StandardMaterial>>()
            .add(StandardMaterial {
                base_color: to_color(entity.base_color.as_ref()),
                ..default()
            });
        world
            .entity_mut(id)
            .insert((Mesh3d(mesh), MeshMaterial3d(material)));
    }

    match proto::LightKind::try_from(entity.light).unwrap_or_default() {
        proto::LightKind::LightDirectional => {
            world.entity_mut(id).insert(DirectionalLight::default());
        }
        proto::LightKind::LightPoint => {
            world.entity_mut(id).insert(PointLight::default());
        }
        proto::LightKind::LightNone => {}
    }

    if entity.is_camera {
        let target = world.resource::<ViewportTarget>().image.clone();
        world.entity_mut(id).insert((
            Camera3d::default(),
            Camera::default(),
            RenderTarget::from(target),
        ));
    }

    id
}

fn apply_deltas(world: &mut World, deltas: Vec<proto::EntityDelta>) {
    for delta in deltas {
        crate::input::cancel_drag_if_path(world, &delta.path);
        let Some(entity) = world.resource::<EntityIndex>().0.get(&delta.path).copied() else {
            warn!("adv_bridge: delta for unknown path {}", delta.path);
            continue;
        };

        if delta.removed {
            if let Ok(entity_mut) = world.get_entity_mut(entity) {
                entity_mut.despawn();
            }
            // Children go with the parent; drop every index entry that is now dead.
            let dead: Vec<String> = world
                .resource::<EntityIndex>()
                .0
                .iter()
                .filter(|(_, live)| world.get_entity(**live).is_err())
                .map(|(path, _)| path.clone())
                .collect();
            let index = &mut world.resource_mut::<EntityIndex>().0;
            for path in dead {
                index.remove(&path);
            }
            continue;
        }

        if let Some(local) = &delta.local
            && let Some(mut transform) = world.get_mut::<Transform>(entity)
        {
            *transform = to_transform(Some(local));
        }
        if let Some(color) = &delta.base_color {
            let material = world
                .get::<MeshMaterial3d<StandardMaterial>>(entity)
                .map(|material| material.0.clone());
            if let Some(material) = material
                && let Some(mut standard) = world
                    .resource_mut::<Assets<StandardMaterial>>()
                    .get_mut(&material)
            {
                standard.base_color = to_color(Some(color));
            }
        }
    }
}

fn set_viewport(world: &mut World, width: u32, height: u32) {
    let image = world.resource::<ViewportTarget>().image.clone();
    if let Some(mut target) = world.resource_mut::<Assets<Image>>().get_mut(&image) {
        target.resize(bevy::render::render_resource::Extent3d {
            width,
            height,
            depth_or_array_layers: 1,
        });
    }
    let mut viewport = world.resource_mut::<ViewportTarget>();
    viewport.width = width;
    viewport.height = height;
    // Publish the size actually applied, so Hello reports what frames will look like.
    *world
        .resource::<AppliedViewport>()
        .0
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = (width, height);
    // Readbacks of the old size are dropped by the size check in `on_frame_ready`.
}

/// Spawns the fallback camera and/or light, tagged as bridged so the next
/// `LoadStage` replaces them too.
pub(crate) fn spawn_default_camera_and_light(world: &mut World, camera: bool, light: bool) {
    if camera {
        let target = world.resource::<ViewportTarget>().image.clone();
        world.spawn((
            Name::new("adv_bridge/default_camera"),
            Camera3d::default(),
            Camera::default(),
            RenderTarget::from(target),
            Transform::from_xyz(0.0, 2.0, 6.0).looking_at(Vec3::ZERO, Vec3::Y),
            Bridged,
        ));
    }
    if light {
        world.spawn((
            Name::new("adv_bridge/default_light"),
            DirectionalLight::default(),
            Transform::from_xyz(4.0, 8.0, 4.0).looking_at(Vec3::ZERO, Vec3::Y),
            Bridged,
        ));
    }
}

fn despawn_bridged(world: &mut World) {
    let mut query = world.query_filtered::<Entity, With<Bridged>>();
    let bridged: Vec<Entity> = query.iter(world).collect();
    for entity in bridged {
        if let Ok(entity_mut) = world.get_entity_mut(entity) {
            entity_mut.despawn();
        }
    }
    world.resource_mut::<EntityIndex>().0.clear();
}

/// Editor transform to Bevy. Missing or degenerate parts fall back to identity.
fn to_transform(local: Option<&proto::Transform>) -> Transform {
    let Some(local) = local else {
        return Transform::IDENTITY;
    };
    let translation = local
        .translation
        .as_ref()
        .map_or(Vec3::ZERO, |v| Vec3::new(v.x, v.y, v.z));
    let rotation = local
        .rotation
        .as_ref()
        .map(|q| Quat::from_xyzw(q.x, q.y, q.z, q.w))
        .filter(|q| q.length_squared() > 1e-12)
        .map_or(Quat::IDENTITY, Quat::normalize);
    let scale = local
        .scale
        .as_ref()
        .map_or(Vec3::ONE, |v| Vec3::new(v.x, v.y, v.z));
    Transform {
        translation,
        rotation,
        scale,
    }
}

/// Editor color to Bevy. Absent colors are light grey; alpha is taken as sent,
/// so the editor must send `a = 1.0` for opaque surfaces.
fn to_color(color: Option<&proto::Color>) -> Color {
    match color {
        Some(c) => Color::srgba(c.r, c.g, c.b, c.a),
        None => Color::srgb(0.8, 0.8, 0.8),
    }
}
