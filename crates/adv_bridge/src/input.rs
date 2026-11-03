//! Viewport input from the editor: picking, dragging on the horizontal plane,
//! and the selection outline.
//!
//! Picking is a ray test against each bridged primitive's local shape (cube,
//! sphere, plane), not Bevy's mesh picking: the pointer must come from the
//! editor, and the offscreen camera's ray is computed directly with
//! `Camera::viewport_to_world`. Lights and cameras are not pickable (they have
//! no `PickShape`).

use bevy::{camera::Camera, prelude::*};
use crossbeam_channel::Receiver;
use tokio::sync::{broadcast, oneshot};

use crate::{
    proto,
    scene::{EntityIndex, PickShape},
};

/// Bevy-side outline colour for the selection.
pub(crate) const OUTLINE_COLOR: Color = Color::srgb(1.0, 0.85, 0.2);
/// Mouse button code for the primary button in `MouseButton.button`.
const BUTTON_LEFT: u32 = 0;
/// Minimum time between `TransformChanged { final: false }` events (20 Hz).
const DRAG_EVENT_INTERVAL_SECS: f32 = 0.05;

/// Input events queued by the gRPC `SendInput` stream.
#[derive(Resource)]
pub(crate) struct InputQueue(pub Receiver<proto::InputEvent>);

/// Engine -> editor events (picks, transform edits). Broadcast so several
/// `StreamEvents` subscribers can listen; the engine never waits on them.
#[derive(Resource)]
pub(crate) struct EventSink(pub broadcast::Sender<proto::EngineEvent>);

/// Editor-facing interaction state: selection and the active drag.
#[derive(Resource, Default)]
pub(crate) struct Interaction {
    /// Prim path of the editor's selection, drawn with an outline.
    pub selection: Option<String>,
    drag: Option<Drag>,
}

/// An entity being dragged along the horizontal plane through its origin.
struct Drag {
    entity: Entity,
    path: String,
    /// World-space height of the plane the entity slides on.
    plane_y: f32,
    /// Horizontal world offset from the pointer's plane hit to the entity origin,
    /// captured at press so the entity does not jump to the cursor.
    grab_offset: Vec3,
    /// Elapsed seconds when the last interim event was sent.
    last_emit: f32,
    /// Whether the entity actually moved; a click without motion emits no transform event.
    moved: bool,
}

/// Cancels the active drag (the dragged entity was replaced or removed).
pub(crate) fn cancel_drag(world: &mut World) {
    world.resource_mut::<Interaction>().drag = None;
}

/// Cancels the active drag when `path` is the dragged entity.
pub(crate) fn cancel_drag_if_path(world: &mut World, path: &str) {
    let dragged = world
        .resource::<Interaction>()
        .drag
        .as_ref()
        .is_some_and(|drag| drag.path == path);
    if dragged {
        cancel_drag(world);
    }
}

/// Sets or clears the selection. Errors for a path that is not in the stage.
pub(crate) fn set_selection(
    world: &mut World,
    path: String,
    reply: oneshot::Sender<Result<(), String>>,
) {
    let result = if path.is_empty() {
        world.resource_mut::<Interaction>().selection = None;
        Ok(())
    } else if world.resource::<EntityIndex>().0.contains_key(&path) {
        world.resource_mut::<Interaction>().selection = Some(path);
        Ok(())
    } else {
        Err(format!("no entity at path {path:?}"))
    };
    // The caller may have gone away; nothing to do about it.
    let _ = reply.send(result);
}

/// Consumes queued input events in arrival order.
pub(crate) fn apply_input(world: &mut World) {
    let events: Vec<proto::InputEvent> = world.resource::<InputQueue>().0.try_iter().collect();
    for event in events {
        use proto::input_event::Kind;
        match event.kind {
            Some(Kind::MouseMove(move_event)) => {
                mouse_move(world, Vec2::new(move_event.x, move_event.y));
            }
            Some(Kind::MouseButton(button)) => mouse_button(world, button),
            // TODO: wheel (camera zoom) and keyboard (shortcuts) are not handled yet.
            Some(Kind::MouseWheel(_)) | Some(Kind::Key(_)) | None => {}
        }
    }

    // A drag whose entity disappeared (removed by a delta or a stage reload) ends quietly.
    let gone = world
        .resource::<Interaction>()
        .drag
        .as_ref()
        .is_some_and(|drag| world.get_entity(drag.entity).is_err());
    if gone {
        cancel_drag(world);
    }
}

fn mouse_move(world: &mut World, position: Vec2) {
    let dragging = world.resource::<Interaction>().drag.is_some();
    if dragging {
        update_drag(world, position);
    }
}

fn mouse_button(world: &mut World, button: proto::MouseButton) {
    if button.button != BUTTON_LEFT {
        return;
    }
    let position = Vec2::new(button.x, button.y);
    if button.pressed {
        press(world, position);
    } else {
        release(world);
    }
}

fn press(world: &mut World, position: Vec2) {
    let picked = pick(world, position);
    let path = picked
        .as_ref()
        .map(|(_, path)| path.clone())
        .unwrap_or_default();
    emit(
        world,
        proto::engine_event::Kind::Picked(proto::PickResult { path: path.clone() }),
    );

    let Some((entity, path)) = picked else {
        return;
    };
    let Some(global) = world
        .get::<GlobalTransform>(entity)
        .map(|g| g.translation())
    else {
        return;
    };
    let Some(ray) = active_camera_ray(world, position) else {
        return;
    };
    let plane_y = global.y;
    let Some(hit) = plane_hit(&ray, plane_y) else {
        return;
    };
    let now = elapsed(world);
    world.resource_mut::<Interaction>().drag = Some(Drag {
        entity,
        path,
        plane_y,
        grab_offset: Vec3::new(global.x - hit.x, 0.0, global.z - hit.z),
        last_emit: now - DRAG_EVENT_INTERVAL_SECS,
        moved: false,
    });
}

fn release(world: &mut World) {
    let Some(drag) = world.resource_mut::<Interaction>().drag.take() else {
        return;
    };
    if !drag.moved {
        return;
    }
    if let Some(local) = world.get::<Transform>(drag.entity).copied() {
        emit_transform(world, drag.path, local, true);
    }
}

fn update_drag(world: &mut World, position: Vec2) {
    let Some((entity, plane_y, grab_offset, last_emit)) = world
        .resource::<Interaction>()
        .drag
        .as_ref()
        .map(|drag| (drag.entity, drag.plane_y, drag.grab_offset, drag.last_emit))
    else {
        return;
    };
    let Some(ray) = active_camera_ray(world, position) else {
        return;
    };
    let Some(hit) = plane_hit(&ray, plane_y) else {
        return;
    };

    let target_world = Vec3::new(hit.x + grab_offset.x, plane_y, hit.z + grab_offset.z);
    let parent_affine = world
        .get::<ChildOf>(entity)
        .map(|child_of| child_of.parent())
        .and_then(|parent| world.get::<GlobalTransform>(parent))
        .map(|global| global.affine())
        .unwrap_or(bevy::math::Affine3A::IDENTITY);
    let local_position = parent_affine.inverse().transform_point3(target_world);

    let Some(mut transform) = world.get_mut::<Transform>(entity) else {
        return;
    };
    if transform.translation == local_position {
        return;
    }
    transform.translation = local_position;
    let local = *transform;

    let now = elapsed(world);
    let due = now - last_emit >= DRAG_EVENT_INTERVAL_SECS;
    let path = {
        let mut interaction = world.resource_mut::<Interaction>();
        let Some(drag) = interaction.drag.as_mut() else {
            return;
        };
        drag.moved = true;
        if !due {
            return;
        }
        drag.last_emit = now;
        drag.path.clone()
    };
    emit_transform(world, path, local, false);
}

/// Picks the nearest bridged primitive under the viewport position.
fn pick(world: &mut World, position: Vec2) -> Option<(Entity, String)> {
    let ray = active_camera_ray(world, position)?;
    let mut best: Option<(f32, Entity)> = None;
    let mut query = world.query::<(Entity, &PickShape, &GlobalTransform)>();
    for (entity, shape, global) in query.iter(world) {
        let Some(distance) = ray_hit_distance(&ray, shape.0, global) else {
            continue;
        };
        if best.is_none_or(|(nearest, _)| distance < nearest) {
            best = Some((distance, entity));
        }
    }
    let (_, entity) = best?;
    let path = world.get::<Name>(entity)?.as_str().to_owned();
    Some((entity, path))
}

/// Ray from the active camera through a viewport pixel (top-left origin).
fn active_camera_ray(world: &mut World, position: Vec2) -> Option<Ray3d> {
    let mut query = world.query::<(&Camera, &GlobalTransform)>();
    for (camera, transform) in query.iter(world) {
        if camera.is_active {
            return camera.viewport_to_world(transform, position).ok();
        }
    }
    None
}

/// Distance along the world ray to the entity's primitive, if hit.
fn ray_hit_distance(
    ray: &Ray3d,
    kind: proto::PrimitiveKind,
    global: &GlobalTransform,
) -> Option<f32> {
    let affine = global.affine();
    let inverse = affine.inverse();
    let origin = inverse.transform_point3(ray.origin);
    let direction = inverse.transform_vector3(*ray.direction);
    let t = match kind {
        proto::PrimitiveKind::PrimitiveCube => ray_box(origin, direction, Vec3::splat(0.5)),
        proto::PrimitiveKind::PrimitiveSphere => ray_sphere(origin, direction, 0.5),
        // Plane3d::default() is 1x1 (half size 0.5) in the XZ plane.
        proto::PrimitiveKind::PrimitivePlane => ray_plane(origin, direction, 0.5),
        proto::PrimitiveKind::PrimitiveNone => None,
    }?;
    let hit_world = affine.transform_point3(origin + direction * t);
    Some((hit_world - ray.origin).dot(*ray.direction))
}

/// Ray vs axis-aligned box centred at the origin. `t` is in units of `direction`.
fn ray_box(origin: Vec3, direction: Vec3, half: Vec3) -> Option<f32> {
    let inverse = direction.recip();
    let t1 = (-half - origin) * inverse;
    let t2 = (half - origin) * inverse;
    let t_near = t1.min(t2).max_element();
    let t_far = t1.max(t2).min_element();
    if t_far < 0.0 || t_near > t_far {
        return None;
    }
    Some(if t_near >= 0.0 { t_near } else { t_far })
}

/// Ray vs sphere centred at the origin.
fn ray_sphere(origin: Vec3, direction: Vec3, radius: f32) -> Option<f32> {
    let a = direction.length_squared();
    let b = origin.dot(direction);
    let c = origin.length_squared() - radius * radius;
    let discriminant = b * b - a * c;
    if discriminant < 0.0 {
        return None;
    }
    let root = discriminant.sqrt();
    let near = (-b - root) / a;
    let far = (-b + root) / a;
    if near >= 0.0 {
        Some(near)
    } else if far >= 0.0 {
        Some(far)
    } else {
        None
    }
}

/// Ray vs square in the XZ plane (y = 0) with the given half size.
fn ray_plane(origin: Vec3, direction: Vec3, half: f32) -> Option<f32> {
    if direction.y.abs() < 1e-6 {
        return None;
    }
    let t = -origin.y / direction.y;
    if t < 0.0 {
        return None;
    }
    let p = origin + direction * t;
    (p.x.abs() <= half && p.z.abs() <= half).then_some(t)
}

/// Where the world ray meets the horizontal plane at `plane_y`.
fn plane_hit(ray: &Ray3d, plane_y: f32) -> Option<Vec3> {
    let direction = *ray.direction;
    if direction.y.abs() < 1e-6 {
        return None;
    }
    let t = (plane_y - ray.origin.y) / direction.y;
    (t >= 0.0).then(|| ray.origin + direction * t)
}

fn elapsed(world: &World) -> f32 {
    world.resource::<Time>().elapsed_secs()
}

fn emit(world: &World, kind: proto::engine_event::Kind) {
    let Some(sink) = world.get_resource::<EventSink>() else {
        return;
    };
    // No subscribers is fine: events are only useful to a connected editor.
    let _ = sink.0.send(proto::EngineEvent { kind: Some(kind) });
}

fn emit_transform(world: &World, path: String, local: Transform, final_: bool) {
    emit(
        world,
        proto::engine_event::Kind::TransformChanged(proto::TransformChanged {
            path,
            local: Some(to_proto_transform(&local)),
            r#final: final_,
        }),
    );
}

fn to_proto_transform(transform: &Transform) -> proto::Transform {
    let t = transform.translation;
    let r = transform.rotation;
    let s = transform.scale;
    proto::Transform {
        translation: Some(proto::Vec3 {
            x: t.x,
            y: t.y,
            z: t.z,
        }),
        rotation: Some(proto::Quat {
            x: r.x,
            y: r.y,
            z: r.z,
            w: r.w,
        }),
        scale: Some(proto::Vec3 {
            x: s.x,
            y: s.y,
            z: s.z,
        }),
    }
}

/// Outlines the selected entity with a box around its bounds.
pub(crate) fn draw_selection(
    mut gizmos: Gizmos,
    interaction: Res<Interaction>,
    index: Res<EntityIndex>,
    entities: Query<(&GlobalTransform, Option<&PickShape>)>,
) {
    let Some(path) = interaction.selection.as_ref() else {
        return;
    };
    let Some(&entity) = index.0.get(path) else {
        return;
    };
    let Ok((global, shape)) = entities.get(entity) else {
        return;
    };
    // Planes are flat; give them a little height so the outline is visible.
    let bounds = match shape.map(|s| s.0) {
        Some(proto::PrimitiveKind::PrimitivePlane) => Vec3::new(1.0, 0.02, 1.0),
        _ => Vec3::ONE,
    };
    let mut transform = global.compute_transform();
    transform.scale *= bounds * 1.02;
    gizmos.cube(transform, OUTLINE_COLOR);
}
