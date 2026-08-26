//! Build the listener `Volume`/`PersistentVolumeClaim` exposing a rolegroup.

use std::str::FromStr;

use stackable_operator::{
    builder::pod::volume::{ListenerOperatorVolumeSourceBuilder, ListenerReference, VolumeBuilder},
    constant,
    k8s_openapi::api::core::v1::{PersistentVolumeClaim, Volume},
    kvp::Labels,
    v2::{
        builder::pod::volume::{
            ListenerReference as TypedListenerReference,
            listener_operator_volume_source_builder_build_pvc,
        },
        types::kubernetes::PersistentVolumeClaimName,
    },
};

use crate::crd::{AnyServiceConfig, HbaseRole, LISTENER_VOLUME_NAME};

constant!(pub LISTENER_PVC_NAME: PersistentVolumeClaimName = LISTENER_VOLUME_NAME);

/// The ephemeral listener [`Volume`] for the masters and region servers, or `None` for the rest
/// servers (which use a [`PersistentVolumeClaim`] instead, see [`build_listener_pvc`]).
pub fn build_listener_volume(
    role: &HbaseRole,
    merged_config: &AnyServiceConfig,
    recommended_labels: &Labels,
) -> Option<Volume> {
    match role {
        // Master and regionservers should use ephemeral listener volumes
        // since clients pull the latest address from ZooKeeper
        HbaseRole::Master | HbaseRole::RegionServer => Some(
            VolumeBuilder::new(LISTENER_VOLUME_NAME)
                .ephemeral(
                    // The v2 framework only exposes a PVC builder
                    // (`listener_operator_volume_source_builder_build_pvc`, used by
                    // `build_listener_pvc`), so the ephemeral case still uses the legacy
                    // `ListenerOperatorVolumeSourceBuilder` and its stringly-typed
                    // `ListenerReference`. We keep the typed `listener_class()` and convert to a
                    // `String` only at this boundary; switch to a v2 helper once one exists.
                    ListenerOperatorVolumeSourceBuilder::new(
                        &ListenerReference::ListenerClass(
                            merged_config.listener_class().to_string(),
                        ),
                        recommended_labels,
                    )
                    .build_ephemeral()
                    .expect(
                        "The annotations are built from a validated listener class and validated labels.",
                    ),
                )
                .build(),
        ),
        HbaseRole::RestServer => None,
    }
}

/// The listener [`PersistentVolumeClaim`] template for the rest servers, or `None` for the masters
/// and region servers (which use an ephemeral [`Volume`] instead, see [`build_listener_volume`]).
pub fn build_listener_pvc(
    role: &HbaseRole,
    merged_config: &AnyServiceConfig,
    recommended_labels: &Labels,
) -> Option<Vec<PersistentVolumeClaim>> {
    match role {
        HbaseRole::Master | HbaseRole::RegionServer => None,
        HbaseRole::RestServer => Some(vec![listener_operator_volume_source_builder_build_pvc(
            &TypedListenerReference::ListenerClass(merged_config.listener_class()),
            recommended_labels,
            &LISTENER_PVC_NAME,
        )]),
    }
}
