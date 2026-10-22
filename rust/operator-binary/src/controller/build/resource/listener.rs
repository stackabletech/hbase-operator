//! Build the listener `Volume`/`PersistentVolumeClaim` exposing a rolegroup.

use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::pod::volume::{
        ListenerOperatorVolumeSourceBuilder, ListenerOperatorVolumeSourceBuilderError,
        ListenerReference, VolumeBuilder,
    },
    constant,
    k8s_openapi::api::core::v1::{PersistentVolumeClaim, Volume},
    kvp::Labels,
    v2::{
        builder::pod::volume::{
            ListenerReference as TypedListenerReference,
            listener_operator_volume_source_builder_build_pvc,
        },
        types::kubernetes::{ListenerClassName, PersistentVolumeClaimName},
    },
};

use crate::crd::LISTENER_VOLUME_NAME;

constant!(pub(crate) LISTENER_PVC_NAME: PersistentVolumeClaimName = LISTENER_VOLUME_NAME);

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build listener volume"))]
    BuildListenerVolume {
        source: ListenerOperatorVolumeSourceBuilderError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// The ephemeral listener [`Volume`] for the `listener_class`.
pub fn build_listener_volume(
    listener_class: &ListenerClassName,
    recommended_labels: &Labels,
) -> Result<Volume> {
    Ok(VolumeBuilder::new(LISTENER_VOLUME_NAME)
        .ephemeral(
            // The v2 framework only exposes a PVC builder
            // (`listener_operator_volume_source_builder_build_pvc`, used by
            // `build_listener_pvc`), so the ephemeral case still uses the legacy
            // `ListenerOperatorVolumeSourceBuilder` and its stringly-typed
            // `ListenerReference`. We keep the typed `listener_class` and convert to a
            // `String` only at this boundary; switch to a v2 helper once one exists.
            ListenerOperatorVolumeSourceBuilder::new(
                &ListenerReference::ListenerClass(listener_class.to_string()),
                recommended_labels,
            )
            .build_ephemeral()
            .context(BuildListenerVolumeSnafu)?,
        )
        .build())
}

/// A listener [`PersistentVolumeClaim`] template for the `listener_class`.
pub fn build_listener_pvc(
    listener_class: &ListenerClassName,
    recommended_labels: &Labels,
) -> PersistentVolumeClaim {
    listener_operator_volume_source_builder_build_pvc(
        &TypedListenerReference::ListenerClass(listener_class.clone()),
        recommended_labels,
        &LISTENER_PVC_NAME,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *LISTENER_PVC_NAME;
    }
}
