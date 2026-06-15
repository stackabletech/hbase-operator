//! Build the listener `Volume`/`PersistentVolumeClaim` exposing a rolegroup.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::pod::volume::{
        ListenerOperatorVolumeSourceBuilder, ListenerOperatorVolumeSourceBuilderError,
        ListenerReference, VolumeBuilder,
    },
    k8s_openapi::api::core::v1::{PersistentVolumeClaim, Volume},
    kvp::Labels,
};

use crate::crd::{AnyServiceConfig, HbaseRole, LISTENER_VOLUME_NAME};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build listener volume"))]
    BuildListenerVolume {
        source: ListenerOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to build listener pvc"))]
    BuildListenerPvc {
        source: ListenerOperatorVolumeSourceBuilderError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// The ephemeral listener [`Volume`] for the masters and region servers, or `None` for the rest
/// servers (which use a [`PersistentVolumeClaim`] instead, see [`build_listener_pvc`]).
pub fn build_listener_volume(
    role: &HbaseRole,
    merged_config: &AnyServiceConfig,
    recommended_labels: &Labels,
) -> Result<Option<Volume>> {
    let volume = match role {
        // Master and regionservers should use ephemeral listener volumes
        // since clients pull the latest address from ZooKeeper
        HbaseRole::Master | HbaseRole::RegionServer => Some(
            VolumeBuilder::new(LISTENER_VOLUME_NAME)
                .ephemeral(
                    ListenerOperatorVolumeSourceBuilder::new(
                        &ListenerReference::ListenerClass(
                            merged_config.listener_class().to_string(),
                        ),
                        recommended_labels,
                    )
                    .build_ephemeral()
                    .context(BuildListenerVolumeSnafu)?,
                )
                .build(),
        ),
        HbaseRole::RestServer => None,
    };
    Ok(volume)
}

/// The listener [`PersistentVolumeClaim`] template for the rest servers, or `None` for the masters
/// and region servers (which use an ephemeral [`Volume`] instead, see [`build_listener_volume`]).
pub fn build_listener_pvc(
    role: &HbaseRole,
    merged_config: &AnyServiceConfig,
    recommended_labels: &Labels,
) -> Result<Option<Vec<PersistentVolumeClaim>>> {
    let pvc = match role {
        HbaseRole::Master | HbaseRole::RegionServer => None,
        HbaseRole::RestServer => Some(vec![
            ListenerOperatorVolumeSourceBuilder::new(
                &ListenerReference::ListenerClass(merged_config.listener_class().to_string()),
                recommended_labels,
            )
            .build_pvc(LISTENER_VOLUME_NAME.to_string())
            .context(BuildListenerPvcSnafu)?,
        ]),
    };
    Ok(pvc)
}
