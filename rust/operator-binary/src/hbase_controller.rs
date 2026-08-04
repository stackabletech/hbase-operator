//! Ensures that `Pod`s are configured and running for each [`v1alpha1::HbaseCluster`].
//!
//! This is the controller driver: it runs the
//! `dereference -> validate -> build -> apply -> update_status` pipeline. The validated cluster type and the resource builders live under the
//! [`crate::controller`] module tree; this file is kept next to `main.rs` for consistency with
//! the other Stackable operators.

use std::sync::Arc;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    cluster_resources::ClusterResourceApplyStrategy,
    kube::{
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::{
        apply::{self, Applier},
        build,
        update_status::{self, update_status},
    },
    crd::v1alpha1,
};

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub operator_environment: OperatorEnvironmentOptions,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to apply the Kubernetes resources"))]
    ApplyResources { source: apply::Error },

    #[snafu(display("failed to build cluster resources"))]
    BuildResources { source: build::Error },

    #[snafu(display("HBaseCluster object is invalid"))]
    InvalidHBaseCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to dereference cluster resources"))]
    Dereference {
        source: crate::controller::dereference::Error,
    },

    #[snafu(display("failed to validate cluster configuration"))]
    Validate {
        source: crate::controller::validate::Error,
    },

    #[snafu(display("failed to update the cluster status"))]
    UpdateStatus { source: update_status::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_hbase(
    hbase: Arc<DeserializeGuard<v1alpha1::HbaseCluster>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let hbase = hbase
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidHBaseClusterSnafu)?;

    let client = &ctx.client;

    let dereferenced_objects = crate::controller::dereference::dereference(client, hbase)
        .await
        .context(DereferenceSnafu)?;

    let validated_cluster = crate::controller::validate::validate_cluster(
        hbase,
        &ctx.operator_environment.image_repository,
        dereferenced_objects,
    )
    .context(ValidateSnafu)?;

    let resources = build::build(&validated_cluster, &client.kubernetes_cluster_info)
        .context(BuildResourcesSnafu)?;

    let applied = Applier::new(
        client,
        &validated_cluster,
        ClusterResourceApplyStrategy::from(&hbase.spec.cluster_operation),
        &hbase.spec.object_overrides,
    )
    .apply(resources)
    .await
    .context(ApplyResourcesSnafu)?;

    update_status(client, hbase, &applied)
        .await
        .context(UpdateStatusSnafu)?;

    Ok(Action::await_change())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::HbaseCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        // root object is invalid, will be requeued when modified
        Error::InvalidHBaseCluster { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}
