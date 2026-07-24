//! Ensures that `Pod`s are configured and running for each [`v1alpha1::HbaseCluster`].
//!
//! This is the controller driver: it runs the `dereference -> validate -> build -> apply`
//! pipeline. The validated cluster type and the resource builders live under the
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
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    v2::cluster_resources::cluster_resources_new,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::{build, controller_name, operator_name, product_name},
    crd::{HbaseClusterStatus, OPERATOR_NAME, v1alpha1},
};

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub operator_environment: OperatorEnvironmentOptions,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build cluster resources"))]
    BuildResources { source: build::Error },

    #[snafu(display("failed to apply cluster resource"))]
    ApplyResource {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

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

    let mut cluster_resources = cluster_resources_new(
        &product_name(),
        &operator_name(),
        &controller_name(),
        &validated_cluster.name,
        &validated_cluster.namespace,
        &validated_cluster.uid,
        ClusterResourceApplyStrategy::from(&hbase.spec.cluster_operation),
        &hbase.spec.object_overrides,
    );

    let resources = build::build(&validated_cluster, &client.kubernetes_cluster_info)
        .context(BuildResourcesSnafu)?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    // Apply order: everything before the StatefulSets, StatefulSets last. A changed ConfigMap or
    // Secret a Pod mounts must exist before the Pod restarts, otherwise the Pod restarts again
    // unnecessarily. See https://github.com/stackabletech/commons-operator/issues/111 for details.
    for service_account in resources.service_accounts {
        cluster_resources
            .add(client, service_account)
            .await
            .context(ApplyResourceSnafu)?;
    }
    for role_binding in resources.role_bindings {
        cluster_resources
            .add(client, role_binding)
            .await
            .context(ApplyResourceSnafu)?;
    }
    for service in resources.services {
        cluster_resources
            .add(client, service)
            .await
            .context(ApplyResourceSnafu)?;
    }
    for config_map in resources.config_maps {
        cluster_resources
            .add(client, config_map)
            .await
            .context(ApplyResourceSnafu)?;
    }
    for pdb in resources.pod_disruption_budgets {
        cluster_resources
            .add(client, pdb)
            .await
            .context(ApplyResourceSnafu)?;
    }
    for statefulset in resources.stateful_sets {
        ss_cond_builder.add(
            cluster_resources
                .add(client, statefulset)
                .await
                .context(ApplyResourceSnafu)?,
        );
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&hbase.spec.cluster_operation);

    let status = HbaseClusterStatus {
        conditions: compute_conditions(hbase, &[&ss_cond_builder, &cluster_operation_cond_builder]),
    };

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;
    client
        .apply_patch_status(OPERATOR_NAME, hbase, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::await_change())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::HbaseCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        // root object is invalid, will be requed when modified
        Error::InvalidHBaseCluster { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}
