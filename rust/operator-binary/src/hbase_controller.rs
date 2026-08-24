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
        Resource,
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

    if hbase.meta().deletion_timestamp.is_some() {
        return Ok(Action::await_change());
    }

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

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use stackable_operator::{
        client::Client,
        kube::{Client as KubeClient, Config},
    };

    use super::*;
    use crate::test_utils;

    /// The client points at a closed port, so any API call would fail the reconciliation: an `Ok`
    /// proves that a cluster being deleted returns before the reconciler touches the Kubernetes
    /// API, and because the spec is invalid, before the [`DeserializeGuard`] is unwrapped.
    #[tokio::test]
    async fn reconcile_exits_early_for_deleted_cluster() {
        let hbase = serde_yaml::from_str(indoc! {r#"
            ---
            apiVersion: hbase.stackable.tech/v1alpha1
            kind: HbaseCluster
            metadata:
              name: hbase
              namespace: default
              deletionTimestamp: "2026-08-14T12:00:00Z"
            spec: {}
        "#})
        .expect("YAML parses; the invalid spec is captured inside the DeserializeGuard");

        let ctx = Arc::new(Ctx {
            client: Client::new(
                KubeClient::try_from(Config::new(
                    "http://127.0.0.1:1".parse().expect("valid static URI"),
                ))
                .expect("client from static config"),
                None,
                "default".to_owned(),
                test_utils::cluster_info(),
            ),
            operator_environment: OperatorEnvironmentOptions {
                operator_namespace: "stackable-operators".to_owned(),
                operator_service_name: "hbase-operator".to_owned(),
                image_repository: "oci.stackable.tech/sdp".to_owned(),
            },
        });

        let action = reconcile_hbase(Arc::new(hbase), ctx)
            .await
            .expect("a deleted cluster reconciles without any API call");

        assert_eq!(action, Action::await_change());
    }
}
