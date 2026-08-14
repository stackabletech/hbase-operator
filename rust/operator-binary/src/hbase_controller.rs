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
    use stackable_operator::{
        client::Client,
        kube::{Client as KubeClient, Config},
    };

    use super::*;
    use crate::test_utils;

    /// A [`Ctx`] whose client points at a closed port. Any API call made through it fails the
    /// reconciliation, so an `Ok` result proves the reconciler returned before touching the
    /// Kubernetes API.
    fn unreachable_ctx() -> Arc<Ctx> {
        let config = Config::new(
            "http://127.0.0.1:1"
                .parse::<http::Uri>()
                .expect("valid static URI"),
        );
        let kube_client = KubeClient::try_from(config).expect("client from static config");

        Arc::new(Ctx {
            client: Client::new(
                kube_client,
                None,
                "default".to_owned(),
                test_utils::cluster_info(),
            ),
            operator_environment: OperatorEnvironmentOptions {
                operator_namespace: "stackable-operators".to_owned(),
                operator_service_name: "hbase-operator".to_owned(),
                image_repository: "oci.stackable.tech/sdp".to_owned(),
            },
        })
    }

    /// Drives the async reconciler from the synchronous tests used in this repo.
    /// The [`Ctx`] is built inside `block_on` because the kube client needs a running reactor
    /// already at construction time.
    fn reconcile(hbase: DeserializeGuard<v1alpha1::HbaseCluster>) -> Result<Action> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread tokio runtime")
            .block_on(async { reconcile_hbase(Arc::new(hbase), unreachable_ctx()).await })
    }

    #[test]
    fn reconcile_exits_early_for_deleted_cluster() {
        let hbase = serde_yaml::from_str(
            r#"
apiVersion: hbase.stackable.tech/v1alpha1
kind: HbaseCluster
metadata:
  name: hbase
  namespace: default
  deletionTimestamp: "2026-08-14T12:00:00Z"
spec:
  image:
    productVersion: 2.6.3
  clusterConfig:
    hdfsConfigMapName: simple-hdfs
    zookeeperConfigMapName: simple-znode
"#,
        )
        .expect("valid HbaseCluster YAML");

        let action = reconcile(hbase).expect("a deleted cluster reconciles without any API call");

        assert_eq!(action, Action::await_change());
    }

    #[test]
    fn reconcile_exits_early_for_deleted_cluster_with_invalid_spec() {
        // The spec is missing all required fields, so the DeserializeGuard captures a
        // deserialization error. During deletion the spec is irrelevant and the reconciler must
        // still exit quietly instead of erroring through the whole teardown.
        let hbase = serde_yaml::from_str(
            r#"
apiVersion: hbase.stackable.tech/v1alpha1
kind: HbaseCluster
metadata:
  name: hbase
  namespace: default
  deletionTimestamp: "2026-08-14T12:00:00Z"
spec: {}
"#,
        )
        .expect("YAML parses; the invalid spec is captured inside the DeserializeGuard");

        let action =
            reconcile(hbase).expect("a deleted cluster reconciles even when its spec is invalid");

        assert_eq!(action, Action::await_change());
    }

    #[test]
    fn reconcile_proceeds_for_live_cluster() {
        // Without a deletion timestamp the reconciler must not exit early: it proceeds to
        // dereference, which fails against the unreachable test API server.
        let hbase = serde_yaml::from_str(
            r#"
apiVersion: hbase.stackable.tech/v1alpha1
kind: HbaseCluster
metadata:
  name: hbase
  namespace: default
spec:
  image:
    productVersion: 2.6.3
  clusterConfig:
    hdfsConfigMapName: simple-hdfs
    zookeeperConfigMapName: simple-znode
"#,
        )
        .expect("valid HbaseCluster YAML");

        let result = reconcile(hbase);

        assert!(
            matches!(result, Err(Error::Dereference { .. })),
            "a live cluster must reach the API (and fail dereferencing against the unreachable \
             test server), not exit early: {result:?}"
        );
    }
}
