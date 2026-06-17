//! Build the discovery `ConfigMap` for the HbaseCluster.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    kube::Resource,
    kvp::ObjectLabels,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{builder::meta::ownerreference_from_resource, config_file_writer::to_hadoop_xml},
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{kerberos, properties::ConfigFileName},
    },
    crd::{APP_NAME, HbaseRole, OPERATOR_NAME},
};

// The discovery `ConfigMap` is a cluster-wide object (not tied to a single role group), so it is
// labelled with the region-server role and a `discovery` placeholder role-group.
const DISCOVERY_ROLE_GROUP: &str = "discovery";

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },
}

/// Creates a discovery config map containing the `hbase-site.xml` for clients.
pub fn build_discovery_config_map(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> Result<ConfigMap> {
    let cluster_config = &cluster.cluster_config;

    let mut hbase_site = cluster_config
        .zookeeper_connection_information
        .as_hbase_settings();
    hbase_site.extend(kerberos::discovery_kerberos_config(cluster, cluster_info));

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(cluster)
                .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
                .with_recommended_labels(&build_recommended_labels(
                    cluster,
                    &cluster.image.app_version_label_value,
                    &HbaseRole::RegionServer.to_string(),
                    DISCOVERY_ROLE_GROUP,
                ))
                .context(ObjectMetaSnafu)?
                .build(),
        )
        .add_data(
            ConfigFileName::HbaseSite.to_string(),
            to_hadoop_xml(hbase_site.iter()),
        )
        .build()
        .context(BuildConfigMapSnafu)
}

/// Recommended labels for the cluster-wide discovery `ConfigMap`.
fn build_recommended_labels<'a, R>(
    owner: &'a R,
    app_version: &'a str,
    role: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, R>
where
    R: Resource,
{
    ObjectLabels {
        owner,
        app_name: APP_NAME,
        app_version,
        operator_name: OPERATOR_NAME,
        controller_name: crate::controller::HBASE_CONTROLLER_NAME,
        role,
        role_group,
    }
}
