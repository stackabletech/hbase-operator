//! Build the discovery `ConfigMap` for the HbaseCluster.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder, k8s_openapi::api::core::v1::ConfigMap,
    utils::cluster_info::KubernetesClusterInfo, v2::config_file_writer::to_hadoop_xml,
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{PLACEHOLDER_DISCOVERY_ROLE_GROUP, kerberos, properties::ConfigFileName},
    },
    crd::HbaseRole,
};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
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
            // The discovery `ConfigMap` is a cluster-wide object (not tied to a single role
            // group), so it is named after the cluster and labelled with the region-server role
            // and a `discovery` placeholder role-group.
            cluster
                .object_meta(
                    cluster.name.to_string(),
                    &HbaseRole::RegionServer,
                    &PLACEHOLDER_DISCOVERY_ROLE_GROUP,
                )
                .build(),
        )
        .add_data(
            ConfigFileName::HbaseSite.to_string(),
            to_hadoop_xml(hbase_site.iter()),
        )
        .build()
        .context(BuildConfigMapSnafu)
}
