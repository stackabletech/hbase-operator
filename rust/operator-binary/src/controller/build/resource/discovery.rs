//! Build the discovery `ConfigMap` for the HbaseCluster.

use stackable_operator::{
    builder::configmap::ConfigMapBuilder, k8s_openapi::api::core::v1::ConfigMap,
    utils::cluster_info::KubernetesClusterInfo, v2::config_file_writer::to_hadoop_xml,
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{
            kerberos, object_meta, properties::ConfigFileName,
            recommended_labels_for_role_resources,
        },
    },
    crd::HbaseRole,
};

/// Creates a discovery config map containing the `hbase-site.xml` for clients.
pub fn build_discovery_config_map(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> ConfigMap {
    let cluster_config = &cluster.cluster_config;

    let mut hbase_site = cluster_config
        .zookeeper_connection_information
        .as_hbase_settings();
    hbase_site.extend(kerberos::discovery_kerberos_config(cluster, cluster_info));

    ConfigMapBuilder::new()
        .metadata(
            // The discovery `ConfigMap` is a cluster-wide object (not tied to
            // a single role group), so it is named after the cluster and
            // labelled with the region-server role.
            object_meta(
                cluster,
                cluster.name.to_string(),
                recommended_labels_for_role_resources(cluster, &HbaseRole::RegionServer),
            )
            .build(),
        )
        .add_data(
            ConfigFileName::HbaseSite.to_string(),
            to_hadoop_xml(hbase_site.iter()),
        )
        .build()
        .expect("The ConfigMap metadata is set in this function.")
}
