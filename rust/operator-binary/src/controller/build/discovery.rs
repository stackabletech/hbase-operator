//! Build the discovery `ConfigMap` for the HbaseCluster.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    v2::{builder::meta::ownerreference_from_resource, config_file_writer::to_hadoop_xml},
};

use crate::{
    controller::build::properties::ConfigFileName,
    crd::HbaseRole,
    hbase_controller::{ValidatedCluster, build_recommended_labels},
};

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
pub fn build_discovery_config_map(cluster: &ValidatedCluster) -> Result<ConfigMap> {
    let cluster_config = &cluster.cluster_config;

    let mut hbase_site = cluster_config
        .zookeeper_connection_information
        .as_hbase_settings();
    hbase_site.extend(cluster_config.discovery_kerberos_config.clone());

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(cluster)
                .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
                .with_recommended_labels(&build_recommended_labels(
                    cluster,
                    &cluster.image.app_version_label_value,
                    &HbaseRole::RegionServer.to_string(),
                    "discovery",
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
