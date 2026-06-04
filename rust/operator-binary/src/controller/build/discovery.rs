//! Build the discovery `ConfigMap` for the HbaseCluster.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    kube::runtime::reflector::ObjectRef,
};

use crate::{
    config::writer::to_hadoop_xml,
    controller::build::properties::ConfigFileName,
    crd::{HbaseRole, v1alpha1},
    hbase_controller::{ValidatedCluster, build_recommended_labels},
};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object {hbase} is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
        hbase: ObjectRef<v1alpha1::HbaseCluster>,
    },

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
///
/// The rendered content comes entirely from `cluster`; `owner_ref` is retained only for the
/// ConfigMap ObjectMeta / owner reference.
pub fn build_discovery_config_map(
    cluster: &ValidatedCluster,
    owner_ref: &v1alpha1::HbaseCluster,
) -> Result<ConfigMap> {
    let cluster_config = &cluster.cluster_config;

    let mut hbase_site = cluster_config
        .zookeeper_connection_information
        .as_hbase_settings();
    hbase_site.extend(cluster_config.discovery_kerberos_config.clone());

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(owner_ref)
                .ownerreference_from_resource(owner_ref, None, Some(true))
                .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
                    hbase: ObjectRef::from_obj(owner_ref),
                })?
                .with_recommended_labels(&build_recommended_labels(
                    owner_ref,
                    &cluster.image.app_version_label_value,
                    &HbaseRole::RegionServer.to_string(),
                    "discovery",
                ))
                .context(ObjectMetaSnafu)?
                .build(),
        )
        .add_data(
            ConfigFileName::HbaseSite.to_string(),
            to_hadoop_xml(
                hbase_site
                    .into_iter()
                    .map(|(k, v)| (k, Some(v)))
                    .collect::<BTreeMap<_, _>>()
                    .iter(),
            ),
        )
        .build()
        .context(BuildConfigMapSnafu)
}
