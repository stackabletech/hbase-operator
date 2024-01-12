use std::collections::BTreeMap;

use product_config::writer::to_hadoop_xml;
use snafu::{ResultExt, Snafu};
use stackable_hbase_crd::{HbaseCluster, HbaseRole, HBASE_SITE_XML};
use stackable_operator::{
    builder::{ConfigMapBuilder, ObjectMetaBuilder, ObjectMetaBuilderError},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::runtime::reflector::ObjectRef,
};

use crate::{
    hbase_controller::build_recommended_labels, zookeeper::ZookeeperConnectionInformation,
};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object {hbase} is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
        hbase: ObjectRef<HbaseCluster>,
    },

    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::error::Error,
    },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta { source: ObjectMetaBuilderError },
}

/// Creates a discovery config map containing the `hbase-site.xml` for clients.
pub fn build_discovery_configmap(
    hbase: &HbaseCluster,
    zookeeper_connection_information: &ZookeeperConnectionInformation,
    resolved_product_image: &ResolvedProductImage,
) -> Result<ConfigMap> {
    let hbase_site = zookeeper_connection_information.as_hbase_settings();

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(hbase)
                .ownerreference_from_resource(hbase, None, Some(true))
                .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
                    hbase: ObjectRef::from_obj(hbase),
                })?
                .with_recommended_labels(build_recommended_labels(
                    hbase,
                    &resolved_product_image.app_version_label,
                    &HbaseRole::RegionServer.to_string(),
                    "discovery",
                ))
                .context(ObjectMetaSnafu)?
                .build(),
        )
        .add_data(
            HBASE_SITE_XML,
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
