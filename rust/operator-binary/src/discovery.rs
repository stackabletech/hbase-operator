use std::collections::BTreeMap;

use crate::{
    hbase_controller::build_recommended_labels, zookeeper::ZookeeperConnectionInformation,
};
use stackable_hbase_crd::{HbaseCluster, HbaseRole, HBASE_SITE_XML};
use stackable_operator::{
    builder::{ConfigMapBuilder, ObjectMetaBuilder},
    commons::product_image_selection::ResolvedProductImage,
    error::OperatorResult,
    k8s_openapi::api::core::v1::ConfigMap,
};

/// Creates a discovery config map containing the `hbase-site.xml` for clients.
pub fn build_discovery_configmap(
    hbase: &HbaseCluster,
    zookeeper_connection_information: &ZookeeperConnectionInformation,
    resolved_product_image: &ResolvedProductImage,
) -> OperatorResult<ConfigMap> {
    let hbase_site = zookeeper_connection_information.as_hbase_settings();

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(hbase)
                .ownerreference_from_resource(hbase, None, Some(true))?
                .with_recommended_labels(build_recommended_labels(
                    hbase,
                    &resolved_product_image.app_version_label,
                    &HbaseRole::RegionServer.to_string(),
                    "discovery",
                ))
                .build(),
        )
        .add_data(
            HBASE_SITE_XML,
            product_config::writer::to_hadoop_xml(
                hbase_site
                    .into_iter()
                    .map(|(k, v)| (k, Some(v)))
                    .collect::<BTreeMap<_, _>>()
                    .iter(),
            ),
        )
        .build()
}
