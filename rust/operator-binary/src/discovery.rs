use crate::hbase_controller::build_recommended_labels;
use stackable_hbase_crd::{HbaseCluster, HbaseRole, HBASE_SITE_XML, HBASE_ZOOKEEPER_QUORUM};
use stackable_operator::{
    builder::{ConfigMapBuilder, ObjectMetaBuilder},
    commons::product_image_selection::ResolvedProductImage,
    error::OperatorResult,
    k8s_openapi::api::core::v1::ConfigMap,
};
use std::collections::HashMap;

/// Creates a discovery config map containing the `hbase-site.xml` for clients.
pub fn build_discovery_configmap(
    hbase: &HbaseCluster,
    zookeeper_connect_string: &str,
    resolved_product_image: &ResolvedProductImage,
) -> OperatorResult<ConfigMap> {
    let hbase_site_data: HashMap<String, Option<String>> = [(
        HBASE_ZOOKEEPER_QUORUM.to_string(),
        Some(zookeeper_connect_string.to_string()),
    )]
    .into();

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
            stackable_operator::product_config::writer::to_hadoop_xml(hbase_site_data.iter()),
        )
        .build()
}
