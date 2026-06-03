//! Build the per-rolegroup `ConfigMap` for the HbaseCluster.

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    role_utils::RoleGroupRef,
    utils::cluster_info::KubernetesClusterInfo,
};

use crate::{
    config::writer::PropertiesWriterError,
    controller::build::properties::{
        ConfigFileName, hbase_env, hbase_site, security_properties, ssl_client, ssl_server,
    },
    crd::{HbaseRole, v1alpha1},
    hbase_controller::{ValidatedCluster, build_recommended_labels},
    product_logging::extend_role_group_config_map,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("the validated cluster has no role group {role_group:?} for role {role:?}"))]
    MissingRoleGroup { role: String, role_group: String },

    #[snafu(display("failed to build hbase-site.xml"))]
    BuildHbaseSite { source: hbase_site::Error },

    #[snafu(display("failed to build hbase-env.sh"))]
    BuildHbaseEnv { source: hbase_env::Error },

    #[snafu(display("failed to serialize {} for {role_group}", ConfigFileName::Security))]
    JvmSecurityProperties {
        source: PropertiesWriterError,
        role_group: String,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to add the logging configuration to the ConfigMap {cm_name:?}"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },

    #[snafu(display("cannot build config map for role {role:?} and role group {role_group:?}"))]
    Assemble {
        source: stackable_operator::builder::configmap::Error,
        role: String,
        role_group: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(dead_code)]
pub fn build_rolegroup_config_map(
    hbase: &v1alpha1::HbaseCluster,
    cluster: &ValidatedCluster,
    role: &HbaseRole,
    cluster_info: &KubernetesClusterInfo,
    rolegroup_ref: &RoleGroupRef<v1alpha1::HbaseCluster>,
) -> Result<ConfigMap> {
    tracing::info!("Setting up ConfigMap for {:?}", rolegroup_ref);

    let rg = cluster
        .role_group_configs
        .get(role)
        .and_then(|groups| groups.get(&rolegroup_ref.role_group))
        .with_context(|| MissingRoleGroupSnafu {
            role: rolegroup_ref.role.clone(),
            role_group: rolegroup_ref.role_group.clone(),
        })?;

    let cluster_config = &cluster.cluster_config;
    let overrides = &rg.config_overrides;

    let hbase_site_xml = hbase_site::build(
        hbase,
        role,
        cluster_info,
        &rg.merged_config,
        cluster_config
            .zookeeper_connection_information
            .as_hbase_settings(),
        cluster_config.hbase_opa_config.as_ref(),
        overrides.hbase_site_xml.clone(),
    )
    .context(BuildHbaseSiteSnafu)?;

    let hbase_env_sh = hbase_env::build(
        hbase,
        &rg.merged_config,
        role,
        &rolegroup_ref.role_group,
        overrides.hbase_env_sh.clone(),
    )
    .context(BuildHbaseEnvSnafu)?;

    let ssl_server_xml = ssl_server::build(hbase, overrides.ssl_server_xml.clone());
    let ssl_client_xml = ssl_client::build(hbase, overrides.ssl_client_xml.clone());

    let security_properties = security_properties::build(role, overrides.security_properties.clone())
        .with_context(|_| JvmSecurityPropertiesSnafu {
            role_group: rolegroup_ref.role_group.clone(),
        })?;

    let cm_metadata = ObjectMetaBuilder::new()
        .name_and_namespace(hbase)
        .name(rolegroup_ref.object_name())
        .ownerreference_from_resource(hbase, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(&build_recommended_labels(
            hbase,
            &cluster.image.app_version_label_value,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        .context(ObjectMetaSnafu)?
        .build();

    let mut builder = ConfigMapBuilder::new();
    builder
        .metadata(cm_metadata)
        .add_data(ConfigFileName::HbaseSite.to_string(), hbase_site_xml)
        .add_data(ConfigFileName::HbaseEnv.to_string(), hbase_env_sh)
        .add_data(ConfigFileName::Security.to_string(), security_properties);

    // HBase does not like empty config files:
    // Caused by: com.ctc.wstx.exc.WstxEOFException: Unexpected EOF in prolog at [row,col,system-id]: [1,0,"file:/stackable/conf/ssl-server.xml"]
    if !ssl_server_xml.is_empty() {
        builder.add_data(ConfigFileName::SslServer.to_string(), ssl_server_xml);
    }
    if !ssl_client_xml.is_empty() {
        builder.add_data(ConfigFileName::SslClient.to_string(), ssl_client_xml);
    }

    extend_role_group_config_map(rolegroup_ref, rg.merged_config.logging(), &mut builder)
        .context(InvalidLoggingConfigSnafu {
            cm_name: rolegroup_ref.object_name(),
        })?;

    builder.build().with_context(|_| AssembleSnafu {
        role: rolegroup_ref.role.clone(),
        role_group: rolegroup_ref.role_group.clone(),
    })
}
