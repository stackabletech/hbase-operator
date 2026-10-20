//! Build the per-rolegroup `ConfigMap` for the HbaseCluster.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder, k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE, v2::config_file_writer::PropertiesWriterError,
};

use crate::controller::build::{
    jvm::construct_role_specific_non_heap_jvm_args,
    kerberos, object_meta,
    properties::{
        ConfigFileName, hbase_env, hbase_site, product_logging, security_properties, ssl_client,
        ssl_server,
    },
    recommended_labels_for_role_group_resources,
    role_group::{RoleGroupBuilder, RoleGroupInputs},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build hbase-env.sh"))]
    BuildHbaseEnv { source: hbase_env::Error },

    #[snafu(display("failed to serialize {}", ConfigFileName::Security))]
    JvmSecurityProperties { source: PropertiesWriterError },

    #[snafu(display("failed to assemble the ConfigMap"))]
    Assemble {
        source: stackable_operator::builder::configmap::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub fn build_config_map(builder: &RoleGroupBuilder) -> Result<ConfigMap> {
    let cluster = builder.cluster;
    let role = &builder.role;
    let role_group_name = &builder.role_group_name;

    tracing::info!(
        "Setting up ConfigMap for {role}/{role_group_name}",
        role = role.to_string()
    );

    let data = builder.config_map_data.clone();

    let cm_metadata = object_meta(
        cluster,
        cluster
            .role_group_resource_names(role, role_group_name)
            .role_group_config_map()
            .to_string(),
        recommended_labels_for_role_group_resources(cluster, role, role_group_name),
    )
    .build();

    ConfigMapBuilder::new()
        .metadata(cm_metadata)
        .data(data)
        .build()
        .context(AssembleSnafu)
}

/// Renders the role group's config files, keyed by file name.
///
/// The `ssl-*.xml` files are present only when they carry settings and `log4j2.properties` only
/// for the automatic log config, so the set of keys is part of what this renders.
pub(crate) fn config_map_data(inputs: &RoleGroupInputs) -> Result<BTreeMap<String, String>> {
    let cluster = inputs.cluster;
    let cluster_config = &cluster.cluster_config;

    let hbase_site_xml = hbase_site::build(
        &inputs.hbase_rootdir,
        inputs.role_hbase_site.clone(),
        cluster_config
            .zookeeper_connection_information
            .as_hbase_settings(),
        kerberos::hbase_site_kerberos_config(cluster, inputs.cluster_info),
        cluster_config.hbase_opa_config.as_ref(),
        hbase_site::client_bootstrap_servers(cluster, inputs.cluster_info),
        inputs.config_overrides.hbase_site_xml.clone(),
    );

    let hbase_env_sh = hbase_env::build(
        &inputs.resources,
        &inputs.hbase_env_opts_variable,
        cluster.has_kerberos_enabled(),
        construct_role_specific_non_heap_jvm_args(cluster, &inputs.jvm_argument_overrides),
        inputs.config_overrides.hbase_env_sh.clone(),
    )
    .context(BuildHbaseEnvSnafu)?;

    let ssl_server_xml = ssl_server::build(
        kerberos::ssl_server_settings(cluster),
        inputs.config_overrides.ssl_server_xml.clone(),
    );
    let ssl_client_xml = ssl_client::build(
        kerberos::ssl_client_settings(cluster),
        inputs.config_overrides.ssl_client_xml.clone(),
    );

    let security_properties = security_properties::build(
        inputs.dns_cache_ttl_seconds,
        inputs.config_overrides.security_properties.clone(),
    )
    .context(JvmSecurityPropertiesSnafu)?;

    let mut data = BTreeMap::from([
        (ConfigFileName::HbaseSite.to_string(), hbase_site_xml),
        (ConfigFileName::HbaseEnv.to_string(), hbase_env_sh),
        (ConfigFileName::Security.to_string(), security_properties),
    ]);

    // HBase does not like empty config files, so the ssl-*.xml files are only
    // written when they actually carry settings (see `ssl_server::build`):
    // Caused by: com.ctc.wstx.exc.WstxEOFException: Unexpected EOF in prolog
    // at [row,col,system-id]: [1,0,"file:/stackable/conf/ssl-server.xml"]
    if let Some(ssl_server_xml) = ssl_server_xml {
        data.insert(ConfigFileName::SslServer.to_string(), ssl_server_xml);
    }
    if let Some(ssl_client_xml) = ssl_client_xml {
        data.insert(ConfigFileName::SslClient.to_string(), ssl_client_xml);
    }

    if let Some(log4j2_properties) = product_logging::build_log4j2(&inputs.logging.hbase_container)
    {
        data.insert(ConfigFileName::Log4j2.to_string(), log4j2_properties);
    }
    if inputs.logging.enable_vector_agent {
        data.insert(
            VECTOR_CONFIG_FILE.to_string(),
            product_logging::vector_config_file_content(),
        );
    }

    Ok(data)
}
