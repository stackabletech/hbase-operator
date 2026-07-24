//! Build the per-rolegroup `ConfigMap` for the HbaseCluster.

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{config_file_writer::PropertiesWriterError, types::operator::RoleGroupName},
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{
            jvm::construct_role_specific_non_heap_jvm_args,
            kerberos,
            properties::{
                ConfigFileName, hbase_env, hbase_site, product_logging, security_properties,
                ssl_client, ssl_server,
            },
        },
    },
    crd::HbaseRole,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("the validated cluster has no role group {role_group:?} for role {role:?}"))]
    MissingRoleGroup { role: String, role_group: String },

    #[snafu(display("failed to build hbase-env.sh"))]
    BuildHbaseEnv { source: hbase_env::Error },

    #[snafu(display("failed to serialize {} for {role_group}", ConfigFileName::Security))]
    JvmSecurityProperties {
        source: PropertiesWriterError,
        role_group: String,
    },

    #[snafu(display("cannot build config map for role {role:?} and role group {role_group:?}"))]
    Assemble {
        source: stackable_operator::builder::configmap::Error,
        role: String,
        role_group: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub fn build_rolegroup_config_map(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
    role: &HbaseRole,
    role_group_name: &RoleGroupName,
) -> Result<ConfigMap> {
    tracing::info!(
        "Setting up ConfigMap for {role}/{role_group_name}",
        role = role.to_string()
    );

    let rg = cluster
        .role_group_configs
        .get(role)
        .and_then(|groups| groups.get(role_group_name))
        .with_context(|| MissingRoleGroupSnafu {
            role: role.to_string(),
            role_group: role_group_name.to_string(),
        })?;

    let cluster_config = &cluster.cluster_config;
    let merged_config = &rg.config.config;
    let overrides = &rg.config_overrides;

    let hbase_site_xml = hbase_site::build(
        role,
        merged_config,
        cluster_config
            .zookeeper_connection_information
            .as_hbase_settings(),
        kerberos::hbase_site_kerberos_config(cluster, cluster_info),
        cluster_config.hbase_opa_config.as_ref(),
        overrides.hbase_site_xml.clone(),
    );

    let hbase_env_sh = hbase_env::build(
        merged_config,
        role,
        cluster.has_kerberos_enabled(),
        construct_role_specific_non_heap_jvm_args(cluster, rg),
        overrides.hbase_env_sh.clone(),
    )
    .context(BuildHbaseEnvSnafu)?;

    let ssl_server_xml = ssl_server::build(
        kerberos::ssl_server_settings(cluster),
        overrides.ssl_server_xml.clone(),
    );
    let ssl_client_xml = ssl_client::build(
        kerberos::ssl_client_settings(cluster),
        overrides.ssl_client_xml.clone(),
    );

    let security_properties =
        security_properties::build(role, overrides.security_properties.clone()).with_context(
            |_| JvmSecurityPropertiesSnafu {
                role_group: role_group_name.to_string(),
            },
        )?;

    let cm_metadata = cluster
        .object_meta(
            cluster
                .role_group_resource_names(role, role_group_name)
                .role_group_config_map()
                .to_string(),
            role,
            role_group_name,
        )
        .build();

    let mut builder = ConfigMapBuilder::new();
    builder
        .metadata(cm_metadata)
        .add_data(ConfigFileName::HbaseSite.to_string(), hbase_site_xml)
        .add_data(ConfigFileName::HbaseEnv.to_string(), hbase_env_sh)
        .add_data(ConfigFileName::Security.to_string(), security_properties);

    // HBase does not like empty config files, so the ssl-*.xml files are only
    // written when they actually carry settings (see `ssl_server::build`):
    // Caused by: com.ctc.wstx.exc.WstxEOFException: Unexpected EOF in prolog at [row,col,system-id]: [1,0,"file:/stackable/conf/ssl-server.xml"]
    if let Some(ssl_server_xml) = ssl_server_xml {
        builder.add_data(ConfigFileName::SslServer.to_string(), ssl_server_xml);
    }
    if let Some(ssl_client_xml) = ssl_client_xml {
        builder.add_data(ConfigFileName::SslClient.to_string(), ssl_client_xml);
    }

    if let Some(log4j2_properties) =
        product_logging::build_log4j2(&rg.config.logging.hbase_container)
    {
        builder.add_data(ConfigFileName::Log4j2.to_string(), log4j2_properties);
    }
    if rg.config.logging.enable_vector_agent {
        builder.add_data(
            VECTOR_CONFIG_FILE,
            product_logging::vector_config_file_content(),
        );
    }

    builder.build().with_context(|_| AssembleSnafu {
        role: role.to_string(),
        role_group: role_group_name.to_string(),
    })
}
