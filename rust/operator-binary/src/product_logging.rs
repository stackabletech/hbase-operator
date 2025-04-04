use snafu::Snafu;
use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    memory::BinaryMultiple,
    product_logging::{
        self,
        spec::{
            AutomaticContainerLogConfig, ContainerLogConfig, ContainerLogConfigChoice, Logging,
        },
    },
    role_utils::RoleGroupRef,
};

use crate::{
    crd::{v1alpha1, Container},
    hbase_controller::MAX_HBASE_LOG_FILES_SIZE,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("failed to retrieve the ConfigMap [{cm_name}]"))]
    ConfigMapNotFound {
        source: stackable_operator::client::Error,
        cm_name: String,
    },

    #[snafu(display("failed to retrieve the entry [{entry}] for ConfigMap [{cm_name}]"))]
    MissingConfigMapEntry {
        entry: &'static str,
        cm_name: String,
    },

    #[snafu(display("crd validation failure"))]
    CrdValidationFailure { source: crate::crd::Error },

    #[snafu(display("vectorAggregatorConfigMapName must be set"))]
    MissingVectorAggregatorAddress,
}

type Result<T, E = Error> = std::result::Result<T, E>;

const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} %-5p [%t] %c{2}: %.1000m%n";
const HBASE_LOG4J_FILE: &str = "hbase.log4j.xml";
const HBASE_LOG4J2_FILE: &str = "hbase.log4j2.xml";
pub const LOG4J_CONFIG_FILE: &str = "log4j.properties";
pub const LOG4J2_CONFIG_FILE: &str = "log4j2.properties";
pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
pub static CONTAINERDEBUG_LOG_DIRECTORY: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| format!("{STACKABLE_LOG_DIR}/containerdebug"));

/// Extend the role group ConfigMap with logging and Vector configurations
pub fn extend_role_group_config_map(
    rolegroup: &RoleGroupRef<v1alpha1::HbaseCluster>,
    logging: &Logging<Container>,
    cm_builder: &mut ConfigMapBuilder,
    hbase_version: &str,
) -> Result<()> {
    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&Container::Hbase)
    {
        cm_builder.add_data(
            log4j_properties_file_name(hbase_version),
            log4j_config(hbase_version, log_config),
        );
    }

    let vector_log_config = if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&Container::Vector)
    {
        Some(log_config)
    } else {
        None
    };

    if logging.enable_vector_agent {
        cm_builder.add_data(
            product_logging::framework::VECTOR_CONFIG_FILE,
            product_logging::framework::create_vector_config(rolegroup, vector_log_config),
        );
    }

    Ok(())
}

pub fn log4j_properties_file_name(hbase_version: &str) -> &'static str {
    if needs_log4j2(hbase_version) {
        LOG4J2_CONFIG_FILE
    } else {
        LOG4J_CONFIG_FILE
    }
}

fn log4j_config(hbase_version: &str, log_config: &AutomaticContainerLogConfig) -> String {
    if needs_log4j2(hbase_version) {
        product_logging::framework::create_log4j2_config(
            &format!("{STACKABLE_LOG_DIR}/hbase"),
            HBASE_LOG4J2_FILE,
            MAX_HBASE_LOG_FILES_SIZE
                .scale_to(BinaryMultiple::Mebi)
                .floor()
                .value as u32,
            CONSOLE_CONVERSION_PATTERN,
            log_config,
        )
    } else {
        product_logging::framework::create_log4j_config(
            &format!("{STACKABLE_LOG_DIR}/hbase"),
            HBASE_LOG4J_FILE,
            MAX_HBASE_LOG_FILES_SIZE
                .scale_to(BinaryMultiple::Mebi)
                .floor()
                .value as u32,
            CONSOLE_CONVERSION_PATTERN,
            log_config,
        )
    }
}

// HBase 2.6 moved from log4j to log4j2
fn needs_log4j2(hbase_version: &str) -> bool {
    !hbase_version.starts_with(r"2.4")
}
