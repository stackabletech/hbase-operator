use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging::{
        self,
        spec::{
            AutomaticContainerLogConfig, ContainerLogConfig, ContainerLogConfigChoice, Logging,
        },
    },
    role_utils::RoleGroupRef,
};

use crate::{
    controller::build::properties::ConfigFileName,
    crd::{Container, v1alpha1},
};

pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
pub const MAX_HBASE_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} %-5p [%t] %c{2}: %.1000m%n";
const HBASE_LOG4J2_FILE: &str = "hbase.log4j2.xml";

/// Extend the role group ConfigMap with logging and Vector configurations
pub fn extend_role_group_config_map(
    rolegroup: &RoleGroupRef<v1alpha1::HbaseCluster>,
    logging: &Logging<Container>,
    cm_builder: &mut ConfigMapBuilder,
) {
    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&Container::Hbase)
    {
        cm_builder.add_data(ConfigFileName::Log4j2.to_string(), log4j_config(log_config));
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
}

fn log4j_config(log_config: &AutomaticContainerLogConfig) -> String {
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
}
