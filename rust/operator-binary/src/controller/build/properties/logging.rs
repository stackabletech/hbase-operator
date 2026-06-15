use stackable_operator::{
    kube::runtime::reflector::ObjectRef,
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging::{
        self,
        spec::{
            AutomaticContainerLogConfig, ContainerLogConfig, ContainerLogConfigChoice, Logging,
        },
    },
    role_utils::RoleGroupRef,
    v2::types::operator::RoleGroupName,
};

use crate::{
    controller::ValidatedCluster,
    crd::{Container, HbaseRole},
};

pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
pub const MAX_HBASE_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} %-5p [%t] %c{2}: %.1000m%n";
const HBASE_LOG4J2_FILE: &str = "hbase.log4j2.xml";

/// Renders `log4j2.properties` for the HBase container.
///
/// Returns `None` when the HBase container does not use the operator's automatic logging
/// configuration (e.g. a custom log ConfigMap is referenced instead), in which case no
/// `log4j2.properties` should be added to the rolegroup `ConfigMap`.
pub fn build_log4j2(logging: &Logging<Container>) -> Option<String> {
    match logging.containers.get(&Container::Hbase) {
        Some(ContainerLogConfig {
            choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
        }) => Some(log4j_config(log_config)),
        _ => None,
    }
}

/// Renders the Vector agent config (`vector.yaml`).
///
/// Returns `None` when the Vector agent is disabled for this role group.
pub fn build_vector_config(
    cluster: &ValidatedCluster,
    role: &HbaseRole,
    role_group_name: &RoleGroupName,
    logging: &Logging<Container>,
) -> Option<String> {
    if !logging.enable_vector_agent {
        return None;
    }

    let vector_log_config = match logging.containers.get(&Container::Vector) {
        Some(ContainerLogConfig {
            choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
        }) => Some(log_config),
        _ => None,
    };

    let rolegroup = RoleGroupRef {
        cluster: ObjectRef::from_obj(cluster),
        role: role.to_string(),
        role_group: role_group_name.to_string(),
    };

    Some(product_logging::framework::create_vector_config(
        &rolegroup,
        vector_log_config,
    ))
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
