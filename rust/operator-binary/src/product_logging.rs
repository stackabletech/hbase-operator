use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hbase_crd::{Container, HbaseCluster};
use stackable_operator::{
    builder::ConfigMapBuilder,
    client::Client,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::ResourceExt,
    product_logging::{
        self,
        spec::{
            AutomaticContainerLogConfig, ContainerLogConfig, ContainerLogConfigChoice, Logging,
        },
    },
    role_utils::RoleGroupRef,
};

use crate::hbase_controller::{MAX_HBASE_LOG_FILES_SIZE_IN_MIB, STACKABLE_LOG_DIR};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("failed to retrieve the ConfigMap [{cm_name}]"))]
    ConfigMapNotFound {
        source: stackable_operator::error::Error,
        cm_name: String,
    },
    #[snafu(display("failed to retrieve the entry [{entry}] for ConfigMap [{cm_name}]"))]
    MissingConfigMapEntry {
        entry: &'static str,
        cm_name: String,
    },
    #[snafu(display("crd validation failure"))]
    CrdValidationFailure { source: stackable_hbase_crd::Error },
    #[snafu(display("vectorAggregatorConfigMapName must be set"))]
    MissingVectorAggregatorAddress,
}

type Result<T, E = Error> = std::result::Result<T, E>;

const VECTOR_AGGREGATOR_CM_ENTRY: &str = "ADDRESS";
const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} %-5p [%t] %c{2}: %.1000m%n";

// TODO: Move to operator-rs when other products switch from log4j1 to log4j2.
#[derive(Debug, PartialEq, Eq)]
pub enum Log4JVersion {
    Log4J1,
    Log4J2,
}

impl Log4JVersion {
    fn config_file_name(&self) -> &str {
        match &self {
            Log4JVersion::Log4J1 => "log4j.properties",
            Log4JVersion::Log4J2 => "log4j2.properties",
        }
    }

    fn log_file_name(&self) -> &str {
        match &self {
            Log4JVersion::Log4J1 => "hbase.log4j.xml",
            Log4JVersion::Log4J2 => "hbase.log4j2.xml",
        }
    }

    fn create_log_config(
        &self,
        log_dir: &str,
        max_size_in_mib: u32,
        console_conversion_pattern: &str,
        config: &AutomaticContainerLogConfig,
    ) -> String {
        match &self {
            Log4JVersion::Log4J1 => product_logging::framework::create_log4j_config(
                log_dir,
                self.log_file_name(),
                max_size_in_mib,
                console_conversion_pattern,
                config,
            ),
            Log4JVersion::Log4J2 => product_logging::framework::create_log4j2_config(
                log_dir,
                self.log_file_name(),
                max_size_in_mib,
                console_conversion_pattern,
                config,
            ),
        }
    }
}

fn calculate_log4j_version(product_version: &str) -> Log4JVersion {
    // The first version we support is 2.4.x, so we don't need to care about versions below that
    if product_version.starts_with("2.4.") {
        Log4JVersion::Log4J1
    } else {
        Log4JVersion::Log4J2
    }
}

/// Return the address of the Vector aggregator if the corresponding ConfigMap name is given in the
/// cluster spec
pub async fn resolve_vector_aggregator_address(
    hbase: &HbaseCluster,
    client: &Client,
) -> Result<Option<String>> {
    let vector_aggregator_address = if let Some(vector_aggregator_config_map_name) =
        &hbase.spec.cluster_config.vector_aggregator_config_map_name
    {
        let vector_aggregator_address = client
            .get::<ConfigMap>(
                vector_aggregator_config_map_name,
                hbase
                    .namespace()
                    .as_deref()
                    .context(ObjectHasNoNamespaceSnafu)?,
            )
            .await
            .context(ConfigMapNotFoundSnafu {
                cm_name: vector_aggregator_config_map_name.to_string(),
            })?
            .data
            .and_then(|mut data| data.remove(VECTOR_AGGREGATOR_CM_ENTRY))
            .context(MissingConfigMapEntrySnafu {
                entry: VECTOR_AGGREGATOR_CM_ENTRY,
                cm_name: vector_aggregator_config_map_name.to_string(),
            })?;
        Some(vector_aggregator_address)
    } else {
        None
    };

    Ok(vector_aggregator_address)
}

/// Extend the role group ConfigMap with logging and Vector configurations
pub fn extend_role_group_config_map(
    product_version: &str,
    rolegroup: &RoleGroupRef<HbaseCluster>,
    vector_aggregator_address: Option<&str>,
    logging: &Logging<Container>,
    cm_builder: &mut ConfigMapBuilder,
) -> Result<()> {
    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&Container::Hbase)
    {
        let log4j_version = calculate_log4j_version(product_version);
        cm_builder.add_data(
            log4j_version.config_file_name(),
            log4j_version.create_log_config(
                &format!("{STACKABLE_LOG_DIR}/hbase"),
                MAX_HBASE_LOG_FILES_SIZE_IN_MIB,
                CONSOLE_CONVERSION_PATTERN,
                log_config,
            ),
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
            product_logging::framework::create_vector_config(
                rolegroup,
                vector_aggregator_address.context(MissingVectorAggregatorAddressSnafu)?,
                vector_log_config,
            ),
        );
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_calculate_log4j_version() {
        assert_eq!(calculate_log4j_version("2.4.0"), Log4JVersion::Log4J1);
        assert_eq!(calculate_log4j_version("2.4.17"), Log4JVersion::Log4J1);
        assert_eq!(calculate_log4j_version("2.5.0"), Log4JVersion::Log4J2);
        assert_eq!(calculate_log4j_version("2.5.1"), Log4JVersion::Log4J2);
        assert_eq!(calculate_log4j_version("2.6.0"), Log4JVersion::Log4J2);
        assert_eq!(calculate_log4j_version("3.0.0"), Log4JVersion::Log4J2);
        assert_eq!(calculate_log4j_version("42.43.44"), Log4JVersion::Log4J2);
    }
}
