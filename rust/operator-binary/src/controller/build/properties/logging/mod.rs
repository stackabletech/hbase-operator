//! Renders the logging config files (`log4j2.properties` and the Vector agent config)
//! assembled into the rolegroup `ConfigMap`.

pub use stackable_operator::v2::product_logging::framework::STACKABLE_LOG_DIR;
use stackable_operator::{
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging::{self, spec::AutomaticContainerLogConfig},
    v2::product_logging::framework::ValidatedContainerLogConfigChoice,
};

pub const MAX_HBASE_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} %-5p [%t] %c{2}: %.1000m%n";
const HBASE_LOG4J2_FILE: &str = "hbase.log4j2.xml";

/// The Vector agent configuration (`vector.yaml`).
///
/// This is a static, env-var-parameterized file (mirroring the hive- and opensearch-operators).
/// It is validated by the `test-vector.sh` / `vector-test.yaml` harness next to this file.
const VECTOR_CONFIG: &str = include_str!("vector.yaml");

/// Returns the Vector agent config (`vector.yaml`) content.
pub fn vector_config_file_content() -> String {
    VECTOR_CONFIG.to_owned()
}

/// Renders `log4j2.properties` for the HBase container.
///
/// Returns `None` when the HBase container uses a custom log ConfigMap instead of the operator's
/// automatic logging configuration, in which case no `log4j2.properties` should be added to the
/// rolegroup `ConfigMap`.
pub fn build_log4j2(hbase_container: &ValidatedContainerLogConfigChoice) -> Option<String> {
    match hbase_container {
        ValidatedContainerLogConfigChoice::Automatic(log_config) => Some(log4j_config(log_config)),
        ValidatedContainerLogConfigChoice::Custom(_) => None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_config_file_content() {
        let content = vector_config_file_content();
        assert!(!content.is_empty());
        // A kept source must be present ...
        assert!(content.contains("files_log4j2"));
        // ... while a product-specific source we don't emit must not.
        assert!(!content.contains("files_tracing_rs"));
    }
}
