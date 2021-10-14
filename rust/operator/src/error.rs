use std::num::ParseIntError;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Error with HDFS connection. Could not retrieve the HDFS connection. This is a bug. Please open a ticket.")]
    HdfsConnectionInformationError,

    #[error("Error from HDFS: {source}")]
    HdfsError {
        #[from]
        source: stackable_hdfs_crd::error::Error,
    },

    #[error(
        "ConfigMap of type [{cm_type}] is for pod with generate_name [{pod_name}] is missing."
    )]
    MissingConfigMapError {
        cm_type: &'static str,
        pod_name: String,
    },

    #[error("ConfigMap of type [{cm_type}] is missing the metadata.name. Maybe the config map was not created yet?")]
    MissingConfigMapNameError { cm_type: &'static str },

    #[error("Kubernetes reported error: {source}")]
    KubeError {
        #[from]
        source: kube::Error,
    },

    #[error("Error from Operator framework: {source}")]
    OperatorError {
        #[from]
        source: stackable_operator::error::Error,
    },

    #[error("Error from serde_json: {source}")]
    SerdeError {
        #[from]
        source: serde_json::Error,
    },

    #[error("Pod contains invalid id: {source}")]
    InvalidId {
        #[from]
        source: ParseIntError,
    },

    #[error("Error creating properties file")]
    PropertiesError(#[from] product_config::writer::PropertiesWriterError),

    #[error("ProductConfig Framework reported error: {source}")]
    ProductConfigError {
        #[from]
        source: product_config::error::Error,
    },

    #[error("Operator Framework reported config error: {source}")]
    OperatorConfigError {
        #[from]
        source: stackable_operator::product_config_utils::ConfigError,
    },

    #[error("ParserError: {source}")]
    StrumParseError {
        #[from]
        source: strum::ParseError,
    },

    #[error("Error from ZooKeeper: {source}")]
    ZookeeperError {
        #[from]
        source: stackable_zookeeper_crd::error::Error,
    },

    #[error("Error with ZooKeeper connection. Could not retrieve the ZooKeeper connection. This is a bug. Please open a ticket.")]
    ZookeeperConnectionInformationError,
}
