use product_config::types::PropertyNameKind;
use security::AuthenticationConfig;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::k8s_openapi::api::core::v1::PodTemplateSpec;
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        cluster_operation::ClusterOperation,
        product_image_selection::ProductImage,
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            Resources, ResourcesFragment,
        },
    },
    config::{
        fragment::{self, Fragment, ValidationError},
        merge::{Atomic, Merge},
    },
    k8s_openapi::{api::core::v1::EnvVar, apimachinery::pkg::api::resource::Quantity, DeepMerge},
    kube::{runtime::reflector::ObjectRef, CustomResource, ResourceExt},
    product_config_utils::Configuration,
    product_logging::{self, spec::Logging},
    role_utils::{GenericRoleConfig, Role, RoleGroupRef},
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
    time::Duration,
};
use std::collections::BTreeMap;
use std::collections::HashMap;
use strum::{Display, EnumIter, EnumString};

use crate::affinity::get_affinity;
use crate::security::AuthorizationConfig;

pub mod affinity;
pub mod security;

pub const APP_NAME: &str = "hbase";

pub const CONFIG_DIR_NAME: &str = "/stackable/conf";

pub const TLS_STORE_DIR: &str = "/stackable/tls";
pub const TLS_STORE_VOLUME_NAME: &str = "tls";
pub const TLS_STORE_PASSWORD: &str = "changeit";

pub const JVM_SECURITY_PROPERTIES_FILE: &str = "security.properties";

pub const HBASE_ENV_SH: &str = "hbase-env.sh";
pub const HBASE_SITE_XML: &str = "hbase-site.xml";
pub const SSL_SERVER_XML: &str = "ssl-server.xml";
pub const SSL_CLIENT_XML: &str = "ssl-client.xml";

pub const HBASE_MANAGES_ZK: &str = "HBASE_MANAGES_ZK";
pub const HBASE_MASTER_OPTS: &str = "HBASE_MASTER_OPTS";
pub const HBASE_REGIONSERVER_OPTS: &str = "HBASE_REGIONSERVER_OPTS";
pub const HBASE_REST_OPTS: &str = "HBASE_REST_OPTS";

pub const HBASE_CLUSTER_DISTRIBUTED: &str = "hbase.cluster.distributed";
pub const HBASE_ROOTDIR: &str = "hbase.rootdir";
pub const HBASE_UNSAFE_REGIONSERVER_HOSTNAME_DISABLE_MASTER_REVERSEDNS: &str =
    "hbase.unsafe.regionserver.hostname.disable.master.reversedns";
pub const HBASE_HEAPSIZE: &str = "HBASE_HEAPSIZE";
pub const HBASE_ROOT_DIR_DEFAULT: &str = "/hbase";

pub const HBASE_UI_PORT_NAME_HTTP: &str = "ui-http";
pub const HBASE_UI_PORT_NAME_HTTPS: &str = "ui-https";
pub const HBASE_REST_PORT_NAME_HTTP: &str = "rest-http";
pub const HBASE_REST_PORT_NAME_HTTPS: &str = "rest-https";
pub const METRICS_PORT_NAME: &str = "metrics";

pub const HBASE_MASTER_PORT: u16 = 16000;
// HBase always uses 16010, regardless of http or https. On 2024-01-17 we decided in Arch-meeting that we want to stick
// the port numbers to what the product is doing, so we get the least surprise for users - even when this means we have
// inconsistency between Stackable products.
pub const HBASE_MASTER_UI_PORT: u16 = 16010;
pub const HBASE_REGIONSERVER_PORT: u16 = 16020;
pub const HBASE_REGIONSERVER_UI_PORT: u16 = 16030;
pub const HBASE_REST_PORT: u16 = 8080;
pub const HBASE_REST_UI_PORT: u16 = 8085;
// This port is only used by Hbase prior to version 2.6 with a third-party JMX exporter.
// Newer versions use the same port as the UI because Hbase provides it's own metrics API
pub const METRICS_PORT: u16 = 9100;

pub const JVM_HEAP_FACTOR: f32 = 0.8;

const DEFAULT_MASTER_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_minutes_unchecked(20);
const DEFAULT_REGION_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT: Duration =
    Duration::from_minutes_unchecked(60);
const DEFAULT_REST_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_minutes_unchecked(5);

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("expected role [{expected}] but got role [{got}]"))]
    ExpectedRole { expected: String, got: String },

    #[snafu(display("the role [{role}] is invalid and does not exist in HBase"))]
    InvalidRole {
        source: strum::ParseError,
        role: String,
    },

    #[snafu(display("the HBase role [{role}] is missing from spec"))]
    MissingHbaseRole { role: String },

    #[snafu(display("the HBase role group [{role_group}] is missing from spec"))]
    MissingHbaseRoleGroup { role_group: String },

    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },

    #[snafu(display("object defines no master role"))]
    NoMasterRole,

    #[snafu(display("object defines no regionserver role"))]
    NoRegionServerRole,
}

/// An HBase cluster stacklet. This resource is managed by the Stackable operator for Apache HBase.
/// Find more information on how to use it and the resources that the operator generates in the
/// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/hbase/).
///
/// The CRD contains three roles: `masters`, `regionServers` and `restServers`.
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "hbase.stackable.tech",
    version = "v1alpha1",
    kind = "HbaseCluster",
    plural = "hbaseclusters",
    shortname = "hbase",
    status = "HbaseClusterStatus",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct HbaseClusterSpec {
    // no doc string - See ProductImage struct
    pub image: ProductImage,

    /// Configuration that applies to all roles and role groups.
    /// This includes settings for logging, ZooKeeper and HDFS connection, among other things.
    pub cluster_config: HbaseClusterConfig,

    // no doc string - See ClusterOperation struct
    #[serde(default)]
    pub cluster_operation: ClusterOperation,

    /// The HBase master process is responsible for assigning regions to region servers and
    /// manages the cluster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub masters: Option<Role<HbaseConfigFragment>>,

    /// Region servers hold the data and handle requests from clients for their region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region_servers: Option<Role<RegionServerConfigFragment>>,

    /// Rest servers provide a REST API to interact with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rest_servers: Option<Role<HbaseConfigFragment>>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HbaseClusterConfig {
    /// Name of the [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery)
    /// for an HDFS cluster.
    pub hdfs_config_map_name: String,

    /// Name of the Vector aggregator [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery).
    /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
    /// Follow the [logging tutorial](DOCS_BASE_URL_PLACEHOLDER/tutorials/logging-vector-aggregator)
    /// to learn how to configure log aggregation with Vector.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_aggregator_config_map_name: Option<String>,

    /// Name of the [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery)
    /// for a ZooKeeper cluster.
    pub zookeeper_config_map_name: String,

    /// This field controls which type of Service the Operator creates for this HbaseCluster:
    ///
    /// * cluster-internal: Use a ClusterIP service
    ///
    /// * external-unstable: Use a NodePort service
    ///
    /// This is a temporary solution with the goal to keep yaml manifests forward compatible.
    /// In the future, this setting will control which [ListenerClass](DOCS_BASE_URL_PLACEHOLDER/listener-operator/listenerclass.html)
    /// will be used to expose the service, and ListenerClass names will stay the same, allowing for a non-breaking change.
    #[serde(default)]
    pub listener_class: CurrentlySupportedListenerClasses,

    /// Settings related to user [authentication](DOCS_BASE_URL_PLACEHOLDER/usage-guide/security).
    pub authentication: Option<AuthenticationConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<AuthorizationConfig>,
}

// TODO: Temporary solution until listener-operator is finished
#[derive(Clone, Debug, Default, Display, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum CurrentlySupportedListenerClasses {
    #[default]
    #[serde(rename = "cluster-internal")]
    ClusterInternal,

    #[serde(rename = "external-unstable")]
    ExternalUnstable,
}

impl CurrentlySupportedListenerClasses {
    pub fn k8s_service_type(&self) -> String {
        match self {
            CurrentlySupportedListenerClasses::ClusterInternal => "ClusterIP".to_string(),
            CurrentlySupportedListenerClasses::ExternalUnstable => "NodePort".to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KerberosConfig {
    /// Name of the SecretClass providing the keytab for the HDFS services.
    #[serde(default = "default_kerberos_kerberos_secret_class")]
    kerberos_secret_class: String,
    /// Name of the SecretClass providing the tls certificates for the WebUIs.
    #[serde(default = "default_kerberos_tls_secret_class")]
    tls_secret_class: String,
    /// Wether a principal including the Kubernetes node name should be requested.
    /// The principal could e.g. be `HTTP/my-k8s-worker-0.mycorp.lan`.
    /// This feature is disabled by default, as the resulting principals can already by existent
    /// e.g. in Active Directory which can cause problems.
    #[serde(default)]
    request_node_principals: bool,
}

fn default_kerberos_tls_secret_class() -> String {
    "tls".to_string()
}

fn default_kerberos_kerberos_secret_class() -> String {
    "kerberos".to_string()
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    Hash,
    JsonSchema,
    PartialEq,
    Serialize,
    EnumString,
)]
pub enum HbaseRole {
    #[serde(rename = "master")]
    #[strum(serialize = "master")]
    Master,

    #[serde(rename = "regionserver")]
    #[strum(serialize = "regionserver")]
    RegionServer,

    #[serde(rename = "restserver")]
    #[strum(serialize = "restserver")]
    RestServer,
}

impl HbaseRole {
    /// Returns the name of the role as it is needed by the `bin/hbase {cli_role_name} start` command.
    pub fn cli_role_name(&self) -> String {
        match self {
            HbaseRole::Master | HbaseRole::RegionServer => self.to_string(),
            // Of course it is not called "restserver", so we need to have this match
            // instead of just letting the Display impl do it's thing ;P
            HbaseRole::RestServer => "rest".to_string(),
        }
    }
}

fn default_regionserver_config(
    cluster_name: &str,
    hdfs_discovery_cm_name: &str,
) -> RegionServerConfigFragment {
    let resources = ResourcesFragment {
        cpu: CpuLimitsFragment {
            min: Some(Quantity("250m".to_owned())),
            max: Some(Quantity("1".to_owned())),
        },
        memory: MemoryLimitsFragment {
            limit: Some(Quantity("1Gi".to_owned())),
            runtime_limits: NoRuntimeLimitsFragment {},
        },
        storage: HbaseStorageConfigFragment {},
    };

    RegionServerConfigFragment {
        hbase_rootdir: None,
        hbase_opts: None,
        resources,
        logging: product_logging::spec::default_logging(),
        affinity: get_affinity(
            cluster_name,
            &HbaseRole::RegionServer,
            hdfs_discovery_cm_name,
        ),
        graceful_shutdown_timeout: Some(DEFAULT_REGION_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT),
        graceful_shutdown_region_mover_opts: Some(CliArgList { args: vec![] }),
    }
}

fn default_rest_config(cluster_name: &str, hdfs_discovery_cm_name: &str) -> HbaseConfigFragment {
    let resources = ResourcesFragment {
        cpu: CpuLimitsFragment {
            min: Some(Quantity("100m".to_owned())),
            max: Some(Quantity("400m".to_owned())),
        },
        memory: MemoryLimitsFragment {
            limit: Some(Quantity("512Mi".to_owned())),
            runtime_limits: NoRuntimeLimitsFragment {},
        },
        storage: HbaseStorageConfigFragment {},
    };

    HbaseConfigFragment {
        hbase_rootdir: None,
        hbase_opts: None,
        resources,
        logging: product_logging::spec::default_logging(),
        affinity: get_affinity(cluster_name, &HbaseRole::RestServer, hdfs_discovery_cm_name),
        graceful_shutdown_timeout: Some(DEFAULT_REST_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT),
    }
}

fn default_master_config(cluster_name: &str, hdfs_discovery_cm_name: &str) -> HbaseConfigFragment {
    let resources = ResourcesFragment {
        cpu: CpuLimitsFragment {
            min: Some(Quantity("250m".to_owned())),
            max: Some(Quantity("1".to_owned())),
        },
        memory: MemoryLimitsFragment {
            limit: Some(Quantity("1Gi".to_owned())),
            runtime_limits: NoRuntimeLimitsFragment {},
        },
        storage: HbaseStorageConfigFragment {},
    };

    HbaseConfigFragment {
        hbase_rootdir: None,
        hbase_opts: None,
        resources,
        logging: product_logging::spec::default_logging(),
        affinity: get_affinity(cluster_name, &HbaseRole::Master, hdfs_discovery_cm_name),
        graceful_shutdown_timeout: Some(DEFAULT_MASTER_GRACEFUL_SHUTDOWN_TIMEOUT),
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Fragment)]
#[fragment_attrs(
    allow(clippy::derive_partial_eq_without_eq),
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct HbaseStorageConfig {}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    Eq,
    EnumIter,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(rename_all = "camelCase")]
pub enum Container {
    Hbase,
    Vector,
}

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct HbaseConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hbase_rootdir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hbase_opts: Option<String>,
    #[fragment_attrs(serde(default))]
    pub resources: Resources<HbaseStorageConfig, NoRuntimeLimits>,
    #[fragment_attrs(serde(default))]
    pub logging: Logging<Container>,
    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,

    /// Time period Pods have to gracefully shut down, e.g. `30m`, `1h` or `2d`. Consult the operator documentation for details.
    #[fragment_attrs(serde(default))]
    pub graceful_shutdown_timeout: Option<Duration>,
}

impl Configuration for HbaseConfigFragment {
    type Configurable = HbaseCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
        // Maps env var name to env var object. This allows env_overrides to work
        // as expected (i.e. users can override the env var value).
        let mut vars: BTreeMap<String, Option<String>> = BTreeMap::new();

        vars.insert(
            "HBASE_CONF_DIR".to_string(),
            Some(CONFIG_DIR_NAME.to_string()),
        );
        // required by phoenix (for cases where Kerberos is enabled): see https://issues.apache.org/jira/browse/PHOENIX-2369
        vars.insert(
            "HADOOP_CONF_DIR".to_string(),
            Some(CONFIG_DIR_NAME.to_string()),
        );
        Ok(vars)
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
        let mut result = BTreeMap::new();

        match file {
            HBASE_ENV_SH => {
                // The contents of this file cannot be built entirely here because we don't have
                // access to the clusterConfig or product version.
                // These are needed to set up Kerberos and JMX exporter settings.
                // To avoid fragmentation of the code needed to build this file, we moved the
                // implementation to the hbase_controller::build_hbase_env_sh() function.
            }
            HBASE_SITE_XML => {
                result.insert(
                    HBASE_CLUSTER_DISTRIBUTED.to_string(),
                    Some("true".to_string()),
                );
                result.insert(
                    HBASE_UNSAFE_REGIONSERVER_HOSTNAME_DISABLE_MASTER_REVERSEDNS.to_string(),
                    Some("true".to_string()),
                );
                result.insert(
                    HBASE_ROOTDIR.to_string(),
                    Some(
                        self.hbase_rootdir
                            .as_deref()
                            .unwrap_or(HBASE_ROOT_DIR_DEFAULT)
                            .to_string(),
                    ),
                );
            }
            _ => {}
        }

        result.retain(|_, maybe_value| maybe_value.is_some());

        Ok(result)
    }
}

#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct CliArgList {
    // todo how to serialize this properly?
    args: Vec<String>,
}

impl Atomic for CliArgList {}

impl Merge for CliArgList {
    fn merge(&mut self, other: &Self) {
        self.args.extend(other.args.clone());
    }
}

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct RegionServerConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hbase_rootdir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hbase_opts: Option<String>,
    #[fragment_attrs(serde(default))]
    pub resources: Resources<HbaseStorageConfig, NoRuntimeLimits>,
    #[fragment_attrs(serde(default))]
    pub logging: Logging<Container>,
    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,

    /// Time period Pods have to gracefully shut down, e.g. `30m`, `1h` or `2d`. Consult the operator documentation for details.
    #[fragment_attrs(serde(default))]
    pub graceful_shutdown_timeout: Option<Duration>,
    pub graceful_shutdown_region_mover_opts: CliArgList,
}

impl Configuration for RegionServerConfigFragment {
    type Configurable = HbaseCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
        // Maps env var name to env var object. This allows env_overrides to work
        // as expected (i.e. users can override the env var value).
        let mut vars: BTreeMap<String, Option<String>> = BTreeMap::new();

        vars.insert(
            "HBASE_CONF_DIR".to_string(),
            Some(CONFIG_DIR_NAME.to_string()),
        );
        // required by phoenix (for cases where Kerberos is enabled): see https://issues.apache.org/jira/browse/PHOENIX-2369
        vars.insert(
            "HADOOP_CONF_DIR".to_string(),
            Some(CONFIG_DIR_NAME.to_string()),
        );
        Ok(vars)
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
        let mut result = BTreeMap::new();

        match file {
            HBASE_ENV_SH => {
                // The contents of this file cannot be built entirely here because we don't have
                // access to the clusterConfig or product version.
                // These are needed to set up Kerberos and JMX exporter settings.
                // To avoid fragmentation of the code needed to build this file, we moved the
                // implementation to the hbase_controller::build_hbase_env_sh() function.
            }
            HBASE_SITE_XML => {
                result.insert(
                    HBASE_CLUSTER_DISTRIBUTED.to_string(),
                    Some("true".to_string()),
                );
                result.insert(
                    HBASE_UNSAFE_REGIONSERVER_HOSTNAME_DISABLE_MASTER_REVERSEDNS.to_string(),
                    Some("true".to_string()),
                );
                result.insert(
                    HBASE_ROOTDIR.to_string(),
                    Some(
                        self.hbase_rootdir
                            .as_deref()
                            .unwrap_or(HBASE_ROOT_DIR_DEFAULT)
                            .to_string(),
                    ),
                );
            }
            _ => {}
        }

        result.retain(|_, maybe_value| maybe_value.is_some());

        Ok(result)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HbaseClusterStatus {
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

impl HasStatusCondition for HbaseCluster {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

impl HbaseCluster {
    /// Retrieve and merge resource configs for role and role groups
    pub fn merged_config(
        &self,
        role: &HbaseRole,
        role_group: &str,
        hdfs_discovery_cm_name: &str,
    ) -> Result<Box<dyn UnifiedRoleConfiguration>, Error> {
        match role {
            HbaseRole::Master => {
                let config = self.merged_master_config(role_group, hdfs_discovery_cm_name)?;
                Ok(Box::new(config))
            }
            HbaseRole::RegionServer => {
                let config = self.merged_regionserver_config(role_group, hdfs_discovery_cm_name)?;
                Ok(Box::new(config))
            }
            HbaseRole::RestServer => {
                let config = self.merged_rest_config(role_group, hdfs_discovery_cm_name)?;
                Ok(Box::new(config))
            }
        }
    }

    fn merged_regionserver_config(
        &self,
        role_group: &str,
        hdfs_discovery_cm_name: &str,
    ) -> Result<RegionServerConfig, Error> {
        let role = HbaseRole::RegionServer;

        // Initialize the result with all default values as baseline
        let conf_defaults = default_regionserver_config(&self.name_any(), hdfs_discovery_cm_name);

        let role = self
            .spec
            .region_servers
            .clone()
            .context(MissingHbaseRoleSnafu {
                role: role.to_string(),
            })?;

        // Retrieve role resource config
        let mut conf_role = role.config.config.to_owned();

        // Retrieve rolegroup specific resource config
        let mut conf_rolegroup = role
            .role_groups
            .get(role_group)
            .map(|rg| rg.config.config.clone())
            .unwrap_or_default();

        // Merge more specific configs into default config
        // Hierarchy is:
        // 1. RoleGroup
        // 2. Role
        // 3. Default
        conf_role.merge(&conf_defaults);
        conf_rolegroup.merge(&conf_role);

        tracing::debug!("Merged config: {:?}", conf_rolegroup);
        fragment::validate(conf_rolegroup).context(FragmentValidationFailureSnafu)
    }

    fn merged_rest_config(
        &self,
        role_group: &str,
        hdfs_discovery_cm_name: &str,
    ) -> Result<HbaseConfig, Error> {
        let role = HbaseRole::RestServer;

        // Initialize the result with all default values as baseline
        let conf_defaults = default_rest_config(&self.name_any(), hdfs_discovery_cm_name);

        let role = self
            .spec
            .rest_servers
            .clone()
            .context(MissingHbaseRoleSnafu {
                role: role.to_string(),
            })?;

        // Retrieve role resource config
        let mut conf_role = role.config.config.to_owned();

        // Retrieve rolegroup specific resource config
        let mut conf_rolegroup = role
            .role_groups
            .get(role_group)
            .map(|rg| rg.config.config.clone())
            .unwrap_or_default();

        // Merge more specific configs into default config
        // Hierarchy is:
        // 1. RoleGroup
        // 2. Role
        // 3. Default
        conf_role.merge(&conf_defaults);
        conf_rolegroup.merge(&conf_role);

        tracing::debug!("Merged config: {:?}", conf_rolegroup);
        fragment::validate(conf_rolegroup).context(FragmentValidationFailureSnafu)
    }

    fn merged_master_config(
        &self,
        role_group: &str,
        hdfs_discovery_cm_name: &str,
    ) -> Result<HbaseConfig, Error> {
        let role = HbaseRole::Master;

        // Initialize the result with all default values as baseline
        let conf_defaults = default_master_config(&self.name_any(), hdfs_discovery_cm_name);

        let role = self.spec.masters.clone().context(MissingHbaseRoleSnafu {
            role: role.to_string(),
        })?;

        // Retrieve role resource config
        let mut conf_role = role.config.config.to_owned();

        // Retrieve rolegroup specific resource config
        let mut conf_rolegroup = role
            .role_groups
            .get(role_group)
            .map(|rg| rg.config.config.clone())
            .unwrap_or_default();

        // Merge more specific configs into default config
        // Hierarchy is:
        // 1. RoleGroup
        // 2. Role
        // 3. Default
        conf_role.merge(&conf_defaults);
        conf_rolegroup.merge(&conf_role);

        tracing::debug!("Merged config: {:?}", conf_rolegroup);
        fragment::validate(conf_rolegroup).context(FragmentValidationFailureSnafu)
    }

    // The result type is only defined once, there is no value in extracting it into a type definition.
    #[allow(clippy::type_complexity)]
    pub fn build_role_properties(
        &self,
    ) -> Result<
        HashMap<
            String,
            (
                Vec<PropertyNameKind>,
                Role<impl Configuration<Configurable = HbaseCluster>>,
            ),
        >,
        Error,
    > {
        let config_types = vec![
            PropertyNameKind::Env,
            PropertyNameKind::File(HBASE_ENV_SH.to_string()),
            PropertyNameKind::File(HBASE_SITE_XML.to_string()),
            PropertyNameKind::File(SSL_SERVER_XML.to_string()),
            PropertyNameKind::File(SSL_CLIENT_XML.to_string()),
            PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
        ];

        let mut roles = HashMap::from([(
            HbaseRole::Master.to_string(),
            (
                config_types.to_owned(),
                self.spec
                    .masters
                    .clone()
                    .context(NoMasterRoleSnafu)?
                    .erase(),
            ),
        )]);
        roles.insert(
            HbaseRole::RegionServer.to_string(),
            (
                config_types.to_owned(),
                self.spec
                    .region_servers
                    .clone()
                    .context(NoRegionServerRoleSnafu)?
                    .erase(),
            ),
        );

        if let Some(rest_servers) = self.spec.rest_servers.as_ref() {
            roles.insert(
                HbaseRole::RestServer.to_string(),
                (config_types, rest_servers.to_owned().erase()),
            );
        }

        Ok(roles)
    }

    pub fn merge_pod_overrides(
        &self,
        pod_template: &mut PodTemplateSpec,
        role: &HbaseRole,
        role_group_ref: &RoleGroupRef<HbaseCluster>,
    ) {
        let (role_pod_overrides, role_group_pod_overrides) = match role {
            HbaseRole::Master => (
                self.spec
                    .masters
                    .as_ref()
                    .map(|r| r.config.pod_overrides.clone()),
                self.spec
                    .masters
                    .as_ref()
                    .and_then(|r| r.role_groups.get(&role_group_ref.role_group))
                    .map(|r| r.config.pod_overrides.clone()),
            ),
            HbaseRole::RegionServer => (
                self.spec
                    .region_servers
                    .as_ref()
                    .map(|r| r.config.pod_overrides.clone()),
                self.spec
                    .region_servers
                    .as_ref()
                    .and_then(|r| r.role_groups.get(&role_group_ref.role_group))
                    .map(|r| r.config.pod_overrides.clone()),
            ),
            HbaseRole::RestServer => (
                self.spec
                    .rest_servers
                    .as_ref()
                    .map(|r| r.config.pod_overrides.clone()),
                self.spec
                    .rest_servers
                    .as_ref()
                    .and_then(|r| r.role_groups.get(&role_group_ref.role_group))
                    .map(|r| r.config.pod_overrides.clone()),
            ),
        };

        if let Some(rpo) = role_pod_overrides {
            pod_template.merge_from(rpo);
        }
        if let Some(rgpo) = role_group_pod_overrides {
            pod_template.merge_from(rgpo);
        }
    }

    pub fn replicas(
        &self,
        hbase_role: &HbaseRole,
        role_group_ref: &RoleGroupRef<HbaseCluster>,
    ) -> Option<i32> {
        match hbase_role {
            HbaseRole::Master => self
                .spec
                .masters
                .as_ref()
                .and_then(|r| r.role_groups.get(&role_group_ref.role_group))
                .and_then(|rg| rg.replicas)
                .map(i32::from),
            HbaseRole::RegionServer => self
                .spec
                .region_servers
                .as_ref()
                .and_then(|r| r.role_groups.get(&role_group_ref.role_group))
                .and_then(|rg| rg.replicas)
                .map(i32::from),
            HbaseRole::RestServer => self
                .spec
                .rest_servers
                .as_ref()
                .and_then(|r| r.role_groups.get(&role_group_ref.role_group))
                .and_then(|rg| rg.replicas)
                .map(i32::from),
        }
    }

    /// The name of the role-level load-balanced Kubernetes `Service`
    pub fn server_role_service_name(&self) -> Option<String> {
        self.metadata.name.clone()
    }

    /// Metadata about a server rolegroup
    pub fn server_rolegroup_ref(
        &self,
        role_name: impl Into<String>,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<HbaseCluster> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(self),
            role: role_name.into(),
            role_group: group_name.into(),
        }
    }

    pub fn role_config(&self, role: &HbaseRole) -> Option<&GenericRoleConfig> {
        match role {
            HbaseRole::Master => self.spec.masters.as_ref().map(|m| &m.role_config),
            HbaseRole::RegionServer => self.spec.region_servers.as_ref().map(|rs| &rs.role_config),
            HbaseRole::RestServer => self.spec.rest_servers.as_ref().map(|rs| &rs.role_config),
        }
    }

    pub fn has_kerberos_enabled(&self) -> bool {
        self.kerberos_secret_class().is_some()
    }

    pub fn kerberos_secret_class(&self) -> Option<String> {
        self.spec
            .cluster_config
            .authentication
            .as_ref()
            .map(|a| &a.kerberos)
            .map(|k| k.secret_class.clone())
    }

    pub fn has_https_enabled(&self) -> bool {
        self.https_secret_class().is_some()
    }

    pub fn https_secret_class(&self) -> Option<String> {
        self.spec
            .cluster_config
            .authentication
            .as_ref()
            .map(|a| a.tls_secret_class.clone())
    }

    /// Returns required port name and port number tuples depending on the role.
    /// Hbase versions 2.4.* will have three ports for each role
    /// Hbase versions 2.6.* will have two ports for each role. The metrics are available over the
    /// UI port.
    pub fn ports(&self, role: &HbaseRole, hbase_version: &str) -> Vec<(String, u16)> {
        let result_without_metric_port: Vec<(String, u16)> = match role {
            HbaseRole::Master => vec![
                ("master".to_string(), HBASE_MASTER_PORT),
                (self.ui_port_name(), HBASE_MASTER_UI_PORT),
            ],
            HbaseRole::RegionServer => vec![
                ("regionserver".to_string(), HBASE_REGIONSERVER_PORT),
                (self.ui_port_name(), HBASE_REGIONSERVER_UI_PORT),
            ],
            HbaseRole::RestServer => vec![
                (
                    if self.has_https_enabled() {
                        HBASE_REST_PORT_NAME_HTTPS
                    } else {
                        HBASE_REST_PORT_NAME_HTTP
                    }
                    .to_string(),
                    HBASE_REST_PORT,
                ),
                (self.ui_port_name(), HBASE_REST_UI_PORT),
            ],
        };
        if hbase_version.starts_with(r"2.4") {
            result_without_metric_port
                .into_iter()
                .chain(vec![(METRICS_PORT_NAME.to_string(), METRICS_PORT)])
                .collect()
        } else {
            result_without_metric_port
        }
    }

    /// Name of the port used by the Web UI, which depends on HTTPS usage
    fn ui_port_name(&self) -> String {
        if self.has_https_enabled() {
            HBASE_UI_PORT_NAME_HTTPS
        } else {
            HBASE_UI_PORT_NAME_HTTP
        }
        .to_string()
    }
}

pub fn merged_env(rolegroup_config: Option<&BTreeMap<String, String>>) -> Vec<EnvVar> {
    let merged_env: Vec<EnvVar> = if let Some(rolegroup_config) = rolegroup_config {
        let env_vars_from_config: BTreeMap<String, EnvVar> = rolegroup_config
            .iter()
            .map(|(env_name, env_value)| {
                (
                    env_name.clone(),
                    EnvVar {
                        name: env_name.clone(),
                        value: Some(env_value.to_owned()),
                        value_from: None,
                    },
                )
            })
            .collect();
        env_vars_from_config.into_values().collect()
    } else {
        vec![]
    };
    merged_env
}

/// TODO: describe the purpose of this trait
pub trait UnifiedRoleConfiguration: Send {
    fn resources(&self) -> &Resources<HbaseStorageConfig, NoRuntimeLimits>;
    fn logging(&self) -> &Logging<Container>;
    fn affinity(&self) -> &StackableAffinity;
    fn graceful_shutdown_timeout(&self) -> &Option<Duration>;
    fn hbase_opts(&self) -> &Option<String>;
}

impl UnifiedRoleConfiguration for HbaseConfig {
    fn resources(&self) -> &Resources<HbaseStorageConfig, NoRuntimeLimits> {
        &self.resources
    }
    fn logging(&self) -> &Logging<Container> {
        &self.logging
    }
    fn affinity(&self) -> &StackableAffinity {
        &self.affinity
    }
    fn graceful_shutdown_timeout(&self) -> &Option<Duration> {
        &self.graceful_shutdown_timeout
    }
    fn hbase_opts(&self) -> &Option<String> {
        &self.hbase_opts
    }
}

impl UnifiedRoleConfiguration for RegionServerConfig {
    fn resources(&self) -> &Resources<HbaseStorageConfig, NoRuntimeLimits> {
        &self.resources
    }
    fn logging(&self) -> &Logging<Container> {
        &self.logging
    }
    fn affinity(&self) -> &StackableAffinity {
        &self.affinity
    }
    fn graceful_shutdown_timeout(&self) -> &Option<Duration> {
        &self.graceful_shutdown_timeout
    }
    fn hbase_opts(&self) -> &Option<String> {
        &self.hbase_opts
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use indoc::indoc;
    use stackable_operator::product_config_utils::{
        transform_all_roles_to_config, validate_all_roles_and_groups_config,
    };

    use crate::{merged_env, HbaseCluster, HbaseRole};

    use product_config::{types::PropertyNameKind, ProductConfigManager};

    #[test]
    pub fn test_env_overrides() {
        let input = indoc! {r#"
---
apiVersion: hbase.stackable.tech/v1alpha1
kind: HbaseCluster
metadata:
  name: test-hbase
spec:
  image:
    productVersion: 2.4.18
  clusterConfig:
    hdfsConfigMapName: test-hdfs
    zookeeperConfigMapName: test-znode
  masters:
    envOverrides:
      TEST_VAR_FROM_MASTER: MASTER
      TEST_VAR: MASTER
    config:
      logging:
        enableVectorAgent: False
    roleGroups:
      default:
        replicas: 1
        envOverrides:
          TEST_VAR_FROM_MRG: MASTER
          TEST_VAR: MASTER_RG
  regionServers:
    config:
      logging:
        enableVectorAgent: False
    roleGroups:
      default:
        replicas: 1
  restServers:
    config:
      logging:
        enableVectorAgent: False
    roleGroups:
      default:
        replicas: 1
        "#};

        let deserializer = serde_yaml::Deserializer::from_str(input);
        let hbase: HbaseCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let roles = HashMap::from([(
            HbaseRole::Master.to_string(),
            (
                vec![PropertyNameKind::Env],
                hbase.spec.masters.clone().unwrap(),
            ),
        )]);

        let validated_config = validate_all_roles_and_groups_config(
            "2.4.18",
            &transform_all_roles_to_config(&hbase, roles).unwrap(),
            &ProductConfigManager::from_yaml_file("../../deploy/config-spec/properties.yaml")
                .unwrap(),
            false,
            false,
        )
        .unwrap();

        let rolegroup_config = validated_config
            .get(&HbaseRole::Master.to_string())
            .unwrap()
            .get("default")
            .unwrap()
            .get(&PropertyNameKind::Env);
        let merged_env = merged_env(rolegroup_config);

        let env_map: BTreeMap<&str, Option<String>> = merged_env
            .iter()
            .map(|env_var| (env_var.name.as_str(), env_var.value.clone()))
            .collect();

        assert_eq!(
            Some(&Some("MASTER_RG".to_string())),
            env_map.get("TEST_VAR")
        );
        assert_eq!(
            Some(&Some("MASTER".to_string())),
            env_map.get("TEST_VAR_FROM_MASTER")
        );
        assert_eq!(
            Some(&Some("MASTER".to_string())),
            env_map.get("TEST_VAR_FROM_MRG")
        );
    }
}
