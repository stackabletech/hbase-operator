use std::{collections::BTreeMap, str::FromStr};

use security::AuthenticationConfig;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
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
        merge::Merge,
    },
    k8s_openapi::{api::core::v1::EnvVar, apimachinery::pkg::api::resource::Quantity},
    kube::{runtime::reflector::ObjectRef, CustomResource, ResourceExt},
    product_config_utils::Configuration,
    product_logging::{self, spec::Logging},
    role_utils::{GenericRoleConfig, JavaCommonConfig, Role, RoleGroup, RoleGroupRef},
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
    time::Duration,
};
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

pub const HBASE_CLUSTER_DISTRIBUTED: &str = "hbase.cluster.distributed";
pub const HBASE_ROOTDIR: &str = "hbase.rootdir";
pub const HBASE_UNSAFE_REGIONSERVER_HOSTNAME_DISABLE_MASTER_REVERSEDNS: &str =
    "hbase.unsafe.regionserver.hostname.disable.master.reversedns";

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

#[derive(Snafu, Debug)]
pub enum Error {
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
    pub masters: Option<Role<HbaseConfigFragment, GenericRoleConfig, JavaCommonConfig>>,

    /// Region servers hold the data and handle requests from clients for their region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region_servers: Option<Role<HbaseConfigFragment, GenericRoleConfig, JavaCommonConfig>>,

    /// Rest servers provide a REST API to interact with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rest_servers: Option<Role<HbaseConfigFragment, GenericRoleConfig, JavaCommonConfig>>,
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
    const DEFAULT_MASTER_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_minutes_unchecked(20);
    const DEFAULT_REGION_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT: Duration =
        Duration::from_minutes_unchecked(60);
    const DEFAULT_REST_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT: Duration =
        Duration::from_minutes_unchecked(5);

    // Auto TLS certificate lifetime
    const DEFAULT_MASTER_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);
    const DEFAULT_REGION_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);
    const DEFAULT_REST_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);

    pub fn default_config(
        &self,
        cluster_name: &str,
        hdfs_discovery_cm_name: &str,
    ) -> HbaseConfigFragment {
        let resources = match &self {
            HbaseRole::Master => ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("250m".to_owned())),
                    max: Some(Quantity("1".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("1Gi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: HbaseStorageConfigFragment {},
            },
            HbaseRole::RegionServer => ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("250m".to_owned())),
                    max: Some(Quantity("1".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("1Gi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: HbaseStorageConfigFragment {},
            },
            HbaseRole::RestServer => ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("100m".to_owned())),
                    max: Some(Quantity("400m".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("512Mi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: HbaseStorageConfigFragment {},
            },
        };

        let graceful_shutdown_timeout = match &self {
            HbaseRole::Master => Self::DEFAULT_MASTER_GRACEFUL_SHUTDOWN_TIMEOUT,
            HbaseRole::RegionServer => Self::DEFAULT_REGION_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT,
            HbaseRole::RestServer => Self::DEFAULT_REST_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT,
        };

        let requested_secret_lifetime = match &self {
            HbaseRole::Master => Self::DEFAULT_MASTER_SECRET_LIFETIME,
            HbaseRole::RegionServer => Self::DEFAULT_REGION_SECRET_LIFETIME,
            HbaseRole::RestServer => Self::DEFAULT_REST_SECRET_LIFETIME,
        };

        HbaseConfigFragment {
            hbase_rootdir: None,
            resources,
            logging: product_logging::spec::default_logging(),
            affinity: get_affinity(cluster_name, self, hdfs_discovery_cm_name),
            graceful_shutdown_timeout: Some(graceful_shutdown_timeout),
            requested_secret_lifetime: Some(requested_secret_lifetime),
        }
    }

    /// Returns the name of the role as it is needed by the `bin/hbase {cli_role_name} start` command.
    pub fn cli_role_name(&self) -> String {
        match self {
            HbaseRole::Master | HbaseRole::RegionServer => self.to_string(),
            // Of course it is not called "restserver", so we need to have this match
            // instead of just letting the Display impl do it's thing ;P
            HbaseRole::RestServer => "rest".to_string(),
        }
    }

    /// We could have different service names depended on the role (e.g. "hbase-master", "hbase-regionserver" and
    /// "hbase-restserver"). However this produces error messages such as
    /// [RpcServer.priority.RWQ.Fifo.write.handler=0,queue=0,port=16020] security.ShellBasedUnixGroupsMapping: unable to return groups for user hbase-master PartialGroupNameException The user name 'hbase-master' is not found. id: 'hbase-master': no such user
    /// or
    /// Caused by: org.apache.hadoop.hbase.ipc.RemoteWithExtrasException(org.apache.hadoop.hbase.security.AccessDeniedException): org.apache.hadoop.hbase.security.AccessDeniedException: Insufficient permissions (user=hbase-master/hbase-master-default-1.hbase-master-default.kuttl-test-poetic-sunbeam.svc.cluster.local@CLUSTER.LOCAL, scope=hbase:meta, family=table:state, params=[table=hbase:meta,family=table:state],action=WRITE)
    ///
    /// Also the documentation states:
    /// > A Kerberos principal has three parts, with the form username/fully.qualified.domain.name@YOUR-REALM.COM. We recommend using hbase as the username portion.
    ///
    /// As a result we use "hbase" everywhere (which e.g. differs from the current hdfs implementation)
    pub fn kerberos_service_name(&self) -> &'static str {
        "hbase"
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

    #[fragment_attrs(serde(default))]
    pub resources: Resources<HbaseStorageConfig, NoRuntimeLimits>,

    #[fragment_attrs(serde(default))]
    pub logging: Logging<Container>,

    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,

    /// Time period Pods have to gracefully shut down, e.g. `30m`, `1h` or `2d`. Consult the operator documentation for details.
    #[fragment_attrs(serde(default))]
    pub graceful_shutdown_timeout: Option<Duration>,

    /// Request secret (currently only autoTls certificates) lifetime from the secret operator, e.g. `7d`, or `30d`.
    /// Please note that this can be shortened by the `maxCertificateLifetime` setting on the SecretClass issuing the TLS certificate.
    #[fragment_attrs(serde(default))]
    pub requested_secret_lifetime: Option<Duration>,
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
                result.insert(HBASE_ROOTDIR.to_string(), self.hbase_rootdir.clone());
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

    pub fn get_role(
        &self,
        role: &HbaseRole,
    ) -> Option<&Role<HbaseConfigFragment, GenericRoleConfig, JavaCommonConfig>> {
        match role {
            HbaseRole::Master => self.spec.masters.as_ref(),
            HbaseRole::RegionServer => self.spec.region_servers.as_ref(),
            HbaseRole::RestServer => self.spec.rest_servers.as_ref(),
        }
    }

    /// Get the RoleGroup struct for the given ref
    pub fn get_role_group(
        &self,
        rolegroup_ref: &RoleGroupRef<HbaseCluster>,
    ) -> Result<&RoleGroup<HbaseConfigFragment, JavaCommonConfig>, Error> {
        let role_variant =
            HbaseRole::from_str(&rolegroup_ref.role).with_context(|_| InvalidRoleSnafu {
                role: rolegroup_ref.role.to_owned(),
            })?;
        let role = self
            .get_role(&role_variant)
            .with_context(|| MissingHbaseRoleSnafu {
                role: role_variant.to_string(),
            })?;
        role.role_groups
            .get(&rolegroup_ref.role_group)
            .with_context(|| MissingHbaseRoleGroupSnafu {
                role_group: rolegroup_ref.role_group.to_owned(),
            })
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

    /// Retrieve and merge resource configs for role and role groups
    pub fn merged_config(
        &self,
        role: &HbaseRole,
        role_group: &str,
        hdfs_discovery_cm_name: &str,
    ) -> Result<HbaseConfig, Error> {
        // Initialize the result with all default values as baseline
        let conf_defaults = role.default_config(&self.name_any(), hdfs_discovery_cm_name);

        let role = self.get_role(role).context(MissingHbaseRoleSnafu {
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
                hbase.get_role(&HbaseRole::Master).cloned().unwrap(),
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

        println!("{:#?}", merged_env);

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
