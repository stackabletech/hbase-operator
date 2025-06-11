use std::collections::{BTreeMap, HashMap};

use product_config::types::PropertyNameKind;
use security::AuthenticationConfig;
use serde::{Deserialize, Serialize};
use shell_escape::escape;
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
        merge::{Atomic, Merge},
    },
    k8s_openapi::{
        DeepMerge,
        api::core::v1::{EnvVar, PodTemplateSpec},
        apimachinery::pkg::api::resource::Quantity,
    },
    kube::{CustomResource, ResourceExt, runtime::reflector::ObjectRef},
    product_config_utils::Configuration,
    product_logging::{self, spec::Logging},
    role_utils::{GenericRoleConfig, JavaCommonConfig, Role, RoleGroupRef},
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
    time::Duration,
    versioned::versioned,
};
use strum::{Display, EnumIter, EnumString};

use crate::crd::{affinity::get_affinity, security::AuthorizationConfig};

pub mod affinity;
pub mod security;

pub const APP_NAME: &str = "hbase";

// This constant is hard coded in hbase-entrypoint.sh
// You need to change it there too.
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
pub const LISTENER_VOLUME_NAME: &str = "listener";
pub const LISTENER_VOLUME_DIR: &str = "/stackable/listener";

const DEFAULT_REGION_MOVER_TIMEOUT: Duration = Duration::from_minutes_unchecked(59);
const DEFAULT_REGION_MOVER_DELTA_TO_SHUTDOWN: Duration = Duration::from_minutes_unchecked(1);

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("the role [{role}] is invalid and does not exist in HBase"))]
    InvalidRole {
        source: strum::ParseError,
        role: String,
    },

    #[snafu(display("the HBase role [{role}] is missing from spec"))]
    MissingHbaseRole { role: String },

    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },

    #[snafu(display("object defines no master role"))]
    NoMasterRole,

    #[snafu(display("object defines no regionserver role"))]
    NoRegionServerRole,

    #[snafu(display("incompatible merge types"))]
    IncompatibleMergeTypes,

    #[snafu(display("role-group is not valid"))]
    NoRoleGroup,
}

#[versioned(version(name = "v1alpha1"))]
pub mod versioned {
    /// An HBase cluster stacklet. This resource is managed by the Stackable operator for Apache HBase.
    /// Find more information on how to use it and the resources that the operator generates in the
    /// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/hbase/).
    ///
    /// The CRD contains three roles: `masters`, `regionServers` and `restServers`.
    #[versioned(k8s(
        group = "hbase.stackable.tech",
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
    ))]
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct HbaseClusterSpec {
        // no doc string - See ProductImage struct
        pub image: ProductImage,

        /// Configuration that applies to all roles and role groups.
        /// This includes settings for logging, ZooKeeper and HDFS connection, among other things.
        pub cluster_config: v1alpha1::HbaseClusterConfig,

        // no doc string - See ClusterOperation struct
        #[serde(default)]
        pub cluster_operation: ClusterOperation,

        /// The HBase master process is responsible for assigning regions to region servers and
        /// manages the cluster.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub masters: Option<Role<HbaseConfigFragment, GenericRoleConfig, JavaCommonConfig>>,

        /// Region servers hold the data and handle requests from clients for their region.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub region_servers:
            Option<Role<RegionServerConfigFragment, GenericRoleConfig, JavaCommonConfig>>,

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

        /// Settings related to user [authentication](DOCS_BASE_URL_PLACEHOLDER/usage-guide/security).
        pub authentication: Option<AuthenticationConfig>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub authorization: Option<AuthorizationConfig>,
    }
}

impl HasStatusCondition for v1alpha1::HbaseCluster {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

impl v1alpha1::HbaseCluster {
    /// Retrieve and merge resource configs for role and role groups
    pub fn merged_config(
        &self,
        role: &HbaseRole,
        role_group: &str,
        hdfs_discovery_cm_name: &str,
    ) -> Result<AnyServiceConfig, Error> {
        // Initialize the result with all default values as baseline
        let defaults =
            AnyConfigFragment::default_for(role, &self.name_any(), hdfs_discovery_cm_name);

        // Trivial values for role-groups are not allowed
        if role_group.is_empty() {
            return Err(Error::NoRoleGroup);
        }

        let (mut role_config, mut role_group_config) = match role {
            HbaseRole::RegionServer => {
                let role = self
                    .spec
                    .region_servers
                    .clone()
                    .context(MissingHbaseRoleSnafu {
                        role: role.to_string(),
                    })?;

                let role_config = role.config.config.to_owned();
                let role_group_config = role
                    .role_groups
                    .get(role_group)
                    .map(|rg| rg.config.config.clone())
                    .expect(
                        "Cannot be empty as trivial values of role-group have already been checked",
                    );

                (
                    AnyConfigFragment::RegionServer(role_config),
                    AnyConfigFragment::RegionServer(role_group_config),
                )
            }
            HbaseRole::RestServer => {
                let role = self
                    .spec
                    .rest_servers
                    .clone()
                    .context(MissingHbaseRoleSnafu {
                        role: role.to_string(),
                    })?;

                let role_config = role.config.config.to_owned();

                let role_group_config = role
                    .role_groups
                    .get(role_group)
                    .map(|rg| rg.config.config.clone())
                    .expect(
                        "Cannot be empty as trivial values of role-group have already been checked",
                    );

                // Retrieve role resource config
                (
                    AnyConfigFragment::RestServer(role_config),
                    AnyConfigFragment::RestServer(role_group_config),
                )
            }
            HbaseRole::Master => {
                let role = self.spec.masters.clone().context(MissingHbaseRoleSnafu {
                    role: role.to_string(),
                })?;

                let role_config = role.config.config.to_owned();

                // Retrieve rolegroup specific resource config
                let role_group_config = role
                    .role_groups
                    .get(role_group)
                    .map(|rg| rg.config.config.clone())
                    .expect(
                        "Cannot be empty as trivial values of role-group have already been checked",
                    );

                // Retrieve role resource config
                (
                    AnyConfigFragment::Master(role_config),
                    AnyConfigFragment::Master(role_group_config),
                )
            }
        };

        // Merge more specific configs into default config
        // Hierarchy is:
        // 1. RoleGroup
        // 2. Role
        // 3. Default
        role_config = role_config.merge(&defaults)?;
        role_group_config = role_group_config.merge(&role_config)?;

        tracing::debug!("Merged config: {:?}", role_group_config);

        Ok(match role_group_config {
            AnyConfigFragment::RegionServer(conf) => AnyServiceConfig::RegionServer(
                fragment::validate(conf).context(FragmentValidationFailureSnafu)?,
            ),
            AnyConfigFragment::RestServer(conf) => AnyServiceConfig::RestServer(
                fragment::validate(conf).context(FragmentValidationFailureSnafu)?,
            ),
            AnyConfigFragment::Master(conf) => AnyServiceConfig::Master(
                fragment::validate(conf).context(FragmentValidationFailureSnafu)?,
            ),
        })
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
                Role<impl Configuration<Configurable = Self>, GenericRoleConfig, JavaCommonConfig>,
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
        role_group_ref: &RoleGroupRef<Self>,
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
        role_group_ref: &RoleGroupRef<Self>,
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
    ) -> RoleGroupRef<Self> {
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

    pub fn service_port(&self, role: &HbaseRole) -> u16 {
        match role {
            HbaseRole::Master => HBASE_MASTER_PORT,
            HbaseRole::RegionServer => HBASE_REGIONSERVER_PORT,
            HbaseRole::RestServer => HBASE_REST_PORT,
        }
    }

    /// Name of the port used by the Web UI, which depends on HTTPS usage
    pub fn ui_port_name(&self) -> String {
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
        rolegroup_config
            .iter()
            .map(|(env_name, env_value)| EnvVar {
                name: env_name.clone(),
                value: Some(env_value.to_owned()),
                value_from: None,
            })
            .collect()
    } else {
        vec![]
    };
    merged_env
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
    // Auto TLS certificate lifetime
    const DEFAULT_MASTER_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);
    const DEFAULT_REGION_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);
    const DEFAULT_REGION_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT: Duration =
        Duration::from_minutes_unchecked(60);
    const DEFAULT_REST_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);
    const DEFAULT_REST_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT: Duration =
        Duration::from_minutes_unchecked(5);

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
            listener_class: Some("cluster-internal".to_string()),
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
}

fn default_resources(role: &HbaseRole) -> ResourcesFragment<HbaseStorageConfig, NoRuntimeLimits> {
    match role {
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
    }
}

#[derive(Debug, Clone)]
enum AnyConfigFragment {
    RegionServer(RegionServerConfigFragment),
    RestServer(HbaseConfigFragment),
    Master(HbaseConfigFragment),
}

impl AnyConfigFragment {
    fn merge(self, other: &AnyConfigFragment) -> Result<Self, Error> {
        match (self, other) {
            (AnyConfigFragment::RegionServer(mut me), AnyConfigFragment::RegionServer(you)) => {
                me.merge(you);
                Ok(AnyConfigFragment::RegionServer(me.clone()))
            }
            (AnyConfigFragment::RestServer(mut me), AnyConfigFragment::RestServer(you)) => {
                me.merge(you);
                Ok(AnyConfigFragment::RestServer(me.clone()))
            }
            (AnyConfigFragment::Master(mut me), AnyConfigFragment::Master(you)) => {
                me.merge(you);
                Ok(AnyConfigFragment::Master(me.clone()))
            }
            (_, _) => Err(Error::IncompatibleMergeTypes),
        }
    }

    fn default_for(
        role: &HbaseRole,
        cluster_name: &str,
        hdfs_discovery_cm_name: &str,
    ) -> AnyConfigFragment {
        match role {
            HbaseRole::RegionServer => {
                AnyConfigFragment::RegionServer(RegionServerConfigFragment {
                    hbase_rootdir: None,
                    resources: default_resources(role),
                    logging: product_logging::spec::default_logging(),
                    affinity: get_affinity(cluster_name, role, hdfs_discovery_cm_name),
                    graceful_shutdown_timeout: Some(
                        HbaseRole::DEFAULT_REGION_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT,
                    ),
                    region_mover: RegionMoverFragment {
                        run_before_shutdown: Some(false),
                        max_threads: Some(1),
                        ack: Some(true),
                        cli_opts: None,
                    },
                    requested_secret_lifetime: Some(HbaseRole::DEFAULT_REGION_SECRET_LIFETIME),
                    listener_class: Some("cluster-internal".to_string()),
                })
            }
            HbaseRole::RestServer => AnyConfigFragment::RestServer(HbaseConfigFragment {
                hbase_rootdir: None,
                resources: default_resources(role),
                logging: product_logging::spec::default_logging(),
                affinity: get_affinity(cluster_name, role, hdfs_discovery_cm_name),
                graceful_shutdown_timeout: Some(
                    HbaseRole::DEFAULT_REST_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT,
                ),
                requested_secret_lifetime: Some(HbaseRole::DEFAULT_REST_SECRET_LIFETIME),
                listener_class: Some("cluster-internal".to_string()),
            }),
            HbaseRole::Master => AnyConfigFragment::Master(HbaseConfigFragment {
                hbase_rootdir: None,
                resources: default_resources(role),
                logging: product_logging::spec::default_logging(),
                affinity: get_affinity(cluster_name, role, hdfs_discovery_cm_name),
                graceful_shutdown_timeout: Some(
                    HbaseRole::DEFAULT_MASTER_GRACEFUL_SHUTDOWN_TIMEOUT,
                ),
                requested_secret_lifetime: Some(HbaseRole::DEFAULT_MASTER_SECRET_LIFETIME),
                listener_class: Some("cluster-internal".to_string()),
            }),
        }
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

    /// This field controls which [ListenerClass](DOCS_BASE_URL_PLACEHOLDER/listener-operator/listenerclass.html) is used to expose this rolegroup.
    pub listener_class: String,
}

impl Configuration for HbaseConfigFragment {
    type Configurable = v1alpha1::HbaseCluster;

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
                result.insert(HBASE_ROOTDIR.to_string(), self.hbase_rootdir.clone());
            }
            _ => {}
        }

        result.retain(|_, maybe_value| maybe_value.is_some());

        Ok(result)
    }
}

#[derive(Fragment, Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
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
pub struct RegionMover {
    /// Move local regions to other servers before terminating a region server's pod.
    run_before_shutdown: bool,

    /// Maximum number of threads to use for moving regions.
    max_threads: u16,

    /// If enabled (default), the region mover will confirm that regions are available on the
    /// source as well as the target pods before and after the move.
    ack: bool,

    #[fragment_attrs(serde(flatten))]
    cli_opts: Option<RegionMoverExtraCliOpts>,
}

#[derive(Clone, Debug, Eq, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
#[schemars(deny_unknown_fields)]
pub struct RegionMoverExtraCliOpts {
    /// Additional options to pass to the region mover.
    #[serde(default)]
    pub additional_mover_options: Vec<String>,
}

impl Atomic for RegionMoverExtraCliOpts {}

#[derive(Clone, Debug, Fragment, JsonSchema, PartialEq)]
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

    /// Before terminating a region server pod, the RegionMover tool can be invoked to transfer
    /// local regions to other servers.
    /// This may cause a lot of network traffic in the Kubernetes cluster if the entire HBase stacklet is being
    /// restarted.
    /// The operator will compute a timeout period for the region move that will not exceed the graceful shutdown timeout.
    #[fragment_attrs(serde(default))]
    pub region_mover: RegionMover,

    /// This field controls which [ListenerClass](DOCS_BASE_URL_PLACEHOLDER/listener-operator/listenerclass.html) is used to expose this rolegroup.
    pub listener_class: String,
}

impl Configuration for RegionServerConfigFragment {
    type Configurable = v1alpha1::HbaseCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
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

pub enum AnyServiceConfig {
    Master(HbaseConfig),
    RegionServer(RegionServerConfig),
    RestServer(HbaseConfig),
}

impl AnyServiceConfig {
    pub fn resources(&self) -> &Resources<HbaseStorageConfig, NoRuntimeLimits> {
        match self {
            AnyServiceConfig::Master(config) => &config.resources,
            AnyServiceConfig::RegionServer(config) => &config.resources,
            AnyServiceConfig::RestServer(config) => &config.resources,
        }
    }

    pub fn logging(&self) -> &Logging<Container> {
        match self {
            AnyServiceConfig::Master(config) => &config.logging,
            AnyServiceConfig::RegionServer(config) => &config.logging,
            AnyServiceConfig::RestServer(config) => &config.logging,
        }
    }

    pub fn affinity(&self) -> &StackableAffinity {
        match self {
            AnyServiceConfig::Master(config) => &config.affinity,
            AnyServiceConfig::RegionServer(config) => &config.affinity,
            AnyServiceConfig::RestServer(config) => &config.affinity,
        }
    }

    pub fn graceful_shutdown_timeout(&self) -> &Option<Duration> {
        match self {
            AnyServiceConfig::Master(config) => &config.graceful_shutdown_timeout,
            AnyServiceConfig::RegionServer(config) => &config.graceful_shutdown_timeout,
            AnyServiceConfig::RestServer(config) => &config.graceful_shutdown_timeout,
        }
    }

    pub fn requested_secret_lifetime(&self) -> Option<Duration> {
        match self {
            AnyServiceConfig::Master(config) => config.requested_secret_lifetime,
            AnyServiceConfig::RegionServer(config) => config.requested_secret_lifetime,
            AnyServiceConfig::RestServer(config) => config.requested_secret_lifetime,
        }
    }

    pub fn listener_class(&self) -> String {
        match self {
            AnyServiceConfig::Master(config) => config.listener_class.clone(),
            AnyServiceConfig::RegionServer(config) => config.listener_class.clone(),
            AnyServiceConfig::RestServer(config) => config.listener_class.clone(),
        }
    }

    /// Returns command line arguments to pass on to the region mover tool.
    /// The following arguments are excluded because they are already part of the
    /// hbase-entrypoint.sh script.
    /// The most important argument, '--regionserverhost' can only be computed on the Pod
    /// because it contains the pod's hostname.
    ///
    /// Returns an empty string if the region mover is disabled or any other role is "self".
    pub fn region_mover_args(&self) -> String {
        match self {
            AnyServiceConfig::RegionServer(config) => {
                if config.region_mover.run_before_shutdown {
                    let timeout = config
                        .graceful_shutdown_timeout
                        .map(|d| {
                            if d.as_secs() <= DEFAULT_REGION_MOVER_DELTA_TO_SHUTDOWN.as_secs() {
                                d.as_secs()
                            } else {
                                d.as_secs() - DEFAULT_REGION_MOVER_DELTA_TO_SHUTDOWN.as_secs()
                            }
                        })
                        .unwrap_or(DEFAULT_REGION_MOVER_TIMEOUT.as_secs());
                    let mut command = vec![
                        "--maxthreads".to_string(),
                        config.region_mover.max_threads.to_string(),
                        "--timeout".to_string(),
                        timeout.to_string(),
                    ];
                    if !config.region_mover.ack {
                        command.push("--noack".to_string());
                    }

                    command.extend(
                        config
                            .region_mover
                            .cli_opts
                            .iter()
                            .flat_map(|o| o.additional_mover_options.clone())
                            .map(|s| escape(std::borrow::Cow::Borrowed(&s)).to_string()),
                    );
                    command.join(" ")
                } else {
                    "".to_string()
                }
            }
            _ => "".to_string(),
        }
    }

    pub fn run_region_mover(&self) -> bool {
        match self {
            AnyServiceConfig::RegionServer(config) => config.region_mover.run_before_shutdown,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use indoc::indoc;
    use product_config::{ProductConfigManager, types::PropertyNameKind};
    use rstest::rstest;
    use stackable_operator::product_config_utils::{
        transform_all_roles_to_config, validate_all_roles_and_groups_config,
    };

    use super::*;

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
    productVersion: 2.6.2
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
      regionMover:
        runBeforeShutdown: false
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
        let hbase: v1alpha1::HbaseCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let roles = HashMap::from([(
            HbaseRole::Master.to_string(),
            (
                vec![PropertyNameKind::Env],
                hbase.spec.masters.clone().unwrap(),
            ),
        )]);

        let validated_config = validate_all_roles_and_groups_config(
            "2.6.2",
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

    #[rstest]
    #[case("default", false, 1, vec![])]
    #[case("groupRegionMover", true, 5, vec!["--some".to_string(), "extra".to_string()])]
    pub fn test_region_mover_merge(
        #[case] role_group_name: &str,
        #[case] run_before_shutdown: bool,
        #[case] max_threads: u16,
        #[case] additional_mover_options: Vec<String>,
    ) {
        let input = indoc! {r#"
---
apiVersion: hbase.stackable.tech/v1alpha1
kind: HbaseCluster
metadata:
  name: test-hbase
spec:
  image:
    productVersion: 2.6.2
  clusterConfig:
    hdfsConfigMapName: test-hdfs
    zookeeperConfigMapName: test-znode
  masters:
    roleGroups:
      default:
        replicas: 1
  restServers:
    roleGroups:
      default:
        replicas: 1
  regionServers:
    config:
      regionMover:
        runBeforeShutdown: False
    roleGroups:
      default:
        replicas: 1
      groupRegionMover:
        replicas: 1
        config:
          regionMover:
            runBeforeShutdown: True
            maxThreads: 5
            additionalMoverOptions: ["--some", "extra"]
        "#};

        let deserializer = serde_yaml::Deserializer::from_str(input);
        let hbase: v1alpha1::HbaseCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let hbase_role = HbaseRole::RegionServer;
        let rolegroup = hbase.server_rolegroup_ref(hbase_role.to_string(), role_group_name);

        let merged_config = hbase
            .merged_config(
                &hbase_role,
                &rolegroup.role_group,
                &hbase.spec.cluster_config.hdfs_config_map_name,
            )
            .unwrap();
        if let AnyServiceConfig::RegionServer(config) = merged_config {
            assert_eq!(run_before_shutdown, config.region_mover.run_before_shutdown);
            assert_eq!(max_threads, config.region_mover.max_threads);
            assert_eq!(
                Some(RegionMoverExtraCliOpts {
                    additional_mover_options
                }),
                config.region_mover.cli_opts
            );
        } else {
            panic!("this shouldn't happen");
        };
    }
}
