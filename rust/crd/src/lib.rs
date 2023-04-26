pub mod affinity;

use affinity::get_affinity;
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
    config::{fragment, fragment::Fragment, fragment::ValidationError, merge::Merge},
    k8s_openapi::apimachinery::pkg::api::resource::Quantity,
    kube::{runtime::reflector::ObjectRef, CustomResource, ResourceExt},
    product_config_utils::{ConfigError, Configuration},
    product_logging::{self, spec::Logging},
    role_utils::{Role, RoleGroup, RoleGroupRef},
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
};
use std::{collections::BTreeMap, str::FromStr};
use strum::{Display, EnumIter, EnumString};

pub const APP_NAME: &str = "hbase";

pub const HBASE_ENV_SH: &str = "hbase-env.sh";
pub const HBASE_SITE_XML: &str = "hbase-site.xml";

pub const HBASE_MANAGES_ZK: &str = "HBASE_MANAGES_ZK";
pub const HBASE_MASTER_OPTS: &str = "HBASE_MASTER_OPTS";
pub const HBASE_REGIONSERVER_OPTS: &str = "HBASE_REGIONSERVER_OPTS";
pub const HBASE_REST_OPTS: &str = "HBASE_REST_OPTS";

pub const HBASE_CLUSTER_DISTRIBUTED: &str = "hbase.cluster.distributed";
pub const HBASE_ROOTDIR: &str = "hbase.rootdir";
pub const HBASE_ZOOKEEPER_QUORUM: &str = "hbase.zookeeper.quorum";
pub const HBASE_HEAPSIZE: &str = "HBASE_HEAPSIZE";
pub const HBASE_ROOT_DIR_DEFAULT: &str = "/hbase";

pub const HBASE_UI_PORT_NAME: &str = "ui";
pub const METRICS_PORT_NAME: &str = "metrics";

pub const HBASE_MASTER_PORT: u16 = 16000;
pub const HBASE_MASTER_UI_PORT: u16 = 16010;
pub const HBASE_REGIONSERVER_PORT: u16 = 16020;
pub const HBASE_REGIONSERVER_UI_PORT: u16 = 16030;
pub const HBASE_REST_PORT: u16 = 8080;
pub const METRICS_PORT: u16 = 8081;

pub const JVM_HEAP_FACTOR: f32 = 0.8;

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
    /// Desired HBase image
    pub image: ProductImage,
    /// Global HBase cluster configuration
    pub cluster_config: HbaseClusterConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub masters: Option<Role<HbaseConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region_servers: Option<Role<HbaseConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rest_servers: Option<Role<HbaseConfigFragment>>,
    /// Cluster operations like pause reconciliation or cluster stop.
    #[serde(default)]
    pub cluster_operation: ClusterOperation,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HbaseClusterConfig {
    /// HDFS cluster connection details from discovery config map
    pub hdfs_config_map_name: String,
    /// Name of the Vector aggregator discovery ConfigMap.
    /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_aggregator_config_map_name: Option<String>,
    /// ZooKeeper cluster connection details from discovery config map
    pub zookeeper_config_map_name: String,
    /// Configuration to set up a cluster secured using Kerberos.
    pub kerberos: Option<KerberosConfig>,
    /// In the future this setting will control, which ListenerClass <https://docs.stackable.tech/home/stable/listener-operator/listenerclass.html>
    /// will be used to expose the service.
    /// Currently only a subset of the ListenerClasses are supported by choosing the type of the created Services
    /// by looking at the ListenerClass name specified,
    /// In a future release support for custom ListenerClasses will be introduced without a breaking change:
    ///
    /// * cluster-internal: Use a ClusterIP service
    ///
    /// * external-unstable: Use a NodePort service
    #[serde(default)]
    pub listener_class: CurrentlySupportedListenerClasses,
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
    pub fn kerberos_service_name(&self) -> &'static str {
        // match self {
        //     HbaseRole::Master => "hbase-master",
        //     HbaseRole::RegionServer => "hbase-regionserver",
        //     HbaseRole::RestServer => "hbase-restserver",
        // }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hbase_opts: Option<String>,
    #[fragment_attrs(serde(default))]
    pub resources: Resources<HbaseStorageConfig, NoRuntimeLimits>,
    #[fragment_attrs(serde(default))]
    pub logging: Logging<Container>,
    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,
}

impl HbaseConfig {
    fn default_config(
        cluster_name: &str,
        role: &HbaseRole,
        hdfs_discovery_cm_name: &str,
    ) -> HbaseConfigFragment {
        HbaseConfigFragment {
            hbase_rootdir: None,
            hbase_opts: None,
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("200m".to_owned())),
                    max: Some(Quantity("4".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("2Gi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: HbaseStorageConfigFragment {},
            },
            logging: product_logging::spec::default_logging(),
            affinity: get_affinity(cluster_name, role, hdfs_discovery_cm_name),
        }
    }
}

impl Configuration for HbaseConfigFragment {
    type Configurable = HbaseCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _resource: &Self::Configurable,
        role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        match file {
            HBASE_ENV_SH => {
                result.insert(HBASE_MANAGES_ZK.to_string(), Some("false".to_string()));
                result.insert(
                    "KRB5_CONFIG".to_string(),
                    Some("/stackable/kerberos/krb5.conf".to_string()),
                );
                let mut all_hbase_opts = format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={METRICS_PORT}:/stackable/jmx/region-server.yaml -Djava.security.krb5.conf=/stackable/kerberos/krb5.conf");
                if let Some(hbase_opts) = &self.hbase_opts {
                    all_hbase_opts += " ";
                    all_hbase_opts += hbase_opts;
                }
                // set the jmx exporter in HBASE_MASTER_OPTS, HBASE_REGIONSERVER_OPTS and HBASE_REST_OPTS instead of HBASE_OPTS
                // to prevent a port-conflict i.e. CLI tools read HBASE_OPTS and may then try to re-start the exporter
                if role_name == HbaseRole::Master.to_string() {
                    result.insert(HBASE_MASTER_OPTS.to_string(), Some(all_hbase_opts));
                } else if role_name == HbaseRole::RegionServer.to_string() {
                    result.insert(HBASE_REGIONSERVER_OPTS.to_string(), Some(all_hbase_opts));
                } else if role_name == HbaseRole::RestServer.to_string() {
                    result.insert(HBASE_REST_OPTS.to_string(), Some(all_hbase_opts));
                }
            }
            HBASE_SITE_XML => {
                result.insert(
                    HBASE_CLUSTER_DISTRIBUTED.to_string(),
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

    pub fn get_role(&self, role: &HbaseRole) -> Option<&Role<HbaseConfigFragment>> {
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
    ) -> Result<&RoleGroup<HbaseConfigFragment>, Error> {
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

    pub fn has_kerberos_enabled(&self) -> bool {
        self.kerberos_secret_class().is_some()
    }

    pub fn kerberos_request_node_principals(&self) -> Option<bool> {
        self.spec
            .cluster_config
            .kerberos
            .as_ref()
            .map(|k| k.request_node_principals)
    }

    pub fn kerberos_secret_class(&self) -> Option<&str> {
        self.spec
            .cluster_config
            .kerberos
            .as_ref()
            .map(|k| k.kerberos_secret_class.as_str())
    }

    pub fn has_https_enabled(&self) -> bool {
        self.https_secret_class().is_some()
    }

    pub fn https_secret_class(&self) -> Option<&str> {
        self.spec
            .cluster_config
            .kerberos
            .as_ref()
            .map(|k| k.tls_secret_class.as_str())
    }

    /// Returns required port name and port number tuples depending on the role.
    pub fn ports(&self, role: &HbaseRole) -> Vec<(String, u16)> {
        // TODO: Respect HTTPS settings
        match role {
            HbaseRole::Master => vec![
                ("master".to_string(), HBASE_MASTER_PORT),
                (HBASE_UI_PORT_NAME.to_string(), HBASE_MASTER_UI_PORT),
                (METRICS_PORT_NAME.to_string(), METRICS_PORT),
            ],
            HbaseRole::RegionServer => vec![
                ("regionserver".to_string(), HBASE_REGIONSERVER_PORT),
                (HBASE_UI_PORT_NAME.to_string(), HBASE_REGIONSERVER_UI_PORT),
                (METRICS_PORT_NAME.to_string(), METRICS_PORT),
            ],
            HbaseRole::RestServer => vec![
                ("rest".to_string(), HBASE_REST_PORT),
                (METRICS_PORT_NAME.to_string(), METRICS_PORT),
            ],
        }
    }

    /// Retrieve and merge resource configs for role and role groups
    pub fn merged_config(
        &self,
        role: &HbaseRole,
        role_group: &str,
        hdfs_discovery_cm_name: &str,
    ) -> Result<HbaseConfig, Error> {
        // Initialize the result with all default values as baseline
        let conf_defaults =
            HbaseConfig::default_config(&self.name_any(), role, hdfs_discovery_cm_name);

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

        if let Some(RoleGroup {
            selector: Some(selector),
            ..
        }) = role.role_groups.get(role_group)
        {
            // Migrate old `selector` attribute, see ADR 26 affinities.
            // TODO Can be removed after support for the old `selector` field is dropped.
            #[allow(deprecated)]
            conf_rolegroup.affinity.add_legacy_selector(selector);
        }

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
