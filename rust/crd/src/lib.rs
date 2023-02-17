pub mod affinity;

use affinity::get_affinity;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
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

pub const HBASE_UI_PORT_NAME: &str = "ui";
pub const METRICS_PORT_NAME: &str = "metrics";

pub const HBASE_MASTER_PORT: i32 = 16000;
pub const HBASE_MASTER_UI_PORT: i32 = 16010;
pub const HBASE_REGIONSERVER_PORT: i32 = 16020;
pub const HBASE_REGIONSERVER_UI_PORT: i32 = 16030;
pub const HBASE_REST_PORT: i32 = 8080;
pub const METRICS_PORT: i32 = 8081;

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
    /// Emergency stop button, if `true` then all pods are stopped without affecting configuration (as setting `replicas` to `0` would)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped: Option<bool>,
    /// Desired HBase image
    pub image: ProductImage,
    /// ZooKeeper cluster connection details from discovery config map
    pub zookeeper_config_map_name: String,
    /// HDFS cluster connection details from discovery config map
    pub hdfs_config_map_name: String,
    /// Name of the Vector aggregator discovery ConfigMap.
    /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_aggregator_config_map_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<HbaseConfigFragment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub masters: Option<Role<HbaseConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region_servers: Option<Role<HbaseConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rest_servers: Option<Role<HbaseConfigFragment>>,
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
    /// Returns a port name, the port number, and the protocol for the given role.
    pub fn port_properties(&self) -> Vec<(&'static str, i32, &'static str)> {
        match self {
            HbaseRole::Master => vec![
                ("master", HBASE_MASTER_PORT, "TCP"),
                (HBASE_UI_PORT_NAME, HBASE_MASTER_UI_PORT, "TCP"),
                (METRICS_PORT_NAME, METRICS_PORT, "TCP"),
            ],
            HbaseRole::RegionServer => vec![
                ("regionserver", HBASE_REGIONSERVER_PORT, "TCP"),
                (HBASE_UI_PORT_NAME, HBASE_REGIONSERVER_UI_PORT, "TCP"),
                (METRICS_PORT_NAME, METRICS_PORT, "TCP"),
            ],
            HbaseRole::RestServer => vec![
                ("rest", HBASE_REST_PORT, "TCP"),
                (METRICS_PORT_NAME, METRICS_PORT, "TCP"),
            ],
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
        resource: &Self::Configurable,
        role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = if role_name.is_empty() {
            BTreeMap::new()
        } else if let Some(config) = &resource.spec.config {
            config.compute_files(resource, "", file)?
        } else {
            BTreeMap::new()
        };

        match file {
            HBASE_ENV_SH => {
                result.insert(HBASE_MANAGES_ZK.to_string(), Some("false".to_string()));
                let mut all_hbase_opts = format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={METRICS_PORT}:/stackable/jmx/region-server.yaml");
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
                result.insert(HBASE_ROOTDIR.to_string(), Some(resource.root_dir()));
            }
            _ => {}
        }

        result.retain(|_, maybe_value| maybe_value.is_some());

        Ok(result)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HbaseClusterStatus {}

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

    pub fn root_dir(&self) -> String {
        self.spec
            .config
            .as_ref()
            .and_then(|c| c.hbase_rootdir.as_deref())
            .unwrap_or("/hbase")
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
