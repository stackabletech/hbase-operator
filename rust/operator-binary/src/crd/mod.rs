use security::AuthenticationConfig;
use serde::{Deserialize, Serialize};
use shell_escape::escape;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::pod::volume::{
        ListenerOperatorVolumeSourceBuilder, ListenerOperatorVolumeSourceBuilderError,
        ListenerReference, VolumeBuilder,
    },
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
        fragment::Fragment,
        merge::{Atomic, Merge},
    },
    deep_merger::ObjectOverrides,
    k8s_openapi::{
        api::core::v1::{PersistentVolumeClaim, Volume},
        apimachinery::pkg::api::resource::Quantity,
    },
    kube::CustomResource,
    kvp::Labels,
    product_logging::{self, spec::Logging},
    role_utils::{GenericRoleConfig, Role},
    schemars::{self, JsonSchema},
    shared::time::Duration,
    status::condition::{ClusterCondition, HasStatusCondition},
    v2::{config_overrides::KeyValueConfigOverrides, role_utils::JavaCommonConfig},
    versioned::versioned,
};
use strum::{Display, EnumIter, EnumString};

use crate::crd::{affinity::get_affinity, security::AuthorizationConfig};

pub mod affinity;
pub mod security;

pub const APP_NAME: &str = "hbase";
pub const FIELD_MANAGER: &str = "hbase-operator";
pub const OPERATOR_NAME: &str = "hbase.stackable.com";

// This constant is hard coded in hbase-entrypoint.sh
// You need to change it there too.
pub const CONFIG_DIR_NAME: &str = "/stackable/conf";

pub const TLS_STORE_DIR: &str = "/stackable/tls";
pub const TLS_STORE_VOLUME_NAME: &str = "tls";
pub const TLS_STORE_PASSWORD: &str = "changeit";

pub const JVM_SECURITY_PROPERTIES_FILE: &str = "security.properties";

pub const HBASE_CLUSTER_DISTRIBUTED: &str = "hbase.cluster.distributed";
pub const HBASE_ROOTDIR: &str = "hbase.rootdir";
const DEFAULT_HBASE_ROOTDIR: &str = "/hbase";

const HBASE_UI_PORT_NAME_HTTP: &str = "ui-http";
const HBASE_UI_PORT_NAME_HTTPS: &str = "ui-https";
const HBASE_REST_PORT_NAME_HTTP: &str = "rest-http";
const HBASE_REST_PORT_NAME_HTTPS: &str = "rest-https";
const HBASE_METRICS_PORT_NAME: &str = "metrics";

pub const HBASE_MASTER_PORT: u16 = 16000;
// HBase always uses 16010, regardless of http or https. On 2024-01-17 we decided in Arch-meeting that we want to stick
// the port numbers to what the product is doing, so we get the least surprise for users - even when this means we have
// inconsistency between Stackable products.
pub const HBASE_MASTER_UI_PORT: u16 = 16010;
pub const HBASE_MASTER_METRICS_PORT: u16 = 16010;
pub const HBASE_REGIONSERVER_PORT: u16 = 16020;
pub const HBASE_REGIONSERVER_UI_PORT: u16 = 16030;
pub const HBASE_REGIONSERVER_METRICS_PORT: u16 = 16030;
pub const HBASE_REST_PORT: u16 = 8080;
pub const HBASE_REST_UI_PORT: u16 = 8085;
pub const HBASE_REST_METRICS_PORT: u16 = 8085;
pub const LISTENER_VOLUME_NAME: &str = "listener";
pub const LISTENER_VOLUME_DIR: &str = "/stackable/listener";

const DEFAULT_REGION_MOVER_TIMEOUT: Duration = Duration::from_minutes_unchecked(59);
const DEFAULT_REGION_MOVER_DELTA_TO_SHUTDOWN: Duration = Duration::from_minutes_unchecked(1);

const DEFAULT_LISTENER_CLASS: &str = "cluster-internal";

fn default_hbase_rootdir() -> String {
    DEFAULT_HBASE_ROOTDIR.to_string()
}

pub type MasterRoleType =
    Role<HbaseConfigFragment, v1alpha1::HbaseConfigOverrides, GenericRoleConfig, JavaCommonConfig>;

pub type RegionServerRoleType = Role<
    RegionServerConfigFragment,
    v1alpha1::HbaseConfigOverrides,
    GenericRoleConfig,
    JavaCommonConfig,
>;

pub type RestServerRoleType =
    Role<HbaseConfigFragment, v1alpha1::HbaseConfigOverrides, GenericRoleConfig, JavaCommonConfig>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build listener volume"))]
    BuildListenerVolume {
        source: ListenerOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to build listener pvc"))]
    BuildListenerPvc {
        source: ListenerOperatorVolumeSourceBuilderError,
    },
}

#[versioned(
    version(name = "v1alpha1"),
    crates(
        kube_core = "stackable_operator::kube::core",
        kube_client = "stackable_operator::kube::client",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars",
        versioned = "stackable_operator::versioned"
    )
)]
pub mod versioned {
    /// An HBase cluster stacklet. This resource is managed by the Stackable operator for Apache HBase.
    /// Find more information on how to use it and the resources that the operator generates in the
    /// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/hbase/).
    ///
    /// The CRD contains three roles: `masters`, `regionServers` and `restServers`.
    #[versioned(crd(
        group = "hbase.stackable.tech",
        kind = "HbaseCluster",
        plural = "hbaseclusters",
        shortname = "hbase",
        status = "HbaseClusterStatus",
        namespaced
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

        // no doc string - See ObjectOverrides struct
        #[serde(default)]
        pub object_overrides: ObjectOverrides,

        /// The HBase master process is responsible for assigning regions to region servers and
        /// manages the cluster.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub masters: Option<MasterRoleType>,

        /// Region servers hold the data and handle requests from clients for their region.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub region_servers: Option<RegionServerRoleType>,

        /// Rest servers provide a REST API to interact with.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub rest_servers: Option<RestServerRoleType>,
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

    #[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, Merge, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct HbaseConfigOverrides {
        #[serde(default, rename = "hbase-site.xml")]
        pub hbase_site_xml: KeyValueConfigOverrides,

        #[serde(default, rename = "hbase-env.sh")]
        pub hbase_env_sh: KeyValueConfigOverrides,

        #[serde(default, rename = "ssl-server.xml")]
        pub ssl_server_xml: KeyValueConfigOverrides,

        #[serde(default, rename = "ssl-client.xml")]
        pub ssl_client_xml: KeyValueConfigOverrides,

        #[serde(default, rename = "security.properties")]
        pub security_properties: KeyValueConfigOverrides,
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
    Ord,
    PartialEq,
    PartialOrd,
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

    /// Returns the name of the role as it is needed by the `bin/hbase {cli_role_name} start` command.
    pub fn cli_role_name(&self) -> String {
        match self {
            HbaseRole::Master | HbaseRole::RegionServer => self.to_string(),
            // Of course it is not called "restserver", so we need to have this match
            // instead of just letting the Display impl do it's thing ;P
            HbaseRole::RestServer => "rest".to_string(),
        }
    }

    pub fn listener_volume(
        &self,
        merged_config: &AnyServiceConfig,
        recommended_labels: &Labels,
    ) -> Result<Option<Volume>, Error> {
        let volume = match &self {
            // Master and regionservers should use ephemeral listener volumes
            // since clients pull the latest address from ZooKeeper
            HbaseRole::Master | HbaseRole::RegionServer => Some(
                VolumeBuilder::new(LISTENER_VOLUME_NAME)
                    .ephemeral(
                        ListenerOperatorVolumeSourceBuilder::new(
                            &ListenerReference::ListenerClass(
                                merged_config.listener_class().to_string(),
                            ),
                            recommended_labels,
                        )
                        .build_ephemeral()
                        .context(BuildListenerVolumeSnafu)?,
                    )
                    .build(),
            ),
            HbaseRole::RestServer => None,
        };
        Ok(volume)
    }

    pub fn listener_pvc(
        &self,
        merged_config: &AnyServiceConfig,
        recommended_labels: &Labels,
    ) -> Result<Option<Vec<PersistentVolumeClaim>>, Error> {
        let pvc = match &self {
            HbaseRole::Master | HbaseRole::RegionServer => None,
            HbaseRole::RestServer => Some(vec![
                ListenerOperatorVolumeSourceBuilder::new(
                    &ListenerReference::ListenerClass(merged_config.listener_class().to_string()),
                    recommended_labels,
                )
                .build_pvc(LISTENER_VOLUME_NAME.to_string())
                .context(BuildListenerPvcSnafu)?,
            ]),
        };
        Ok(pvc)
    }

    /// Returns required port name and port number tuples depending on the role.
    ///
    /// Hbase versions 2.6.* will have two ports for each role. The metrics are available on the
    /// UI port.
    pub fn ports(&self, hbase: &v1alpha1::HbaseCluster) -> Vec<(String, u16)> {
        vec![
            (self.data_port_name(hbase), self.data_port()),
            (
                Self::ui_port_name(hbase.has_https_enabled()).to_string(),
                self.ui_port(),
            ),
        ]
    }

    pub fn data_port(&self) -> u16 {
        match self {
            HbaseRole::Master => HBASE_MASTER_PORT,
            HbaseRole::RegionServer => HBASE_REGIONSERVER_PORT,
            HbaseRole::RestServer => HBASE_REST_PORT,
        }
    }

    pub fn data_port_name(&self, hbase: &v1alpha1::HbaseCluster) -> String {
        match self {
            HbaseRole::Master | HbaseRole::RegionServer => self.to_string(),
            HbaseRole::RestServer => {
                if hbase.has_https_enabled() {
                    HBASE_REST_PORT_NAME_HTTPS.to_owned()
                } else {
                    HBASE_REST_PORT_NAME_HTTP.to_owned()
                }
            }
        }
    }

    pub fn ui_port(&self) -> u16 {
        match self {
            HbaseRole::Master => HBASE_MASTER_UI_PORT,
            HbaseRole::RegionServer => HBASE_REGIONSERVER_UI_PORT,
            HbaseRole::RestServer => HBASE_REST_UI_PORT,
        }
    }

    /// Name of the port used by the Web UI, which depends on HTTPS usage
    pub fn ui_port_name(has_https_enabled: bool) -> &'static str {
        if has_https_enabled {
            HBASE_UI_PORT_NAME_HTTPS
        } else {
            HBASE_UI_PORT_NAME_HTTP
        }
    }

    pub fn metrics_port(&self) -> u16 {
        match self {
            HbaseRole::Master => HBASE_MASTER_METRICS_PORT,
            HbaseRole::RegionServer => HBASE_REGIONSERVER_METRICS_PORT,
            HbaseRole::RestServer => HBASE_REST_METRICS_PORT,
        }
    }

    pub fn metrics_port_name() -> &'static str {
        HBASE_METRICS_PORT_NAME
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

impl HbaseConfigFragment {
    /// The operator defaults for a `masters` or `restServers` role group.
    pub fn default_config(
        role: &HbaseRole,
        cluster_name: &str,
        hdfs_discovery_cm_name: &str,
    ) -> Self {
        let graceful_shutdown_timeout = match role {
            HbaseRole::Master => HbaseRole::DEFAULT_MASTER_GRACEFUL_SHUTDOWN_TIMEOUT,
            HbaseRole::RegionServer => HbaseRole::DEFAULT_REGION_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT,
            HbaseRole::RestServer => HbaseRole::DEFAULT_REST_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT,
        };
        let requested_secret_lifetime = match role {
            HbaseRole::Master => HbaseRole::DEFAULT_MASTER_SECRET_LIFETIME,
            HbaseRole::RegionServer => HbaseRole::DEFAULT_REGION_SECRET_LIFETIME,
            HbaseRole::RestServer => HbaseRole::DEFAULT_REST_SECRET_LIFETIME,
        };
        HbaseConfigFragment {
            hbase_rootdir: Some(default_hbase_rootdir()),
            resources: default_resources(role),
            logging: product_logging::spec::default_logging(),
            affinity: get_affinity(cluster_name, role, hdfs_discovery_cm_name),
            graceful_shutdown_timeout: Some(graceful_shutdown_timeout),
            requested_secret_lifetime: Some(requested_secret_lifetime),
            listener_class: Some(DEFAULT_LISTENER_CLASS.to_string()),
        }
    }
}

impl RegionServerConfigFragment {
    /// The operator defaults for a `regionServers` role group.
    pub fn default_config(
        role: &HbaseRole,
        cluster_name: &str,
        hdfs_discovery_cm_name: &str,
    ) -> Self {
        RegionServerConfigFragment {
            hbase_rootdir: Some(default_hbase_rootdir()),
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
            listener_class: Some(DEFAULT_LISTENER_CLASS.to_string()),
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
    /// Root directory for Hbase on the filesystem (usually a path in HDFS). Default is `/hbase`.
    #[serde(default = "default_hbase_rootdir")]
    pub hbase_rootdir: String,

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
    #[serde(default = "default_hbase_rootdir")]
    pub hbase_rootdir: String,
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

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HbaseClusterStatus {
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

#[derive(Clone, Debug)]
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

    /// The configured `hbase.rootdir`.
    pub fn hbase_rootdir(&self) -> String {
        match self {
            AnyServiceConfig::Master(config) => config.hbase_rootdir.clone(),
            AnyServiceConfig::RegionServer(config) => config.hbase_rootdir.clone(),
            AnyServiceConfig::RestServer(config) => config.hbase_rootdir.clone(),
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
pub(crate) mod test_helpers {
    use stackable_operator::{
        config::{fragment::FromFragment, merge::Merge},
        kube::ResourceExt,
        role_utils::{GenericRoleConfig, Role},
        v2::{
            jvm_argument_overrides::JvmArgumentOverrides,
            role_utils::{JavaCommonConfig, with_validated_config},
        },
    };

    use super::{
        AnyServiceConfig, HbaseConfig, HbaseConfigFragment, HbaseRole, RegionServerConfig,
        RegionServerConfigFragment, v1alpha1,
    };

    /// Test helper: merge + validate a single role group via the production
    /// [`with_validated_config`] path (the same merge the controller runs), returning the
    /// role-specific [`AnyServiceConfig`] and the merged [`JvmArgumentOverrides`].
    pub(crate) fn merged_role_group_config(
        hbase: &v1alpha1::HbaseCluster,
        role: &HbaseRole,
        role_group: &str,
        hdfs_discovery_cm_name: &str,
    ) -> (AnyServiceConfig, JvmArgumentOverrides) {
        match role {
            HbaseRole::Master => merge::<HbaseConfig, _>(
                hbase
                    .spec
                    .masters
                    .as_ref()
                    .expect("master role must be defined"),
                role_group,
                HbaseConfigFragment::default_config(
                    role,
                    &hbase.name_any(),
                    hdfs_discovery_cm_name,
                ),
                AnyServiceConfig::Master,
            ),
            HbaseRole::RegionServer => merge::<RegionServerConfig, _>(
                hbase
                    .spec
                    .region_servers
                    .as_ref()
                    .expect("region server role must be defined"),
                role_group,
                RegionServerConfigFragment::default_config(
                    role,
                    &hbase.name_any(),
                    hdfs_discovery_cm_name,
                ),
                AnyServiceConfig::RegionServer,
            ),
            HbaseRole::RestServer => merge::<HbaseConfig, _>(
                hbase
                    .spec
                    .rest_servers
                    .as_ref()
                    .expect("rest server role must be defined"),
                role_group,
                HbaseConfigFragment::default_config(
                    role,
                    &hbase.name_any(),
                    hdfs_discovery_cm_name,
                ),
                AnyServiceConfig::RestServer,
            ),
        }
    }

    fn merge<ValidatedConfig, Config>(
        role: &Role<Config, v1alpha1::HbaseConfigOverrides, GenericRoleConfig, JavaCommonConfig>,
        role_group: &str,
        default_config: Config,
        wrap: fn(ValidatedConfig) -> AnyServiceConfig,
    ) -> (AnyServiceConfig, JvmArgumentOverrides)
    where
        Config: Clone + Merge,
        ValidatedConfig: FromFragment<Fragment = Config>,
    {
        let role_group = role
            .role_groups
            .get(role_group)
            .expect("role group must be defined");
        let validated = with_validated_config::<
            ValidatedConfig,
            JavaCommonConfig,
            Config,
            GenericRoleConfig,
            v1alpha1::HbaseConfigOverrides,
        >(role_group, role, &default_config)
        .expect("role group config should merge and validate");
        (
            wrap(validated.config.config),
            validated
                .config
                .product_specific_common_config
                .jvm_argument_overrides,
        )
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use rstest::rstest;
    use stackable_operator::versioned::test_utils::RoundtripTestData;

    use super::*;

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
    productVersion: 2.6.4
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

        let (merged_config, _) = super::test_helpers::merged_role_group_config(
            &hbase,
            &hbase_role,
            role_group_name,
            &hbase.spec.cluster_config.hdfs_config_map_name,
        );
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

    impl RoundtripTestData for v1alpha1::HbaseClusterSpec {
        fn roundtrip_test_data() -> Vec<Self> {
            stackable_operator::utils::yaml_from_str_singleton_map(indoc::indoc! {r#"
              - image:
                  productVersion: 2.6.4
                  pullPolicy: IfNotPresent
                clusterOperation:
                  reconciliationPaused: false
                  stopped: true
                clusterConfig:
                  hdfsConfigMapName: test-hdfs
                  zookeeperConfigMapName: test-znode
                  vectorAggregatorConfigMapName: vector-aggregator-discovery
                  authentication:
                    tlsSecretClass: my-tls
                    kerberos:
                      secretClass: my-kerberos
                  authorization:
                    opa:
                      configMapName: opa
                      package: hbase
                masters:
                  envOverrides:
                    COMMON_VAR: role-value
                    ROLE_VAR: role-value
                  config:
                    gracefulShutdownTimeout: 1m
                    resources:
                      cpu:
                        min: 250m
                        max: "1"
                      memory:
                        limit: 1Gi
                    logging:
                      enableVectorAgent: true
                    listenerClass: cluster-internal
                  configOverrides:
                    hbase-site.xml:
                      hbase.master.info.port: "16010"
                  roleGroups:
                    default:
                      replicas: 2
                      configOverrides:
                        hbase-site.xml:
                          hbase.master.info.port: "16011"
                      envOverrides:
                        COMMON_VAR: group-value
                        GROUP_VAR: group-value
                regionServers:
                  config:
                    gracefulShutdownTimeout: 2m
                    resources:
                      cpu:
                        min: 250m
                        max: "2"
                      memory:
                        limit: 2Gi
                    logging:
                      enableVectorAgent: true
                    regionMover:
                      runBeforeShutdown: true
                      ack: true
                      maxThreads: 1
                    listenerClass: cluster-internal
                  roleGroups:
                    default:
                      replicas: 3
                restServers:
                  config:
                    gracefulShutdownTimeout: 1m
                    resources:
                      cpu:
                        min: 100m
                        max: "1"
                      memory:
                        limit: 1Gi
                    logging:
                      enableVectorAgent: true
                    listenerClass: cluster-internal
                  roleGroups:
                    default:
                      replicas: 1
        "#})
            .expect("Failed to parse HbaseClusterSpec YAML")
        }
    }
}
