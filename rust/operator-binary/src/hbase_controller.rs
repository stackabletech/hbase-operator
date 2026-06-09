//! Ensures that `Pod`s are configured and running for each [`v1alpha1::HbaseCluster`]

use std::{collections::BTreeMap, sync::Arc};

use const_format::concatcp;
use indoc::formatdoc;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder, container::ContainerBuilder, resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder,
        },
    },
    cli::OperatorEnvironmentOptions,
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{product_image_selection::ResolvedProductImage, rbac::build_rbac_resources},
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMapVolumeSource, ContainerPort, EnvVar, PodTemplateSpec, Probe, Service,
                ServiceAccount, ServicePort, ServiceSpec, TCPSocketAction, Volume,
            },
        },
        apimachinery::pkg::{
            apis::meta::v1::{LabelSelector, ObjectMeta},
            util::intstr::IntOrString,
        },
    },
    kube::{
        Resource, ResourceExt,
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    kvp::{Annotations, Label, LabelError, Labels, ObjectLabels},
    logging::controller::ReconcilerError,
    product_logging::{
        self,
        framework::LoggingError,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::RoleGroupRef,
    shared::time::Duration,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        builder::pod::container::EnvVarSet,
        types::{
            kubernetes::{NamespaceName, Uid},
            operator::ClusterName,
        },
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::build::{
        discovery::build_discovery_config_map,
        properties::logging::{MAX_HBASE_LOG_FILES_SIZE, STACKABLE_LOG_DIR},
    },
    crd::{
        APP_NAME, AnyServiceConfig, CONFIG_DIR_NAME, Container, HbaseClusterStatus, HbaseRole,
        LISTENER_VOLUME_DIR, LISTENER_VOLUME_NAME, OPERATOR_NAME, merged_env, v1alpha1,
    },
    kerberos::{self, add_kerberos_pod_config},
    operations::{graceful_shutdown::add_graceful_shutdown_config, pdb::add_pdbs},
    security::opa::HbaseOpaConfig,
    zookeeper::ZookeeperConnectionInformation,
};

pub const HBASE_CONTROLLER_NAME: &str = "hbasecluster";
pub const FULL_HBASE_CONTROLLER_NAME: &str = concatcp!(HBASE_CONTROLLER_NAME, '.', OPERATOR_NAME);

pub static CONTAINERDEBUG_LOG_DIRECTORY: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| format!("{STACKABLE_LOG_DIR}/containerdebug"));

// These constants are hard coded in hbase-entrypoint.sh
// You need to change them there too.
const HDFS_DISCOVERY_TMP_DIR: &str = "/stackable/tmp/hdfs";
const HBASE_CONFIG_TMP_DIR: &str = "/stackable/tmp/hbase";
const HBASE_LOG_CONFIG_TMP_DIR: &str = "/stackable/tmp/log_config";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub operator_environment: OperatorEnvironmentOptions,
}

/// The validated cluster: proves that config merging and validation succeeded for
/// every role and role group before any resources are created.
#[derive(Clone, Debug)]
pub struct ValidatedCluster {
    /// Backs the [`Resource`] implementation (provides `meta()`/`name_any()`) so the build
    /// functions can derive `ObjectMeta`, owner references and labels without the full
    /// `HbaseCluster` object. Holds only name, namespace and uid.
    metadata: ObjectMeta,
    /// The logical (and Kubernetes object) name of the cluster.
    pub name: ClusterName,
    /// The namespace the cluster lives in. Part of the cluster identity; currently consumed via
    /// the [`Resource`] metadata (`name_and_namespace`) rather than read directly.
    #[allow(dead_code)]
    pub namespace: NamespaceName,
    /// The UID of the `HbaseCluster` object, used to build owner references.
    pub uid: Uid,
    pub image: ResolvedProductImage,
    pub cluster_config: ValidatedClusterConfig,
    pub role_group_configs: BTreeMap<HbaseRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
    pub role_configs: BTreeMap<HbaseRole, ValidatedRoleConfig>,
}

impl ValidatedCluster {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: ClusterName,
        namespace: NamespaceName,
        uid: Uid,
        image: ResolvedProductImage,
        cluster_config: ValidatedClusterConfig,
        role_group_configs: BTreeMap<HbaseRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
        role_configs: BTreeMap<HbaseRole, ValidatedRoleConfig>,
    ) -> Self {
        Self {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                uid: Some(uid.to_string()),
                ..ObjectMeta::default()
            },
            name,
            namespace,
            uid,
            image,
            cluster_config,
            role_group_configs,
            role_configs,
        }
    }
}

impl Resource for ValidatedCluster {
    type DynamicType = <v1alpha1::HbaseCluster as Resource>::DynamicType;
    type Scope = <v1alpha1::HbaseCluster as Resource>::Scope;

    fn group(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HbaseCluster::group(dt)
    }

    fn version(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HbaseCluster::version(dt)
    }

    fn kind(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HbaseCluster::kind(dt)
    }

    fn plural(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HbaseCluster::plural(dt)
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

impl HasName for ValidatedCluster {
    fn to_name(&self) -> String {
        self.name.to_string()
    }
}

impl HasUid for ValidatedCluster {
    fn to_uid(&self) -> Uid {
        self.uid.clone()
    }
}

impl NameIsValidLabelValue for ValidatedCluster {
    fn to_label_value(&self) -> String {
        self.name.to_label_value()
    }
}

/// Cluster-wide settings resolved once during validation.
#[derive(Clone, Debug)]
pub struct ValidatedClusterConfig {
    // Pre-resolved OPA connection configuration.
    pub hbase_opa_config: Option<HbaseOpaConfig>,
    pub kerberos_enabled: bool,
    // Pre-resolved kerberos properties for hbase-site.xml (empty when kerberos is disabled).
    pub hbase_site_kerberos_config: BTreeMap<String, String>,
    // Pre-resolved kerberos properties for the discovery `hbase-site.xml` exposed to clients
    // (empty when kerberos is disabled).
    pub discovery_kerberos_config: BTreeMap<String, String>,
    // Pre-resolved ssl-server.xml settings (empty when HTTPS is disabled).
    pub ssl_server_settings: BTreeMap<String, String>,
    // Pre-resolved ssl-client.xml settings (empty when HTTPS is disabled).
    pub ssl_client_settings: BTreeMap<String, String>,
    // Pre-resolved zookeeper connection settings.
    pub zookeeper_connection_information: ZookeeperConnectionInformation,
}

/// Per-role configuration extracted during validation.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: stackable_operator::commons::pdb::PdbConfig,
}

/// Per-rolegroup configuration: the merged CRD config plus the merged
/// (role <- role group) `configOverrides`, `envOverrides` and `podOverrides`.
///
/// This carries every override channel so that the build step is a pure function of
/// [`ValidatedCluster`] and never has to reach back into the raw `HbaseCluster`.
#[derive(Clone, Debug)]
pub struct ValidatedRoleGroupConfig {
    /// The desired number of replicas (`None` lets Kubernetes default to 1).
    pub replicas: Option<u16>,
    pub merged_config: AnyServiceConfig,
    pub config_overrides: v1alpha1::HbaseConfigOverrides,
    pub env_overrides: EnvVarSet,
    /// Merged (role <- role group) pod template overrides.
    pub pod_overrides: PodTemplateSpec,
    /// Pre-resolved role-specific non-heap JVM args (operator-generated + role/role-group overrides).
    pub non_heap_jvm_args: String,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::HbaseCluster>,
    },

    #[snafu(display("failed to apply discovery configmap"))]
    ApplyDiscoveryConfigMap {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build discovery configmap"))]
    BuildDiscoveryConfigMap {
        source: crate::controller::build::discovery::Error,
    },

    #[snafu(display("failed to build rolegroup ConfigMap"))]
    BuildRolegroupConfigMap {
        source: crate::controller::build::config_map::Error,
    },

    #[snafu(display("failed to apply ConfigMap for {}", rolegroup))]
    ApplyRoleGroupConfig {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::HbaseCluster>,
    },

    #[snafu(display("failed to apply StatefulSet for {}", rolegroup))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::HbaseCluster>,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to patch service account"))]
    ApplyServiceAccount {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to patch role binding"))]
    ApplyRoleBinding {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,

    #[snafu(display("failed to add kerberos config"))]
    AddKerberosConfig { source: kerberos::Error },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("failed to create PodDisruptionBudget"))]
    FailedToCreatePdb {
        source: crate::operations::pdb::Error,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::operations::graceful_shutdown::Error,
    },

    #[snafu(display("failed to build label"))]
    BuildLabel { source: LabelError },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to configure logging"))]
    ConfigureLogging { source: LoggingError },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("HBaseCluster object is invalid"))]
    InvalidHBaseCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("failed to build listener volume"))]
    ListenerVolume { source: crate::crd::Error },

    #[snafu(display("failed to build listener persistent volume claim"))]
    ListenerPersistentVolumeClaim { source: crate::crd::Error },

    #[snafu(display("failed to dereference cluster resources"))]
    Dereference {
        source: crate::controller::dereference::Error,
    },

    #[snafu(display("failed to validate cluster configuration"))]
    Validate {
        source: crate::controller::validate::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_hbase(
    hbase: Arc<DeserializeGuard<v1alpha1::HbaseCluster>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let hbase = hbase
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidHBaseClusterSnafu)?;

    let client = &ctx.client;

    let dereferenced_objects = crate::controller::dereference::dereference(client, hbase)
        .await
        .context(DereferenceSnafu)?;

    let validated_cluster = crate::controller::validate::validate_cluster(
        hbase,
        &ctx.operator_environment.image_repository,
        &client.kubernetes_cluster_info,
        dereferenced_objects,
    )
    .context(ValidateSnafu)?;

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        HBASE_CONTROLLER_NAME,
        &hbase.object_ref(&()),
        ClusterResourceApplyStrategy::from(&hbase.spec.cluster_operation),
        &hbase.spec.object_overrides,
    )
    .context(CreateClusterResourcesSnafu)?;

    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
        hbase,
        APP_NAME,
        cluster_resources
            .get_required_labels()
            .context(BuildLabelSnafu)?,
    )
    .context(BuildRbacResourcesSnafu)?;
    cluster_resources
        .add(client, rbac_sa.clone())
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    for (hbase_role, role_group_configs) in &validated_cluster.role_group_configs {
        for (rolegroup_name, validated_rg_config) in role_group_configs {
            let rolegroup = hbase.server_rolegroup_ref(hbase_role.to_string(), rolegroup_name);

            let rg_service =
                build_rolegroup_service(hbase, hbase_role, &rolegroup, &validated_cluster.image)?;

            let rg_metrics_service = build_rolegroup_metrics_service(
                hbase,
                hbase_role,
                &rolegroup,
                &validated_cluster.image,
            )?;

            let rg_configmap = crate::controller::build::config_map::build_rolegroup_config_map(
                &validated_cluster,
                hbase_role,
                &rolegroup,
            )
            .context(BuildRolegroupConfigMapSnafu)?;
            let rg_statefulset = build_rolegroup_statefulset(
                hbase,
                hbase_role,
                &rolegroup,
                validated_rg_config,
                &validated_cluster.image,
                &rbac_sa,
            )?;
            cluster_resources
                .add(client, rg_service)
                .await
                .with_context(|_| ApplyRoleGroupServiceSnafu {
                    rolegroup: rolegroup.clone(),
                })?;
            cluster_resources
                .add(client, rg_metrics_service)
                .await
                .with_context(|_| ApplyRoleGroupServiceSnafu {
                    rolegroup: rolegroup.clone(),
                })?;
            cluster_resources
                .add(client, rg_configmap)
                .await
                .with_context(|_| ApplyRoleGroupConfigSnafu {
                    rolegroup: rolegroup.clone(),
                })?;

            // Note: The StatefulSet needs to be applied after all ConfigMaps and Secrets it mounts
            // to prevent unnecessary Pod restarts.
            // See https://github.com/stackabletech/commons-operator/issues/111 for details.
            ss_cond_builder.add(
                cluster_resources
                    .add(client, rg_statefulset)
                    .await
                    .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                        rolegroup: rolegroup.clone(),
                    })?,
            );
        }

        if let Some(role_config) = validated_cluster.role_configs.get(hbase_role) {
            add_pdbs(
                &role_config.pdb,
                hbase,
                hbase_role,
                client,
                &mut cluster_resources,
            )
            .await
            .context(FailedToCreatePdbSnafu)?;
        }
    }

    // Discovery CM will fail to build until the rest of the cluster has been deployed, so do it last
    // so that failure won't inhibit the rest of the cluster from booting up.
    let discovery_cm =
        build_discovery_config_map(&validated_cluster).context(BuildDiscoveryConfigMapSnafu)?;
    cluster_resources
        .add(client, discovery_cm)
        .await
        .context(ApplyDiscoveryConfigMapSnafu)?;

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&hbase.spec.cluster_operation);

    let status = HbaseClusterStatus {
        conditions: compute_conditions(hbase, &[&ss_cond_builder, &cluster_operation_cond_builder]),
    };

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;
    client
        .apply_patch_status(OPERATOR_NAME, hbase, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::await_change())
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_rolegroup_service(
    hbase: &v1alpha1::HbaseCluster,
    hbase_role: &HbaseRole,
    rolegroup: &RoleGroupRef<v1alpha1::HbaseCluster>,
    resolved_product_image: &ResolvedProductImage,
) -> Result<Service> {
    let ports = hbase_role
        .ports(hbase)
        .into_iter()
        .map(|(name, value)| ServicePort {
            name: Some(name),
            port: i32::from(value),
            protocol: Some("TCP".to_string()),
            ..ServicePort::default()
        })
        .collect();

    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(hbase)
        .name(rolegroup.rolegroup_headless_service_name())
        .ownerreference_from_resource(hbase, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(&build_recommended_labels(
            hbase,
            &resolved_product_image.app_version_label_value,
            &rolegroup.role,
            &rolegroup.role_group,
        ))
        .context(ObjectMetaSnafu)?
        .build();

    let service_selector =
        Labels::role_group_selector(hbase, APP_NAME, &rolegroup.role, &rolegroup.role_group)
            .context(BuildLabelSnafu)?;

    let service_spec = ServiceSpec {
        // Internal communication does not need to be exposed
        type_: Some("ClusterIP".to_string()),
        cluster_ip: Some("None".to_string()),
        ports: Some(ports),
        selector: Some(service_selector.into()),
        publish_not_ready_addresses: Some(true),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata,
        spec: Some(service_spec),
        status: None,
    })
}

/// The rolegroup metrics [`Service`] is a service that exposes metrics and a prometheus scraping label.
fn build_rolegroup_metrics_service(
    hbase: &v1alpha1::HbaseCluster,
    hbase_role: &HbaseRole,
    rolegroup: &RoleGroupRef<v1alpha1::HbaseCluster>,
    resolved_product_image: &ResolvedProductImage,
) -> Result<Service, Error> {
    let ports = vec![ServicePort {
        name: Some(HbaseRole::metrics_port_name().to_owned()),
        port: i32::from(hbase_role.metrics_port()),
        protocol: Some("TCP".to_owned()),
        ..ServicePort::default()
    }];

    let service_selector =
        Labels::role_group_selector(hbase, APP_NAME, &rolegroup.role, &rolegroup.role_group)
            .context(BuildLabelSnafu)?;

    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hbase)
            .name(rolegroup.rolegroup_metrics_service_name())
            .ownerreference_from_resource(hbase, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(&build_recommended_labels(
                hbase,
                &resolved_product_image.app_version_label_value,
                &rolegroup.role,
                &rolegroup.role_group,
            ))
            .context(ObjectMetaSnafu)?
            .with_label(Label::try_from(("prometheus.io/scrape", "true")).context(LabelBuildSnafu)?)
            .with_annotations(prometheus_annotations(hbase, hbase_role))
            .build(),
        spec: Some(ServiceSpec {
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_owned()),
            cluster_ip: Some("None".to_owned()),
            ports: Some(ports),
            selector: Some(service_selector.into()),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// Common annotations for Prometheus
///
/// These annotations can be used in a ServiceMonitor.
///
/// see also <https://github.com/prometheus-community/helm-charts/blob/prometheus-27.32.0/charts/prometheus/values.yaml#L983-L1036>
fn prometheus_annotations(hbase: &v1alpha1::HbaseCluster, hbase_role: &HbaseRole) -> Annotations {
    Annotations::try_from([
        ("prometheus.io/path".to_owned(), "/prometheus".to_owned()),
        (
            "prometheus.io/port".to_owned(),
            hbase_role.metrics_port().to_string(),
        ),
        (
            "prometheus.io/scheme".to_owned(),
            if hbase.has_https_enabled() {
                "https".to_owned()
            } else {
                "http".to_owned()
            },
        ),
        ("prometheus.io/scrape".to_owned(), "true".to_owned()),
    ])
    .expect("should be valid annotations")
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding [`Service`] (from [`build_rolegroup_service`]).
#[allow(clippy::too_many_arguments)]
fn build_rolegroup_statefulset(
    hbase: &v1alpha1::HbaseCluster,
    hbase_role: &HbaseRole,
    rolegroup_ref: &RoleGroupRef<v1alpha1::HbaseCluster>,
    validated_rg_config: &ValidatedRoleGroupConfig,
    resolved_product_image: &ResolvedProductImage,
    service_account: &ServiceAccount,
) -> Result<StatefulSet> {
    let merged_config = &validated_rg_config.merged_config;
    let hbase_version = &resolved_product_image.app_version_label_value;

    let ports = hbase_role
        .ports(hbase)
        .into_iter()
        .map(|(name, value)| ContainerPort {
            name: Some(name),
            container_port: i32::from(value),
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        })
        .collect();

    let probe_template = Probe {
        tcp_socket: Some(TCPSocketAction {
            port: IntOrString::String(hbase_role.data_port_name(hbase)),
            ..TCPSocketAction::default()
        }),
        ..Probe::default()
    };

    let startup_probe = Probe {
        failure_threshold: Some(120),
        initial_delay_seconds: Some(4),
        period_seconds: Some(5),
        timeout_seconds: Some(3),
        ..probe_template.clone()
    };
    let liveness_probe = Probe {
        failure_threshold: Some(3),
        period_seconds: Some(10),
        timeout_seconds: Some(3),
        ..probe_template.clone()
    };
    let readiness_probe = Probe {
        failure_threshold: Some(1),
        period_seconds: Some(10),
        timeout_seconds: Some(2),
        ..probe_template
    };

    let mut env_map: BTreeMap<String, String> = BTreeMap::from([
        ("HBASE_CONF_DIR".to_string(), CONFIG_DIR_NAME.to_string()),
        ("HADOOP_CONF_DIR".to_string(), CONFIG_DIR_NAME.to_string()),
    ]);
    for env_var in validated_rg_config.env_overrides.clone() {
        env_map.insert(env_var.name, env_var.value.unwrap_or_default());
    }
    let mut merged_env = merged_env(&env_map);
    // This env var is set for all roles to avoid bash's "unbound variable" errors
    merged_env.extend([
        EnvVar {
            name: "REGION_MOVER_OPTS".to_string(),
            value: Some(merged_config.region_mover_args()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "RUN_REGION_MOVER".to_string(),
            value: Some(merged_config.run_region_mover().to_string()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "STACKABLE_LOG_DIR".to_string(),
            value: Some(STACKABLE_LOG_DIR.to_string()),
            ..EnvVar::default()
        },
    ]);

    let role_name = hbase_role.cli_role_name();
    let mut hbase_container = ContainerBuilder::new("hbase").expect("ContainerBuilder not created");

    hbase_container
        .image_from_product_image(resolved_product_image)
        .command(command())
        .args(vec![formatdoc! {"
            {entrypoint} {role} {port} {port_name} {ui_port_name}",
            entrypoint = "/stackable/hbase/bin/hbase-entrypoint.sh".to_string(),
            role = role_name,
            port = hbase_role.data_port(),
            port_name = hbase_role.data_port_name(hbase),
            ui_port_name = HbaseRole::ui_port_name(hbase.has_https_enabled()),
        }])
        .add_env_vars(merged_env)
        // Needed for the `containerdebug` process to log it's tracing information to.
        .add_env_var(
            "CONTAINERDEBUG_LOG_DIRECTORY",
            &*CONTAINERDEBUG_LOG_DIRECTORY,
        )
        .add_volume_mount("hbase-config", HBASE_CONFIG_TMP_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("hdfs-discovery", HDFS_DISCOVERY_TMP_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log-config", HBASE_LOG_CONFIG_TMP_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log", STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(LISTENER_VOLUME_NAME, LISTENER_VOLUME_DIR)
        .context(AddVolumeMountSnafu)?
        .add_container_ports(ports)
        .resources(merged_config.resources().clone().into())
        .startup_probe(startup_probe)
        .liveness_probe(liveness_probe)
        .readiness_probe(readiness_probe);

    let mut pod_builder = PodBuilder::new();

    let recommended_object_labels = build_recommended_labels(
        hbase,
        hbase_version,
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    );
    let recommended_labels =
        Labels::recommended(&recommended_object_labels).context(LabelBuildSnafu)?;

    let pb_metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(&recommended_object_labels)
        .context(ObjectMetaSnafu)?
        .build();

    pod_builder
        .metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .affinity(merged_config.affinity())
        .add_volume(stackable_operator::k8s_openapi::api::core::v1::Volume {
            name: "hbase-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: rolegroup_ref.object_name(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .context(AddVolumeSnafu)?
        .add_volume(stackable_operator::k8s_openapi::api::core::v1::Volume {
            name: "hdfs-discovery".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: hbase.spec.cluster_config.hdfs_config_map_name.clone(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .context(AddVolumeSnafu)?
        .add_empty_dir_volume(
            "log",
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_HBASE_LOG_FILES_SIZE],
            )),
        )
        .context(AddVolumeSnafu)?
        .service_account_name(service_account.name_any())
        .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());

    if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = merged_config.logging().containers.get(&Container::Hbase)
    {
        pod_builder
            .add_volume(Volume {
                name: "log-config".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: config_map.into(),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            })
            .context(AddVolumeSnafu)?;
    } else {
        pod_builder
            .add_volume(Volume {
                name: "log-config".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: rolegroup_ref.object_name(),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            })
            .context(AddVolumeSnafu)?;
    }

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;
    if hbase.has_kerberos_enabled() {
        add_kerberos_pod_config(
            hbase,
            rolegroup_ref,
            &mut hbase_container,
            &mut pod_builder,
            merged_config
                .requested_secret_lifetime()
                .context(MissingSecretLifetimeSnafu)?,
        )
        .context(AddKerberosConfigSnafu)?;
    }
    pod_builder.add_container(hbase_container.build());

    // Vector sidecar shall be the last container in the list
    if merged_config.logging().enable_vector_agent {
        if let Some(vector_aggregator_config_map_name) =
            &hbase.spec.cluster_config.vector_aggregator_config_map_name
        {
            pod_builder.add_container(
                product_logging::framework::vector_container(
                    resolved_product_image,
                    "hbase-config",
                    "log",
                    merged_config.logging().containers.get(&Container::Vector),
                    ResourceRequirementsBuilder::new()
                        .with_cpu_request("250m")
                        .with_cpu_limit("500m")
                        .with_memory_request("128Mi")
                        .with_memory_limit("128Mi")
                        .build(),
                    vector_aggregator_config_map_name,
                )
                .context(ConfigureLoggingSnafu)?,
            );
        } else {
            VectorAggregatorConfigMapMissingSnafu.fail()?;
        }
    }

    let listener_pvc = hbase_role
        .listener_pvc(merged_config, &recommended_labels)
        .context(ListenerPersistentVolumeClaimSnafu)?;

    if let Some(listener_volume) = hbase_role
        .listener_volume(merged_config, &recommended_labels)
        .context(ListenerVolumeSnafu)?
    {
        pod_builder
            .add_volume(listener_volume)
            .context(AddVolumeSnafu)?;
    };

    let mut pod_template = pod_builder.build_template();

    pod_template.merge_from(validated_rg_config.pod_overrides.clone());

    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(hbase)
        .name(rolegroup_ref.object_name())
        .ownerreference_from_resource(hbase, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(&build_recommended_labels(
            hbase,
            hbase_version,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        .context(ObjectMetaSnafu)?
        .with_label(RESTART_CONTROLLER_ENABLED_LABEL.to_owned())
        .build();

    let statefulset_match_labels = Labels::role_group_selector(
        hbase,
        APP_NAME,
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    )
    .context(BuildLabelSnafu)?;

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some("Parallel".to_string()),
        replicas: validated_rg_config.replicas.map(i32::from),
        selector: LabelSelector {
            match_labels: Some(statefulset_match_labels.into()),
            ..LabelSelector::default()
        },
        service_name: Some(rolegroup_ref.rolegroup_headless_service_name()),
        template: pod_template,
        volume_claim_templates: listener_pvc,
        ..StatefulSetSpec::default()
    };

    Ok(StatefulSet {
        metadata,
        spec: Some(statefulset_spec),
        status: None,
    })
}

/// Returns the container command.
fn command() -> Vec<String> {
    vec![
        "/bin/bash".to_string(),
        "-x".to_string(),
        "-euo".to_string(),
        "pipefail".to_string(),
        "-c".to_string(),
    ]
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::HbaseCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        // root object is invalid, will be requed when modified
        Error::InvalidHBaseCluster { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}

pub fn build_recommended_labels<'a, R>(
    owner: &'a R,
    app_version: &'a str,
    role: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, R> {
    ObjectLabels {
        owner,
        app_name: APP_NAME,
        app_version,
        operator_name: OPERATOR_NAME,
        controller_name: HBASE_CONTROLLER_NAME,
        role,
        role_group,
    }
}

#[cfg(test)]
mod test {
    use rstest::rstest;
    use stackable_operator::kube::runtime::reflector::ObjectRef;

    use super::*;

    #[rstest]
    #[case("2.6.3", HbaseRole::Master, vec!["master", "ui-http"])]
    #[case("2.6.3", HbaseRole::RegionServer, vec!["regionserver", "ui-http"])]
    #[case("2.6.3", HbaseRole::RestServer, vec!["rest-http", "ui-http"])]
    #[case("2.6.4", HbaseRole::Master, vec!["master", "ui-http"])]
    #[case("2.6.4", HbaseRole::RegionServer, vec!["regionserver", "ui-http"])]
    #[case("2.6.4", HbaseRole::RestServer, vec!["rest-http", "ui-http"])]
    fn test_rolegroup_service_ports(
        #[case] hbase_version: &str,
        #[case] role: HbaseRole,
        #[case] expected_ports: Vec<&str>,
    ) {
        let input = format!(
            "
        apiVersion: hbase.stackable.tech/v1alpha1
        kind: HbaseCluster
        metadata:
          name: hbase
          uid: c2e98fc1-6b88-4d11-9381-52530e3f431e
        spec:
          image:
            productVersion: {hbase_version}
          clusterConfig:
            hdfsConfigMapName: simple-hdfs
            zookeeperConfigMapName: simple-znode
          masters:
            roleGroups:
              default:
                replicas: 1
          regionServers:
            roleGroups:
              default:
                replicas: 1
          restServers:
            roleGroups:
              default:
                replicas: 1
        "
        );
        let hbase: v1alpha1::HbaseCluster =
            serde_yaml::from_str(&input).expect("illegal test input");

        let resolved_image = ResolvedProductImage {
            image: format!("oci.stackable.tech/sdp/hbase:{hbase_version}-stackable0.0.0-dev"),
            app_version_label_value: hbase_version
                .parse()
                .expect("test: hbase version is always valid"),
            product_version: hbase_version.to_string(),
            image_pull_policy: "Never".to_string(),
            pull_secrets: None,
        };

        let role_group_ref = RoleGroupRef {
            cluster: ObjectRef::<v1alpha1::HbaseCluster>::from_obj(&hbase),
            role: role.to_string(),
            role_group: "default".to_string(),
        };
        let service = build_rolegroup_service(&hbase, &role, &role_group_ref, &resolved_image)
            .expect("failed to build service");

        assert_eq!(
            expected_ports,
            service
                .spec
                .unwrap()
                .ports
                .unwrap()
                .iter()
                .map(|port| { port.clone().name.unwrap() })
                .collect::<Vec<String>>()
        );
    }
}
