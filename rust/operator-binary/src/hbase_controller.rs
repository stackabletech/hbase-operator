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
    commons::rbac::build_rbac_resources,
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMapVolumeSource, ContainerPort, EnvVar, Probe, Service, ServiceAccount,
                ServicePort, ServiceSpec, TCPSocketAction, Volume,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    kube::{
        Resource, ResourceExt,
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    kvp::{Annotations, Label, LabelError, ObjectLabels},
    logging::controller::ReconcilerError,
    product_logging::{
        self,
        framework::LoggingError,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    shared::time::Duration,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    v2::types::operator::RoleGroupName,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::{
        ValidatedCluster, ValidatedRoleGroupConfig,
        build::{
            discovery::build_discovery_config_map,
            properties::logging::{MAX_HBASE_LOG_FILES_SIZE, STACKABLE_LOG_DIR},
        },
    },
    crd::{
        APP_NAME, CONFIG_DIR_NAME, Container, HbaseClusterStatus, HbaseRole, LISTENER_VOLUME_DIR,
        LISTENER_VOLUME_NAME, OPERATOR_NAME, merged_env, v1alpha1,
    },
    kerberos::{self, add_kerberos_pod_config},
    operations::{graceful_shutdown::add_graceful_shutdown_config, pdb::add_pdbs},
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

    #[snafu(display("failed to apply Service for role group {role_group}"))]
    ApplyRoleGroupService {
        source: stackable_operator::cluster_resources::Error,
        role_group: RoleGroupName,
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

    #[snafu(display("failed to apply ConfigMap for role group {role_group}"))]
    ApplyRoleGroupConfig {
        source: stackable_operator::cluster_resources::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to apply StatefulSet for role group {role_group}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        role_group: RoleGroupName,
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
        for (role_group_name, validated_rg_config) in role_group_configs {
            let rg_service =
                build_rolegroup_service(hbase, &validated_cluster, hbase_role, role_group_name)?;

            let rg_metrics_service = build_rolegroup_metrics_service(
                hbase,
                &validated_cluster,
                hbase_role,
                role_group_name,
            )?;

            let rg_configmap = crate::controller::build::config_map::build_rolegroup_config_map(
                &validated_cluster,
                hbase_role,
                role_group_name,
            )
            .context(BuildRolegroupConfigMapSnafu)?;
            let rg_statefulset = build_rolegroup_statefulset(
                hbase,
                &validated_cluster,
                hbase_role,
                role_group_name,
                validated_rg_config,
                &rbac_sa,
            )?;
            cluster_resources
                .add(client, rg_service)
                .await
                .with_context(|_| ApplyRoleGroupServiceSnafu {
                    role_group: role_group_name.clone(),
                })?;
            cluster_resources
                .add(client, rg_metrics_service)
                .await
                .with_context(|_| ApplyRoleGroupServiceSnafu {
                    role_group: role_group_name.clone(),
                })?;
            cluster_resources
                .add(client, rg_configmap)
                .await
                .with_context(|_| ApplyRoleGroupConfigSnafu {
                    role_group: role_group_name.clone(),
                })?;

            // Note: The StatefulSet needs to be applied after all ConfigMaps and Secrets it mounts
            // to prevent unnecessary Pod restarts.
            // See https://github.com/stackabletech/commons-operator/issues/111 for details.
            ss_cond_builder.add(
                cluster_resources
                    .add(client, rg_statefulset)
                    .await
                    .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                        role_group: role_group_name.clone(),
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
    cluster: &ValidatedCluster,
    hbase_role: &HbaseRole,
    role_group_name: &RoleGroupName,
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

    let metadata = cluster
        .object_meta(
            cluster
                .resource_names(hbase_role, role_group_name)
                .headless_service_name()
                .to_string(),
            hbase_role,
            role_group_name,
        )
        .build();

    let service_selector = cluster.role_group_selector(hbase_role, role_group_name);

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
    cluster: &ValidatedCluster,
    hbase_role: &HbaseRole,
    role_group_name: &RoleGroupName,
) -> Result<Service, Error> {
    let ports = vec![ServicePort {
        name: Some(HbaseRole::metrics_port_name().to_owned()),
        port: i32::from(hbase_role.metrics_port()),
        protocol: Some("TCP".to_owned()),
        ..ServicePort::default()
    }];

    let service_selector = cluster.role_group_selector(hbase_role, role_group_name);

    Ok(Service {
        metadata: cluster
            .object_meta(
                cluster
                    .resource_names(hbase_role, role_group_name)
                    .metrics_service_name()
                    .to_string(),
                hbase_role,
                role_group_name,
            )
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
fn build_rolegroup_statefulset(
    hbase: &v1alpha1::HbaseCluster,
    cluster: &ValidatedCluster,
    hbase_role: &HbaseRole,
    role_group_name: &RoleGroupName,
    validated_rg_config: &ValidatedRoleGroupConfig,
    service_account: &ServiceAccount,
) -> Result<StatefulSet> {
    let resolved_product_image = &cluster.image;
    let merged_config = &validated_rg_config.config;
    let resource_names = cluster.resource_names(hbase_role, role_group_name);

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
        // required by phoenix (for cases where Kerberos is enabled): see https://issues.apache.org/jira/browse/PHOENIX-2369
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

    let recommended_labels = cluster.recommended_labels(hbase_role, role_group_name);

    let pb_metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels.clone())
        .build();

    pod_builder
        .metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .affinity(merged_config.affinity())
        .add_volume(stackable_operator::k8s_openapi::api::core::v1::Volume {
            name: "hbase-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: resource_names.role_group_config_map().to_string(),
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
                    name: resource_names.role_group_config_map().to_string(),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            })
            .context(AddVolumeSnafu)?;
    }

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;
    if cluster.has_kerberos_enabled() {
        add_kerberos_pod_config(
            hbase,
            resource_names.metrics_service_name().as_ref(),
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

    let metadata = cluster
        .object_meta(
            resource_names.stateful_set_name().to_string(),
            hbase_role,
            role_group_name,
        )
        .with_label(RESTART_CONTROLLER_ENABLED_LABEL.to_owned())
        .build();

    let statefulset_match_labels = cluster.role_group_selector(hbase_role, role_group_name);

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some("Parallel".to_string()),
        replicas: Some(i32::from(validated_rg_config.replicas)),
        selector: LabelSelector {
            match_labels: Some(statefulset_match_labels.into()),
            ..LabelSelector::default()
        },
        service_name: Some(resource_names.headless_service_name().to_string()),
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
    use std::str::FromStr;

    use rstest::rstest;
    use stackable_operator::v2::types::operator::RoleGroupName;

    use super::*;
    use crate::controller::build::properties::test_support;

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

        let cluster = test_support::validated_cluster();
        let role_group_name = RoleGroupName::from_str("default").expect("valid role group name");
        let service = build_rolegroup_service(&hbase, &cluster, &role, &role_group_name)
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
