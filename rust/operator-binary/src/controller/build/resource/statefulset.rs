//! Build the per-rolegroup `StatefulSet` for the HbaseCluster.

use std::{collections::BTreeMap, str::FromStr};

use indoc::formatdoc;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        meta::ObjectMetaBuilder,
        pod::{PodBuilder, container::ContainerBuilder, security::PodSecurityContextBuilder},
    },
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMapVolumeSource, ContainerPort, EnvVar, Probe, ServiceAccount,
                TCPSocketAction, Volume,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    kube::ResourceExt,
    product_logging,
    v2::{
        builder::pod::container::EnvVarSet,
        product_logging::framework::{ValidatedContainerLogConfigChoice, vector_container},
        types::{
            kubernetes::{ContainerName, VolumeName},
            operator::RoleGroupName,
        },
    },
};

use crate::{
    controller::{
        ValidatedCluster, ValidatedRoleGroupConfig,
        build::{
            graceful_shutdown::{self, add_graceful_shutdown_config},
            kerberos::{self, add_kerberos_pod_config},
            properties::logging::{MAX_HBASE_LOG_FILES_SIZE, STACKABLE_LOG_DIR},
        },
    },
    crd::{
        CONFIG_DIR_NAME, HbaseRole, LISTENER_VOLUME_DIR, LISTENER_VOLUME_NAME, merged_env, v1alpha1,
    },
};

stackable_operator::constant!(VECTOR_CONTAINER_NAME: ContainerName = "vector");

// The Vector container reads `vector.yaml` from the rolegroup ConfigMap (mounted as the
// `hbase-config` volume) and writes to the shared `log` volume. These reuse the existing
// volume-name string values so the produced volume mounts match the rest of the Pod.
stackable_operator::constant!(VECTOR_LOG_CONFIG_VOLUME_NAME: VolumeName = "hbase-config");
stackable_operator::constant!(VECTOR_LOG_VOLUME_NAME: VolumeName = "log");

pub static CONTAINERDEBUG_LOG_DIRECTORY: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| format!("{STACKABLE_LOG_DIR}/containerdebug"));

// These constants are hard coded in hbase-entrypoint.sh
// You need to change them there too.
const HDFS_DISCOVERY_TMP_DIR: &str = "/stackable/tmp/hdfs";
const HBASE_CONFIG_TMP_DIR: &str = "/stackable/tmp/hbase";
const HBASE_LOG_CONFIG_TMP_DIR: &str = "/stackable/tmp/log_config";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("failed to add kerberos config"))]
    AddKerberosConfig { source: kerberos::Error },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown { source: graceful_shutdown::Error },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("failed to build listener volume"))]
    ListenerVolume { source: crate::crd::Error },

    #[snafu(display("failed to build listener persistent volume claim"))]
    ListenerPersistentVolumeClaim { source: crate::crd::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](stackable_operator::k8s_openapi::api::core::v1::Pod)s are accessible through the
/// corresponding headless [`Service`](stackable_operator::k8s_openapi::api::core::v1::Service).
pub fn build_rolegroup_statefulset(
    hbase: &v1alpha1::HbaseCluster,
    cluster: &ValidatedCluster,
    hbase_role: &HbaseRole,
    role_group_name: &RoleGroupName,
    validated_rg_config: &ValidatedRoleGroupConfig,
    service_account: &ServiceAccount,
) -> Result<StatefulSet> {
    let resolved_product_image = &cluster.image;
    let merged_config = &validated_rg_config.config.config;
    let logging = &validated_rg_config.config.logging;
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
        .add_volume(Volume {
            name: "hbase-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: resource_names.role_group_config_map().to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .context(AddVolumeSnafu)?
        .add_volume(Volume {
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

    // The HBase container's log config ConfigMap: either the operator-generated one (the
    // rolegroup ConfigMap, which carries the automatic `log4j2.properties`) or a user-provided
    // custom ConfigMap. This branches on the *validated* logging choice (see `ValidatedLogging`).
    let log_config_config_map = match &logging.hbase_container {
        ValidatedContainerLogConfigChoice::Custom(config_map_name) => config_map_name.to_string(),
        ValidatedContainerLogConfigChoice::Automatic(_) => {
            resource_names.role_group_config_map().to_string()
        }
    };
    pod_builder
        .add_volume(Volume {
            name: "log-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: log_config_config_map,
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?;

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

    // Vector sidecar shall be the last container in the list.
    if let Some(vector_log_config) = &logging.vector_container {
        pod_builder.add_container(vector_container(
            &VECTOR_CONTAINER_NAME,
            resolved_product_image,
            vector_log_config,
            &resource_names,
            &VECTOR_LOG_CONFIG_VOLUME_NAME,
            &VECTOR_LOG_VOLUME_NAME,
            EnvVarSet::new(),
        ));
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
