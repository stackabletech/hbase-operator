//! Builds the containers of a role group's pod and collects the volumes they mount.
//!
//! Every HBase role runs the same containers: the `hbase` container and, when the Vector agent is
//! enabled, the Vector sidecar. What differs between roles arrives resolved in the
//! [`RoleGroupInputs`]; the role itself is read only for the values that are functions of it (its
//! CLI name and ports) and for the object names.

use std::str::FromStr;

use indoc::formatdoc;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::pod::volume::VolumeBuilder,
    constant,
    k8s_openapi::{
        api::core::v1::{
            ConfigMapVolumeSource, Container, ContainerPort, PersistentVolumeClaim, Probe,
            TCPSocketAction, Volume,
        },
        apimachinery::pkg::util::intstr::IntOrString,
    },
    product_logging,
    v2::{
        builder::pod::container::{EnvVarName, EnvVarSet, new_container_builder},
        product_logging::framework::{
            STACKABLE_LOG_DIR, ValidatedContainerLogConfigChoice, vector_container,
        },
        types::kubernetes::{ContainerName, VolumeName},
    },
};

use crate::{
    controller::build::{
        kerberos::{self, mount_kerberos_volumes},
        properties::product_logging::MAX_HBASE_LOG_FILES_SIZE,
        role_group::{ListenerPlacement, RoleGroupInputs},
    },
    crd::{CONFIG_DIR_NAME, HbaseRole, LISTENER_VOLUME_DIR, LISTENER_VOLUME_NAME},
};

constant!(pub(crate) HBASE_CONTAINER_NAME: ContainerName = "hbase");
constant!(VECTOR_CONTAINER_NAME: ContainerName = "vector");

// Pod volume names. The Vector container reuses the `hbase-config` (rolegroup ConfigMap, which
// carries `vector.yaml`) and `log` volumes, so the produced volume mounts match the rest of the
// Pod.
constant!(pub(crate) HBASE_CONFIG_VOLUME_NAME: VolumeName = "hbase-config");
constant!(pub(crate) HDFS_DISCOVERY_VOLUME_NAME: VolumeName = "hdfs-discovery");
constant!(pub(crate) LOG_CONFIG_VOLUME_NAME: VolumeName = "log-config");
constant!(pub(crate) LOG_VOLUME_NAME: VolumeName = "log");

// Environment variable names set on the HBase container. Declared as typed constants (instead of
// `EnvVarName::from_str_unsafe` at the use site) and validated by `test_constants`.
constant!(HBASE_CONF_DIR_ENV: EnvVarName = "HBASE_CONF_DIR");
constant!(HADOOP_CONF_DIR_ENV: EnvVarName = "HADOOP_CONF_DIR");
constant!(pub(crate) REGION_MOVER_OPTS_ENV: EnvVarName = "REGION_MOVER_OPTS");
constant!(pub(crate) RUN_REGION_MOVER_ENV: EnvVarName = "RUN_REGION_MOVER");
constant!(STACKABLE_LOG_DIR_ENV: EnvVarName = "STACKABLE_LOG_DIR");
constant!(CONTAINERDEBUG_LOG_DIRECTORY_ENV: EnvVarName = "CONTAINERDEBUG_LOG_DIRECTORY");

// These constants are hard coded in hbase-entrypoint.sh
// You need to change them there too.
const HDFS_DISCOVERY_TMP_DIR: &str = "/stackable/tmp/hdfs";
const HBASE_CONFIG_TMP_DIR: &str = "/stackable/tmp/hbase";
const HBASE_LOG_CONFIG_TMP_DIR: &str = "/stackable/tmp/log_config";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to mount the Kerberos volumes"))]
    MountKerberosVolumes { source: kerberos::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// The containers of a role group's pod and the volumes they mount, in pod order.
pub(crate) struct ContainersAndVolumes {
    /// The `hbase` container, then the Vector sidecar when the Vector agent is enabled.
    pub(crate) containers: Vec<Container>,
    /// The pod volumes in the order the pod lists them; see [`containers_and_volumes`].
    pub(crate) pod_volumes: Vec<Volume>,
    /// The listener claim template of a rest server role group. `None` for the other roles,
    /// which keeps their StatefulSets without a `volumeClaimTemplates` field.
    pub(crate) volume_claim_templates: Option<Vec<PersistentVolumeClaim>>,
}

/// Builds the containers every role runs and collects the volumes they mount.
///
/// The pod volumes are collected in the order the pod lists them: the role group's ConfigMap,
/// the HDFS discovery ConfigMap, the log directory, the log config, then the Kerberos keytab and
/// TLS keystore when configured, then the ephemeral listener volume of masters and region
/// servers. This order is part of the pod template, so changing it restarts every pod; it is
/// pinned by `pod_volumes_keep_their_order` in `build/mod.rs`.
pub(crate) fn containers_and_volumes(inputs: &RoleGroupInputs) -> Result<ContainersAndVolumes> {
    let cluster = inputs.cluster;
    let hbase_role = &inputs.role;
    let resolved_product_image = &cluster.image;
    let logging = &inputs.logging;
    let resource_names = cluster.role_group_resource_names(hbase_role, &inputs.role_group_name);
    let https_enabled = cluster.has_https_enabled();

    let ports = hbase_role
        .ports(https_enabled)
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
            port: IntOrString::String(hbase_role.data_port_name(https_enabled)),
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

    let merged_env = EnvVarSet::new()
        .with_value(&HBASE_CONF_DIR_ENV, CONFIG_DIR_NAME)
        // required by phoenix (for cases where Kerberos is enabled): see https://issues.apache.org/jira/browse/PHOENIX-2369
        .with_value(&HADOOP_CONF_DIR_ENV, CONFIG_DIR_NAME)
        // These env vars are set for all roles to avoid bash's "unbound variable" errors.
        .with_value(&REGION_MOVER_OPTS_ENV, inputs.region_mover_opts.clone())
        .with_value(&RUN_REGION_MOVER_ENV, inputs.run_region_mover.to_string())
        .with_value(&STACKABLE_LOG_DIR_ENV, STACKABLE_LOG_DIR)
        // Needed for the `containerdebug` process to log its tracing information to.
        .with_value(
            &CONTAINERDEBUG_LOG_DIRECTORY_ENV,
            format!("{STACKABLE_LOG_DIR}/containerdebug"),
        )
        // Empty when Kerberos is disabled.
        .merge(kerberos::kerberos_env_vars(cluster))
        // apply overrides last of all; `EnvVarSet` is keyed by name, so iteration is already
        // in a fixed (sorted-by-name) order
        .merge(inputs.env_overrides.clone());

    let role_name = hbase_role.cli_role_name();
    let mut hbase_container = new_container_builder(&HBASE_CONTAINER_NAME);

    hbase_container
        .image_from_product_image(resolved_product_image)
        .command(command())
        .args(vec![formatdoc! {"
            {entrypoint} {role} {port} {port_name} {ui_port_name}",
            entrypoint = "/stackable/hbase/bin/hbase-entrypoint.sh".to_string(),
            role = role_name,
            port = hbase_role.data_port(),
            port_name = hbase_role.data_port_name(https_enabled),
            ui_port_name = HbaseRole::ui_port_name(https_enabled),
        }])
        .add_env_vars(merged_env)
        .add_volume_mount(&*HBASE_CONFIG_VOLUME_NAME, HBASE_CONFIG_TMP_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*HDFS_DISCOVERY_VOLUME_NAME, HDFS_DISCOVERY_TMP_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*LOG_CONFIG_VOLUME_NAME, HBASE_LOG_CONFIG_TMP_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*LOG_VOLUME_NAME, STACKABLE_LOG_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(LISTENER_VOLUME_NAME, LISTENER_VOLUME_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_container_ports(ports)
        .resources(inputs.resources.clone().into())
        .startup_probe(startup_probe)
        .liveness_probe(liveness_probe)
        .readiness_probe(readiness_probe);

    // The HBase container's log config ConfigMap: either the operator-generated one (the
    // rolegroup ConfigMap, which carries the automatic `log4j2.properties`) or a user-provided
    // custom ConfigMap. This branches on the *validated* logging choice (see `ValidatedLogging`).
    let log_config_config_map = match &logging.hbase_container {
        ValidatedContainerLogConfigChoice::Custom(config_map_name) => config_map_name.to_string(),
        ValidatedContainerLogConfigChoice::Automatic(_) => {
            resource_names.role_group_config_map().to_string()
        }
    };

    let mut pod_volumes = vec![
        Volume {
            name: HBASE_CONFIG_VOLUME_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: resource_names.role_group_config_map().to_string(),
                ..Default::default()
            }),
            ..Default::default()
        },
        Volume {
            name: HDFS_DISCOVERY_VOLUME_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: cluster.cluster_config.hdfs_config_map_name.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        },
        VolumeBuilder::new(&*LOG_VOLUME_NAME)
            .with_empty_dir(
                None::<String>,
                Some(product_logging::framework::calculate_log_volume_size_limit(
                    &[MAX_HBASE_LOG_FILES_SIZE],
                )),
            )
            .build(),
        Volume {
            name: LOG_CONFIG_VOLUME_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: log_config_config_map,
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        },
    ];

    if cluster.has_kerberos_enabled() {
        pod_volumes.extend(
            mount_kerberos_volumes(
                cluster,
                resource_names.metrics_service_name().as_ref(),
                &mut hbase_container,
                inputs.requested_secret_lifetime,
            )
            .context(MountKerberosVolumesSnafu)?,
        );
    }

    let volume_claim_templates = match &inputs.listener {
        ListenerPlacement::PodVolume(volume) => {
            pod_volumes.push(volume.clone());
            None
        }
        ListenerPlacement::ClaimTemplate(claim) => Some(vec![claim.clone()]),
    };

    let mut containers = vec![hbase_container.build()];

    // Vector sidecar shall be the last container in the list.
    if let Some(vector_log_config) = &logging.vector_container {
        containers.push(vector_container(
            &VECTOR_CONTAINER_NAME,
            resolved_product_image,
            vector_log_config,
            &resource_names,
            &HBASE_CONFIG_VOLUME_NAME,
            &LOG_VOLUME_NAME,
            EnvVarSet::new(),
        ));
    }

    Ok(ContainersAndVolumes {
        containers,
        pod_volumes,
        volume_claim_templates,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *HBASE_CONTAINER_NAME;
        let _ = *VECTOR_CONTAINER_NAME;
        let _ = *HBASE_CONFIG_VOLUME_NAME;
        let _ = *HDFS_DISCOVERY_VOLUME_NAME;
        let _ = *LOG_CONFIG_VOLUME_NAME;
        let _ = *LOG_VOLUME_NAME;
        let _ = *HBASE_CONF_DIR_ENV;
        let _ = *HADOOP_CONF_DIR_ENV;
        let _ = *REGION_MOVER_OPTS_ENV;
        let _ = *RUN_REGION_MOVER_ENV;
        let _ = *STACKABLE_LOG_DIR_ENV;
        let _ = *CONTAINERDEBUG_LOG_DIRECTORY_ENV;
    }
}
