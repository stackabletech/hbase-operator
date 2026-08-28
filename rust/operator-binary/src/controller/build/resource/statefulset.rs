//! Build the per-rolegroup `StatefulSet` for the HbaseCluster.

use std::str::FromStr;

use indoc::formatdoc;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        meta::ObjectMetaBuilder,
        pod::{PodBuilder, security::PodSecurityContextBuilder},
    },
    constant,
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{ConfigMapVolumeSource, ContainerPort, Probe, TCPSocketAction, Volume},
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    product_logging,
    v2::{
        builder::pod::container::{EnvVarName, EnvVarSet, new_container_builder},
        product_logging::framework::{
            STACKABLE_LOG_DIR, ValidatedContainerLogConfigChoice, vector_container,
        },
        types::{
            kubernetes::{ContainerName, VolumeName},
            operator::RoleGroupName,
        },
    },
};

use crate::{
    controller::{
        HbaseRoleGroupConfig, ValidatedCluster,
        build::{
            graceful_shutdown::{self, add_graceful_shutdown_config},
            kerberos::{self, add_kerberos_pod_config},
            object_meta,
            properties::product_logging::MAX_HBASE_LOG_FILES_SIZE,
            recommended_labels_for_role_group_resources,
            recommended_labels_for_unversioned_role_group_resources, role_group_selector,
        },
    },
    crd::{CONFIG_DIR_NAME, HbaseRole, LISTENER_VOLUME_DIR, LISTENER_VOLUME_NAME},
};

constant!(HBASE_CONTAINER_NAME: ContainerName = "hbase");
constant!(VECTOR_CONTAINER_NAME: ContainerName = "vector");

// Pod volume names. The Vector container reuses the `hbase-config` (rolegroup ConfigMap, which
// carries `vector.yaml`) and `log` volumes, so the produced volume mounts match the rest of the
// Pod.
constant!(HBASE_CONFIG_VOLUME_NAME: VolumeName = "hbase-config");
constant!(HDFS_DISCOVERY_VOLUME_NAME: VolumeName = "hdfs-discovery");
constant!(LOG_CONFIG_VOLUME_NAME: VolumeName = "log-config");
constant!(LOG_VOLUME_NAME: VolumeName = "log");

// Environment variable names set on the HBase container. Declared as typed constants (instead of
// `EnvVarName::from_str_unsafe` at the use site) and validated by `test_constants`.
constant!(HBASE_CONF_DIR_ENV: EnvVarName = "HBASE_CONF_DIR");
constant!(HADOOP_CONF_DIR_ENV: EnvVarName = "HADOOP_CONF_DIR");
constant!(REGION_MOVER_OPTS_ENV: EnvVarName = "REGION_MOVER_OPTS");
constant!(RUN_REGION_MOVER_ENV: EnvVarName = "RUN_REGION_MOVER");
constant!(STACKABLE_LOG_DIR_ENV: EnvVarName = "STACKABLE_LOG_DIR");
constant!(CONTAINERDEBUG_LOG_DIRECTORY_ENV: EnvVarName = "CONTAINERDEBUG_LOG_DIRECTORY");

// These constants are hard coded in hbase-entrypoint.sh
// You need to change them there too.
const HDFS_DISCOVERY_TMP_DIR: &str = "/stackable/tmp/hdfs";
const HBASE_CONFIG_TMP_DIR: &str = "/stackable/tmp/hbase";
const HBASE_LOG_CONFIG_TMP_DIR: &str = "/stackable/tmp/log_config";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown { source: graceful_shutdown::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](stackable_operator::k8s_openapi::api::core::v1::Pod)s are accessible through the
/// corresponding headless [`Service`](stackable_operator::k8s_openapi::api::core::v1::Service).
pub fn build_rolegroup_statefulset(
    cluster: &ValidatedCluster,
    hbase_role: &HbaseRole,
    role_group_name: &RoleGroupName,
    validated_rg_config: &HbaseRoleGroupConfig,
) -> Result<StatefulSet> {
    let resolved_product_image = &cluster.image;
    let merged_config = &validated_rg_config.config.config;
    let logging = &validated_rg_config.config.logging;
    let resource_names = cluster.role_group_resource_names(hbase_role, role_group_name);
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
        .with_value(&REGION_MOVER_OPTS_ENV, merged_config.region_mover_args())
        .with_value(
            &RUN_REGION_MOVER_ENV,
            merged_config.run_region_mover().to_string(),
        )
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
        .merge(validated_rg_config.env_overrides.clone());

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
        .resources(merged_config.resources().clone().into())
        .startup_probe(startup_probe)
        .liveness_probe(liveness_probe)
        .readiness_probe(readiness_probe);

    let mut pod_builder = PodBuilder::new();

    let recommended_labels =
        recommended_labels_for_role_group_resources(cluster, hbase_role, role_group_name);

    let pb_metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels.clone())
        .build();

    pod_builder
        .metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .affinity(merged_config.affinity())
        .add_volume(Volume {
            name: HBASE_CONFIG_VOLUME_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: resource_names.role_group_config_map().to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect("The volume names are statically defined and there should be no duplicates.")
        .add_volume(Volume {
            name: HDFS_DISCOVERY_VOLUME_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: cluster.cluster_config.hdfs_config_map_name.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect("The volume names are statically defined and there should be no duplicates.")
        .add_empty_dir_volume(
            &*LOG_VOLUME_NAME,
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_HBASE_LOG_FILES_SIZE],
            )),
        )
        .expect("The volume names are statically defined and there should be no duplicates.")
        .service_account_name(
            cluster
                .cluster_resource_names()
                .service_account_name()
                .to_string(),
        )
        .security_context(
            PodSecurityContextBuilder::with_stackable_defaults()
                .fs_group(1000)
                .build(),
        );

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
            name: LOG_CONFIG_VOLUME_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: log_config_config_map,
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .expect("The volume names are statically defined and there should be no duplicates.");

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;
    if cluster.has_kerberos_enabled() {
        add_kerberos_pod_config(
            cluster,
            resource_names.metrics_service_name().as_ref(),
            &mut hbase_container,
            &mut pod_builder,
            merged_config
                .requested_secret_lifetime()
                .context(MissingSecretLifetimeSnafu)?,
        );
    }
    pod_builder.add_container(hbase_container.build());

    // Vector sidecar shall be the last container in the list.
    if let Some(vector_log_config) = &logging.vector_container {
        pod_builder.add_container(vector_container(
            &VECTOR_CONTAINER_NAME,
            resolved_product_image,
            vector_log_config,
            &resource_names,
            &HBASE_CONFIG_VOLUME_NAME,
            &LOG_VOLUME_NAME,
            EnvVarSet::new(),
        ));
    }

    // Used for PVC templates, which cannot be modified once they are deployed.
    // The version label is omitted so the labels stay stable across version
    // upgrades.
    let unversioned_labels = recommended_labels_for_unversioned_role_group_resources(
        cluster,
        hbase_role,
        role_group_name,
    );

    let listener_pvc =
        super::listener::build_listener_pvc(hbase_role, merged_config, &unversioned_labels);

    if let Some(listener_volume) =
        super::listener::build_listener_volume(hbase_role, merged_config, &recommended_labels)
    {
        pod_builder
            .add_volume(listener_volume)
            .expect("The volume names are statically defined and there should be no duplicates.");
    };

    let mut pod_template = pod_builder.build_template();

    pod_template.merge_from(validated_rg_config.pod_overrides.clone());

    let metadata = object_meta(
        cluster,
        resource_names.stateful_set_name().to_string(),
        recommended_labels_for_role_group_resources(cluster, hbase_role, role_group_name),
    )
    .with_label(RESTART_CONTROLLER_ENABLED_LABEL.to_owned())
    .build();

    let statefulset_match_labels = role_group_selector(cluster, hbase_role, role_group_name);

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some("Parallel".to_string()),
        replicas: validated_rg_config.replicas.map(i32::from),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{controller::build::resource::listener::LISTENER_PVC_NAME, test_utils};

    /// `envOverrides` are applied after every operator-set environment variable, so users can
    /// override any of them (previously the operator's value silently won for the variables set
    /// after the merge, and `CONTAINERDEBUG_LOG_DIRECTORY` was appended separately, producing a
    /// duplicate entry when overridden).
    #[test]
    fn env_overrides_take_precedence_over_operator_set_env_vars() {
        let hbase = test_utils::hbase_from_yaml(&test_utils::MINIMAL_HBASE_YAML.replace(
            "  masters:\n",
            concat!(
                "  masters:\n",
                "    envOverrides:\n",
                "      RUN_REGION_MOVER: \"overridden\"\n",
                "      CONTAINERDEBUG_LOG_DIRECTORY: /debug-override\n",
            ),
        ));
        let cluster = test_utils::validated_cluster_from(&hbase);
        let role_group_name = test_utils::role_group_name("default");
        let rg_config = &cluster.role_group_configs[&HbaseRole::Master][&role_group_name];

        let stateful_set =
            build_rolegroup_statefulset(&cluster, &HbaseRole::Master, &role_group_name, rg_config)
                .expect("the StatefulSet builds");

        let env: Vec<(String, String)> = stateful_set
            .spec
            .expect("the StatefulSet has a spec")
            .template
            .spec
            .expect("the pod template has a spec")
            .containers
            .into_iter()
            .find(|container| container.name == HBASE_CONTAINER_NAME.to_string())
            .expect("the hbase container exists")
            .env
            .expect("the hbase container has env vars")
            .into_iter()
            .filter(|env_var| {
                ["RUN_REGION_MOVER", "CONTAINERDEBUG_LOG_DIRECTORY"].contains(&&*env_var.name)
            })
            .map(|env_var| (env_var.name, env_var.value.unwrap_or_default()))
            .collect();

        // Exact comparison so a duplicate entry (operator value alongside the override) fails too.
        assert_eq!(
            env,
            [
                (
                    "CONTAINERDEBUG_LOG_DIRECTORY".to_string(),
                    "/debug-override".to_string()
                ),
                ("RUN_REGION_MOVER".to_string(), "overridden".to_string()),
            ]
        );
    }

    /// [`test_utils::MINIMAL_HBASE_YAML`] with Kerberos enabled.
    const KERBEROS_HBASE_YAML: &str = r#"
---
apiVersion: hbase.stackable.tech/v1alpha1
kind: HbaseCluster
metadata:
  name: hbase
  namespace: default
  uid: c2c8c5c0-0b5a-4b1e-9f3e-1a2b3c4d5e6f
spec:
  image:
    productVersion: 2.6.3
  clusterConfig:
    hdfsConfigMapName: simple-hdfs
    zookeeperConfigMapName: simple-znode
    authentication:
      kerberos:
        secretClass: kerberos
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
"#;

    /// [`KERBEROS_HBASE_YAML`] with the operator-set `KRB5_CONFIG` overridden on the masters.
    const KERBEROS_OVERRIDE_HBASE_YAML: &str = r#"
---
apiVersion: hbase.stackable.tech/v1alpha1
kind: HbaseCluster
metadata:
  name: hbase
  namespace: default
  uid: c2c8c5c0-0b5a-4b1e-9f3e-1a2b3c4d5e6f
spec:
  image:
    productVersion: 2.6.3
  clusterConfig:
    hdfsConfigMapName: simple-hdfs
    zookeeperConfigMapName: simple-znode
    authentication:
      kerberos:
        secretClass: kerberos
  masters:
    envOverrides:
      KRB5_CONFIG: /custom/krb5.conf
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
"#;

    /// The `KRB5_CONFIG` values of the hbase container of the master `default` role group built
    /// from `yaml`.
    fn krb5_config_values(yaml: &str) -> Vec<(String, String)> {
        let hbase = test_utils::hbase_from_yaml(yaml);
        let cluster = test_utils::validated_cluster_from(&hbase);
        let role_group_name = test_utils::role_group_name("default");
        let rg_config = &cluster.role_group_configs[&HbaseRole::Master][&role_group_name];

        build_rolegroup_statefulset(&cluster, &HbaseRole::Master, &role_group_name, rg_config)
            .expect("the StatefulSet builds")
            .spec
            .expect("the StatefulSet has a spec")
            .template
            .spec
            .expect("the pod template has a spec")
            .containers
            .into_iter()
            .find(|container| container.name == HBASE_CONTAINER_NAME.to_string())
            .expect("the hbase container exists")
            .env
            .expect("the hbase container has env vars")
            .into_iter()
            .filter(|env_var| env_var.name == "KRB5_CONFIG")
            .map(|env_var| (env_var.name, env_var.value.unwrap_or_default()))
            .collect()
    }

    /// With Kerberos enabled, the operator sets `KRB5_CONFIG` — as part of the merged env set, so
    /// an `envOverrides` entry replaces it instead of producing a duplicate whose precedence
    /// depended on Kubernetes' duplicate-name handling (previously it was appended to the
    /// container after the overrides and could not be overridden).
    #[test]
    fn env_overrides_take_precedence_over_kerberos_env_vars() {
        // Without an override, the operator's value is set (exactly once).
        assert_eq!(
            krb5_config_values(KERBEROS_HBASE_YAML),
            [(
                "KRB5_CONFIG".to_string(),
                kerberos::KRB5_CONFIG_PATH.to_string()
            )]
        );

        // An override replaces it; exact comparison so a duplicate entry fails too.
        assert_eq!(
            krb5_config_values(KERBEROS_OVERRIDE_HBASE_YAML),
            [("KRB5_CONFIG".to_string(), "/custom/krb5.conf".to_string())]
        );
    }

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
        let _ = *LISTENER_PVC_NAME;
    }
}
