//! Build the per-rolegroup `StatefulSet` for the HbaseCluster.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        meta::ObjectMetaBuilder,
        pod::{PodBuilder, security::PodSecurityContextBuilder},
    },
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        DeepMerge,
        api::apps::v1::{StatefulSet, StatefulSetSpec},
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
};

use crate::controller::build::{
    container::{self, ContainersAndVolumes, containers_and_volumes},
    graceful_shutdown::{self, add_graceful_shutdown_config},
    object_meta, recommended_labels_for_role_group_resources,
    role_group::RoleGroupInputs,
    role_group_selector,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build the containers and volumes"))]
    ContainersAndVolumes { source: container::Error },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown { source: graceful_shutdown::Error },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](stackable_operator::k8s_openapi::api::core::v1::Pod)s are accessible through the
/// corresponding headless [`Service`](stackable_operator::k8s_openapi::api::core::v1::Service).
pub fn build_rolegroup_statefulset(inputs: &RoleGroupInputs) -> Result<StatefulSet> {
    let cluster = inputs.cluster;
    let hbase_role = &inputs.role;
    let role_group_name = &inputs.role_group_name;
    let resource_names = cluster.role_group_resource_names(hbase_role, role_group_name);

    let ContainersAndVolumes {
        containers,
        pod_volumes,
        volume_claim_templates,
    } = containers_and_volumes(inputs).context(ContainersAndVolumesSnafu)?;

    let mut pod_builder = PodBuilder::new();

    let recommended_labels =
        recommended_labels_for_role_group_resources(cluster, hbase_role, role_group_name);

    let pb_metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels.clone())
        .build();

    pod_builder
        .metadata(pb_metadata)
        .image_pull_secrets_from_product_image(&cluster.image)
        .affinity(&inputs.affinity)
        // The volumes in the order the containers mount them, which is part of the pod template
        // (pinned by `pod_volumes_keep_their_order` in `build/mod.rs`). A volume whose name
        // collides with another's is rejected here.
        .add_volumes(pod_volumes)
        .context(AddVolumeSnafu)?
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

    add_graceful_shutdown_config(inputs.graceful_shutdown_timeout, &mut pod_builder)
        .context(GracefulShutdownSnafu)?;

    for container in containers {
        pod_builder.add_container(container);
    }

    let mut pod_template = pod_builder.build_template();

    pod_template.merge_from(inputs.pod_overrides.clone());

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
        replicas: inputs.replicas.map(i32::from),
        selector: LabelSelector {
            match_labels: Some(statefulset_match_labels.into()),
            ..LabelSelector::default()
        },
        service_name: Some(resource_names.headless_service_name().to_string()),
        template: pod_template,
        volume_claim_templates,
        ..StatefulSetSpec::default()
    };

    Ok(StatefulSet {
        metadata,
        spec: Some(statefulset_spec),
        status: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        controller::build::{container::HBASE_CONTAINER_NAME, kerberos, role_group::master},
        test_utils,
    };

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
        let cluster_info = &test_utils::cluster_info();
        let inputs = master::inputs(
            &cluster,
            cluster_info,
            &role_group_name,
            test_utils::master_role_group_config(&cluster, "default"),
        )
        .expect("inputs are valid");

        let stateful_set = build_rolegroup_statefulset(&inputs).expect("the StatefulSet builds");

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
        let cluster_info = &test_utils::cluster_info();
        let inputs = master::inputs(
            &cluster,
            cluster_info,
            &role_group_name,
            test_utils::master_role_group_config(&cluster, "default"),
        )
        .expect("inputs are valid");

        build_rolegroup_statefulset(&inputs)
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
}
