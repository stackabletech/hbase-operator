use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection::{self},
    config::merge::Merge,
    role_utils::GenericRoleConfig,
    utils::cluster_info::KubernetesClusterInfo,
    v2::controller_utils::{get_cluster_name, get_namespace, get_uid},
};
use strum::IntoEnumIterator;

use crate::{
    config::jvm::construct_role_specific_non_heap_jvm_args,
    controller::dereference::DereferencedObjects,
    crd::{HbaseRole, v1alpha1},
    hbase_controller::{
        ValidatedCluster, ValidatedClusterConfig, ValidatedRoleConfig, ValidatedRoleGroupConfig,
    },
    kerberos::{
        self, kerberos_config_properties, kerberos_discovery_config_properties,
        kerberos_ssl_client_settings, kerberos_ssl_server_settings,
    },
};

const CONTAINER_IMAGE_BASE_NAME: &str = "hbase";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to determine the cluster identity (name, namespace and uid)"))]
    GetClusterIdentity {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("the HbaseCluster has no {role} role defined"))]
    MissingRequiredRole { role: String },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crate::crd::Error },

    #[snafu(display("failed to resolve kerberos config"))]
    AddKerberosConfig { source: kerberos::Error },

    #[snafu(display("failed to construct role-specific JVM arguments"))]
    ConstructJvmArgument { source: crate::config::jvm::Error },
}

pub fn validate_cluster(
    hbase: &v1alpha1::HbaseCluster,
    image_repository: &str,
    cluster_info: &KubernetesClusterInfo,
    dereferenced_objects: DereferencedObjects,
) -> Result<ValidatedCluster, Error> {
    let resolved_product_image = hbase
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let mut role_groups = BTreeMap::new();
    let mut role_configs = BTreeMap::new();

    for hbase_role in HbaseRole::iter() {
        let role_group_names = role_group_names(hbase, &hbase_role);

        // masters and region servers are required (preserves the old build_role_properties check);
        // rest servers are optional.
        if role_group_names.is_empty() {
            match hbase_role {
                HbaseRole::Master | HbaseRole::RegionServer => {
                    return MissingRequiredRoleSnafu {
                        role: hbase_role.to_string(),
                    }
                    .fail();
                }
                HbaseRole::RestServer => continue,
            }
        }

        if let Some(GenericRoleConfig {
            pod_disruption_budget: pdb,
        }) = hbase.role_config(&hbase_role)
        {
            role_configs.insert(hbase_role.clone(), ValidatedRoleConfig { pdb: pdb.clone() });
        }

        let mut group_configs = BTreeMap::new();
        for rolegroup_name in role_group_names {
            let merged_config = hbase
                .merged_config(
                    &hbase_role,
                    &rolegroup_name,
                    &hbase.spec.cluster_config.hdfs_config_map_name,
                )
                .context(FailedToResolveConfigSnafu)?;

            group_configs.insert(
                rolegroup_name.clone(),
                ValidatedRoleGroupConfig {
                    merged_config,
                    config_overrides: merged_config_overrides(hbase, &hbase_role, &rolegroup_name),
                    env_overrides: merged_env_overrides(hbase, &hbase_role, &rolegroup_name),
                    non_heap_jvm_args: construct_role_specific_non_heap_jvm_args(
                        hbase,
                        &hbase_role,
                        &rolegroup_name,
                    )
                    .context(ConstructJvmArgumentSnafu)?,
                },
            );
        }

        role_groups.insert(hbase_role, group_configs);
    }

    let hbase_site_kerberos_config =
        kerberos_config_properties(hbase, cluster_info).context(AddKerberosConfigSnafu)?;
    let discovery_kerberos_config = kerberos_discovery_config_properties(hbase, cluster_info)
        .context(AddKerberosConfigSnafu)?;
    let ssl_server_settings = kerberos_ssl_server_settings(hbase);
    let ssl_client_settings = kerberos_ssl_client_settings(hbase);

    let name = get_cluster_name(hbase).context(GetClusterIdentitySnafu)?;
    let namespace = get_namespace(hbase).context(GetClusterIdentitySnafu)?;
    let uid = get_uid(hbase).context(GetClusterIdentitySnafu)?;

    Ok(ValidatedCluster::new(
        name,
        namespace,
        uid,
        resolved_product_image,
        ValidatedClusterConfig {
            zookeeper_connection_information: dereferenced_objects.zookeeper_connection_information,
            hbase_opa_config: dereferenced_objects.hbase_opa_config,
            kerberos_enabled: hbase.has_kerberos_enabled(),
            hbase_site_kerberos_config,
            discovery_kerberos_config,
            ssl_server_settings,
            ssl_client_settings,
        },
        role_groups,
        role_configs,
    ))
}

/// The names of the role groups defined for `role` in the spec.
fn role_group_names(hbase: &v1alpha1::HbaseCluster, role: &HbaseRole) -> Vec<String> {
    match role {
        HbaseRole::Master => hbase
            .spec
            .masters
            .as_ref()
            .map(|r| r.role_groups.keys().cloned().collect()),
        HbaseRole::RegionServer => hbase
            .spec
            .region_servers
            .as_ref()
            .map(|r| r.role_groups.keys().cloned().collect()),
        HbaseRole::RestServer => hbase
            .spec
            .rest_servers
            .as_ref()
            .map(|r| r.role_groups.keys().cloned().collect()),
    }
    .unwrap_or_default()
}

/// Merge role-level then role-group-level `configOverrides` (role group wins).
fn merged_config_overrides(
    hbase: &v1alpha1::HbaseCluster,
    role: &HbaseRole,
    role_group: &str,
) -> v1alpha1::HbaseConfigOverrides {
    let (role_overrides, role_group_overrides) = match role {
        HbaseRole::Master => (
            hbase
                .spec
                .masters
                .as_ref()
                .map(|r| r.config.config_overrides.clone()),
            hbase
                .spec
                .masters
                .as_ref()
                .and_then(|r| r.role_groups.get(role_group))
                .map(|rg| rg.config.config_overrides.clone()),
        ),
        HbaseRole::RegionServer => (
            hbase
                .spec
                .region_servers
                .as_ref()
                .map(|r| r.config.config_overrides.clone()),
            hbase
                .spec
                .region_servers
                .as_ref()
                .and_then(|r| r.role_groups.get(role_group))
                .map(|rg| rg.config.config_overrides.clone()),
        ),
        HbaseRole::RestServer => (
            hbase
                .spec
                .rest_servers
                .as_ref()
                .map(|r| r.config.config_overrides.clone()),
            hbase
                .spec
                .rest_servers
                .as_ref()
                .and_then(|r| r.role_groups.get(role_group))
                .map(|rg| rg.config.config_overrides.clone()),
        ),
    };

    let role_overrides = role_overrides.unwrap_or_default();
    let mut merged = role_group_overrides.unwrap_or_default();
    merged.merge(&role_overrides);
    merged
}

/// Merge role-level then role-group-level `envOverrides` (role group wins).
fn merged_env_overrides(
    hbase: &v1alpha1::HbaseCluster,
    role: &HbaseRole,
    role_group: &str,
) -> BTreeMap<String, String> {
    let (role_overrides, role_group_overrides) = match role {
        HbaseRole::Master => (
            hbase
                .spec
                .masters
                .as_ref()
                .map(|r| r.config.env_overrides.clone()),
            hbase
                .spec
                .masters
                .as_ref()
                .and_then(|r| r.role_groups.get(role_group))
                .map(|rg| rg.config.env_overrides.clone()),
        ),
        HbaseRole::RegionServer => (
            hbase
                .spec
                .region_servers
                .as_ref()
                .map(|r| r.config.env_overrides.clone()),
            hbase
                .spec
                .region_servers
                .as_ref()
                .and_then(|r| r.role_groups.get(role_group))
                .map(|rg| rg.config.env_overrides.clone()),
        ),
        HbaseRole::RestServer => (
            hbase
                .spec
                .rest_servers
                .as_ref()
                .map(|r| r.config.env_overrides.clone()),
            hbase
                .spec
                .rest_servers
                .as_ref()
                .and_then(|r| r.role_groups.get(role_group))
                .map(|rg| rg.config.env_overrides.clone()),
        ),
    };

    let mut env_overrides = BTreeMap::new();
    if let Some(role_overrides) = role_overrides {
        env_overrides.extend(role_overrides);
    }
    if let Some(role_group_overrides) = role_group_overrides {
        env_overrides.extend(role_group_overrides);
    }
    env_overrides
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;

    #[test]
    fn test_env_overrides() {
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

        let env_overrides = merged_env_overrides(&hbase, &HbaseRole::Master, "default");

        assert_eq!(
            env_overrides.get("TEST_VAR"),
            Some(&"MASTER_RG".to_string())
        );
        assert_eq!(
            env_overrides.get("TEST_VAR_FROM_MASTER"),
            Some(&"MASTER".to_string())
        );
        assert_eq!(
            env_overrides.get("TEST_VAR_FROM_MRG"),
            Some(&"MASTER".to_string())
        );
    }
}
