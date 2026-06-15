use std::{collections::BTreeMap, str::FromStr};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection::{self},
    config::{
        fragment::{self, FromFragment},
        merge::Merge,
    },
    kube::ResourceExt,
    role_utils::{CommonConfiguration, GenericRoleConfig, Role},
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        builder::pod::container::{self, EnvVarName, EnvVarSet},
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        role_utils::{JavaCommonConfig, with_validated_config},
        types::operator::RoleGroupName,
    },
};
use strum::IntoEnumIterator;

use crate::{
    controller::{
        ValidatedCluster, ValidatedClusterConfig, ValidatedRoleConfig, ValidatedRoleGroupConfig,
        build::jvm::construct_role_specific_non_heap_jvm_args, dereference::DereferencedObjects,
    },
    crd::{AnyServiceConfig, HbaseConfigFragment, HbaseRole, RegionServerConfigFragment, v1alpha1},
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

    #[snafu(display("failed to merge and validate the role group config"))]
    ValidateRoleGroupConfig { source: fragment::ValidationError },

    #[snafu(display("invalid environment variable override name"))]
    ParseEnvVarName { source: container::Error },

    #[snafu(display("invalid role group name {role_group}"))]
    ParseRoleGroupName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
        role_group: String,
    },

    #[snafu(display("failed to resolve kerberos config"))]
    AddKerberosConfig { source: kerberos::Error },
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

    let hdfs_discovery_cm_name = &hbase.spec.cluster_config.hdfs_config_map_name;
    let cluster_name = hbase.name_any();

    for hbase_role in HbaseRole::iter() {
        let group_configs = match hbase_role {
            HbaseRole::Master => validate_role_group_configs(
                hbase,
                hbase.spec.masters.as_ref(),
                HbaseConfigFragment::default_config(
                    &hbase_role,
                    &cluster_name,
                    hdfs_discovery_cm_name,
                ),
                AnyServiceConfig::Master,
            )?,
            HbaseRole::RegionServer => validate_role_group_configs(
                hbase,
                hbase.spec.region_servers.as_ref(),
                RegionServerConfigFragment::default_config(
                    &hbase_role,
                    &cluster_name,
                    hdfs_discovery_cm_name,
                ),
                AnyServiceConfig::RegionServer,
            )?,
            HbaseRole::RestServer => validate_role_group_configs(
                hbase,
                hbase.spec.rest_servers.as_ref(),
                HbaseConfigFragment::default_config(
                    &hbase_role,
                    &cluster_name,
                    hdfs_discovery_cm_name,
                ),
                AnyServiceConfig::RestServer,
            )?,
        };

        // masters and region servers are required; rest servers are optional.
        if group_configs.is_empty() {
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

/// Validates every role group of a role into a map keyed by role group name.
///
/// Each role group is merged and validated via
/// [`with_validated_config`](stackable_operator::v2::role_utils::with_validated_config),
/// which folds the CRD config fragment (default <- role <- role group) plus the
/// `configOverrides`, `envOverrides`, `cliOverrides`, `podOverrides` and the
/// `jvmArgumentOverrides` (role group wins) into a single merged
/// [`RoleGroup`](stackable_operator::role_utils::RoleGroup). The per-role validated config
/// is wrapped into [`AnyServiceConfig`] via `wrap`; the merged `envOverrides` are converted
/// into an [`EnvVarSet`] (validating each name eagerly), and the role-specific non-heap JVM
/// args are pre-resolved from the merged `jvmArgumentOverrides` so the build step stays a
/// pure function of [`ValidatedCluster`].
///
/// Returns an empty map if the role is not configured.
fn validate_role_group_configs<Config, ValidatedConfig>(
    hbase: &v1alpha1::HbaseCluster,
    role: Option<
        &Role<Config, v1alpha1::HbaseConfigOverrides, GenericRoleConfig, JavaCommonConfig>,
    >,
    default_config: Config,
    wrap: fn(ValidatedConfig) -> AnyServiceConfig,
) -> Result<BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>, Error>
where
    Config: Clone + Merge,
    ValidatedConfig: FromFragment<Fragment = Config>,
{
    let Some(role) = role else {
        return Ok(BTreeMap::new());
    };

    role.role_groups
        .iter()
        .map(|(role_group_name, role_group)| {
            let role_group_name = RoleGroupName::from_str(role_group_name).with_context(|_| {
                ParseRoleGroupNameSnafu {
                    role_group: role_group_name.clone(),
                }
            })?;
            let validated = with_validated_config::<
                ValidatedConfig,
                JavaCommonConfig,
                Config,
                GenericRoleConfig,
                v1alpha1::HbaseConfigOverrides,
            >(role_group, role, &default_config)
            .context(ValidateRoleGroupConfigSnafu)?;

            let CommonConfiguration {
                config,
                config_overrides,
                env_overrides,
                cli_overrides: _,
                pod_overrides,
                product_specific_common_config,
            } = validated.config;

            let non_heap_jvm_args = construct_role_specific_non_heap_jvm_args(
                hbase,
                &product_specific_common_config.jvm_argument_overrides,
            );

            // Convert the merged env-override HashMap into an EnvVarSet, validating each name
            // eagerly. Keys are unique (HashMap), so insertion order is irrelevant.
            let mut env_overrides_set = EnvVarSet::new();
            for (name, value) in env_overrides {
                env_overrides_set = env_overrides_set.with_value(
                    &EnvVarName::from_str(&name).context(ParseEnvVarNameSnafu)?,
                    value,
                );
            }

            let validated = ValidatedRoleGroupConfig {
                replicas: validated.replicas.unwrap_or(1),
                config: wrap(config),
                config_overrides,
                env_overrides: env_overrides_set,
                pod_overrides,
                non_heap_jvm_args,
            };
            Ok((role_group_name.clone(), validated))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;
    use crate::crd::HbaseConfig;

    /// Role-level `envOverrides` are merged with role-group-level ones, with the role
    /// group winning on key collisions.
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

        let role = hbase.spec.masters.as_ref().unwrap();
        let role_group = role.role_groups.get("default").unwrap();
        let default_config = HbaseConfigFragment::default_config(
            &HbaseRole::Master,
            &hbase.name_any(),
            &hbase.spec.cluster_config.hdfs_config_map_name,
        );

        let validated = with_validated_config::<
            HbaseConfig,
            JavaCommonConfig,
            HbaseConfigFragment,
            GenericRoleConfig,
            v1alpha1::HbaseConfigOverrides,
        >(role_group, role, &default_config)
        .unwrap();

        let env = validated.config.env_overrides;

        assert_eq!(env.get("TEST_VAR"), Some(&"MASTER_RG".to_string()));
        assert_eq!(env.get("TEST_VAR_FROM_MASTER"), Some(&"MASTER".to_string()));
        assert_eq!(env.get("TEST_VAR_FROM_MRG"), Some(&"MASTER".to_string()));
    }
}
