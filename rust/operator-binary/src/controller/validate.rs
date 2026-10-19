use std::{collections::BTreeMap, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu, ensure};
use stackable_operator::{
    commons::product_image_selection::{self},
    config::{
        fragment::{self, FromFragment},
        merge::Merge,
    },
    kube::ResourceExt,
    product_logging::spec::Logging,
    role_utils::GenericRoleConfig,
    v2::{
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        product_logging::framework::{
            ValidatedContainerLogConfigChoice, VectorContainerLogConfig,
            validate_logging_configuration_for_container,
        },
        role_utils::{
            CommonConfiguration, JavaCommonConfig, Role, RoleGroup, RoleGroupConfig,
            with_validated_config,
        },
        types::{kubernetes::ConfigMapName, operator::RoleGroupName},
    },
};

use crate::{
    controller::{
        MasterRoleGroupConfig, RegionServerRoleGroupConfig, RestServerRoleGroupConfig,
        ValidatedCluster, ValidatedClusterConfig, ValidatedMasterConfig,
        ValidatedRegionServerConfig, ValidatedRestServerConfig, ValidatedRoleConfig,
        dereference::DereferencedObjects,
    },
    crd::{
        Container, HbaseConfig, HbaseConfigFragment, HbaseRole, RegionServerConfig,
        RegionServerConfigFragment, v1alpha1,
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
    MissingRequiredRole { role: HbaseRole },

    #[snafu(display("failed to merge and validate the {role} role group config"))]
    ValidateRoleGroupConfig {
        source: fragment::ValidationError,
        role: HbaseRole,
    },

    #[snafu(display("invalid role group name {role_group} for role {role}"))]
    ParseRoleGroupName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
        role: HbaseRole,
        role_group: String,
    },

    #[snafu(display("failed to validate logging configuration"))]
    ValidateLoggingConfig {
        source: stackable_operator::v2::product_logging::framework::Error,
    },

    #[snafu(display(
        "the Vector aggregator discovery ConfigMap name is required when the Vector agent is enabled"
    ))]
    MissingVectorAggregatorConfigMapName,
}

/// Validated logging configuration for the HBase and (optional) Vector container.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedLogging {
    pub hbase_container: ValidatedContainerLogConfigChoice,
    pub vector_container: Option<VectorContainerLogConfig>,
    pub enable_vector_agent: bool,
}

/// Validates the logging configuration for the HBase (and optional Vector) container.
///
/// `vector_aggregator_config_map_name` is the discovery ConfigMap name of the Vector aggregator;
/// it is required (and validated) only when the Vector agent is enabled.
fn validate_logging(
    logging: &Logging<Container>,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<ValidatedLogging, Error> {
    let hbase_container = validate_logging_configuration_for_container(logging, &Container::Hbase)
        .context(ValidateLoggingConfigSnafu)?;

    let vector_container = if logging.enable_vector_agent {
        let vector_aggregator_config_map_name = vector_aggregator_config_map_name
            .clone()
            .context(MissingVectorAggregatorConfigMapNameSnafu)?;
        Some(VectorContainerLogConfig {
            log_config: validate_logging_configuration_for_container(logging, &Container::Vector)
                .context(ValidateLoggingConfigSnafu)?,
            vector_aggregator_config_map_name,
        })
    } else {
        None
    };

    Ok(ValidatedLogging {
        hbase_container,
        vector_container,
        enable_vector_agent: logging.enable_vector_agent,
    })
}

pub fn validate_cluster(
    hbase: &v1alpha1::HbaseCluster,
    image_repository: &str,
    dereferenced_objects: DereferencedObjects,
) -> Result<ValidatedCluster, Error> {
    let resolved_product_image = hbase
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            image_repository,
            &crate::built_info::PKG_VERSION_SEMVER,
        )
        .context(ResolveProductImageSnafu)?;

    let hdfs_discovery_cm_name = hbase.spec.cluster_config.hdfs_config_map_name.as_ref();
    let cluster_name = hbase.name_any();

    // The Vector aggregator discovery ConfigMap name. It is validated already at deserialize time
    // (it is a `ConfigMapName`), and only required when the Vector agent is enabled for a role
    // group.
    let vector_aggregator_config_map_name = hbase
        .spec
        .cluster_config
        .vector_aggregator_config_map_name
        .clone();

    let opa_config = hbase
        .spec
        .cluster_config
        .authorization
        .as_ref()
        .and_then(|authorization| authorization.opa.as_ref());

    // The first failure propagates, so this order decides which misconfiguration the user is told
    // about when more than one role is wrong. It is `HbaseRole`'s variant order.
    let masters = hbase
        .spec
        .masters
        .as_ref()
        .context(MissingRequiredRoleSnafu {
            role: HbaseRole::Master,
        })?;
    let master_role_group_configs = validate_master_role_group_configs(
        masters,
        &HbaseConfigFragment::default_config(
            &HbaseRole::Master,
            &cluster_name,
            hdfs_discovery_cm_name,
            opa_config,
        ),
        &vector_aggregator_config_map_name,
    )?;
    ensure!(
        !master_role_group_configs.is_empty(),
        MissingRequiredRoleSnafu {
            role: HbaseRole::Master
        }
    );

    let GenericRoleConfig {
        pod_disruption_budget: master_pdb,
    } = &masters.role_config;

    let region_servers = hbase
        .spec
        .region_servers
        .as_ref()
        .context(MissingRequiredRoleSnafu {
            role: HbaseRole::RegionServer,
        })?;
    let region_server_role_group_configs = validate_region_server_role_group_configs(
        region_servers,
        &RegionServerConfigFragment::default_config(
            &HbaseRole::RegionServer,
            &cluster_name,
            hdfs_discovery_cm_name,
            opa_config,
        ),
        &vector_aggregator_config_map_name,
    )?;
    ensure!(
        !region_server_role_group_configs.is_empty(),
        MissingRequiredRoleSnafu {
            role: HbaseRole::RegionServer
        }
    );

    let GenericRoleConfig {
        pod_disruption_budget: region_server_pdb,
    } = &region_servers.role_config;

    let rest_server_role_group_configs = match &hbase.spec.rest_servers {
        Some(rest_servers) => validate_rest_server_role_group_configs(
            rest_servers,
            &HbaseConfigFragment::default_config(
                &HbaseRole::RestServer,
                &cluster_name,
                hdfs_discovery_cm_name,
                opa_config,
            ),
            &vector_aggregator_config_map_name,
        )?,
        None => BTreeMap::new(),
    };
    let rest_server_config = match &hbase.spec.rest_servers {
        Some(rest_servers) if !rest_server_role_group_configs.is_empty() => {
            let GenericRoleConfig {
                pod_disruption_budget,
            } = &rest_servers.role_config;
            Some(ValidatedRoleConfig {
                pdb: pod_disruption_budget.clone(),
            })
        }
        _ => None,
    };

    let name = get_cluster_name(hbase).context(GetClusterIdentitySnafu)?;
    let namespace = get_namespace(hbase).context(GetClusterIdentitySnafu)?;
    let uid = get_uid(hbase).context(GetClusterIdentitySnafu)?;

    Ok(ValidatedCluster {
        metadata: ValidatedCluster::object_meta(&name, &namespace, &uid),
        name,
        namespace,
        uid,
        product_version: ValidatedCluster::product_version(&resolved_product_image),
        image: resolved_product_image,
        cluster_config: ValidatedClusterConfig {
            zookeeper_connection_information: dereferenced_objects.zookeeper_connection_information,
            hbase_opa_config: dereferenced_objects.hbase_opa_config,
            kerberos_secret_class: hbase.kerberos_secret_class(),
            https_secret_class: hbase.https_secret_class(),
            hdfs_config_map_name: hbase.spec.cluster_config.hdfs_config_map_name.clone(),
        },
        master_config: ValidatedRoleConfig {
            pdb: master_pdb.clone(),
        },
        master_role_group_configs,
        region_server_config: ValidatedRoleConfig {
            pdb: region_server_pdb.clone(),
        },
        region_server_role_group_configs,
        rest_server_config,
        rest_server_role_group_configs,
    })
}

/// Validates every role group of a role into a map keyed by role group name.
///
/// Each role group is merged and validated via
/// [`with_validated_config`],
/// which folds the CRD config fragment (default <- role <- role group) plus the
/// `configOverrides`, `envOverrides`, `cliOverrides`, `podOverrides` and the
/// `jvmArgumentOverrides` (role group wins) into a single merged
/// `RoleGroup`.
///
/// Returns an empty map if the role is not configured.
fn validate_role_group_config<Config, ValidatedConfig>(
    hbase_role: &HbaseRole,
    role: &Role<Config, v1alpha1::HbaseConfigOverrides, GenericRoleConfig, JavaCommonConfig>,
    role_group: &RoleGroup<Config, JavaCommonConfig, v1alpha1::HbaseConfigOverrides>,
    default_config: &Config,
) -> Result<RoleGroupConfig<ValidatedConfig, JavaCommonConfig, v1alpha1::HbaseConfigOverrides>, Error>
where
    Config: Clone + Merge,
    ValidatedConfig: FromFragment<Fragment = Config>,
{
    let validated = with_validated_config::<
        ValidatedConfig,
        JavaCommonConfig,
        Config,
        GenericRoleConfig,
        v1alpha1::HbaseConfigOverrides,
    >(role_group, role, default_config)
    .context(ValidateRoleGroupConfigSnafu {
        role: hbase_role.clone(),
    })?;

    let CommonConfiguration {
        config,
        config_overrides,
        env_overrides,
        cli_overrides,
        pod_overrides,
        product_specific_common_config,
    } = validated.config;

    let validated = RoleGroupConfig {
        replicas: validated.replicas,
        config,
        config_overrides,
        env_overrides: env_overrides.into(),
        cli_overrides,
        pod_overrides,
        product_specific_common_config,
    };
    Ok(validated)
}

fn validate_master_role_group_configs(
    role: &Role<
        HbaseConfigFragment,
        v1alpha1::HbaseConfigOverrides,
        GenericRoleConfig,
        JavaCommonConfig,
    >,
    default_config: &HbaseConfigFragment,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<BTreeMap<RoleGroupName, MasterRoleGroupConfig>, Error> {
    role.role_groups
        .iter()
        .map(|(role_group_name, role_group)| {
            let merged = validate_role_group_config::<_, HbaseConfig>(
                &HbaseRole::Master,
                role,
                role_group,
                default_config,
            )?;
            let logging =
                validate_logging(&merged.config.logging, vector_aggregator_config_map_name)?;
            let validated = MasterRoleGroupConfig {
                replicas: merged.replicas,
                config: ValidatedMasterConfig {
                    config: merged.config,
                    logging,
                },
                config_overrides: merged.config_overrides,
                env_overrides: merged.env_overrides,
                cli_overrides: merged.cli_overrides,
                pod_overrides: merged.pod_overrides,
                product_specific_common_config: merged.product_specific_common_config,
            };
            let role_group_name = RoleGroupName::from_str(role_group_name).with_context(|_| {
                ParseRoleGroupNameSnafu {
                    role: HbaseRole::Master,
                    role_group: role_group_name.clone(),
                }
            })?;
            Ok((role_group_name, validated))
        })
        .collect()
}

fn validate_region_server_role_group_configs(
    role: &Role<
        RegionServerConfigFragment,
        v1alpha1::HbaseConfigOverrides,
        GenericRoleConfig,
        JavaCommonConfig,
    >,
    default_config: &RegionServerConfigFragment,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<BTreeMap<RoleGroupName, RegionServerRoleGroupConfig>, Error> {
    role.role_groups
        .iter()
        .map(|(role_group_name, role_group)| {
            let merged = validate_role_group_config::<_, RegionServerConfig>(
                &HbaseRole::RegionServer,
                role,
                role_group,
                default_config,
            )?;
            let logging =
                validate_logging(&merged.config.logging, vector_aggregator_config_map_name)?;
            let validated = RegionServerRoleGroupConfig {
                replicas: merged.replicas,
                config: ValidatedRegionServerConfig {
                    config: merged.config,
                    logging,
                },
                config_overrides: merged.config_overrides,
                env_overrides: merged.env_overrides,
                cli_overrides: merged.cli_overrides,
                pod_overrides: merged.pod_overrides,
                product_specific_common_config: merged.product_specific_common_config,
            };
            let role_group_name = RoleGroupName::from_str(role_group_name).with_context(|_| {
                ParseRoleGroupNameSnafu {
                    role: HbaseRole::RegionServer,
                    role_group: role_group_name.clone(),
                }
            })?;
            Ok((role_group_name, validated))
        })
        .collect()
}

fn validate_rest_server_role_group_configs(
    role: &Role<
        HbaseConfigFragment,
        v1alpha1::HbaseConfigOverrides,
        GenericRoleConfig,
        JavaCommonConfig,
    >,
    default_config: &HbaseConfigFragment,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<BTreeMap<RoleGroupName, RestServerRoleGroupConfig>, Error> {
    role.role_groups
        .iter()
        .map(|(role_group_name, role_group)| {
            let merged = validate_role_group_config::<_, HbaseConfig>(
                &HbaseRole::RestServer,
                role,
                role_group,
                default_config,
            )?;
            let logging =
                validate_logging(&merged.config.logging, vector_aggregator_config_map_name)?;
            let validated = RestServerRoleGroupConfig {
                replicas: merged.replicas,
                config: ValidatedRestServerConfig {
                    config: merged.config,
                    logging,
                },
                config_overrides: merged.config_overrides,
                env_overrides: merged.env_overrides,
                cli_overrides: merged.cli_overrides,
                pod_overrides: merged.pod_overrides,
                product_specific_common_config: merged.product_specific_common_config,
            };
            let role_group_name = RoleGroupName::from_str(role_group_name).with_context(|_| {
                ParseRoleGroupNameSnafu {
                    role: HbaseRole::RestServer,
                    role_group: role_group_name.clone(),
                }
            })?;
            Ok((role_group_name, validated))
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

        let hbase: v1alpha1::HbaseCluster =
            stackable_operator::utils::yaml_from_str_singleton_map(input).unwrap();

        let role = hbase.spec.masters.as_ref().unwrap();
        let role_group = role.role_groups.get("default").unwrap();
        let default_config = HbaseConfigFragment::default_config(
            &HbaseRole::Master,
            &hbase.name_any(),
            hbase.spec.cluster_config.hdfs_config_map_name.as_ref(),
            None,
        );

        let validated = with_validated_config::<
            HbaseConfig,
            JavaCommonConfig,
            HbaseConfigFragment,
            GenericRoleConfig,
            v1alpha1::HbaseConfigOverrides,
        >(role_group, role, &default_config)
        .unwrap();

        let env: BTreeMap<String, String> = validated
            .config
            .env_overrides
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();

        assert_eq!(env.get("TEST_VAR"), Some(&"MASTER_RG".to_string()));
        assert_eq!(env.get("TEST_VAR_FROM_MASTER"), Some(&"MASTER".to_string()));
        assert_eq!(env.get("TEST_VAR_FROM_MRG"), Some(&"MASTER".to_string()));
    }

    /// A custom log ConfigMap name that is not a valid Kubernetes name is rejected up-front.
    #[test]
    fn validate_logging_rejects_invalid_custom_config_map_name() {
        use stackable_operator::product_logging::spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        };

        let logging = Logging {
            enable_vector_agent: false,
            containers: [(
                Container::Hbase,
                ContainerLogConfig {
                    choice: Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                        custom: ConfigMapLogConfig {
                            config_map: "invalid ConfigMap name".to_owned(),
                        },
                    })),
                },
            )]
            .into(),
        };

        assert!(validate_logging(&logging, &None).is_err());
    }

    /// Enabling the Vector agent without a Vector aggregator discovery ConfigMap name fails, but
    /// succeeds once a valid name is provided.
    #[test]
    fn validate_logging_requires_vector_aggregator_when_enabled() {
        use stackable_operator::product_logging::spec::{
            AutomaticContainerLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
        };

        let automatic = || ContainerLogConfig {
            choice: Some(ContainerLogConfigChoice::Automatic(
                AutomaticContainerLogConfig::default(),
            )),
        };
        let logging = Logging {
            enable_vector_agent: true,
            containers: [
                (Container::Hbase, automatic()),
                (Container::Vector, automatic()),
            ]
            .into(),
        };

        assert!(validate_logging(&logging, &None).is_err());

        let aggregator = ConfigMapName::from_str("vector-aggregator").expect("valid name");
        assert!(validate_logging(&logging, &Some(aggregator)).is_ok());
    }
}
