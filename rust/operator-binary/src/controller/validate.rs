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
        builder::pod::container::EnvVarName,
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        product_logging::framework::{
            ValidatedContainerLogConfigChoice, VectorContainerLogConfig,
            validate_logging_configuration_for_container,
        },
        role_utils::{
            CommonConfiguration, JavaCommonConfig, Role, RoleGroupConfig, with_validated_config,
        },
        types::{kubernetes::ConfigMapName, operator::RoleGroupName},
    },
};

use crate::{
    controller::{
        ValidatedCluster, ValidatedClusterConfig, ValidatedHbaseConfigOverrides,
        ValidatedMasterConfig, ValidatedRegionServerConfig, ValidatedRestServerConfig,
        ValidatedRoleConfig, dereference::DereferencedObjects,
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

    #[snafu(display(
        "invalid hbase-env.sh variable name {variable:?} in the configOverrides of {role} role group {role_group}"
    ))]
    ParseHbaseEnvVariableName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
        role: HbaseRole,
        role_group: String,
        variable: String,
    },
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
    let master_role_group_configs = validate_role_group_configs(
        &HbaseRole::Master,
        masters,
        &HbaseConfigFragment::default_config(
            &HbaseRole::Master,
            HbaseRole::DEFAULT_MASTER_GRACEFUL_SHUTDOWN_TIMEOUT,
            &cluster_name,
            hdfs_discovery_cm_name,
            opa_config,
        ),
        &vector_aggregator_config_map_name,
        // The annotation names the merged config type, which nothing else at this call site does.
        |config: &HbaseConfig| &config.logging,
        |config, logging| ValidatedMasterConfig { config, logging },
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
    let region_server_role_group_configs = validate_role_group_configs(
        &HbaseRole::RegionServer,
        region_servers,
        &RegionServerConfigFragment::default_config(
            &HbaseRole::RegionServer,
            &cluster_name,
            hdfs_discovery_cm_name,
            opa_config,
        ),
        &vector_aggregator_config_map_name,
        |config: &RegionServerConfig| &config.logging,
        |config, logging| ValidatedRegionServerConfig { config, logging },
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
        Some(rest_servers) => validate_role_group_configs(
            &HbaseRole::RestServer,
            rest_servers,
            &HbaseConfigFragment::default_config(
                &HbaseRole::RestServer,
                HbaseRole::DEFAULT_REST_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT,
                &cluster_name,
                hdfs_discovery_cm_name,
                opa_config,
            ),
            &vector_aggregator_config_map_name,
            |config: &HbaseConfig| &config.logging,
            |config, logging| ValidatedRestServerConfig { config, logging },
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
/// Each role group is merged and validated via [`with_validated_config`], which folds the CRD
/// config fragment (default <- role <- role group) plus the `configOverrides`, `envOverrides`,
/// `cliOverrides`, `podOverrides` and the `jvmArgumentOverrides` (role group wins) into a single
/// merged `RoleGroup`. Its logging and `configOverrides` are then validated and its name parsed.
///
/// The roles differ only in their config type and in the wrapper that pairs it with its
/// validated logging, so `logging` reads the logging config out of a merged config and
/// `with_logging` builds that wrapper.
///
/// Returns an empty map if the role has no role groups.
fn validate_role_group_configs<Fragment, Config, ValidatedConfig>(
    hbase_role: &HbaseRole,
    role: &Role<Fragment, v1alpha1::HbaseConfigOverrides, GenericRoleConfig, JavaCommonConfig>,
    default_config: &Fragment,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
    logging: impl Fn(&Config) -> &Logging<Container>,
    with_logging: impl Fn(Config, ValidatedLogging) -> ValidatedConfig,
) -> Result<
    BTreeMap<
        RoleGroupName,
        RoleGroupConfig<ValidatedConfig, JavaCommonConfig, ValidatedHbaseConfigOverrides>,
    >,
    Error,
>
where
    Fragment: Clone + Merge,
    Config: FromFragment<Fragment = Fragment>,
{
    role.role_groups
        .iter()
        .map(|(role_group_name, role_group)| {
            let merged = with_validated_config::<
                Config,
                JavaCommonConfig,
                Fragment,
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
            } = merged.config;

            let validated_logging =
                validate_logging(logging(&config), vector_aggregator_config_map_name)?;
            let validated = RoleGroupConfig {
                replicas: merged.replicas,
                config: with_logging(config, validated_logging),
                config_overrides: validate_config_overrides(
                    hbase_role,
                    role_group_name,
                    config_overrides,
                )?,
                env_overrides: env_overrides.into(),
                cli_overrides,
                pod_overrides,
                product_specific_common_config,
            };
            let role_group_name = RoleGroupName::from_str(role_group_name).with_context(|_| {
                ParseRoleGroupNameSnafu {
                    role: hbase_role.clone(),
                    role_group: role_group_name.clone(),
                }
            })?;
            Ok((role_group_name, validated))
        })
        .collect()
}

/// Validates the merged `configOverrides` of one role group: the `hbase-env.sh` keys become
/// `EnvVarName`s, the other files' overrides are passed through.
fn validate_config_overrides(
    hbase_role: &HbaseRole,
    role_group_name: &str,
    overrides: v1alpha1::HbaseConfigOverrides,
) -> Result<ValidatedHbaseConfigOverrides, Error> {
    let v1alpha1::HbaseConfigOverrides {
        hbase_site_xml,
        hbase_env_sh,
        ssl_server_xml,
        ssl_client_xml,
        security_properties,
    } = overrides;
    let hbase_env_sh = hbase_env_sh
        .overrides
        .into_iter()
        .map(|(variable, value)| {
            let name = EnvVarName::from_str(&variable).with_context(|_| {
                ParseHbaseEnvVariableNameSnafu {
                    role: hbase_role.clone(),
                    role_group: role_group_name.to_owned(),
                    variable: variable.clone(),
                }
            })?;
            Ok((name, value))
        })
        .collect::<Result<BTreeMap<_, _>, Error>>()?;
    Ok(ValidatedHbaseConfigOverrides {
        hbase_site_xml,
        hbase_env_sh,
        ssl_server_xml,
        ssl_client_xml,
        security_properties,
    })
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;
    use crate::{crd::HbaseConfig, test_utils};

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
            HbaseRole::DEFAULT_MASTER_GRACEFUL_SHUTDOWN_TIMEOUT,
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

    /// An `hbase-env.sh` override whose variable name cannot be an environment variable name
    /// (here: it contains `=`) is rejected when the cluster is validated, naming the role, the
    /// role group and the key, instead of being written into the file.
    #[test]
    fn hbase_env_override_with_invalid_variable_name_is_rejected() {
        let hbase = test_utils::hbase_from_yaml(indoc! {r#"
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
              masters:
                roleGroups:
                  default:
                    replicas: 1
              regionServers:
                configOverrides:
                  hbase-env.sh:
                    "BAD=NAME": value
                roleGroups:
                  default:
                    replicas: 1
        "#});

        let error = validate_cluster(
            &hbase,
            "oci.example.org",
            test_utils::dereferenced_objects(),
        )
        .expect_err("an hbase-env.sh variable name containing '=' should be rejected");

        assert!(
            matches!(
                &error,
                Error::ParseHbaseEnvVariableName {
                    role: HbaseRole::RegionServer,
                    role_group,
                    variable,
                    ..
                } if role_group == "default" && variable == "BAD=NAME"
            ),
            "{error:?}"
        );
    }
}
