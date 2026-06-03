use std::{collections::BTreeMap, str::FromStr};

use product_config::ProductConfigManager;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection::{self},
    config::merge::Merge,
    kube::ResourceExt,
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::GenericRoleConfig,
    v2::types::operator::ClusterName,
};

use crate::{
    controller::dereference::DereferencedObjects,
    crd::{HbaseRole, v1alpha1},
    hbase_controller::{
        CONTAINER_IMAGE_BASE_NAME, ValidatedCluster, ValidatedClusterConfig, ValidatedRoleConfig,
        ValidatedRoleGroupConfig,
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("invalid cluster name"))]
    InvalidClusterName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
    },

    #[snafu(display("invalid role properties"))]
    RoleProperties { source: crate::crd::Error },

    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("could not parse Hbase role [{role}]"))]
    UnidentifiedHbaseRole {
        source: strum::ParseError,
        role: String,
    },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crate::crd::Error },
}

pub fn validate_cluster(
    hbase: &v1alpha1::HbaseCluster,
    image_repository: &str,
    product_config_manager: &ProductConfigManager,
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

    let roles = hbase.build_role_properties().context(RolePropertiesSnafu)?;

    let validated_config = validate_all_roles_and_groups_config(
        &resolved_product_image.product_version,
        &transform_all_roles_to_config(hbase, &roles).context(GenerateProductConfigSnafu)?,
        product_config_manager,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let mut role_groups = BTreeMap::new();
    let mut role_configs = BTreeMap::new();

    for (role_name, group_config) in validated_config.iter() {
        let hbase_role = HbaseRole::from_str(role_name).context(UnidentifiedHbaseRoleSnafu {
            role: role_name.to_string(),
        })?;

        if let Some(GenericRoleConfig {
            pod_disruption_budget: pdb,
        }) = hbase.role_config(&hbase_role)
        {
            role_configs.insert(hbase_role.clone(), ValidatedRoleConfig { pdb: pdb.clone() });
        }

        let mut group_configs = BTreeMap::new();
        for (rolegroup_name, rolegroup_config) in group_config.iter() {
            let rolegroup = hbase.server_rolegroup_ref(role_name, rolegroup_name);

            let merged_config = hbase
                .merged_config(
                    &hbase_role,
                    &rolegroup.role_group,
                    &hbase.spec.cluster_config.hdfs_config_map_name,
                )
                .context(FailedToResolveConfigSnafu)?;

            group_configs.insert(
                rolegroup_name.clone(),
                ValidatedRoleGroupConfig {
                    merged_config,
                    config_overrides: merged_config_overrides(hbase, &hbase_role, rolegroup_name),
                    env_overrides: merged_env_overrides(hbase, &hbase_role, rolegroup_name),
                    product_config_properties: rolegroup_config.clone(),
                },
            );
        }

        role_groups.insert(hbase_role, group_configs);
    }

    Ok(ValidatedCluster {
        name: ClusterName::from_str(&hbase.name_any()).context(InvalidClusterNameSnafu)?,
        image: resolved_product_image,
        cluster_config: ValidatedClusterConfig {
            zookeeper_connection_information: dereferenced_objects
                .zookeeper_connection_information,
            hbase_opa_config: dereferenced_objects.hbase_opa_config,
        },
        role_group_configs: role_groups,
        role_configs,
    })
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
