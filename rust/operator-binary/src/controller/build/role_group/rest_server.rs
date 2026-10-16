use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo, v2::types::operator::RoleGroupName,
};

use crate::{
    controller::{RestServerRoleGroupConfig, ValidatedCluster, build::role_group::RoleGroupInputs},
    crd::{HbaseConfig, HbaseRole},
};

pub(crate) const ROLE: HbaseRole = HbaseRole::RestServer;

/// The inputs of one rest server role group.
pub(crate) fn inputs<'a>(
    cluster: &'a ValidatedCluster,
    cluster_info: &'a KubernetesClusterInfo,
    role_group_name: &RoleGroupName,
    rg_config: &RestServerRoleGroupConfig,
) -> RoleGroupInputs<'a> {
    let HbaseConfig {
        hbase_rootdir,
        resources,
        // Validated up front into `rg_config.config.logging`.
        logging: _,
        affinity,
        graceful_shutdown_timeout,
        requested_secret_lifetime,
        listener_class,
    } = &rg_config.config.config;

    RoleGroupInputs {
        cluster,
        cluster_info,
        role: ROLE,
        role_group_name: role_group_name.clone(),
        affinity: affinity.clone(),
        graceful_shutdown_timeout: *graceful_shutdown_timeout,
        requested_secret_lifetime: *requested_secret_lifetime,
        resources: resources.clone(),
        hbase_rootdir: hbase_rootdir.clone(),
        logging: rg_config.config.logging.clone(),
        listener_class: listener_class.clone(),
        region_mover_opts: String::new(),
        run_region_mover: false,
        replicas: rg_config.replicas,
        config_overrides: rg_config.config_overrides.clone(),
        env_overrides: rg_config.env_overrides.clone(),
        pod_overrides: rg_config.pod_overrides.clone(),
        jvm_argument_overrides: rg_config
            .product_specific_common_config
            .jvm_argument_overrides
            .clone(),
    }
}
