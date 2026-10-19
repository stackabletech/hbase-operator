use snafu::{OptionExt, ResultExt};
use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo, v2::types::operator::RoleGroupName,
};

use crate::{
    controller::{
        RegionServerRoleGroupConfig, ValidatedCluster,
        build::{
            properties::{hbase_env::HBASE_REGIONSERVER_OPTS, hbase_site},
            recommended_labels_for_role_group_resources, region_mover,
            resource::listener::build_listener_volume,
            role_group::{
                Error, ListenerPlacement, ListenerSnafu, MissingSecretLifetimeSnafu,
                RoleGroupInputs,
            },
        },
    },
    crd::{HbaseRole, RegionServerConfig},
};

pub(crate) const ROLE: HbaseRole = HbaseRole::RegionServer;

/// The inputs of one region server role group.
pub(crate) fn inputs<'a>(
    cluster: &'a ValidatedCluster,
    cluster_info: &'a KubernetesClusterInfo,
    role_group_name: &RoleGroupName,
    rg_config: &RegionServerRoleGroupConfig,
) -> Result<RoleGroupInputs<'a>, Error> {
    let RegionServerConfig {
        hbase_rootdir,
        resources,
        // Validated up front into `rg_config.config.logging`.
        logging: _,
        affinity,
        graceful_shutdown_timeout,
        requested_secret_lifetime,
        listener_class,
        region_mover,
    } = &rg_config.config.config;

    // Region servers, like masters, get their listener from an ephemeral pod volume.
    let recommended_labels =
        recommended_labels_for_role_group_resources(cluster, &ROLE, role_group_name);
    let listener = ListenerPlacement::PodVolume(
        build_listener_volume(listener_class, &recommended_labels).context(ListenerSnafu)?,
    );

    Ok(RoleGroupInputs {
        cluster,
        cluster_info,
        role: ROLE,
        role_group_name: role_group_name.clone(),
        affinity: affinity.clone(),
        graceful_shutdown_timeout: *graceful_shutdown_timeout,
        requested_secret_lifetime: (*requested_secret_lifetime)
            .context(MissingSecretLifetimeSnafu)?,
        resources: resources.clone(),
        hbase_rootdir: hbase_rootdir.clone(),
        logging: rg_config.config.logging.clone(),
        listener,
        region_mover_opts: region_mover::region_mover_args(&rg_config.config.config),
        run_region_mover: region_mover.run_before_shutdown,
        replicas: rg_config.replicas,
        config_overrides: rg_config.config_overrides.clone(),
        env_overrides: rg_config.env_overrides.clone(),
        pod_overrides: rg_config.pod_overrides.clone(),
        jvm_argument_overrides: rg_config
            .product_specific_common_config
            .jvm_argument_overrides
            .clone(),
        dns_cache_ttl_seconds: "10",
        hbase_env_opts_variable: HBASE_REGIONSERVER_OPTS,
        role_hbase_site: hbase_site::region_server_entries(),
    })
}
