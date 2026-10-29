//! Gathers the inputs of one region server role group. Region servers get their listener from
//! an ephemeral pod volume and are the only role that runs the region mover.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo, v2::types::operator::RoleGroupName,
};

use crate::{
    controller::{
        RegionServerRoleGroupConfig, ValidatedCluster, ValidatedRegionServerConfig,
        build::{
            properties::{hbase_env::HBASE_REGIONSERVER_OPTS, hbase_site},
            recommended_labels_for_role_group_resources, region_mover,
            resource::listener::{self, build_listener_volume},
            role_group::{ListenerPlacement, RoleGroupInputs},
        },
    },
    crd::HbaseRole,
};

pub(crate) const ROLE: HbaseRole = HbaseRole::RegionServer;
/// The JVM's positive DNS cache TTL for this role, written to `security.properties`.
const DNS_CACHE_TTL_SECONDS: u32 = 10;

/// Why gathering one region server role group's inputs failed. The caller adds which role group
/// it was.
#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build the listener volume"))]
    Listener { source: listener::Error },
}

/// The inputs of one region server role group.
pub(crate) fn inputs<'a>(
    cluster: &'a ValidatedCluster,
    cluster_info: &'a KubernetesClusterInfo,
    role_group_name: &RoleGroupName,
    rg_config: &RegionServerRoleGroupConfig,
) -> Result<RoleGroupInputs<'a>, Error> {
    let ValidatedRegionServerConfig {
        hbase_rootdir,
        resources,
        logging,
        affinity,
        graceful_shutdown_timeout,
        requested_secret_lifetime,
        listener_class,
        region_mover,
    } = &rg_config.config;

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
        requested_secret_lifetime: *requested_secret_lifetime,
        resources: resources.clone(),
        hbase_rootdir: hbase_rootdir.clone(),
        logging: logging.clone(),
        listener,
        region_mover_opts: region_mover::region_mover_args(&rg_config.config),
        run_region_mover: region_mover.run_before_shutdown,
        replicas: rg_config.replicas,
        config_overrides: rg_config.config_overrides.clone(),
        env_overrides: rg_config.env_overrides.clone(),
        pod_overrides: rg_config.pod_overrides.clone(),
        jvm_argument_overrides: rg_config
            .product_specific_common_config
            .jvm_argument_overrides
            .clone(),
        dns_cache_ttl_seconds: DNS_CACHE_TTL_SECONDS,
        hbase_env_opts_variable: HBASE_REGIONSERVER_OPTS.clone(),
        role_hbase_site: hbase_site::region_server_entries(),
    })
}
