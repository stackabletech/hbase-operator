//! Gathers the inputs of one master role group. Masters get their listener from an ephemeral
//! pod volume and do not run the region mover.

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo, v2::types::operator::RoleGroupName,
};

use crate::{
    controller::{
        MasterRoleGroupConfig, ValidatedCluster,
        build::{
            properties::{hbase_env::HBASE_MASTER_OPTS, hbase_site},
            recommended_labels_for_role_group_resources,
            resource::listener::{self, build_listener_volume},
            role_group::{ListenerPlacement, RoleGroupInputs},
        },
    },
    crd::{HbaseConfig, HbaseRole},
};

pub(crate) const ROLE: HbaseRole = HbaseRole::Master;
/// The JVM's positive DNS cache TTL for this role, written to `security.properties`.
const DNS_CACHE_TTL_SECONDS: u32 = 5;

/// Why gathering one master role group's inputs failed. The caller adds which role group it was.
#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("failed to build the listener volume"))]
    Listener { source: listener::Error },
}

/// The inputs of one master role group.
pub(crate) fn inputs<'a>(
    cluster: &'a ValidatedCluster,
    cluster_info: &'a KubernetesClusterInfo,
    role_group_name: &RoleGroupName,
    rg_config: &MasterRoleGroupConfig,
) -> Result<RoleGroupInputs<'a>, Error> {
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

    // Masters get their listener from an ephemeral pod volume: clients pull the latest address
    // from ZooKeeper, so the pods need no stable identity.
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
        dns_cache_ttl_seconds: DNS_CACHE_TTL_SECONDS,
        hbase_env_opts_variable: HBASE_MASTER_OPTS.clone(),
        role_hbase_site: hbase_site::master_entries(),
    })
}
