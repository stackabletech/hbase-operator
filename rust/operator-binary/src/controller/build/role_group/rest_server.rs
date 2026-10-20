use snafu::OptionExt;
use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo, v2::types::operator::RoleGroupName,
};

use crate::{
    controller::{
        RestServerRoleGroupConfig, ValidatedCluster,
        build::{
            properties::{hbase_env::HBASE_REST_OPTS, hbase_site},
            recommended_labels_for_unversioned_role_group_resources,
            resource::listener::build_listener_pvc,
            role_group::{
                Error, ListenerPlacement, MissingSecretLifetimeSnafu, RoleGroupBuilder,
                RoleGroupInputs,
            },
        },
    },
    crd::{HbaseConfig, HbaseRole},
};

pub(crate) const ROLE: HbaseRole = HbaseRole::RestServer;

/// The inputs of one rest server role group.
pub(crate) fn build<'a>(
    cluster: &'a ValidatedCluster,
    cluster_info: &'a KubernetesClusterInfo,
    role_group_name: &RoleGroupName,
    rg_config: &RestServerRoleGroupConfig,
) -> Result<RoleGroupBuilder<'a>, Error> {
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

    // Rest servers get their listener from a claim template, for a stable per-pod address. The
    // template cannot be modified once deployed, so its labels omit the version.
    let unversioned_labels =
        recommended_labels_for_unversioned_role_group_resources(cluster, &ROLE, role_group_name);
    let listener =
        ListenerPlacement::ClaimTemplate(build_listener_pvc(listener_class, &unversioned_labels));

    RoleGroupBuilder::new(RoleGroupInputs {
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
        dns_cache_ttl_seconds: "30",
        hbase_env_opts_variable: HBASE_REST_OPTS.clone(),
        role_hbase_site: hbase_site::rest_server_entries(),
    })
}
