use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        resources::{NoRuntimeLimits, Resources},
    },
    k8s_openapi::api::core::v1::PodTemplateSpec,
    shared::time::Duration,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        builder::pod::container::EnvVarSet,
        jvm_argument_overrides::JvmArgumentOverrides,
        types::{kubernetes::ListenerClassName, operator::RoleGroupName},
    },
};

use crate::{
    controller::{ValidatedCluster, validate::ValidatedLogging},
    crd::{HbaseRole, HbaseStorageConfig, v1alpha1},
};

pub(crate) mod master;
pub(crate) mod region_server;
pub(crate) mod rest_server;

/// What one role group's resources are built from: the values every role has, resolved from
/// its typed role-group config by that role's module.
pub(crate) struct RoleGroupInputs<'a> {
    pub(crate) cluster: &'a ValidatedCluster,
    pub(crate) cluster_info: &'a KubernetesClusterInfo,
    pub(crate) role: HbaseRole,
    pub(crate) role_group_name: RoleGroupName,
    pub(crate) affinity: StackableAffinity,
    pub(crate) graceful_shutdown_timeout: Option<Duration>,
    pub(crate) requested_secret_lifetime: Option<Duration>,
    /// Container resources and the `HBASE_HEAPSIZE` derivation both read this.
    pub(crate) resources: Resources<HbaseStorageConfig, NoRuntimeLimits>,
    pub(crate) hbase_rootdir: String,
    pub(crate) logging: ValidatedLogging,
    pub(crate) listener_class: ListenerClassName,
    /// Set on every role because the entrypoint runs under `set -u`; only RegionServers run the
    /// mover, so only they have a non-empty value.
    pub(crate) region_mover_opts: String,
    pub(crate) run_region_mover: bool,
    pub(crate) replicas: Option<u16>,
    pub(crate) config_overrides: v1alpha1::HbaseConfigOverrides,
    pub(crate) env_overrides: EnvVarSet,
    pub(crate) pod_overrides: PodTemplateSpec,
    pub(crate) jvm_argument_overrides: JvmArgumentOverrides,
}
