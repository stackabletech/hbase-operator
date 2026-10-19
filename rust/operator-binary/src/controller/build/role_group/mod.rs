use std::collections::BTreeMap;

use snafu::Snafu;
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        resources::{NoRuntimeLimits, Resources},
    },
    k8s_openapi::api::core::v1::{PersistentVolumeClaim, PodTemplateSpec, Volume},
    shared::time::Duration,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        builder::pod::container::EnvVarSet, jvm_argument_overrides::JvmArgumentOverrides,
        types::operator::RoleGroupName,
    },
};

use crate::{
    controller::{ValidatedCluster, build::resource, validate::ValidatedLogging},
    crd::{HbaseRole, HbaseStorageConfig, v1alpha1},
};

pub(crate) mod master;
pub(crate) mod region_server;
pub(crate) mod rest_server;

/// Why building one role group failed. The caller adds which role and role group it was.
#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("failed to build the listener volume"))]
    Listener { source: resource::listener::Error },

    #[snafu(display("failed to build ConfigMap"))]
    ConfigMap { source: resource::config_map::Error },

    #[snafu(display("failed to build StatefulSet"))]
    StatefulSet {
        source: resource::statefulset::Error,
    },
}

/// Where a role group's listener comes from.
#[allow(
    clippy::large_enum_variant,
    reason = "one value per role group; its size does not matter"
)]
pub(crate) enum ListenerPlacement {
    /// An ephemeral pod volume (Masters and RegionServers): clients pull the latest address from
    /// ZooKeeper, so the pods need no stable identity.
    PodVolume(Volume),
    /// A persistent volume claim template (RestServers), for a stable per-pod address.
    ClaimTemplate(PersistentVolumeClaim),
}

/// What one role group's resources are built from: the values every role has, resolved from
/// its typed role-group config by that role's module.
pub(crate) struct RoleGroupInputs<'a> {
    pub(crate) cluster: &'a ValidatedCluster,
    pub(crate) cluster_info: &'a KubernetesClusterInfo,
    pub(crate) role: HbaseRole,
    pub(crate) role_group_name: RoleGroupName,
    pub(crate) affinity: StackableAffinity,
    pub(crate) graceful_shutdown_timeout: Option<Duration>,
    pub(crate) requested_secret_lifetime: Duration,
    /// Container resources and the `HBASE_HEAPSIZE` derivation both read this.
    pub(crate) resources: Resources<HbaseStorageConfig, NoRuntimeLimits>,
    pub(crate) hbase_rootdir: String,
    pub(crate) logging: ValidatedLogging,
    pub(crate) listener: ListenerPlacement,
    /// Set on every role because the entrypoint runs under `set -u`; only RegionServers run the
    /// mover, so only they have a non-empty value.
    pub(crate) region_mover_opts: String,
    pub(crate) run_region_mover: bool,
    pub(crate) replicas: Option<u16>,
    pub(crate) config_overrides: v1alpha1::HbaseConfigOverrides,
    pub(crate) env_overrides: EnvVarSet,
    pub(crate) pod_overrides: PodTemplateSpec,
    pub(crate) jvm_argument_overrides: JvmArgumentOverrides,
    /// The JVM's positive DNS cache TTL in seconds, written to `security.properties`.
    pub(crate) dns_cache_ttl_seconds: &'static str,
    /// The `hbase-env.sh` variable `bin/hbase` reads for this role's process.
    pub(crate) hbase_env_opts_variable: &'static str,
    /// The role's own `hbase-site.xml` entries (bind addresses and ports, or the REST endpoint).
    pub(crate) role_hbase_site: BTreeMap<String, String>,
}
