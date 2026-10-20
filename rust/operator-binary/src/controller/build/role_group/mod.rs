//! Each role's module gathers its [`RoleGroupInputs`]; [`RoleGroupBuilder::new`] turns those
//! into finished containers, pod volumes and rendered `ConfigMap` entries. Then the `build_*`
//! methods emit the Kubernetes objects. Every HBase role runs the same containers, so the
//! resolve and emit phases live here once; everything that differs between roles happens in the
//! three role modules.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        resources::{NoRuntimeLimits, Resources},
    },
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Container, PersistentVolumeClaim, PodTemplateSpec, Service, Volume},
    },
    shared::time::Duration,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        builder::pod::container::EnvVarSet, jvm_argument_overrides::JvmArgumentOverrides,
        types::operator::RoleGroupName,
    },
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{container, resource},
        validate::ValidatedLogging,
    },
    crd::{HbaseRole, HbaseStorageConfig, v1alpha1},
};

pub(crate) mod master;
pub(crate) mod region_server;
pub(crate) mod rest_server;

/// Why building one role group failed. The caller adds which role and role group it was.
#[derive(Snafu, Debug)]
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

    #[snafu(display("failed to render the config files"))]
    ConfigMapData { source: resource::config_map::Error },

    #[snafu(display("failed to build the containers and volumes"))]
    ContainersAndVolumes { source: container::Error },
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

/// Everything one role group's Kubernetes objects are built from, already resolved.
///
/// These fields are outputs rather than configuration: the containers are built and the
/// `ConfigMap` entries rendered. The `build_*` methods therefore read straight through, and
/// nothing about which role this is reaches them beyond the role's name.
pub(crate) struct RoleGroupBuilder<'a> {
    pub(crate) cluster: &'a ValidatedCluster,
    /// Read for the role group's object names and labels.
    pub(crate) role: HbaseRole,
    pub(crate) role_group_name: RoleGroupName,
    /// Read for the pod's affinity.
    pub(crate) affinity: StackableAffinity,
    /// Read for the pod's termination grace period.
    pub(crate) graceful_shutdown_timeout: Option<Duration>,
    /// The role group's replica count, passed to the StatefulSet as is; `None` makes Kubernetes
    /// run one pod.
    pub(crate) replicas: Option<u16>,
    pub(crate) pod_overrides: PodTemplateSpec,
    /// `Some` for rest servers only, which keeps the other roles' StatefulSets without a
    /// `volumeClaimTemplates` field.
    pub(crate) volume_claim_templates: Option<Vec<PersistentVolumeClaim>>,
    /// The `hbase` container, then the Vector sidecar when the Vector agent is enabled.
    pub(crate) containers: Vec<Container>,
    /// Every pod volume, in the order the pod lists them.
    pub(crate) pod_volumes: Vec<Volume>,
    /// The rendered `ConfigMap` entries, keyed by file name.
    pub(crate) config_map_data: BTreeMap<String, String>,
}

impl<'a> RoleGroupBuilder<'a> {
    /// Resolves one role group: renders its config files, builds the containers every role runs
    /// and collects their pod volumes.
    pub(crate) fn new(inputs: RoleGroupInputs<'a>) -> Result<Self, Error> {
        let config_map_data =
            resource::config_map::config_map_data(&inputs).context(ConfigMapDataSnafu)?;
        let container::ContainersAndVolumes {
            containers,
            pod_volumes,
            volume_claim_templates,
        } = container::containers_and_volumes(&inputs).context(ContainersAndVolumesSnafu)?;

        Ok(Self {
            cluster: inputs.cluster,
            role: inputs.role,
            role_group_name: inputs.role_group_name,
            affinity: inputs.affinity,
            graceful_shutdown_timeout: inputs.graceful_shutdown_timeout,
            replicas: inputs.replicas,
            pod_overrides: inputs.pod_overrides,
            volume_claim_templates,
            containers,
            pod_volumes,
            config_map_data,
        })
    }

    /// The headless and metrics `Service`s.
    pub(crate) fn build_services(&self) -> Vec<Service> {
        vec![
            resource::service::build_rolegroup_service(
                self.cluster,
                &self.role,
                &self.role_group_name,
            ),
            resource::service::build_rolegroup_metrics_service(
                self.cluster,
                &self.role,
                &self.role_group_name,
            ),
        ]
    }

    pub(crate) fn build_config_map(&self) -> Result<ConfigMap, Error> {
        resource::config_map::build_config_map(self).context(ConfigMapSnafu)
    }

    pub(crate) fn build_statefulset(&self) -> Result<StatefulSet, Error> {
        resource::statefulset::build_statefulset(self).context(StatefulSetSnafu)
    }
}
