//! Builders that turn a [`ValidatedCluster`](crate::controller::ValidatedCluster) into
//! Kubernetes resources.

use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo, v2::types::operator::RoleGroupName,
};

use crate::controller::{
    KubernetesResources, ValidatedCluster,
    build::resource::{
        config_map::{self, build_rolegroup_config_map},
        discovery::{self, build_discovery_config_map},
        pdb::build_pdb,
        service::{build_rolegroup_metrics_service, build_rolegroup_service},
        statefulset::{self, build_rolegroup_statefulset},
    },
};

// Placeholder role-group name used for the recommended labels of the role-level discovery
// `ConfigMap` (which is not tied to a single role group).
stackable_operator::constant!(pub(crate) PLACEHOLDER_DISCOVERY_ROLE_GROUP: RoleGroupName = "discovery");

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap for role group {role_group}"))]
    ConfigMap {
        source: config_map::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build StatefulSet for role group {role_group}"))]
    StatefulSet {
        source: statefulset::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build discovery ConfigMap"))]
    Discovery { source: discovery::Error },
}

/// Builds every Kubernetes resource for the given validated cluster.
///
/// Does not need a Kubernetes client: every reference to another Kubernetes resource is already
/// dereferenced and validated by this point, so the errors returned here are resource-assembly
/// failures only. `cluster_info` is static cluster metadata (not a client call), and
/// `service_account_name` is the name of the RBAC `ServiceAccount` the role-group Pods run under
/// (RBAC resources are built and applied separately, in the reconcile step).
pub fn build(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
    service_account_name: &str,
) -> Result<KubernetesResources, Error> {
    let mut stateful_sets = vec![];
    let mut services = vec![];
    let mut config_maps = vec![];
    let mut pod_disruption_budgets = vec![];

    for (hbase_role, role_group_configs) in &cluster.role_group_configs {
        for (role_group_name, rg_config) in role_group_configs {
            services.push(build_rolegroup_service(
                cluster,
                hbase_role,
                role_group_name,
            ));
            services.push(build_rolegroup_metrics_service(
                cluster,
                hbase_role,
                role_group_name,
            ));
            config_maps.push(
                build_rolegroup_config_map(cluster, cluster_info, hbase_role, role_group_name)
                    .context(ConfigMapSnafu {
                        role_group: role_group_name.clone(),
                    })?,
            );
            stateful_sets.push(
                build_rolegroup_statefulset(
                    cluster,
                    hbase_role,
                    role_group_name,
                    rg_config,
                    service_account_name,
                )
                .context(StatefulSetSnafu {
                    role_group: role_group_name.clone(),
                })?,
            );
        }

        if let Some(role_config) = cluster.role_configs.get(hbase_role)
            && let Some(pdb) = build_pdb(&role_config.pdb, cluster, hbase_role)
        {
            pod_disruption_budgets.push(pdb);
        }
    }

    // The role-level discovery ConfigMap advertises the cluster's connection information; it is
    // deterministic (derived only from the validated cluster and static cluster info).
    config_maps.push(build_discovery_config_map(cluster, cluster_info).context(DiscoverySnafu)?);

    Ok(KubernetesResources {
        stateful_sets,
        services,
        config_maps,
        pod_disruption_budgets,
    })
}

pub mod graceful_shutdown;
pub mod jvm;
pub mod kerberos;
pub mod opa;
pub mod properties;
pub mod region_mover;
pub mod resource;
pub mod role;
