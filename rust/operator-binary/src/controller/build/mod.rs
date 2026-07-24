//! Builders that turn a [`ValidatedCluster`] into
//! Kubernetes resources.

use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo, v2::types::operator::RoleGroupName,
};

use crate::{
    controller::{
        KubernetesResources, ValidatedCluster,
        build::resource::{
            config_map::{self, build_rolegroup_config_map},
            discovery::{self, build_discovery_config_map},
            pdb::build_pdb,
            rbac::{build_role_binding, build_service_account},
            service::{build_rolegroup_metrics_service, build_rolegroup_service},
            statefulset::{self, build_rolegroup_statefulset},
        },
    },
    crd::HbaseRole,
};

// Placeholder role-group name used for the recommended labels of the role-level discovery
// `ConfigMap` (which is not tied to a single role group).
stackable_operator::constant!(pub(crate) PLACEHOLDER_DISCOVERY_ROLE_GROUP: RoleGroupName = "discovery");

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap for role {hbase_role} role group {role_group}"))]
    ConfigMap {
        source: config_map::Error,
        hbase_role: HbaseRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build StatefulSet for role {hbase_role} role group {role_group}"))]
    StatefulSet {
        source: statefulset::Error,
        hbase_role: HbaseRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build discovery ConfigMap"))]
    Discovery { source: discovery::Error },
}

/// Builds every Kubernetes resource for the given validated cluster.
///
/// Does not need a Kubernetes client: every reference to another Kubernetes resource is already
/// dereferenced and validated by this point, so the errors returned here are resource-assembly
/// failures only. `cluster_info` is static cluster metadata (not a client call).
pub fn build(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
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
                    .with_context(|_| ConfigMapSnafu {
                        hbase_role: hbase_role.clone(),
                        role_group: role_group_name.clone(),
                    })?,
            );
            stateful_sets.push(
                build_rolegroup_statefulset(cluster, hbase_role, role_group_name, rg_config)
                    .with_context(|_| StatefulSetSnafu {
                        hbase_role: hbase_role.clone(),
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
        service_accounts: vec![build_service_account(cluster)],
        role_bindings: vec![build_role_binding(cluster)],
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use stackable_operator::kube::Resource;

    use super::build;
    use crate::test_utils;

    /// Collects the `.metadata.name`s of the given resources, sorted for stable comparison.
    fn sorted_names(resources: &[impl Resource]) -> Vec<&str> {
        let mut names: Vec<&str> = resources
            .iter()
            .filter_map(|resource| resource.meta().name.as_deref())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn build_produces_expected_resource_names() {
        let cluster = test_utils::validated_cluster();
        let cluster_info = test_utils::cluster_info();
        let resources = build(&cluster, &cluster_info).expect("build succeeds");

        // One StatefulSet per role group (one `default` group for each of the three roles).
        assert_eq!(
            sorted_names(&resources.stateful_sets),
            [
                "hbase-master-default",
                "hbase-regionserver-default",
                "hbase-restserver-default",
            ]
        );
        // One headless and one metrics Service per role group.
        assert_eq!(
            sorted_names(&resources.services),
            [
                "hbase-master-default-headless",
                "hbase-master-default-metrics",
                "hbase-regionserver-default-headless",
                "hbase-regionserver-default-metrics",
                "hbase-restserver-default-headless",
                "hbase-restserver-default-metrics",
            ]
        );
        // One ConfigMap per role group plus the cluster-wide discovery ConfigMap (`hbase`).
        assert_eq!(
            sorted_names(&resources.config_maps),
            [
                "hbase",
                "hbase-master-default",
                "hbase-regionserver-default",
                "hbase-restserver-default",
            ]
        );
        // A default PodDisruptionBudget per role.
        assert_eq!(
            sorted_names(&resources.pod_disruption_budgets),
            ["hbase-master", "hbase-regionserver", "hbase-restserver"]
        );
    }

    /// Locks the RBAC resource names, the roleRef, and the recommended label set against
    /// accidental drift. The cluster name deliberately differs from the product name so that
    /// swapped `name`/`instance` label values cannot pass unnoticed (the shared fixture is named
    /// `hbase`, which would mask exactly that swap).
    #[test]
    fn build_produces_rbac() {
        let hbase = test_utils::hbase_from_yaml(
            &test_utils::MINIMAL_HBASE_YAML.replace("name: hbase", "name: my-hbase"),
        );
        let cluster = test_utils::validated_cluster_from(&hbase);
        let cluster_info = test_utils::cluster_info();
        let resources = build(&cluster, &cluster_info).expect("build succeeds");

        assert_eq!(
            sorted_names(&resources.service_accounts),
            ["my-hbase-serviceaccount"]
        );
        assert_eq!(
            sorted_names(&resources.role_bindings),
            ["my-hbase-rolebinding"]
        );

        let expected_labels = BTreeMap::from(
            [
                ("app.kubernetes.io/component", "none"),
                ("app.kubernetes.io/instance", "my-hbase"),
                (
                    "app.kubernetes.io/managed-by",
                    "hbase.stackable.com_hbasecluster",
                ),
                ("app.kubernetes.io/name", "hbase"),
                ("app.kubernetes.io/role-group", "none"),
                ("app.kubernetes.io/version", "2.6.3-stackable0.0.0-dev"),
                ("stackable.tech/vendor", "Stackable"),
            ]
            .map(|(key, value)| (key.to_string(), value.to_string())),
        );
        let service_account = resources
            .service_accounts
            .first()
            .expect("a ServiceAccount is built");
        assert_eq!(
            service_account.metadata.labels,
            Some(expected_labels.clone())
        );

        let role_binding = resources
            .role_bindings
            .first()
            .expect("a RoleBinding is built");
        assert_eq!(role_binding.metadata.labels, Some(expected_labels));
        assert_eq!(role_binding.role_ref.name, "hbase-clusterrole");
    }
}
