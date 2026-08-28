//! Builders that turn a [`ValidatedCluster`] into
//! Kubernetes resources.

use std::{marker::PhantomData, ops::Deref};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    kvp::Labels,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        builder::meta::ownerreference_from_resource,
        kvp::label,
        types::operator::{RoleGroupName, RoleName},
    },
};

use crate::{
    controller::{
        CONTROLLER_NAME, KubernetesResources, OPERATOR_NAME, PRODUCT_NAME, Prepared,
        ValidatedCluster,
        build::resource::{
            config_map::{self, build_rolegroup_config_map},
            discovery::build_discovery_config_map,
            pdb::build_pdb,
            rbac::{build_role_binding, build_service_account},
            service::{build_rolegroup_metrics_service, build_rolegroup_service},
            statefulset::{self, build_rolegroup_statefulset},
        },
    },
    crd::HbaseRole,
};

/// Returns an [`ObjectMetaBuilder`] pre-filled with the cluster's namespace, an owner
/// reference back to the cluster, the resource `name` and the given `recommended_labels`.
///
/// Consolidates the metadata chain repeated by the child-resource builders. Call sites that
/// need extra labels/annotations chain them onto the returned builder.
pub(crate) fn object_meta(
    cluster: &ValidatedCluster,
    name: impl Into<String>,
    recommended_labels: Labels,
) -> ObjectMetaBuilder {
    let mut builder = ObjectMetaBuilder::new();
    builder
        .name_and_namespace(cluster)
        .name(name)
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(recommended_labels);
    builder
}

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap for role {role} role group {role_group}", role = hbase_role.deref()))]
    ConfigMap {
        source: config_map::Error,
        hbase_role: HbaseRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build StatefulSet for role {role} role group {role_group}", role = hbase_role.deref()))]
    StatefulSet {
        source: statefulset::Error,
        hbase_role: HbaseRole,
        role_group: RoleGroupName,
    },
}

/// Builds every Kubernetes resource for the given validated cluster.
///
/// Does not need a Kubernetes client: every reference to another Kubernetes resource is already
/// dereferenced and validated by this point, so the errors returned here are resource-assembly
/// failures only. `cluster_info` is static cluster metadata (not a client call).
pub fn build(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> Result<KubernetesResources<Prepared>, Error> {
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
    config_maps.push(build_discovery_config_map(cluster, cluster_info));

    Ok(KubernetesResources {
        stateful_sets,
        services,
        config_maps,
        pod_disruption_budgets,
        service_accounts: vec![build_service_account(cluster)],
        role_bindings: vec![build_role_binding(cluster)],
        status: PhantomData,
    })
}

pub(crate) fn recommended_labels_for_cluster_resources(cluster: &ValidatedCluster) -> Labels {
    label::recommended_labels_for_cluster_resources(
        &cluster.name,
        &PRODUCT_NAME,
        &cluster.product_version,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
    )
}

pub(crate) fn recommended_labels_for_role_resources(
    cluster: &ValidatedCluster,
    role_name: &RoleName,
) -> Labels {
    label::recommended_labels_for_role_resources(
        &cluster.name,
        &PRODUCT_NAME,
        &cluster.product_version,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
        role_name,
    )
}

pub(crate) fn recommended_labels_for_role_group_resources(
    cluster: &ValidatedCluster,
    role_name: &RoleName,
    role_group_name: &RoleGroupName,
) -> Labels {
    label::recommended_labels_for_role_group_resources(
        &cluster.name,
        &PRODUCT_NAME,
        &cluster.product_version,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
        role_name,
        role_group_name,
    )
}

pub(crate) fn recommended_labels_for_unversioned_role_group_resources(
    cluster: &ValidatedCluster,
    role_name: &RoleName,
    role_group_name: &RoleGroupName,
) -> Labels {
    label::recommended_labels_for_unversioned_role_group_resources(
        &cluster.name,
        &PRODUCT_NAME,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
        role_name,
        role_group_name,
    )
}

/// Selector labels matching the pods of a role group.
pub(crate) fn role_group_selector(
    cluster: &ValidatedCluster,
    role_name: &RoleName,
    role_group_name: &RoleGroupName,
) -> Labels {
    label::role_group_selector(&cluster.name, &PRODUCT_NAME, role_name, role_group_name)
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

    /// The expected `app.kubernetes.io/version` label value for the given product version.
    ///
    /// The `-stackable` suffix carries the operator's own version, which is `0.0.0-dev` on main
    /// but rewritten by the release process — so tests must derive it rather than hardcode it,
    /// or they fail on release branches.
    fn app_version_label(product_version: &str) -> String {
        format!(
            "{product_version}-stackable{}",
            crate::built_info::PKG_VERSION
        )
    }

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
                ("app.kubernetes.io/instance", "my-hbase".to_string()),
                (
                    "app.kubernetes.io/managed-by",
                    "hbase.stackable.tech_hbasecluster".to_string(),
                ),
                ("app.kubernetes.io/name", "hbase".to_string()),
                ("app.kubernetes.io/version", app_version_label("2.6.3")),
                ("stackable.tech/vendor", "Stackable".to_string()),
            ]
            .map(|(key, value)| (key.to_string(), value)),
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
