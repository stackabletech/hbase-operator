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
            discovery::{self, build_discovery_config_map},
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
    config_maps.push(build_discovery_config_map(cluster, cluster_info).context(DiscoverySnafu)?);

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

    use stackable_operator::{
        k8s_openapi::api::{apps::v1::StatefulSetSpec, core::v1::PodSpec},
        kube::Resource,
    };
    use strum::IntoEnumIterator;

    use super::{
        build,
        kerberos::KERBEROS_VOLUME_NAME,
        opa::HbaseOpaConfig,
        properties::{
            ConfigFileName,
            hbase_env::{HBASE_MASTER_OPTS, HBASE_REGIONSERVER_OPTS, HBASE_REST_OPTS},
            hbase_site::{
                HBASE_MASTER_IPC_ADDRESS, HBASE_REGIONSERVER_IPC_ADDRESS, HBASE_REST_ENDPOINT,
            },
        },
        resource::{
            listener::LISTENER_PVC_NAME,
            statefulset::{
                HBASE_CONFIG_VOLUME_NAME, HBASE_CONTAINER_NAME, HDFS_DISCOVERY_VOLUME_NAME,
                LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME, REGION_MOVER_OPTS_ENV,
                RUN_REGION_MOVER_ENV,
            },
        },
    };
    use crate::{
        controller::{
            KubernetesResources, Prepared, ValidatedCluster, dereference::DereferencedObjects,
            zookeeper::ZookeeperConnectionInformation,
        },
        crd::{HbaseRole, LISTENER_VOLUME_NAME, TLS_STORE_VOLUME_NAME},
        test_utils,
    };

    /// A cluster that takes every role-specific build path at once: Kerberos with TLS (the
    /// `kerberos` and `tls` pod volumes), OPA authorization, the Vector agent on every role, a
    /// custom log ConfigMap on one master role group, overrides for the three rendered config
    /// files, the environment, the pod template and the JVM arguments, the region mover enabled
    /// on the region servers, rest servers, and two master role groups (so
    /// `hbase.client.bootstrap.servers` lists more than one group).
    const RICH_HBASE_YAML: &str = r#"
---
apiVersion: hbase.stackable.tech/v1alpha1
kind: HbaseCluster
metadata:
  name: hbase
  namespace: default
  uid: c2c8c5c0-0b5a-4b1e-9f3e-1a2b3c4d5e6f
spec:
  image:
    productVersion: 2.6.3
  clusterConfig:
    hdfsConfigMapName: simple-hdfs
    zookeeperConfigMapName: simple-znode
    vectorAggregatorConfigMapName: vector-aggregator-discovery
    authentication:
      tlsSecretClass: tls
      kerberos:
        secretClass: kerberos
    authorization:
      opa:
        configMapName: opa
        package: hbase
  masters:
    config:
      logging:
        enableVectorAgent: true
    configOverrides:
      hbase-site.xml:
        hbase.master.custom.setting: from-role-override
    roleGroups:
      default:
        replicas: 2
        configOverrides:
          hbase-env.sh:
            HBASE_CUSTOM_ENV: from-role-group-override
          security.properties:
            networkaddress.cache.negative.ttl: "1"
        envOverrides:
          CUSTOM_ENV: custom-value
        podOverrides:
          metadata:
            labels:
              custom-label: custom-value
        jvmArgumentOverrides:
          add:
            - -Dcustom.jvm.argument=true
      other:
        replicas: 1
        config:
          logging:
            enableVectorAgent: true
            containers:
              hbase:
                custom:
                  configMap: hbase-log-config
  regionServers:
    config:
      logging:
        enableVectorAgent: true
      regionMover:
        runBeforeShutdown: true
    roleGroups:
      default:
        replicas: 2
  restServers:
    config:
      logging:
        enableVectorAgent: true
    roleGroups:
      default:
        replicas: 1
"#;

    /// [`RICH_HBASE_YAML`], validated with a dereferenced ZooKeeper connection and OPA endpoint.
    fn rich_validated_cluster() -> ValidatedCluster {
        test_utils::validated_cluster_with(
            &test_utils::hbase_from_yaml(RICH_HBASE_YAML),
            DereferencedObjects {
                zookeeper_connection_information: ZookeeperConnectionInformation::for_tests(),
                hbase_opa_config: Some(HbaseOpaConfig::new(
                    "http://opa.default.svc.cluster.local:8081/v1/data/hbase/allow",
                )),
            },
        )
    }

    /// Not a test: dumps every built resource as JSON to `$RENDER_DUMP`, so the refactoring can be
    /// diffed against the baseline. Run with `cargo test dump_rendered_resources -- --ignored`.
    #[test]
    #[ignore = "not a test: writes the rendered resources to $RENDER_DUMP for the render diff"]
    fn dump_rendered_resources() {
        let path = std::env::var("RENDER_DUMP").expect("RENDER_DUMP should be set");
        let resources =
            build(&rich_validated_cluster(), &test_utils::cluster_info()).expect("build succeeds");
        let dump = serde_json::json!({
            "services": resources.services,
            "config_maps": resources.config_maps,
            "pod_disruption_budgets": resources.pod_disruption_budgets,
            "stateful_sets": resources.stateful_sets,
            "service_accounts": resources.service_accounts,
            "role_bindings": resources.role_bindings,
        });
        std::fs::write(
            path,
            serde_json::to_string_pretty(&dump).expect("the resources should serialise"),
        )
        .expect("the dump should be written");
    }

    /// The `default` role group's StatefulSet spec for `role`, found by the name the builder
    /// derives from the cluster.
    fn stateful_set_spec<'a>(
        resources: &'a KubernetesResources<Prepared>,
        cluster: &ValidatedCluster,
        role: &HbaseRole,
    ) -> &'a StatefulSetSpec {
        let name = cluster
            .role_group_resource_names(role, &test_utils::role_group_name("default"))
            .stateful_set_name()
            .to_string();
        resources
            .stateful_sets
            .iter()
            .find(|stateful_set| stateful_set.metadata.name.as_deref() == Some(name.as_str()))
            .and_then(|stateful_set| stateful_set.spec.as_ref())
            .expect("the role group's StatefulSet should be built with a spec")
    }

    fn pod_spec(spec: &StatefulSetSpec) -> &PodSpec {
        spec.template
            .spec
            .as_ref()
            .expect("the pod template should have a spec")
    }

    /// The `hbase` container's environment variables, by name.
    fn hbase_container_env(spec: &StatefulSetSpec) -> BTreeMap<String, String> {
        pod_spec(spec)
            .containers
            .iter()
            .find(|container| container.name == HBASE_CONTAINER_NAME.to_string())
            .expect("the hbase container should exist")
            .env
            .iter()
            .flatten()
            .map(|env_var| {
                (
                    env_var.name.clone(),
                    env_var.value.clone().unwrap_or_default(),
                )
            })
            .collect()
    }

    /// The pod volume names, in the order the pod template lists them.
    fn pod_volume_names(spec: &StatefulSetSpec) -> Vec<String> {
        pod_spec(spec)
            .volumes
            .iter()
            .flatten()
            .map(|volume| volume.name.clone())
            .collect()
    }

    fn claim_template_names(spec: &StatefulSetSpec) -> Vec<String> {
        spec.volume_claim_templates
            .iter()
            .flatten()
            .filter_map(|claim| claim.metadata.name.clone())
            .collect()
    }

    /// The `default` role group's ConfigMap data for `role`, found by the name the builder
    /// derives from the cluster.
    fn config_map_data<'a>(
        resources: &'a KubernetesResources<Prepared>,
        cluster: &ValidatedCluster,
        role: &HbaseRole,
    ) -> &'a BTreeMap<String, String> {
        let name = cluster
            .role_group_resource_names(role, &test_utils::role_group_name("default"))
            .role_group_config_map()
            .to_string();
        resources
            .config_maps
            .iter()
            .find(|config_map| config_map.metadata.name.as_deref() == Some(name.as_str()))
            .and_then(|config_map| config_map.data.as_ref())
            .expect("the role group's ConfigMap should be built with data")
    }

    /// RestServers get their listener from a volume claim template; Masters and RegionServers
    /// from an ephemeral pod volume. Both at once for one role would collide by name at apply
    /// time.
    #[test]
    fn the_listener_is_a_claim_template_only_for_rest_servers() {
        let cluster = rich_validated_cluster();
        let resources = build(&cluster, &test_utils::cluster_info()).expect("build succeeds");

        for role in HbaseRole::iter() {
            let claim_template = matches!(role, HbaseRole::RestServer);
            let spec = stateful_set_spec(&resources, &cluster, &role);
            assert_eq!(
                pod_volume_names(spec).contains(&LISTENER_VOLUME_NAME.to_string()),
                !claim_template,
                "{role:?} listener pod volume"
            );
            assert_eq!(
                claim_template_names(spec).contains(&LISTENER_PVC_NAME.to_string()),
                claim_template,
                "{role:?} listener claim template"
            );
        }
    }

    /// The entrypoint runs under `set -u`, so every role carries both region-mover variables;
    /// only RegionServers get values that run the mover (the fixture enables it for them).
    #[test]
    fn every_role_sets_the_region_mover_variables() {
        let cluster = rich_validated_cluster();
        let resources = build(&cluster, &test_utils::cluster_info()).expect("build succeeds");

        for role in HbaseRole::iter() {
            let runs_the_mover = matches!(role, HbaseRole::RegionServer);
            let env = hbase_container_env(stateful_set_spec(&resources, &cluster, &role));
            assert_eq!(
                env.get(&REGION_MOVER_OPTS_ENV.to_string())
                    .map(|options| !options.is_empty()),
                Some(runs_the_mover),
                "{role:?} {} is set, and non-empty only for region servers",
                *REGION_MOVER_OPTS_ENV
            );
            assert_eq!(
                env.get(&RUN_REGION_MOVER_ENV.to_string())
                    .map(String::as_str),
                Some(if runs_the_mover { "true" } else { "false" }),
                "{role:?} {}",
                *RUN_REGION_MOVER_ENV
            );
        }
    }

    /// Each role's `hbase-site.xml` carries only its own bind settings, and its `hbase-env.sh`
    /// its own `*_OPTS` variable.
    #[test]
    fn each_role_gets_only_its_own_site_keys_and_opts_variable() {
        let cluster = rich_validated_cluster();
        let resources = build(&cluster, &test_utils::cluster_info()).expect("build succeeds");

        // The hbase-site key that identifies each role's own section, and its OPTS variable.
        let own = |role: &HbaseRole| match role {
            HbaseRole::Master => (HBASE_MASTER_IPC_ADDRESS, HBASE_MASTER_OPTS),
            HbaseRole::RegionServer => (HBASE_REGIONSERVER_IPC_ADDRESS, HBASE_REGIONSERVER_OPTS),
            HbaseRole::RestServer => (HBASE_REST_ENDPOINT, HBASE_REST_OPTS),
        };
        for role in HbaseRole::iter() {
            let data = config_map_data(&resources, &cluster, &role);
            let hbase_site = &data[&ConfigFileName::HbaseSite.to_string()];
            let hbase_env = &data[&ConfigFileName::HbaseEnv.to_string()];
            let (own_key, own_opts) = own(&role);

            assert!(
                hbase_site.contains(own_key),
                "{role:?} hbase-site has {own_key}"
            );
            for other in HbaseRole::iter().filter(|other| *other != role) {
                let (foreign_key, _) = own(&other);
                assert!(
                    !hbase_site.contains(foreign_key),
                    "{role:?} hbase-site must not have {foreign_key}"
                );
            }
            assert!(
                hbase_env.contains(&format!("export {own_opts}=")),
                "{role:?} hbase-env sets {own_opts}"
            );
        }
    }

    /// The pod volume order is part of the pod template: changing it restarts every pod. The
    /// operator volumes come first, then the Kerberos and TLS volumes, then the ephemeral
    /// listener volume of Masters and RegionServers (RestServers have a claim template instead).
    #[test]
    fn pod_volumes_keep_their_order() {
        let cluster = rich_validated_cluster();
        let resources = build(&cluster, &test_utils::cluster_info()).expect("build succeeds");

        for role in HbaseRole::iter() {
            let mut expected: Vec<String> = vec![
                HBASE_CONFIG_VOLUME_NAME.to_string(),
                HDFS_DISCOVERY_VOLUME_NAME.to_string(),
                LOG_VOLUME_NAME.to_string(),
                LOG_CONFIG_VOLUME_NAME.to_string(),
                KERBEROS_VOLUME_NAME.to_string(),
                TLS_STORE_VOLUME_NAME.to_string(),
            ];
            if matches!(role, HbaseRole::Master | HbaseRole::RegionServer) {
                expected.push(LISTENER_VOLUME_NAME.to_string());
            }
            assert_eq!(
                pod_volume_names(stateful_set_spec(&resources, &cluster, &role)),
                expected,
                "{role:?} pod volumes"
            );
        }
    }

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
