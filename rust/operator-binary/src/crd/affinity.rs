use stackable_operator::{
    commons::affinity::{
        StackableAffinityFragment, affinity_between_cluster_pods, affinity_between_role_pods,
    },
    commons::opa::OpaConfig,
    k8s_openapi::api::core::v1::{PodAffinity, PodAntiAffinity},
};

use crate::crd::{APP_NAME, HbaseRole};

pub fn get_affinity(
    cluster_name: &str,
    role: &HbaseRole,
    hdfs_discovery_cm_name: &str,
    opa_config: Option<&OpaConfig>,
) -> StackableAffinityFragment {
    let mut affinities = vec![affinity_between_cluster_pods(APP_NAME, cluster_name, 20)];
    if role == &HbaseRole::RegionServer {
        affinities.push(affinity_between_role_pods(
            "hdfs",
            hdfs_discovery_cm_name, // The discovery cm has the same name as the HdfsCluster itself
            "datanode",
            50,
        ));
    }
    // We would like an affinity to the ZooKeeper Pods, but the HBase CRD only contains a ZNode
    // reference. Looking up its cluster would require a network call, and it may be in another
    // namespace (which would require namespaceSelector).
    // See https://github.com/stackabletech/zookeeper-operator/issues/644

    // The OPA coprocessors run in masters and regionservers, not in REST servers.
    if let Some(opa_config) = opa_config
        && role != &HbaseRole::RestServer
    {
        affinities.push(affinity_between_role_pods(
            "opa",
            &opa_config.config_map_name, // The discovery ConfigMap has the same name as the OpaCluster.
            "server",
            50,
        ));
    }

    StackableAffinityFragment {
        pod_affinity: Some(PodAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(affinities),
            required_during_scheduling_ignored_during_execution: None,
        }),
        pod_anti_affinity: Some(PodAntiAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                affinity_between_role_pods(APP_NAME, cluster_name, &role.to_string(), 70),
            ]),
            required_during_scheduling_ignored_during_execution: None,
        }),
        node_affinity: None,
        node_selector: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rstest::rstest;
    use stackable_operator::{
        commons::affinity::StackableAffinity,
        k8s_openapi::{
            api::core::v1::{
                PodAffinity, PodAffinityTerm, PodAntiAffinity, WeightedPodAffinityTerm,
            },
            apimachinery::pkg::apis::meta::v1::LabelSelector,
        },
    };

    use super::*;
    use crate::crd::{security::AuthorizationConfig, v1alpha1};

    #[rstest]
    #[case(HbaseRole::Master)]
    #[case(HbaseRole::RegionServer)]
    #[case(HbaseRole::RestServer)]
    fn test_affinity_defaults(#[case] role: HbaseRole) {
        let input = r#"
        apiVersion: hbase.stackable.tech/v1alpha1
        kind: HbaseCluster
        metadata:
          name: simple-hbase
          namespace: default
          uid: 12345678-1234-1234-1234-123456789012
        spec:
          image:
            productVersion: 2.6.4
          clusterConfig:
            hdfsConfigMapName: simple-hdfs
            zookeeperConfigMapName: simple-znode
          masters:
            roleGroups:
              default:
                replicas: 1
          regionServers:
            roleGroups:
              default:
                replicas: 1
          restServers:
            roleGroups:
              default:
                replicas: 1
        "#;
        let hbase: v1alpha1::HbaseCluster =
            serde_yaml::from_str(input).expect("illegal test input");
        let validated_cluster = crate::test_utils::validated_cluster_from(&hbase);
        let affinity = crate::test_utils::merged_config(&validated_cluster, &role)
            .affinity()
            .clone();

        let mut expected_affinities = vec![WeightedPodAffinityTerm {
            pod_affinity_term: PodAffinityTerm {
                label_selector: Some(LabelSelector {
                    match_expressions: None,
                    match_labels: Some(BTreeMap::from([
                        ("app.kubernetes.io/name".to_string(), "hbase".to_string()),
                        (
                            "app.kubernetes.io/instance".to_string(),
                            "simple-hbase".to_string(),
                        ),
                    ])),
                }),
                match_label_keys: None,
                mismatch_label_keys: None,
                namespace_selector: None,
                namespaces: None,
                topology_key: "kubernetes.io/hostname".to_string(),
            },
            weight: 20,
        }];

        match role {
            HbaseRole::Master => (),
            HbaseRole::RegionServer => {
                expected_affinities.push(WeightedPodAffinityTerm {
                    pod_affinity_term: PodAffinityTerm {
                        label_selector: Some(LabelSelector {
                            match_expressions: None,
                            match_labels: Some(BTreeMap::from([
                                ("app.kubernetes.io/name".to_string(), "hdfs".to_string()),
                                (
                                    "app.kubernetes.io/instance".to_string(),
                                    "simple-hdfs".to_string(),
                                ),
                                (
                                    "app.kubernetes.io/component".to_string(),
                                    "datanode".to_string(),
                                ),
                            ])),
                        }),
                        match_label_keys: None,
                        mismatch_label_keys: None,
                        namespace_selector: None,
                        namespaces: None,
                        topology_key: "kubernetes.io/hostname".to_string(),
                    },
                    weight: 50,
                });
            }
            HbaseRole::RestServer => (),
        };

        assert_eq!(
            affinity,
            StackableAffinity {
                pod_affinity: Some(PodAffinity {
                    preferred_during_scheduling_ignored_during_execution: Some(expected_affinities),
                    required_during_scheduling_ignored_during_execution: None,
                }),
                pod_anti_affinity: Some(PodAntiAffinity {
                    preferred_during_scheduling_ignored_during_execution: Some(vec![
                        WeightedPodAffinityTerm {
                            pod_affinity_term: PodAffinityTerm {
                                label_selector: Some(LabelSelector {
                                    match_expressions: None,
                                    match_labels: Some(BTreeMap::from([
                                        ("app.kubernetes.io/name".to_string(), "hbase".to_string(),),
                                        (
                                            "app.kubernetes.io/instance".to_string(),
                                            "simple-hbase".to_string(),
                                        ),
                                        (
                                            "app.kubernetes.io/component".to_string(),
                                            role.to_string(),
                                        )
                                    ]))
                                }),
                                match_label_keys: None,
                                mismatch_label_keys: None,
                                namespace_selector: None,
                                namespaces: None,
                                topology_key: "kubernetes.io/hostname".to_string(),
                            },
                            weight: 70
                        }
                    ]),
                    required_during_scheduling_ignored_during_execution: None,
                }),
                node_affinity: None,
                node_selector: None,
            }
        );
    }

    #[rstest]
    #[case(HbaseRole::Master)]
    #[case(HbaseRole::RegionServer)]
    #[case(HbaseRole::RestServer)]
    fn test_opa_affinity(#[case] role: HbaseRole) {
        let mut hbase = crate::test_utils::minimal_hbase();
        let without_opa = crate::test_utils::validated_cluster_from(&hbase);
        let default_affinity = crate::test_utils::merged_config(&without_opa, &role)
            .affinity()
            .clone();

        hbase.spec.cluster_config.authorization = Some(AuthorizationConfig {
            opa: Some(
                serde_yaml::from_str("configMapName: simple-opa\npackage: hbase")
                    .expect("valid OPA configuration"),
            ),
        });
        let with_opa = crate::test_utils::validated_cluster_from(&hbase);
        let affinity = crate::test_utils::merged_config(&with_opa, &role)
            .affinity()
            .clone();

        if role == HbaseRole::RestServer {
            assert_eq!(affinity, default_affinity);
        } else {
            let mut expected = default_affinity;
            expected
                .pod_affinity
                .as_mut()
                .unwrap()
                .preferred_during_scheduling_ignored_during_execution
                .as_mut()
                .unwrap()
                .push(WeightedPodAffinityTerm {
                    pod_affinity_term: PodAffinityTerm {
                        label_selector: Some(LabelSelector {
                            match_labels: Some(BTreeMap::from([
                                ("app.kubernetes.io/name".to_string(), "opa".to_string()),
                                (
                                    "app.kubernetes.io/instance".to_string(),
                                    "simple-opa".to_string(),
                                ),
                                (
                                    "app.kubernetes.io/component".to_string(),
                                    "server".to_string(),
                                ),
                            ])),
                            ..LabelSelector::default()
                        }),
                        topology_key: "kubernetes.io/hostname".to_string(),
                        ..PodAffinityTerm::default()
                    },
                    weight: 50,
                });
            assert_eq!(affinity, expected);
        }
    }
}
