use stackable_operator::{
    commons::affinity::{
        affinity_between_cluster_pods, affinity_between_role_pods, StackableAffinityFragment,
    },
    k8s_openapi::api::core::v1::{PodAffinity, PodAntiAffinity},
};

use crate::{HbaseRole, APP_NAME};

pub fn get_affinity(
    cluster_name: &str,
    role: &HbaseRole,
    hdfs_discovery_cm_name: &str,
) -> StackableAffinityFragment {
    let affinity_between_cluster_pods = affinity_between_cluster_pods(APP_NAME, cluster_name, 20);
    match role {
        HbaseRole::Master => StackableAffinityFragment {
            pod_affinity: Some(PodAffinity {
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    affinity_between_cluster_pods,
                    // We would like a affinity to the Zookeeper Pods, but the hbase CRD only contains a ZNode reference.
                    // We could look up the ZNode and extract the zk cluster from it but that causes network calls
                    // See https://github.com/stackabletech/zookeeper-operator/issues/644
                    // Watch out: The zk can be in a different namespace, so the namespaceSelector must be used
                ]),
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
        },
        HbaseRole::RegionServer => StackableAffinityFragment {
            pod_affinity: Some(PodAffinity {
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    affinity_between_cluster_pods,
                    affinity_between_role_pods(
                        "hdfs",
                        hdfs_discovery_cm_name, // The discovery cm has the same name as the HdfsCluster itself
                        "datanode",
                        50,
                    ),
                ]),
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
        },
        HbaseRole::RestServer => StackableAffinityFragment {
            pod_affinity: Some(PodAffinity {
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    affinity_between_cluster_pods,
                ]),
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
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;
    use std::collections::BTreeMap;

    use crate::HbaseCluster;
    use stackable_operator::{
        commons::affinity::{StackableAffinity, StackableNodeSelector},
        k8s_openapi::{
            api::core::v1::{
                NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, PodAffinity,
                PodAffinityTerm, PodAntiAffinity, WeightedPodAffinityTerm,
            },
            apimachinery::pkg::apis::meta::v1::LabelSelector,
        },
    };

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
        spec:
          image:
            productVersion: 2.4.17
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
        let hbase: HbaseCluster = serde_yaml::from_str(input).expect("illegal test input");
        let merged_config = hbase
            .merged_config(
                &role,
                "default",
                &hbase.spec.cluster_config.hdfs_config_map_name,
            )
            .unwrap();

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
            merged_config.affinity,
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

    #[test]
    fn test_affinity_legacy_node_selector() {
        let input = r#"
        apiVersion: hbase.stackable.tech/v1alpha1
        kind: HbaseCluster
        metadata:
          name: simple-hbase
        spec:
          image:
            productVersion: 2.4.17
          clusterConfig:
            hdfsConfigMapName: simple-hdfs
            zookeeperConfigMapName: simple-znode
          masters:
            roleGroups:
              default:
                replicas: 1
                selector:
                  matchLabels:
                    disktype: ssd
                  matchExpressions:
                    - key: topology.kubernetes.io/zone
                      operator: In
                      values:
                        - antarctica-east1
                        - antarctica-west1
          regionServers:
            roleGroups:
              default:
                replicas: 1
          restServers:
            roleGroups:
              default:
                replicas: 1
        "#;
        let hbase: HbaseCluster = serde_yaml::from_str(input).expect("illegal test input");
        let merged_config = hbase
            .merged_config(
                &HbaseRole::Master,
                "default",
                &hbase.spec.cluster_config.hdfs_config_map_name,
            )
            .unwrap();

        assert_eq!(
            merged_config.affinity,
            StackableAffinity {
                pod_affinity: Some(PodAffinity {
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
                                    ]))
                                }),
                                namespace_selector: None,
                                namespaces: None,
                                topology_key: "kubernetes.io/hostname".to_string(),
                            },
                            weight: 20
                        }
                    ]),
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
                                            "master".to_string(),
                                        )
                                    ]))
                                }),
                                namespace_selector: None,
                                namespaces: None,
                                topology_key: "kubernetes.io/hostname".to_string(),
                            },
                            weight: 70
                        }
                    ]),
                    required_during_scheduling_ignored_during_execution: None,
                }),
                node_affinity: Some(NodeAffinity {
                    preferred_during_scheduling_ignored_during_execution: None,
                    required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                        node_selector_terms: vec![NodeSelectorTerm {
                            match_expressions: Some(vec![NodeSelectorRequirement {
                                key: "topology.kubernetes.io/zone".to_string(),
                                operator: "In".to_string(),
                                values: Some(vec![
                                    "antarctica-east1".to_string(),
                                    "antarctica-west1".to_string()
                                ]),
                            }]),
                            match_fields: None,
                        }]
                    }),
                }),
                node_selector: Some(StackableNodeSelector {
                    node_selector: BTreeMap::from([("disktype".to_string(), "ssd".to_string())])
                }),
            }
        );
    }
}
