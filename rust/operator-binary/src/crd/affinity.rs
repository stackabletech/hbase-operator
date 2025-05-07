use stackable_operator::{
    commons::affinity::{
        StackableAffinityFragment, affinity_between_cluster_pods, affinity_between_role_pods,
    },
    k8s_openapi::api::core::v1::{PodAffinity, PodAntiAffinity},
};

use crate::crd::{APP_NAME, HbaseRole};

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
    use crate::crd::v1alpha1;

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
            productVersion: 2.6.2
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
        let affinity = hbase
            .merged_config(
                &role,
                "default",
                &hbase.spec.cluster_config.hdfs_config_map_name,
            )
            .unwrap()
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

        assert_eq!(affinity, StackableAffinity {
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
                                    ("app.kubernetes.io/component".to_string(), role.to_string(),)
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
        });
    }
}
