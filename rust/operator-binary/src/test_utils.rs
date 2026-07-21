//! Shared test fixtures.
//!
//! These run the real validation pipeline (`validate_cluster`) against in-memory `HbaseCluster`
//! YAML with a fixed dereferenced ZooKeeper connection (and no OPA), so unit tests can pull
//! merged, validated configs straight from a [`ValidatedCluster`] instead of re-implementing the
//! merge by hand.

use std::str::FromStr;

use stackable_operator::{
    commons::networking::DomainName, utils::cluster_info::KubernetesClusterInfo,
    v2::types::operator::RoleGroupName,
};

use crate::{
    controller::{
        ValidatedCluster, dereference::DereferencedObjects, validate::validate_cluster,
        zookeeper::ZookeeperConnectionInformation,
    },
    crd::{AnyServiceConfig, HbaseRole, v1alpha1},
};

/// A minimal three-role `HbaseCluster` used to drive the builder/property tests.
pub const MINIMAL_HBASE_YAML: &str = r#"
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

/// Parses an `HbaseCluster` from YAML, panicking on invalid input.
pub fn hbase_from_yaml(yaml: &str) -> v1alpha1::HbaseCluster {
    serde_yaml::from_str(yaml).expect("invalid test HbaseCluster YAML")
}

/// The [`MINIMAL_HBASE_YAML`] cluster.
pub fn minimal_hbase() -> v1alpha1::HbaseCluster {
    hbase_from_yaml(MINIMAL_HBASE_YAML)
}

/// Runs the real validation pipeline over `hbase`, with a fixed dereferenced ZooKeeper connection
/// and no OPA.
pub fn validated_cluster_from(hbase: &v1alpha1::HbaseCluster) -> ValidatedCluster {
    validate_cluster(
        hbase,
        "oci.example.org",
        DereferencedObjects {
            zookeeper_connection_information: ZookeeperConnectionInformation::for_tests(),
            hbase_opa_config: None,
        },
    )
    .expect("validate should succeed for the test fixture")
}

/// Runs the real validation pipeline over [`minimal_hbase`].
pub fn validated_cluster() -> ValidatedCluster {
    validated_cluster_from(&minimal_hbase())
}

/// Parses a [`RoleGroupName`], panicking on invalid input.
pub fn role_group_name(name: &str) -> RoleGroupName {
    RoleGroupName::from_str(name).expect("valid role group name")
}

/// A fixed [`KubernetesClusterInfo`] (`cluster.local` domain) for builders that need cluster
/// metadata such as the discovery `ConfigMap` and Kerberos principals.
pub fn cluster_info() -> KubernetesClusterInfo {
    KubernetesClusterInfo {
        cluster_domain: DomainName::from_str("cluster.local").expect("valid cluster domain"),
    }
}

/// The merged [`AnyServiceConfig`] for the given `role` and `role_group`.
pub fn merged_config_for<'a>(
    validated_cluster: &'a ValidatedCluster,
    role: &HbaseRole,
    role_group: &str,
) -> &'a AnyServiceConfig {
    &validated_cluster.role_group_configs[role][&role_group_name(role_group)]
        .config
        .config
}

/// The merged [`AnyServiceConfig`] for the `default` role group of `role`.
pub fn merged_config<'a>(
    validated_cluster: &'a ValidatedCluster,
    role: &HbaseRole,
) -> &'a AnyServiceConfig {
    merged_config_for(validated_cluster, role, "default")
}
