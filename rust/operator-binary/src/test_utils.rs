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
        MasterRoleGroupConfig, RegionServerRoleGroupConfig, RestServerRoleGroupConfig,
        ValidatedCluster, dereference::DereferencedObjects, validate::validate_cluster,
        zookeeper::ZookeeperConnectionInformation,
    },
    crd::v1alpha1,
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
    yaml_serde::from_str(yaml).expect("invalid test HbaseCluster YAML")
}

/// The [`MINIMAL_HBASE_YAML`] cluster.
pub fn minimal_hbase() -> v1alpha1::HbaseCluster {
    hbase_from_yaml(MINIMAL_HBASE_YAML)
}

/// Runs the real validation pipeline over `hbase`, with a fixed dereferenced ZooKeeper connection
/// and no OPA.
pub fn validated_cluster_from(hbase: &v1alpha1::HbaseCluster) -> ValidatedCluster {
    validated_cluster_with(
        hbase,
        DereferencedObjects {
            zookeeper_connection_information: ZookeeperConnectionInformation::for_tests(),
            hbase_opa_config: None,
        },
    )
}

/// Runs the real validation pipeline over `hbase` with the given dereferenced objects, for
/// fixtures that need OPA.
pub fn validated_cluster_with(
    hbase: &v1alpha1::HbaseCluster,
    dereferenced_objects: DereferencedObjects,
) -> ValidatedCluster {
    validate_cluster(hbase, "oci.example.org", dereferenced_objects)
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

pub fn master_role_group_config<'a>(
    cluster: &'a ValidatedCluster,
    role_group: &str,
) -> &'a MasterRoleGroupConfig {
    &cluster.master_role_group_configs[&role_group_name(role_group)]
}

pub fn region_server_role_group_config<'a>(
    cluster: &'a ValidatedCluster,
    role_group: &str,
) -> &'a RegionServerRoleGroupConfig {
    &cluster.region_server_role_group_configs[&role_group_name(role_group)]
}

pub fn rest_server_role_group_config<'a>(
    cluster: &'a ValidatedCluster,
    role_group: &str,
) -> &'a RestServerRoleGroupConfig {
    &cluster.rest_server_role_group_configs[&role_group_name(role_group)]
}
