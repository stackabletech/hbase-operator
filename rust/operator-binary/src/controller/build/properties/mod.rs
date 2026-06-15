//! Per-file builders for the HBase config files assembled into the rolegroup
//! `ConfigMap`. Each `<file>.rs` module produces the rendered content for one
//! config file; the shared [`stackable_operator::v2::config_file_writer`]
//! module serializes maps to the Hadoop-XML / Java-properties on-wire format.

use std::collections::BTreeMap;

use stackable_operator::v2::{
    config_file_writer::to_hadoop_xml, config_overrides::KeyValueConfigOverrides,
};

pub mod hbase_env;
pub mod hbase_site;
pub mod logging;
pub mod security_properties;
pub mod ssl_client;
pub mod ssl_server;

/// Render an XML config file from base `settings` merged with user `overrides`
/// (overrides applied last, so users win), serialized to the Hadoop-XML on-wire format.
fn build_xml_config(
    mut config: BTreeMap<String, String>,
    overrides: KeyValueConfigOverrides,
) -> String {
    config.extend(overrides);
    to_hadoop_xml(config.iter())
}

/// The names of the HBase config files assembled into the rolegroup `ConfigMap`.
#[derive(Clone, Copy, Debug, strum::Display)]
pub enum ConfigFileName {
    #[strum(serialize = "hbase-site.xml")]
    HbaseSite,
    #[strum(serialize = "hbase-env.sh")]
    HbaseEnv,
    #[strum(serialize = "ssl-server.xml")]
    SslServer,
    #[strum(serialize = "ssl-client.xml")]
    SslClient,
    #[strum(serialize = "security.properties")]
    Security,
    #[strum(serialize = "log4j2.properties")]
    Log4j2,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names_match_the_hbase_on_disk_names() {
        assert_eq!(ConfigFileName::HbaseSite.to_string(), "hbase-site.xml");
        assert_eq!(ConfigFileName::HbaseEnv.to_string(), "hbase-env.sh");
        assert_eq!(ConfigFileName::SslServer.to_string(), "ssl-server.xml");
        assert_eq!(ConfigFileName::SslClient.to_string(), "ssl-client.xml");
        assert_eq!(ConfigFileName::Security.to_string(), "security.properties");
        assert_eq!(ConfigFileName::Log4j2.to_string(), "log4j2.properties");
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::str::FromStr;

    use stackable_operator::{
        commons::networking::DomainName, utils::cluster_info::KubernetesClusterInfo,
    };

    use crate::{
        controller::{
            ValidatedCluster, dereference::DereferencedObjects, validate::validate_cluster,
        },
        crd::{AnyServiceConfig, HbaseRole, v1alpha1},
        zookeeper::ZookeeperConnectionInformation,
    };

    /// A minimal three-role HbaseCluster used to drive the per-file builder tests.
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

    pub fn minimal_hbase() -> v1alpha1::HbaseCluster {
        serde_yaml::from_str(MINIMAL_HBASE_YAML).expect("invalid test HbaseCluster YAML")
    }

    pub fn cluster_info() -> KubernetesClusterInfo {
        KubernetesClusterInfo {
            cluster_domain: DomainName::try_from("cluster.local").unwrap(),
        }
    }

    /// Runs the real validation pipeline once over [`minimal_hbase`], with a fixed
    /// dereferenced ZooKeeper connection (and no OPA), so the per-file builder tests can
    /// pull merged configs straight from the [`ValidatedCluster`] instead of re-merging by
    /// hand via `crd::merged_config`.
    pub fn validated_cluster() -> ValidatedCluster {
        validate_cluster(
            &minimal_hbase(),
            "oci.example.org",
            &cluster_info(),
            DereferencedObjects {
                zookeeper_connection_information: ZookeeperConnectionInformation::for_tests(),
                hbase_opa_config: None,
            },
        )
        .expect("validate should succeed for the minimal fixture")
    }

    /// The merged [`AnyServiceConfig`] for the `default` role group of `role`.
    pub fn merged_config<'a>(
        validated_cluster: &'a ValidatedCluster,
        role: &HbaseRole,
    ) -> &'a AnyServiceConfig {
        let default_role_group =
            stackable_operator::v2::types::operator::RoleGroupName::from_str("default")
                .expect("'default' is a valid role group name");
        &validated_cluster.role_group_configs[role][&default_role_group].config
    }
}
