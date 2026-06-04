//! Per-file builders for the HBase config files assembled into the rolegroup
//! `ConfigMap`. Each `<file>.rs` module produces the rendered content for one
//! config file; the shared [`crate::config::writer`] module serializes maps to
//! the Hadoop-XML / Java-properties on-wire format.

use std::collections::BTreeMap;

use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

pub mod hbase_env;
pub mod hbase_site;
pub mod logging;
pub mod security_properties;
pub mod ssl_client;
pub mod ssl_server;

/// Keep only the set (`Some`) entries of a `key -> optional value` map, as `(key, value)` pairs.
fn defined_entries(
    entries: BTreeMap<String, Option<String>>,
) -> impl Iterator<Item = (String, String)> {
    entries
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key, value)))
}

/// Resolve user-provided [`KeyValueConfigOverrides`] into the key/value pairs to merge
/// into a config file, dropping entries whose value is unset (`None`).
fn resolved_overrides(
    overrides: KeyValueConfigOverrides,
) -> impl Iterator<Item = (String, String)> {
    defined_entries(overrides.overrides)
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
    use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

    use crate::crd::v1alpha1;

    /// Builds a [`KeyValueConfigOverrides`] from `(key, value)` pairs for tests.
    pub fn config_overrides(pairs: &[(&str, &str)]) -> KeyValueConfigOverrides {
        KeyValueConfigOverrides {
            overrides: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), Some(v.to_string())))
                .collect(),
        }
    }

    /// A minimal three-role HbaseCluster used to drive the per-file builder tests.
    pub const MINIMAL_HBASE_YAML: &str = r#"
---
apiVersion: hbase.stackable.tech/v1alpha1
kind: HbaseCluster
metadata:
  name: hbase
  namespace: default
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
}
