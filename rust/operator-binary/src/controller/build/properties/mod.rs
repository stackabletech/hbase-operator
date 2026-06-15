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
