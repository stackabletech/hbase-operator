//! Builds the `ssl-client.xml` config file: kerberos/TLS client settings + overrides.
use std::collections::BTreeMap;

use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

use crate::{
    config::writer::to_hadoop_xml, controller::build::properties::resolved_overrides,
    crd::v1alpha1, kerberos::kerberos_ssl_client_settings,
};

/// Renders `ssl-client.xml`. Returns "" (HBase rejects empty XML files) when empty.
pub fn build(hbase: &v1alpha1::HbaseCluster, overrides: KeyValueConfigOverrides) -> String {
    let mut config: BTreeMap<String, Option<String>> = BTreeMap::new();
    config.extend(
        kerberos_ssl_client_settings(hbase)
            .into_iter()
            .map(|(k, v)| (k, Some(v))),
    );
    config.extend(resolved_overrides(overrides).map(|(k, v)| (k, Some(v))));
    if config.is_empty() {
        return String::new();
    }
    to_hadoop_xml(config.iter())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::build::properties::test_support::{config_overrides, minimal_hbase};

    #[test]
    fn non_kerberos_without_overrides_renders_empty_string() {
        assert_eq!(build(&minimal_hbase(), config_overrides(&[])), "");
    }

    #[test]
    fn user_override_appears_in_xml() {
        let xml = build(
            &minimal_hbase(),
            config_overrides(&[("ssl.client.keystore.type", "jks")]),
        );
        assert!(
            xml.contains("<name>ssl.client.keystore.type</name>\n    <value>jks</value>"),
            "{xml}"
        );
    }
}
