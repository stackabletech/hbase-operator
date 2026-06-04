//! Builds the `ssl-server.xml` config file: kerberos/TLS server settings + overrides.
use std::collections::BTreeMap;

use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

use crate::{config::writer::to_hadoop_xml, controller::build::properties::resolved_overrides};

/// Renders `ssl-server.xml`.
pub fn build(settings: BTreeMap<String, String>, overrides: KeyValueConfigOverrides) -> String {
    let mut config: BTreeMap<String, Option<String>> = BTreeMap::new();
    config.extend(settings.into_iter().map(|(k, v)| (k, Some(v))));
    config.extend(resolved_overrides(overrides).map(|(k, v)| (k, Some(v))));
    to_hadoop_xml(config.iter())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::build::properties::test_support::config_overrides;

    #[test]
    fn settings_appear_in_xml() {
        let xml = build(
            BTreeMap::from([("ssl.server.keystore.type".to_string(), "pkcs12".to_string())]),
            config_overrides(&[]),
        );
        assert!(
            xml.contains("<name>ssl.server.keystore.type</name>\n    <value>pkcs12</value>"),
            "{xml}"
        );
    }

    #[test]
    fn user_override_appears_in_xml() {
        let xml = build(
            BTreeMap::new(),
            config_overrides(&[("ssl.server.keystore.type", "jks")]),
        );
        assert!(
            xml.contains("<name>ssl.server.keystore.type</name>\n    <value>jks</value>"),
            "{xml}"
        );
    }
}
