//! Builds the `ssl-server.xml` config file: kerberos/TLS server settings + overrides.
use std::collections::BTreeMap;

use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

use crate::controller::build::properties::build_xml_config;

/// Renders `ssl-server.xml`.
pub fn build(settings: BTreeMap<String, String>, overrides: KeyValueConfigOverrides) -> String {
    build_xml_config(settings, overrides)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_appear_in_xml() {
        let xml = build(
            BTreeMap::from([("ssl.server.keystore.type".to_string(), "pkcs12".to_string())]),
            KeyValueConfigOverrides::default(),
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
            [("ssl.server.keystore.type", "jks")].into(),
        );
        assert!(
            xml.contains("<name>ssl.server.keystore.type</name>\n    <value>jks</value>"),
            "{xml}"
        );
    }
}
