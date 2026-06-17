//! Builds the `ssl-server.xml` config file: kerberos/TLS server settings + overrides.
use std::collections::BTreeMap;

use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

use crate::controller::build::properties::build_optional_xml_config;

/// Renders `ssl-server.xml`, or `None` when there are no settings or overrides
/// (HBase rejects empty XML config files, so the file is omitted entirely).
pub fn build(
    settings: BTreeMap<String, String>,
    overrides: KeyValueConfigOverrides,
) -> Option<String> {
    build_optional_xml_config(settings, overrides)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_when_settings_and_overrides_empty() {
        // HBase rejects empty XML config files, so an all-empty ssl-server.xml
        // must not be rendered at all (the caller omits the ConfigMap entry).
        assert!(build(BTreeMap::new(), KeyValueConfigOverrides::default()).is_none());
    }

    #[test]
    fn settings_appear_in_xml() {
        let xml = build(
            BTreeMap::from([("ssl.server.keystore.type".to_string(), "pkcs12".to_string())]),
            KeyValueConfigOverrides::default(),
        )
        .expect("settings present, so ssl-server.xml is rendered");
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
        )
        .expect("override present, so ssl-server.xml is rendered");
        assert!(
            xml.contains("<name>ssl.server.keystore.type</name>\n    <value>jks</value>"),
            "{xml}"
        );
    }
}
