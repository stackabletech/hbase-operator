//! Builds the `ssl-client.xml` config file: kerberos/TLS client settings + overrides.
use std::collections::BTreeMap;

use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

use crate::controller::build::properties::build_optional_xml_config;

/// Renders `ssl-client.xml`, or `None` when there are no settings or overrides
/// (HBase rejects empty XML config files, so the file is omitted entirely).
pub fn build(
    settings: BTreeMap<String, String>,
    overrides: KeyValueConfigOverrides,
) -> Option<String> {
    build_optional_xml_config(settings, overrides)
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;

    #[test]
    fn omitted_when_settings_and_overrides_empty() {
        // HBase rejects empty XML config files, so an all-empty ssl-client.xml
        // must not be rendered at all (the caller omits the ConfigMap entry).
        assert!(build(BTreeMap::new(), KeyValueConfigOverrides::default()).is_none());
    }

    #[test]
    fn settings_appear_in_xml() {
        let xml = build(
            BTreeMap::from([(
                "ssl.client.truststore.type".to_string(),
                "pkcs12".to_string(),
            )]),
            KeyValueConfigOverrides::default(),
        )
        .expect("settings present, so ssl-client.xml is rendered");
        assert!(
            xml.contains(indoc! {"
                <name>ssl.client.truststore.type</name>
                    <value>pkcs12</value>"}),
            "{xml}"
        );
    }

    #[test]
    fn user_override_appears_in_xml() {
        let xml = build(
            BTreeMap::new(),
            [("ssl.client.keystore.type", "jks")].into(),
        )
        .expect("override present, so ssl-client.xml is rendered");
        assert!(
            xml.contains(indoc! {"
                <name>ssl.client.keystore.type</name>
                    <value>jks</value>"}),
            "{xml}"
        );
    }
}
