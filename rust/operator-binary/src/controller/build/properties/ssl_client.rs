//! Builds the `ssl-client.xml` config file: kerberos/TLS client settings + overrides.
use std::collections::BTreeMap;

use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

use crate::controller::build::properties::build_xml_config;

/// Renders `ssl-client.xml`.
pub fn build(settings: BTreeMap<String, String>, overrides: KeyValueConfigOverrides) -> String {
    build_xml_config(settings, overrides)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::build::properties::test_support::config_overrides;

    #[test]
    fn settings_appear_in_xml() {
        let xml = build(
            BTreeMap::from([(
                "ssl.client.truststore.type".to_string(),
                "pkcs12".to_string(),
            )]),
            config_overrides(&[]),
        );
        assert!(
            xml.contains("<name>ssl.client.truststore.type</name>\n    <value>pkcs12</value>"),
            "{xml}"
        );
    }

    #[test]
    fn user_override_appears_in_xml() {
        let xml = build(
            BTreeMap::new(),
            config_overrides(&[("ssl.client.keystore.type", "jks")]),
        );
        assert!(
            xml.contains("<name>ssl.client.keystore.type</name>\n    <value>jks</value>"),
            "{xml}"
        );
    }
}
