//! Builds the `security.properties` (JVM security) config file.
//!
//! The operator injects role-specific JVM DNS cache TTLs.
//! User `configOverrides` are applied on top.

use std::collections::BTreeMap;

use stackable_operator::v2::{
    config_file_writer::{PropertiesWriterError, to_java_properties_string},
    config_overrides::KeyValueConfigOverrides,
};

use crate::crd::HbaseRole;

/// Renders `security.properties`: role-specific DNS cache TTLs plus user overrides.
pub fn build(
    role: &HbaseRole,
    overrides: KeyValueConfigOverrides,
) -> Result<String, PropertiesWriterError> {
    // Role-specific positive DNS cache TTLs. Caching forever (the JVM default for
    // successful lookups) breaks failover when a peer's IP changes, so cap the
    // positive cache and disable the negative cache.
    let positive_ttl = match role {
        HbaseRole::Master => "5",
        HbaseRole::RegionServer => "10",
        HbaseRole::RestServer => "30",
    };

    let mut config: BTreeMap<String, String> = BTreeMap::from([
        (
            "networkaddress.cache.ttl".to_string(),
            positive_ttl.to_string(),
        ),
        (
            "networkaddress.cache.negative.ttl".to_string(),
            "0".to_string(),
        ),
    ]);
    // Overrides applied last.
    config.extend(overrides);
    to_java_properties_string(config.iter())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::build::properties::test_support::config_overrides;

    #[test]
    fn injects_master_dns_cache_ttl() {
        assert_eq!(
            build(&HbaseRole::Master, config_overrides(&[])).unwrap(),
            "networkaddress.cache.negative.ttl=0\nnetworkaddress.cache.ttl=5\n"
        );
    }

    #[test]
    fn injects_regionserver_dns_cache_ttl() {
        assert_eq!(
            build(&HbaseRole::RegionServer, config_overrides(&[])).unwrap(),
            "networkaddress.cache.negative.ttl=0\nnetworkaddress.cache.ttl=10\n"
        );
    }

    #[test]
    fn injects_restserver_dns_cache_ttl() {
        assert_eq!(
            build(&HbaseRole::RestServer, config_overrides(&[])).unwrap(),
            "networkaddress.cache.negative.ttl=0\nnetworkaddress.cache.ttl=30\n"
        );
    }

    #[test]
    fn user_overrides_win_over_injected_defaults() {
        assert_eq!(
            build(
                &HbaseRole::Master,
                config_overrides(&[("networkaddress.cache.ttl", "60")])
            )
            .unwrap(),
            "networkaddress.cache.negative.ttl=0\nnetworkaddress.cache.ttl=60\n"
        );
    }
}
