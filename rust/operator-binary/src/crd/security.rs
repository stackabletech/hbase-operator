use std::str::FromStr;

use serde::{Deserialize, Serialize};
use stackable_operator::{
    commons::opa::OpaConfig,
    schemars::{self, JsonSchema},
    v2::types::kubernetes::SecretClassName,
};

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationConfig {
    /// Name of the SecretClass providing the tls certificates for the WebUIs.
    #[serde(default = "default_tls_secret_class")]
    pub tls_secret_class: SecretClassName,

    /// Kerberos configuration.
    pub kerberos: KerberosConfig,
}

fn default_tls_secret_class() -> SecretClassName {
    SecretClassName::from_str("tls").expect("\"tls\" is a valid secret class name")
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KerberosConfig {
    /// Name of the SecretClass providing the keytab for the HBase services.
    pub secret_class: SecretClassName,
}
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizationConfig {
    // No doc - it's in the struct.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opa: Option<OpaConfig>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorization_without_opa_is_valid() {
        // `opa` is optional (matching the hive template), so an empty `authorization` deserializes.
        let config: AuthorizationConfig = serde_yaml::from_str("{}").expect("empty authorization");
        assert!(config.opa.is_none());
    }

    #[test]
    fn authorization_with_opa_is_parsed() {
        let yaml = ["opa:", "  configMapName: my-opa", "  package: hbase"].join("\n");
        let config: AuthorizationConfig =
            serde_yaml::from_str(&yaml).expect("authorization with opa");
        assert!(config.opa.is_some());
    }
}
