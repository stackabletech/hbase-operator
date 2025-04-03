use snafu::{ResultExt, Snafu};
use stackable_operator::{client::Client, commons::opa::OpaApiVersion};

use crate::crd::{security::AuthorizationConfig, v1alpha1};

const DEFAULT_DRY_RUN: bool = false;
const DEFAULT_CACHE_ACTIVE: bool = true;
const DEFAULT_CACHE_SECONDS: i32 = 5 * 60; // 5 minutes
const DEFAULT_CACHE_SIZE: i32 = 1000; // max 1000 opa responses
const DEFAULT_OPA_POLICY_URL: &str = "http://localhost:8081/v1/data/hbase/allow";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to construct OPA endpoint URL for authorizer"))]
    ConstructOpaEndpointForAuthorizer {
        source: stackable_operator::commons::opa::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct HbaseOpaConfig {
    authorization_connection_string: String,
    dry_run: bool,
    cache_active: bool,
    cache_seconds: i32,
    cache_size: i32,
}

impl Default for HbaseOpaConfig {
    fn default() -> Self {
        HbaseOpaConfig {
            authorization_connection_string: DEFAULT_OPA_POLICY_URL.to_string(),
            dry_run: DEFAULT_DRY_RUN,
            cache_active: DEFAULT_CACHE_ACTIVE,
            cache_seconds: DEFAULT_CACHE_SECONDS,
            cache_size: DEFAULT_CACHE_SIZE,
        }
    }
}

impl HbaseOpaConfig {
    pub fn new(authorization_connection_string: &str) -> Self {
        HbaseOpaConfig {
            authorization_connection_string: authorization_connection_string.to_string(),
            ..HbaseOpaConfig::default()
        }
    }

    pub async fn from_opa_config(
        client: &Client,
        hbase: &v1alpha1::HbaseCluster,
        authorization_config: &AuthorizationConfig,
    ) -> Result<Self> {
        let authorization_connection_string = authorization_config
            .opa
            .full_document_url_from_config_map(client, hbase, Some("allow"), OpaApiVersion::V1)
            .await
            .context(ConstructOpaEndpointForAuthorizerSnafu)?;

        Ok(HbaseOpaConfig::new(&authorization_connection_string))
    }

    /// Add all the needed configurations to `hbase-site.xml`
    pub fn hbase_site_config(&self) -> Vec<(String, String)> {
        vec![
            (
                "hbase.security.authorization.opa.policy.url".to_string(),
                self.authorization_connection_string.clone(),
            ),
            (
                "hbase.security.authorization.opa.policy.dryrun".to_string(),
                self.dry_run.to_string(),
            ),
            (
                "hbase.security.authorization.opa.policy.cache.active".to_string(),
                self.cache_active.to_string(),
            ),
            (
                "hbase.security.authorization.opa.policy.cache.seconds".to_string(),
                self.cache_seconds.to_string(),
            ),
            (
                "hbase.security.authorization.opa.policy.cache.size".to_string(),
                self.cache_size.to_string(),
            ),
            (
                "hbase.coprocessor.region.classes".to_string(),
                "tech.stackable.hbase.OpenPolicyAgentAccessController".to_string(),
            ),
            (
                "hbase.coprocessor.master.classes".to_string(),
                "tech.stackable.hbase.OpenPolicyAgentAccessController".to_string(),
            ),
            (
                "hbase.coprocessor.regionserver.classes".to_string(),
                "tech.stackable.hbase.OpenPolicyAgentAccessController".to_string(),
            ),
        ]
    }
}
