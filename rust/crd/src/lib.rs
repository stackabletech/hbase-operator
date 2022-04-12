use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kube::CustomResource;
use stackable_operator::product_config_utils::{ConfigError, Configuration};
use stackable_operator::role_utils::{Role, RoleGroupRef};
use stackable_operator::schemars::{self, JsonSchema};
use strum::{Display, EnumIter};

pub const APP_NAME: &str = "hbase";

pub const HBASE_ENV_SH: &str = "hbase-env.sh";
pub const HBASE_SITE_XML: &str = "hbase-site.xml";

pub const HBASE_MANAGES_ZK: &str = "HBASE_MANAGES_ZK";
pub const HBASE_OPTS: &str = "HBASE_OPTS";

pub const HBASE_CLUSTER_DISTRIBUTED: &str = "hbase.cluster.distributed";
pub const HBASE_ROOTDIR: &str = "hbase.rootdir";
pub const HBASE_ZOOKEEPER_QUORUM: &str = "hbase.zookeeper.quorum";

pub const HBASE_UI_PORT_NAME: &str = "ui";
pub const METRICS_PORT_NAME: &str = "metrics";

pub const HBASE_MASTER_PORT: i32 = 16000;
pub const HBASE_MASTER_UI_PORT: i32 = 16010;
pub const HBASE_REGIONSERVER_PORT: i32 = 16020;
pub const HBASE_REGIONSERVER_UI_PORT: i32 = 16030;
pub const HBASE_REST_PORT: i32 = 8080;
pub const METRICS_PORT: i32 = 8081;

#[derive(Clone, CustomResource, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "hbase.stackable.tech",
    version = "v1alpha1",
    kind = "HbaseCluster",
    plural = "hbaseclusters",
    shortname = "hbase",
    status = "HbaseClusterStatus",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct HbaseClusterSpec {
    /// Emergency stop button, if `true` then all pods are stopped without affecting configuration (as setting `replicas` to `0` would)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped: Option<bool>,
    /// Desired HBase version
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub zookeeper_config_map_name: String,
    pub hdfs_config_map_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<HbaseConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub masters: Option<Role<HbaseConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region_servers: Option<Role<HbaseConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rest_servers: Option<Role<HbaseConfig>>,
}

#[derive(
    Clone, Debug, Deserialize, Display, EnumIter, Eq, Hash, JsonSchema, PartialEq, Serialize,
)]
pub enum HbaseRole {
    #[serde(rename = "master")]
    #[strum(serialize = "master")]
    Master,
    #[serde(rename = "regionserver")]
    #[strum(serialize = "regionserver")]
    RegionServer,
    #[serde(rename = "restserver")]
    #[strum(serialize = "restserver")]
    RestServer,
}

impl HbaseRole {
    /// Returns a port name, the port number, and the protocol for the given role.
    pub fn port_properties(&self) -> Vec<(&'static str, i32, &'static str)> {
        match self {
            HbaseRole::Master => vec![
                ("master", HBASE_MASTER_PORT, "TCP"),
                (HBASE_UI_PORT_NAME, HBASE_MASTER_UI_PORT, "TCP"),
                (METRICS_PORT_NAME, METRICS_PORT, "TCP"),
            ],
            HbaseRole::RegionServer => vec![
                ("regionserver", HBASE_REGIONSERVER_PORT, "TCP"),
                (HBASE_UI_PORT_NAME, HBASE_REGIONSERVER_UI_PORT, "TCP"),
                (METRICS_PORT_NAME, METRICS_PORT, "TCP"),
            ],
            HbaseRole::RestServer => vec![
                ("rest", HBASE_REST_PORT, "TCP"),
                (METRICS_PORT_NAME, METRICS_PORT, "TCP"),
            ],
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HbaseConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hbase_rootdir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hbase_opts: Option<String>,
}

impl Configuration for HbaseConfig {
    type Configurable = HbaseCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        resource: &Self::Configurable,
        role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = if role_name.is_empty() {
            BTreeMap::new()
        } else if let Some(config) = &resource.spec.config {
            config.compute_files(resource, "", file)?
        } else {
            BTreeMap::new()
        };

        match file {
            HBASE_ENV_SH => {
                result.insert(HBASE_MANAGES_ZK.to_string(), Some("false".to_string()));
                let mut all_hbase_opts = format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={METRICS_PORT}:/stackable/jmx/region-server.yaml");
                if let Some(hbase_opts) = &self.hbase_opts {
                    all_hbase_opts += " ";
                    all_hbase_opts += hbase_opts;
                }
                result.insert(HBASE_OPTS.to_string(), Some(all_hbase_opts));
            }
            HBASE_SITE_XML => {
                result.insert(
                    HBASE_CLUSTER_DISTRIBUTED.to_string(),
                    Some("true".to_string()),
                );
                result.insert(HBASE_ROOTDIR.to_string(), Some(resource.root_dir()));
            }
            _ => {}
        }

        result.retain(|_, maybe_value| maybe_value.is_some());

        Ok(result)
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HbaseClusterStatus {}

impl HbaseCluster {
    /// The name of the role-level load-balanced Kubernetes `Service`
    pub fn server_role_service_name(&self) -> Option<String> {
        self.metadata.name.clone()
    }

    /// Metadata about a server rolegroup
    pub fn server_rolegroup_ref(
        &self,
        role_name: impl Into<String>,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<HbaseCluster> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(self),
            role: role_name.into(),
            role_group: group_name.into(),
        }
    }

    pub fn get_role(&self, role: HbaseRole) -> Option<&Role<HbaseConfig>> {
        match role {
            HbaseRole::Master => self.spec.masters.as_ref(),
            HbaseRole::RegionServer => self.spec.region_servers.as_ref(),
            HbaseRole::RestServer => self.spec.rest_servers.as_ref(),
        }
    }

    pub fn root_dir(&self) -> String {
        self.spec
            .config
            .as_ref()
            .and_then(|c| c.hbase_rootdir.as_deref())
            .unwrap_or("/hbase")
            .to_string()
    }
}
