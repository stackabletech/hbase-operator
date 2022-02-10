use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kube::CustomResource;
use stackable_operator::product_config_utils::{ConfigError, Configuration};
use stackable_operator::role_utils::{Role, RoleGroupRef};
use stackable_operator::schemars::{self, JsonSchema};
use strum_macros::Display;
use strum_macros::EnumIter;

pub const APP_NAME: &str = "hbase";

pub const HBASE_ENV_SH: &str = "hbase-env.sh";
pub const HBASE_SITE_XML: &str = "hbase-site.xml";
pub const HDFS_SITE_XML: &str = "hdfs-site.xml";

pub const HBASE_MANAGES_ZK: &str = "HBASE_MANAGES_ZK";
pub const HBASE_OPTS: &str = "HBASE_OPTS";

pub const HBASE_CLUSTER_DISTRIBUTED: &str = "hbase.cluster.distributed";
pub const HBASE_ROOTDIR: &str = "hbase.rootdir";
pub const HBASE_ZOOKEEPER_QUORUM: &str = "hbase.zookeeper.quorum";
pub const HDFS_CONFIG: &str = "content";

pub const METRICS_PORT_NAME: &str = "metrics";
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<HbaseConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub masters: Option<Role<HbaseConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region_servers: Option<Role<HbaseConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rest_servers: Option<Role<HbaseConfig>>,
}

impl HbaseCluster {
    pub fn get_role(&self, role: HbaseRole) -> Option<&Role<HbaseConfig>> {
        match role {
            HbaseRole::Master => self.spec.masters.as_ref(),
            HbaseRole::RegionServer => self.spec.region_servers.as_ref(),
            HbaseRole::RestServer => self.spec.rest_servers.as_ref(),
        }
    }
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

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HbaseConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zookeeper_config_map_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hdfs_config_map_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hbase_cluster_distributed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hbase_rootdir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hbase_zookeeper_quorum: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hbase_manages_zk: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hbase_opts: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hdfs_config: Option<String>,
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
                if let Some(hbase_manages_zk) = self.hbase_manages_zk {
                    result.insert(
                        HBASE_MANAGES_ZK.to_string(),
                        Some(hbase_manages_zk.to_string()),
                    );
                }
                let mut all_hbase_opts = format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={METRICS_PORT}:/stackable/jmx/region-server.yaml");
                if let Some(hbase_opts) = &self.hbase_opts {
                    all_hbase_opts += " ";
                    all_hbase_opts += hbase_opts;
                }
                result.insert(HBASE_OPTS.to_string(), Some(all_hbase_opts));
            }
            HBASE_SITE_XML => {
                if let Some(hbase_cluster_distributed) = self.hbase_cluster_distributed {
                    result.insert(
                        HBASE_CLUSTER_DISTRIBUTED.to_string(),
                        Some(hbase_cluster_distributed.to_string()),
                    );
                }
                if let Some(hbase_rootdir) = &self.hbase_rootdir {
                    result.insert(HBASE_ROOTDIR.to_string(), Some(hbase_rootdir.to_owned()));
                }
                if let Some(hbase_zookeeper_quorum) = &self.hbase_zookeeper_quorum {
                    result.insert(
                        HBASE_ZOOKEEPER_QUORUM.to_string(),
                        Some(hbase_zookeeper_quorum.to_owned()),
                    );
                }
            }
            HDFS_SITE_XML => {
                if let Some(hdfs_config) = &self.hdfs_config {
                    result.insert(HDFS_CONFIG.to_string(), Some(hdfs_config.to_owned()));
                }
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
}
