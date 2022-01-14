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
pub const MANAGED_BY: &str = "hbase-operator";

pub const HBASE_SITE_XML: &str = "hbase-site.xml";
pub const CORE_SITE_XML: &str = "core-site.xml";

pub const FS_DEFAULT_FS: &str = "fs.DefaultFS";
pub const HBASE_ROOT_DIR: &str = "hbase.rootdir";
pub const HBASE_ZOOKEEPER_QUORUM: &str = "hbase.zookeeper.quorum";
pub const HBASE_CLUSTER_DISTRIBUTED: &str = "hbase.cluster.distributed";

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
    pub masters: Option<Role<HbaseConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region_servers: Option<Role<HbaseConfig>>,
    pub zookeeper_config_map_name: String,
    pub hdfs_config_map_name: String,
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
}

// TODO Why is Default necessary?
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HbaseConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hbase_rootdir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hbase_cluster_distributed: Option<bool>,
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
        _resource: &Self::Configurable,
        _role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        if file == HBASE_SITE_XML {
            if let Some(hbase_rootdir) = &self.hbase_rootdir {
                result.insert(HBASE_ROOT_DIR.to_string(), Some(hbase_rootdir.to_owned()));
            }
            if let Some(hbase_cluster_distributed) = &self.hbase_cluster_distributed {
                result.insert(
                    HBASE_CLUSTER_DISTRIBUTED.to_string(),
                    Some(hbase_cluster_distributed.to_string()),
                );
            }
        }

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

    /// The fully-qualified domain name of the role-level load-balanced Kubernetes `Service`
    pub fn server_role_service_fqdn(&self) -> Option<String> {
        Some(format!(
            "{}.{}.svc.cluster.local",
            self.server_role_service_name()?,
            self.metadata.namespace.as_ref()?
        ))
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

/// A reference to a [`HbaseCluster`]
#[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HbaseClusterRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}
