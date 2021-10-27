pub mod commands;
pub mod error;

use crate::commands::{Restart, Start, Stop};

use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::json;
use stackable_hdfs_crd::discovery::HdfsReference;
use stackable_operator::command::{CommandRef, HasCommands, HasRoleRestartOrder};
use stackable_operator::controller::HasOwned;
use stackable_operator::crd::HasApplication;
use stackable_operator::identity::PodToNodeMapping;
use stackable_operator::k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use stackable_operator::k8s_openapi::schemars::_serde_json::Value;
use stackable_operator::kube::api::ApiResource;
use stackable_operator::kube::CustomResource;
use stackable_operator::kube::CustomResourceExt;
use stackable_operator::product_config_utils::{ConfigError, Configuration};
use stackable_operator::role_utils::Role;
use stackable_operator::schemars::{self, JsonSchema};
use stackable_operator::status::{
    ClusterExecutionStatus, Conditions, HasClusterExecutionStatus, HasCurrentCommand, Status,
    Versioned,
};
use stackable_operator::versioning::{ProductVersion, Versioning, VersioningState};
use stackable_zookeeper_crd::discovery::ZookeeperReference;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use strum_macros::{Display, EnumIter, EnumString};

pub const APP_NAME: &str = "hbase";
pub const MANAGED_BY: &str = "hbase-operator";

pub const CONFIG_MAP_TYPE_DATA: &str = "data";

pub const FS_DEFAULT_FS: &str = "fs.DefaultFS";
pub const HBASE_ROOT_DIR: &str = "hbase.rootdir";
pub const HBASE_ZOOKEEPER_QUORUM: &str = "hbase.zookeeper.quorum";
pub const HBASE_CLUSTER_DISTRIBUTED: &str = "hbase.cluster.distributed";

pub const HBASE_MASTER_PORT: &str = "hbase.master.port";
pub const HBASE_MASTER_WEB_UI_PORT: &str = "hbase.master.info.port";

pub const HBASE_REGION_SERVER_PORT: &str = "hbase.regionserver.port";
pub const HBASE_REGION_SERVER_WEB_UI_PORT: &str = "hbase.regionserver.info.port";

pub const JAVA_HOME: &str = "JAVA_HOME";
pub const METRICS_PORT: &str = "metricsPort";

pub const HBASE_SITE_XML: &str = "hbase-site.xml";
pub const CORE_SITE_XML: &str = "core-site.xml";

pub const RPC_PORT: &str = "rpc";
pub const HTTP_PORT: &str = "http";

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "hbase.stackable.tech",
    version = "v1alpha1",
    kind = "HbaseCluster",
    plural = "hbaseclusters",
    shortname = "hbase",
    namespaced,
    kube_core = "stackable_operator::kube::core",
    k8s_openapi = "stackable_operator::k8s_openapi",
    schemars = "stackable_operator::schemars"
)]
#[kube(status = "HbaseClusterStatus")]
#[serde(rename_all = "camelCase")]
pub struct HbaseClusterSpec {
    pub version: HbaseVersion,
    pub masters: Role<HbaseConfig>,
    pub region_servers: Role<HbaseConfig>,
    pub zookeeper_reference: ZookeeperReference,
    pub hdfs_reference: HdfsReference,
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    Hash,
    JsonSchema,
    PartialEq,
    Serialize,
    EnumString,
)]
pub enum HbaseRole {
    #[strum(serialize = "master")]
    Master,
    #[strum(serialize = "regionserver")]
    RegionServer,
}

impl HbaseRole {
    pub fn command(&self, version: &HbaseVersion) -> String {
        // TODO: test and fix command
        format!(
            "{}/bin/hbase {} start",
            version.package_name(),
            self.to_string()
        )
    }
}

impl Status<HbaseClusterStatus> for HbaseCluster {
    fn status(&self) -> &Option<HbaseClusterStatus> {
        &self.status
    }
    fn status_mut(&mut self) -> &mut Option<HbaseClusterStatus> {
        &mut self.status
    }
}

impl HasRoleRestartOrder for HbaseCluster {
    fn get_role_restart_order() -> Vec<String> {
        vec![
            HbaseRole::Master.to_string(),
            HbaseRole::RegionServer.to_string(),
        ]
    }
}

impl HasCommands for HbaseCluster {
    fn get_command_types() -> Vec<ApiResource> {
        vec![
            Start::api_resource(),
            Stop::api_resource(),
            Restart::api_resource(),
        ]
    }
}

impl HasOwned for HbaseCluster {
    fn owned_objects() -> Vec<&'static str> {
        vec![Restart::crd_name(), Start::crd_name(), Stop::crd_name()]
    }
}

impl HasApplication for HbaseCluster {
    fn get_application_name() -> &'static str {
        APP_NAME
    }
}

impl HasClusterExecutionStatus for HbaseCluster {
    fn cluster_execution_status(&self) -> Option<ClusterExecutionStatus> {
        self.status
            .as_ref()
            .and_then(|status| status.cluster_execution_status.clone())
    }

    fn cluster_execution_status_patch(&self, execution_status: &ClusterExecutionStatus) -> Value {
        json!({ "clusterExecutionStatus": execution_status })
    }
}

// TODO: These all should be "Property" Enums that can be either simple or complex where complex allows forcing/ignoring errors and/or warnings
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HbaseConfig {
    pub master_port: Option<u16>,
    // master_web_ui_port can be set to -1 to disable the ui
    pub master_web_ui_port: Option<i16>,
    pub region_server_port: Option<u16>,
    // region_server_web_ui_port can be set to -1 to disable the ui
    pub region_server_web_ui_port: Option<i16>,
    pub metrics_port: Option<u16>,
    pub java_home: Option<String>,
}

impl Configuration for HbaseConfig {
    type Configurable = HbaseCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        if let Some(java_home) = &self.java_home {
            result.insert(JAVA_HOME.to_string(), Some(java_home.to_string()));
        }

        if let Some(metrics_port) = &self.metrics_port {
            result.insert(METRICS_PORT.to_string(), Some(metrics_port.to_string()));
        }

        Ok(result)
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
        role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();

        if file == HBASE_SITE_XML {
            if role_name == HbaseRole::Master.to_string() {
                if let Some(master_port) = &self.master_port {
                    result.insert(HBASE_MASTER_PORT.to_string(), Some(master_port.to_string()));
                }
                if let Some(master_web_ui_port) = &self.master_web_ui_port {
                    result.insert(
                        HBASE_MASTER_WEB_UI_PORT.to_string(),
                        Some(master_web_ui_port.to_string()),
                    );
                }
            } else if role_name == HbaseRole::RegionServer.to_string() {
                if let Some(region_server_port) = &self.region_server_port {
                    result.insert(
                        HBASE_REGION_SERVER_PORT.to_string(),
                        Some(region_server_port.to_string()),
                    );
                }
                if let Some(region_server_web_ui_port) = &self.region_server_web_ui_port {
                    result.insert(
                        HBASE_REGION_SERVER_WEB_UI_PORT.to_string(),
                        Some(region_server_web_ui_port.to_string()),
                    );
                }
            }
        }

        Ok(result)
    }
}

#[allow(non_camel_case_types)]
#[derive(
    Clone,
    Debug,
    Deserialize,
    Eq,
    JsonSchema,
    PartialEq,
    Serialize,
    strum_macros::Display,
    strum_macros::EnumString,
)]
pub enum HbaseVersion {
    #[serde(rename = "2.4.6")]
    #[strum(serialize = "2.4.6")]
    v2_4_6,

    // TODO: for now we only support the 2.4.6 version (remove the skip if that changes)
    #[serde(skip)]
    #[serde(rename = "2.3.6")]
    #[strum(serialize = "2.3.6")]
    v2_3_6,
}

impl HbaseVersion {
    pub fn package_name(&self) -> String {
        format!("hbase-{}", self.to_string())
    }
}

impl Versioning for HbaseVersion {
    fn versioning_state(&self, other: &Self) -> VersioningState {
        let from_version = match Version::parse(&self.to_string()) {
            Ok(v) => v,
            Err(e) => {
                return VersioningState::Invalid(format!(
                    "Could not parse [{}] to SemVer: {}",
                    self.to_string(),
                    e.to_string()
                ))
            }
        };

        let to_version = match Version::parse(&other.to_string()) {
            Ok(v) => v,
            Err(e) => {
                return VersioningState::Invalid(format!(
                    "Could not parse [{}] to SemVer: {}",
                    other.to_string(),
                    e.to_string()
                ))
            }
        };

        match to_version.cmp(&from_version) {
            Ordering::Greater => VersioningState::ValidUpgrade,
            Ordering::Less => VersioningState::ValidDowngrade,
            Ordering::Equal => VersioningState::NoOp,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HbaseClusterStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<ProductVersion<HbaseVersion>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<PodToNodeMapping>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_command: Option<CommandRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_execution_status: Option<ClusterExecutionStatus>,
}

impl Versioned<HbaseVersion> for HbaseClusterStatus {
    fn version(&self) -> &Option<ProductVersion<HbaseVersion>> {
        &self.version
    }
    fn version_mut(&mut self) -> &mut Option<ProductVersion<HbaseVersion>> {
        &mut self.version
    }
}

impl Conditions for HbaseClusterStatus {
    fn conditions(&self) -> &[Condition] {
        self.conditions.as_slice()
    }
    fn conditions_mut(&mut self) -> &mut Vec<Condition> {
        &mut self.conditions
    }
}

impl HasCurrentCommand for HbaseClusterStatus {
    fn current_command(&self) -> Option<CommandRef> {
        self.current_command.clone()
    }
    fn set_current_command(&mut self, command: CommandRef) {
        self.current_command = Some(command);
    }
    fn clear_current_command(&mut self) {
        self.current_command = None
    }
    fn tracking_location() -> &'static str {
        "/status/currentCommand"
    }
}

#[cfg(test)]
mod tests {
    use crate::HbaseVersion;
    use stackable_operator::versioning::{Versioning, VersioningState};
    use std::str::FromStr;

    #[test]
    fn test_hbase_version_versioning() {
        assert_eq!(
            HbaseVersion::v2_3_6.versioning_state(&HbaseVersion::v2_4_6),
            VersioningState::ValidUpgrade
        );
        assert_eq!(
            HbaseVersion::v2_4_6.versioning_state(&HbaseVersion::v2_3_6),
            VersioningState::ValidDowngrade
        );
        assert_eq!(
            HbaseVersion::v2_4_6.versioning_state(&HbaseVersion::v2_4_6),
            VersioningState::NoOp
        );
    }

    #[test]
    #[test]
    fn test_version_conversion() {
        HbaseVersion::from_str("2.3.6").unwrap();
        HbaseVersion::from_str("2.4.6").unwrap();
    }

    #[test]
    fn test_package_name() {
        assert_eq!(
            HbaseVersion::v2_4_6.package_name(),
            format!("hbase-{}", HbaseVersion::v2_4_6.to_string())
        );
        assert_eq!(
            HbaseVersion::v2_3_6.package_name(),
            format!("hbase-{}", HbaseVersion::v2_3_6.to_string())
        );
    }
}
