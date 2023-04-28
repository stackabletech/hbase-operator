use std::{collections::BTreeMap, num::ParseIntError};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hbase_crd::HbaseCluster;
use stackable_operator::{
    client::Client, k8s_openapi::api::core::v1::ConfigMap, kube::ResourceExt,
};
use strum::{EnumDiscriminants, IntoStaticStr};

const ZOOKEEPER_DISCOVERY_CM_HOSTS_ENTRY: &str = "ZOOKEEPER_HOSTS";
const ZOOKEEPER_DISCOVERY_CM_CHROOT_ENTRY: &str = "ZOOKEEPER_CHROOT";
const ZOOKEEPER_DISCOVERY_CM_CLIENT_PORT_ENTRY: &str = "ZOOKEEPER_CLIENT_PORT";

const HBASE_ZOOKEEPER_QUORUM: &str = "hbase.zookeeper.quorum";
const HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT: &str = "hbase.zookeeper.property.clientPort";
const ZOOKEEPER_ZNODE_PARENT: &str = "zookeeper.znode.parent";

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object defines no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("failed to retrieve ConfigMap {cm_name}"))]
    MissingConfigMap {
        source: stackable_operator::error::Error,
        cm_name: String,
    },
    #[snafu(display("failed to retrieve the entry {entry} for ConfigMap {cm_name}"))]
    MissingConfigMapEntry { cm_name: String, entry: String },
    #[snafu(display("failed to parse the zookeeper port from ConfigMap {cm_name} entry {entry}"))]
    ParseZookeeperPort {
        source: ParseIntError,
        cm_name: String,
        entry: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ZookeeperConnectionInformation {
    pub hosts: String,
    pub chroot: String,
    pub port: u16,
}

impl ZookeeperConnectionInformation {
    pub fn as_hbase_settings(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (HBASE_ZOOKEEPER_QUORUM.to_string(), self.hosts.clone()),
            (
                HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT.to_string(),
                self.port.to_string(),
            ),
            (ZOOKEEPER_ZNODE_PARENT.to_string(), self.chroot.clone()),
        ])
    }
}

pub async fn retrieve_zookeeper_connection_information(
    hbase: &HbaseCluster,
    client: &Client,
) -> Result<ZookeeperConnectionInformation> {
    let zk_discovery_cm_name = &hbase.spec.cluster_config.zookeeper_config_map_name;
    let mut zk_discovery_cm = client
        .get::<ConfigMap>(
            zk_discovery_cm_name,
            hbase
                .namespace()
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?,
        )
        .await
        .context(MissingConfigMapSnafu {
            cm_name: zk_discovery_cm_name.to_string(),
        })?;

    let hosts = zk_discovery_cm
        .data
        .as_mut()
        .and_then(|data| data.remove(ZOOKEEPER_DISCOVERY_CM_HOSTS_ENTRY))
        .context(MissingConfigMapEntrySnafu {
            cm_name: zk_discovery_cm_name.as_str(),
            entry: ZOOKEEPER_DISCOVERY_CM_HOSTS_ENTRY,
        })?;
    let chroot = zk_discovery_cm
        .data
        .as_mut()
        .and_then(|data| data.remove(ZOOKEEPER_DISCOVERY_CM_CHROOT_ENTRY))
        .context(MissingConfigMapEntrySnafu {
            cm_name: zk_discovery_cm_name.as_str(),
            entry: ZOOKEEPER_DISCOVERY_CM_CHROOT_ENTRY,
        })?;
    let port = zk_discovery_cm
        .data
        .as_mut()
        .and_then(|data| data.remove(ZOOKEEPER_DISCOVERY_CM_CLIENT_PORT_ENTRY))
        .context(MissingConfigMapEntrySnafu {
            cm_name: zk_discovery_cm_name.as_str(),
            entry: ZOOKEEPER_DISCOVERY_CM_CLIENT_PORT_ENTRY,
        })?
        .parse()
        .context(ParseZookeeperPortSnafu {
            cm_name: zk_discovery_cm_name.as_str(),
            entry: ZOOKEEPER_DISCOVERY_CM_CLIENT_PORT_ENTRY,
        })?;

    Ok(ZookeeperConnectionInformation {
        hosts,
        chroot,
        port,
    })
}
