use std::{collections::BTreeMap, num::ParseIntError};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hbase_crd::HbaseCluster;
use stackable_operator::{
    client::Client, k8s_openapi::api::core::v1::ConfigMap, kube::ResourceExt,
};
use strum::{EnumDiscriminants, IntoStaticStr};
use tracing::warn;

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

/// Contains the information as exposed by the Zookeeper/Znode discovery CM (should work with both)
pub struct ZookeeperConnectionInformation {
    /// E.g. `simple-zk-server-default-0.simple-zk-server-default.default.svc.cluster.local:2282,simple-zk-server-default-1.simple-zk-server-default.default.svc.cluster.local:2282,simple-zk-server-default-2.simple-zk-server-default.default.svc.cluster.local:2282`
    hosts: String,
    /// E.g. `/znode-123` in case of ZNode discovery CM or `/` in case of Zookeeper discovery CM directly.
    chroot: String,
    /// E.g. 2282 for tls secured Zookeeper
    port: u16,
}

impl ZookeeperConnectionInformation {
    pub async fn retrieve(hbase: &HbaseCluster, client: &Client) -> Result<Self> {
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
        let mut chroot = zk_discovery_cm
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

        // IMPORTANT!
        // Before https://github.com/stackabletech/hbase-operator/issues/354, hbase automatically added a `/hbase` suffix, ending up with a chroot of e.g. `/znode-fe51edff-8df9-43a8-ac5f-4781b071ae5f/hbase`.
        // Because the chroot we read from the ZNode discovery CM is only `/znode-fe51edff-8df9-43a8-ac5f-4781b071ae5f`, we need to prepend the `/hbase` suffix ourselves.
        // If we don't do so, hbase clusters created before #354 would need to be migrated to the different znode path!

        // Check if a user points to a discovery CM of a HBaseCluster rather than a ZNode.
        if chroot == "/" {
            warn!("It is recommended to let the HBase cluster point to a discovery ConfigMap of a ZNode rater than a ZookeeperCluster. \
            This prevents accidental reuse of the same Zookeeper path for multiple product instances. \
            See https://docs.stackable.tech/home/stable/zookeeper/getting_started/first_steps for details");
            chroot = "/hbase".to_string();
        } else {
            chroot = format!("{chroot}/hbase");
        }

        Ok(Self {
            hosts,
            chroot,
            port,
        })
    }

    pub fn as_hbase_settings(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            // We use ZOOKEEPER_HOSTS (host1:port1,host2:port2) instead of ZOOKEEPER (host1:port1,host2:port2/znode-123) here, because HBase cannot deal with a chroot properly.
            // It is - in theory - a valid ZK connection string but HBase does its own parsing (last checked in HBase 2.5.3) which does not understand chroots properly.
            // It worked for us because we also provide a ZK port and that works but if the port is ever left out the parsing would break.
            // See https://github.com/stackabletech/hbase-operator/issues/354 for details
            (HBASE_ZOOKEEPER_QUORUM.to_string(), self.hosts.clone()),
            // As mentioned it's a good idea to pass this explicitly as well.
            // We had some cases where hbase tried to connect to zookeeper using the wrong (default) port.
            (
                HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT.to_string(),
                self.port.to_string(),
            ),
            // As we only pass in the hosts (and not the znode) in the zookeeper quorum, we need to specify the znode path
            (ZOOKEEPER_ZNODE_PARENT.to_string(), self.chroot.clone()),
        ])
    }
}
