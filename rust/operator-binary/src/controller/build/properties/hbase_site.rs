//! Builds the `hbase-site.xml` config file: operator defaults, ZooKeeper wiring,
//! kerberos/OPA security config, role-specific bind settings, with user
//! `configOverrides` applied last.

use std::collections::BTreeMap;

use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

use crate::{
    controller::build::{opa::HbaseOpaConfig, properties::build_xml_config},
    crd::{
        AnyServiceConfig, HBASE_CLUSTER_DISTRIBUTED, HBASE_MASTER_PORT, HBASE_MASTER_UI_PORT,
        HBASE_REGIONSERVER_PORT, HBASE_REGIONSERVER_UI_PORT, HBASE_ROOTDIR, HbaseRole,
    },
};

// `hbase-site.xml` property keys (the `port` keys carry a `_KEY` suffix to avoid clashing with the
// `Port`-typed `HBASE_*_PORT` constants imported from the `crd` module).
const HBASE_CLIENT_RPC_BIND_ADDRESS: &str = "hbase.client.rpc.bind.address";
const HBASE_MASTER_IPC_ADDRESS: &str = "hbase.master.ipc.address";
const HBASE_MASTER_IPC_PORT: &str = "hbase.master.ipc.port";
const HBASE_MASTER_HOSTNAME: &str = "hbase.master.hostname";
const HBASE_MASTER_PORT_KEY: &str = "hbase.master.port";
const HBASE_MASTER_INFO_PORT: &str = "hbase.master.info.port";
const HBASE_MASTER_BOUND_INFO_PORT: &str = "hbase.master.bound.info.port";
const HBASE_REGIONSERVER_IPC_ADDRESS: &str = "hbase.regionserver.ipc.address";
const HBASE_REGIONSERVER_IPC_PORT: &str = "hbase.regionserver.ipc.port";
const HBASE_UNSAFE_REGIONSERVER_HOSTNAME: &str = "hbase.unsafe.regionserver.hostname";
const HBASE_REGIONSERVER_PORT_KEY: &str = "hbase.regionserver.port";
const HBASE_REGIONSERVER_INFO_PORT: &str = "hbase.regionserver.info.port";
const HBASE_REGIONSERVER_BOUND_INFO_PORT: &str = "hbase.regionserver.bound.info.port";
const HBASE_REST_ENDPOINT: &str = "hbase.rest.endpoint";

// `hbase-site.xml` property values that recur across roles. The `${env:...}` placeholders are
// resolved by HBase at runtime from the Pod's environment.
const BIND_ALL_ADDRESSES: &str = "0.0.0.0";
const ENV_HBASE_SERVICE_HOST: &str = "${env:HBASE_SERVICE_HOST}";
const ENV_HBASE_SERVICE_PORT: &str = "${env:HBASE_SERVICE_PORT}";
const ENV_HBASE_INFO_PORT: &str = "${env:HBASE_INFO_PORT}";

/// Renders `hbase-site.xml`.
pub fn build(
    role: &HbaseRole,
    merged_config: &AnyServiceConfig,
    zookeeper_config: BTreeMap<String, String>,
    kerberos_config: BTreeMap<String, String>,
    opa_config: Option<&HbaseOpaConfig>,
    overrides: KeyValueConfigOverrides,
) -> String {
    let mut config: BTreeMap<String, String> = BTreeMap::new();

    // Defaults
    config.insert(HBASE_CLUSTER_DISTRIBUTED.to_string(), "true".to_string());
    config.insert(HBASE_ROOTDIR.to_string(), merged_config.hbase_rootdir());

    config.extend(zookeeper_config);
    config.extend(kerberos_config);
    config.extend(opa_config.map_or(vec![], |config| config.hbase_site_config()));

    // Set flag to override default behaviour, which is that the
    // RPC client should bind the client address (forcing outgoing
    // RPC traffic to happen from the same network interface that
    // the RPC server is bound on).
    config.insert(
        HBASE_CLIENT_RPC_BIND_ADDRESS.to_string(),
        "false".to_string(),
    );

    match role {
        HbaseRole::Master => {
            config.insert(
                HBASE_MASTER_IPC_ADDRESS.to_string(),
                BIND_ALL_ADDRESSES.to_string(),
            );
            config.insert(
                HBASE_MASTER_IPC_PORT.to_string(),
                HBASE_MASTER_PORT.to_string(),
            );
            config.insert(
                HBASE_MASTER_HOSTNAME.to_string(),
                ENV_HBASE_SERVICE_HOST.to_string(),
            );
            config.insert(
                HBASE_MASTER_PORT_KEY.to_string(),
                ENV_HBASE_SERVICE_PORT.to_string(),
            );
            config.insert(
                HBASE_MASTER_INFO_PORT.to_string(),
                ENV_HBASE_INFO_PORT.to_string(),
            );
            config.insert(
                HBASE_MASTER_BOUND_INFO_PORT.to_string(),
                HBASE_MASTER_UI_PORT.to_string(),
            );
        }
        HbaseRole::RegionServer => {
            config.insert(
                HBASE_REGIONSERVER_IPC_ADDRESS.to_string(),
                BIND_ALL_ADDRESSES.to_string(),
            );
            config.insert(
                HBASE_REGIONSERVER_IPC_PORT.to_string(),
                HBASE_REGIONSERVER_PORT.to_string(),
            );
            config.insert(
                HBASE_UNSAFE_REGIONSERVER_HOSTNAME.to_string(),
                ENV_HBASE_SERVICE_HOST.to_string(),
            );
            config.insert(
                HBASE_REGIONSERVER_PORT_KEY.to_string(),
                ENV_HBASE_SERVICE_PORT.to_string(),
            );
            config.insert(
                HBASE_REGIONSERVER_INFO_PORT.to_string(),
                ENV_HBASE_INFO_PORT.to_string(),
            );
            config.insert(
                HBASE_REGIONSERVER_BOUND_INFO_PORT.to_string(),
                HBASE_REGIONSERVER_UI_PORT.to_string(),
            );
        }
        HbaseRole::RestServer => {
            config.insert(
                // N.B. a custom tag, so as not to interfere with HBase internals.
                // The other roles use a patch to correctly resolve host/port.
                HBASE_REST_ENDPOINT.to_string(),
                format!("{ENV_HBASE_SERVICE_HOST}:{ENV_HBASE_SERVICE_PORT}"),
            );
        }
    };

    // configOverride come last
    build_xml_config(config, overrides)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{merged_config, validated_cluster};

    #[test]
    fn renders_operator_defaults() {
        let validated_cluster = validated_cluster();
        let merged = merged_config(&validated_cluster, &HbaseRole::Master);
        let xml = build(
            &HbaseRole::Master,
            merged,
            BTreeMap::new(),
            BTreeMap::new(),
            None,
            KeyValueConfigOverrides::default(),
        );
        assert!(
            xml.contains("<name>hbase.cluster.distributed</name>\n    <value>true</value>"),
            "{xml}"
        );
        assert!(
            xml.contains("<name>hbase.master.ipc.address</name>\n    <value>0.0.0.0</value>"),
            "{xml}"
        );
    }

    #[test]
    fn renders_region_server_bind_settings() {
        let validated_cluster = validated_cluster();
        let merged = merged_config(&validated_cluster, &HbaseRole::RegionServer);
        let xml = build(
            &HbaseRole::RegionServer,
            merged,
            BTreeMap::new(),
            BTreeMap::new(),
            None,
            KeyValueConfigOverrides::default(),
        );
        assert!(
            xml.contains("<name>hbase.regionserver.ipc.address</name>\n    <value>0.0.0.0</value>"),
            "{xml}"
        );
        assert!(
            xml.contains(
                "<name>hbase.unsafe.regionserver.hostname</name>\n    <value>${env:HBASE_SERVICE_HOST}</value>"
            ),
            "{xml}"
        );
    }

    #[test]
    fn renders_rest_server_endpoint() {
        let validated_cluster = validated_cluster();
        let merged = merged_config(&validated_cluster, &HbaseRole::RestServer);
        let xml = build(
            &HbaseRole::RestServer,
            merged,
            BTreeMap::new(),
            BTreeMap::new(),
            None,
            KeyValueConfigOverrides::default(),
        );
        assert!(
            xml.contains(
                "<name>hbase.rest.endpoint</name>\n    <value>${env:HBASE_SERVICE_HOST}:${env:HBASE_SERVICE_PORT}</value>"
            ),
            "{xml}"
        );
    }

    #[test]
    fn user_override_wins() {
        let validated_cluster = validated_cluster();
        let merged = merged_config(&validated_cluster, &HbaseRole::Master);
        let xml = build(
            &HbaseRole::Master,
            merged,
            BTreeMap::new(),
            BTreeMap::new(),
            None,
            [("hbase.cluster.distributed", "false")].into(),
        );
        assert!(
            xml.contains("<name>hbase.cluster.distributed</name>\n    <value>false</value>"),
            "{xml}"
        );
    }
}
