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
        "hbase.client.rpc.bind.address".to_string(),
        "false".to_string(),
    );

    match role {
        HbaseRole::Master => {
            config.insert(
                "hbase.master.ipc.address".to_string(),
                "0.0.0.0".to_string(),
            );
            config.insert(
                "hbase.master.ipc.port".to_string(),
                HBASE_MASTER_PORT.to_string(),
            );
            config.insert(
                "hbase.master.hostname".to_string(),
                "${env:HBASE_SERVICE_HOST}".to_string(),
            );
            config.insert(
                "hbase.master.port".to_string(),
                "${env:HBASE_SERVICE_PORT}".to_string(),
            );
            config.insert(
                "hbase.master.info.port".to_string(),
                "${env:HBASE_INFO_PORT}".to_string(),
            );
            config.insert(
                "hbase.master.bound.info.port".to_string(),
                HBASE_MASTER_UI_PORT.to_string(),
            );
        }
        HbaseRole::RegionServer => {
            config.insert(
                "hbase.regionserver.ipc.address".to_string(),
                "0.0.0.0".to_string(),
            );
            config.insert(
                "hbase.regionserver.ipc.port".to_string(),
                HBASE_REGIONSERVER_PORT.to_string(),
            );
            config.insert(
                "hbase.unsafe.regionserver.hostname".to_string(),
                "${env:HBASE_SERVICE_HOST}".to_string(),
            );
            config.insert(
                "hbase.regionserver.port".to_string(),
                "${env:HBASE_SERVICE_PORT}".to_string(),
            );
            config.insert(
                "hbase.regionserver.info.port".to_string(),
                "${env:HBASE_INFO_PORT}".to_string(),
            );
            config.insert(
                "hbase.regionserver.bound.info.port".to_string(),
                HBASE_REGIONSERVER_UI_PORT.to_string(),
            );
        }
        HbaseRole::RestServer => {
            config.insert(
                // N.B. a custom tag, so as not to interfere with HBase internals.
                // The other roles use a patch to correctly resolve host/port.
                "hbase.rest.endpoint".to_string(),
                "${env:HBASE_SERVICE_HOST}:${env:HBASE_SERVICE_PORT}".to_string(),
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
