//! Builds the `hbase-site.xml` config file: operator defaults, ZooKeeper wiring,
//! kerberos/OPA security config, role-specific bind settings, with user
//! `configOverrides` applied last.

use std::collections::BTreeMap;

use stackable_operator::{
    utils::cluster_info::KubernetesClusterInfo, v2::config_overrides::KeyValueConfigOverrides,
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{opa::HbaseOpaConfig, properties::build_xml_config},
    },
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

/// Bootstrap nodes for the RPC-based connection registry, the client default since HBase 3.0.0.
/// HBase 2.x defaults to the ZooKeeper-based registry and does not read this key.
pub const HBASE_CLIENT_BOOTSTRAP_SERVERS: &str = "hbase.client.bootstrap.servers";

/// Lists every master Pod as `<sts>-<n>.<headless>.<ns>.svc.<domain>:<port>`.
///
/// A role group's headless Service name is not sufficient: the client resolves each entry to a
/// single address and only fails over between entries, and the headless Service also publishes
/// not-ready Pods.
pub fn client_bootstrap_servers(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> String {
    let namespace = cluster.namespace.as_ref();
    let cluster_domain = &cluster_info.cluster_domain;
    cluster
        .role_group_configs
        .get(&HbaseRole::Master)
        .into_iter()
        .flatten()
        .flat_map(|(role_group_name, role_group)| {
            let resource_names =
                cluster.role_group_resource_names(&HbaseRole::Master, role_group_name);
            let stateful_set = resource_names.stateful_set_name();
            let headless_service = resource_names.headless_service_name();
            // `None` leaves `replicas` unset on the StatefulSet, which Kubernetes defaults to 1
            (0..role_group.replicas.unwrap_or(1)).map(move |ordinal| {
                format!(
                    "{stateful_set}-{ordinal}.{headless_service}.{namespace}.svc.{cluster_domain}:{HBASE_MASTER_PORT}"
                )
            })
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Renders `hbase-site.xml`.
pub fn build(
    role: &HbaseRole,
    merged_config: &AnyServiceConfig,
    zookeeper_config: BTreeMap<String, String>,
    kerberos_config: BTreeMap<String, String>,
    opa_config: Option<&HbaseOpaConfig>,
    client_bootstrap_servers: String,
    overrides: KeyValueConfigOverrides,
) -> String {
    let mut config: BTreeMap<String, String> = BTreeMap::new();

    // Defaults
    config.insert(HBASE_CLUSTER_DISTRIBUTED.to_string(), "true".to_string());
    config.insert(HBASE_ROOTDIR.to_string(), merged_config.hbase_rootdir());
    config.insert(
        HBASE_CLIENT_BOOTSTRAP_SERVERS.to_string(),
        client_bootstrap_servers,
    );

    config.extend(zookeeper_config);
    config.extend(kerberos_config);
    if let Some(opa_config) = opa_config {
        config.extend(opa_config.hbase_site_config());
    }

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
    use indoc::indoc;

    use super::*;
    use crate::test_utils::{
        cluster_info, hbase_from_yaml, merged_config, validated_cluster, validated_cluster_from,
    };

    fn bootstrap_servers_for(masters_yaml: &str) -> String {
        let hbase = hbase_from_yaml(&format!(
            r#"
---
apiVersion: hbase.stackable.tech/v1alpha1
kind: HbaseCluster
metadata:
  name: hbase
  namespace: default
  uid: c2c8c5c0-0b5a-4b1e-9f3e-1a2b3c4d5e6f
spec:
  image:
    productVersion: 2.6.3
  clusterConfig:
    hdfsConfigMapName: simple-hdfs
    zookeeperConfigMapName: simple-znode
  masters:
    roleGroups:
{masters_yaml}
  regionServers:
    roleGroups:
      default:
        replicas: 1
  restServers:
    roleGroups:
      default:
        replicas: 1
"#
        ));
        client_bootstrap_servers(&validated_cluster_from(&hbase), &cluster_info())
    }

    // The role-group lines are spliced under `roleGroups:` and must be 6 spaces deep, so they are
    // written with explicit indentation (`indoc!` would strip it).
    #[test]
    fn bootstrap_servers_list_every_master_pod() {
        let servers = bootstrap_servers_for(
            "      default:\n        replicas: 2\n      other:\n        replicas: 1",
        );
        assert_eq!(
            servers,
            "hbase-master-default-0.hbase-master-default-headless.default.svc.cluster.local:16000,\
             hbase-master-default-1.hbase-master-default-headless.default.svc.cluster.local:16000,\
             hbase-master-other-0.hbase-master-other-headless.default.svc.cluster.local:16000"
        );
    }

    #[test]
    fn bootstrap_servers_default_to_one_replica() {
        let servers = bootstrap_servers_for("      default: {}");
        assert_eq!(
            servers,
            "hbase-master-default-0.hbase-master-default-headless.default.svc.cluster.local:16000"
        );
    }

    #[test]
    fn bootstrap_servers_skip_scaled_down_role_groups() {
        let servers = bootstrap_servers_for("      default:\n        replicas: 0");
        assert_eq!(servers, "");
    }

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
            String::new(),
            KeyValueConfigOverrides::default(),
        );
        assert!(
            xml.contains(indoc! {"
                <name>hbase.cluster.distributed</name>
                    <value>true</value>"}),
            "{xml}"
        );
        assert!(
            xml.contains(indoc! {"
                <name>hbase.master.ipc.address</name>
                    <value>0.0.0.0</value>"}),
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
            String::new(),
            KeyValueConfigOverrides::default(),
        );
        assert!(
            xml.contains(indoc! {"
                <name>hbase.regionserver.ipc.address</name>
                    <value>0.0.0.0</value>"}),
            "{xml}"
        );
        assert!(
            xml.contains(indoc! {"
                <name>hbase.unsafe.regionserver.hostname</name>
                    <value>${env:HBASE_SERVICE_HOST}</value>"}),
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
            String::new(),
            KeyValueConfigOverrides::default(),
        );
        assert!(
            xml.contains(indoc! {"
                <name>hbase.rest.endpoint</name>
                    <value>${env:HBASE_SERVICE_HOST}:${env:HBASE_SERVICE_PORT}</value>"}),
            "{xml}"
        );
    }

    #[test]
    fn renders_client_bootstrap_servers() {
        let validated_cluster = validated_cluster();
        let merged = merged_config(&validated_cluster, &HbaseRole::RestServer);
        let xml = build(
            &HbaseRole::RestServer,
            merged,
            BTreeMap::new(),
            BTreeMap::new(),
            None,
            "m-0.m-headless.ns.svc.cluster.local:16000".to_string(),
            KeyValueConfigOverrides::default(),
        );
        assert!(
            xml.contains(indoc! {"
                <name>hbase.client.bootstrap.servers</name>
                    <value>m-0.m-headless.ns.svc.cluster.local:16000</value>"}),
            "{xml}"
        );
    }

    #[test]
    fn user_override_wins_for_client_bootstrap_servers() {
        let validated_cluster = validated_cluster();
        let merged = merged_config(&validated_cluster, &HbaseRole::RestServer);
        let xml = build(
            &HbaseRole::RestServer,
            merged,
            BTreeMap::new(),
            BTreeMap::new(),
            None,
            "m-0.m-headless.ns.svc.cluster.local:16000".to_string(),
            [("hbase.client.bootstrap.servers", "custom:16000")].into(),
        );
        assert!(
            xml.contains(indoc! {"
                <name>hbase.client.bootstrap.servers</name>
                    <value>custom:16000</value>"}),
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
            String::new(),
            [("hbase.cluster.distributed", "false")].into(),
        );
        assert!(
            xml.contains(indoc! {"
                <name>hbase.cluster.distributed</name>
                    <value>false</value>"}),
            "{xml}"
        );
    }
}
