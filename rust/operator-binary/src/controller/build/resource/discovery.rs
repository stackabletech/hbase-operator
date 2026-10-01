//! Build the discovery `ConfigMap` for the HbaseCluster.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder, k8s_openapi::api::core::v1::ConfigMap,
    utils::cluster_info::KubernetesClusterInfo, v2::config_file_writer::to_hadoop_xml,
};

use crate::{
    controller::{
        ValidatedCluster,
        build::{
            kerberos, object_meta,
            properties::{ConfigFileName, hbase_site},
            recommended_labels_for_role_resources,
        },
    },
    crd::HbaseRole,
};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },
}

/// Creates a discovery config map containing the `hbase-site.xml` for clients.
pub fn build_discovery_config_map(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> Result<ConfigMap> {
    let cluster_config = &cluster.cluster_config;

    let mut hbase_site_config = cluster_config
        .zookeeper_connection_information
        .as_hbase_settings();
    hbase_site_config.extend(kerberos::discovery_kerberos_config(cluster, cluster_info));
    hbase_site_config.insert(
        hbase_site::HBASE_CLIENT_BOOTSTRAP_SERVERS.to_string(),
        hbase_site::client_bootstrap_servers(cluster, cluster_info),
    );

    ConfigMapBuilder::new()
        .metadata(
            // The discovery `ConfigMap` is a cluster-wide object (not tied to
            // a single role group), so it is named after the cluster and
            // labelled with the region-server role.
            object_meta(
                cluster,
                cluster.name.to_string(),
                recommended_labels_for_role_resources(cluster, &HbaseRole::RegionServer),
            )
            .build(),
        )
        .add_data(
            ConfigFileName::HbaseSite.to_string(),
            to_hadoop_xml(hbase_site_config.iter()),
        )
        .build()
        .context(BuildConfigMapSnafu)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        cluster_info, hbase_from_yaml, validated_cluster, validated_cluster_from,
    };

    #[test]
    fn discovery_config_map_keeps_kerberos_settings_next_to_client_bootstrap_servers() {
        let hbase = hbase_from_yaml(
            r#"
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
                authentication:
                  tlsSecretClass: tls
                  kerberos:
                    secretClass: kerberos-simple
              masters:
                roleGroups:
                  default:
                    replicas: 1
              regionServers:
                roleGroups:
                  default:
                    replicas: 1
              restServers:
                roleGroups:
                  default:
                    replicas: 1
            "#,
        );
        let config_map =
            build_discovery_config_map(&validated_cluster_from(&hbase), &cluster_info())
                .expect("discovery ConfigMap builds");
        let hbase_site = &config_map.data.expect("data is set")["hbase-site.xml"];
        assert!(
            hbase_site.contains("<name>hbase.security.authentication</name>"),
            "{hbase_site}"
        );
        assert!(
            hbase_site.contains("<name>hbase.client.bootstrap.servers</name>"),
            "{hbase_site}"
        );
    }

    #[test]
    fn discovery_config_map_contains_client_bootstrap_servers() {
        let config_map = build_discovery_config_map(&validated_cluster(), &cluster_info())
            .expect("discovery ConfigMap builds");
        let hbase_site = &config_map.data.expect("data is set")["hbase-site.xml"];
        assert!(
            hbase_site.contains("<name>hbase.client.bootstrap.servers</name>"),
            "{hbase_site}"
        );
        assert!(
            hbase_site.contains(
                "<value>hbase-master-default-0.hbase-master-default-headless.default.svc.cluster.local:16000</value>"
            ),
            "{hbase_site}"
        );
        // the existing ZooKeeper settings are still there
        assert!(
            hbase_site.contains("<name>hbase.zookeeper.quorum</name>"),
            "{hbase_site}"
        );
    }
}
