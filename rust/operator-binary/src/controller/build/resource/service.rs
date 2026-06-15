//! Build the per-rolegroup `Service`s for the HbaseCluster.

use stackable_operator::{
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    kvp::{Annotations, Labels},
};

use crate::{
    controller::{RoleGroupName, ValidatedCluster},
    crd::{HbaseRole, v1alpha1},
};

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a
/// certain rolegroup.
///
/// This is mostly useful for internal communication between peers, or for clients that perform
/// client-side load balancing.
pub fn build_rolegroup_service(
    hbase: &v1alpha1::HbaseCluster,
    cluster: &ValidatedCluster,
    hbase_role: &HbaseRole,
    role_group_name: &RoleGroupName,
) -> Service {
    let ports = hbase_role
        .ports(hbase)
        .into_iter()
        .map(|(name, value)| ServicePort {
            name: Some(name),
            port: i32::from(value),
            protocol: Some("TCP".to_string()),
            ..ServicePort::default()
        })
        .collect();

    Service {
        metadata: cluster
            .object_meta(
                cluster
                    .resource_names(hbase_role, role_group_name)
                    .headless_service_name()
                    .to_string(),
                hbase_role,
                role_group_name,
            )
            .build(),
        spec: Some(ServiceSpec {
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            ports: Some(ports),
            selector: Some(
                cluster
                    .role_group_selector(hbase_role, role_group_name)
                    .into(),
            ),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

/// The rolegroup metrics [`Service`] is a service that exposes metrics and a prometheus scraping
/// label.
pub fn build_rolegroup_metrics_service(
    hbase: &v1alpha1::HbaseCluster,
    cluster: &ValidatedCluster,
    hbase_role: &HbaseRole,
    role_group_name: &RoleGroupName,
) -> Service {
    let ports = vec![ServicePort {
        name: Some(HbaseRole::metrics_port_name().to_owned()),
        port: i32::from(hbase_role.metrics_port()),
        protocol: Some("TCP".to_owned()),
        ..ServicePort::default()
    }];

    Service {
        metadata: cluster
            .object_meta(
                cluster
                    .resource_names(hbase_role, role_group_name)
                    .metrics_service_name()
                    .to_string(),
                hbase_role,
                role_group_name,
            )
            .with_labels(prometheus_labels())
            .with_annotations(prometheus_annotations(hbase, hbase_role))
            .build(),
        spec: Some(ServiceSpec {
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_owned()),
            cluster_ip: Some("None".to_owned()),
            ports: Some(ports),
            selector: Some(
                cluster
                    .role_group_selector(hbase_role, role_group_name)
                    .into(),
            ),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

/// Common labels for Prometheus.
fn prometheus_labels() -> Labels {
    Labels::try_from([("prometheus.io/scrape", "true")]).expect("should be a valid label")
}

/// Common annotations for Prometheus.
///
/// These annotations can be used in a ServiceMonitor.
///
/// see also <https://github.com/prometheus-community/helm-charts/blob/prometheus-27.32.0/charts/prometheus/values.yaml#L983-L1036>
fn prometheus_annotations(hbase: &v1alpha1::HbaseCluster, hbase_role: &HbaseRole) -> Annotations {
    Annotations::try_from([
        ("prometheus.io/path".to_owned(), "/prometheus".to_owned()),
        (
            "prometheus.io/port".to_owned(),
            hbase_role.metrics_port().to_string(),
        ),
        (
            "prometheus.io/scheme".to_owned(),
            if hbase.has_https_enabled() {
                "https".to_owned()
            } else {
                "http".to_owned()
            },
        ),
        ("prometheus.io/scrape".to_owned(), "true".to_owned()),
    ])
    .expect("should be valid annotations")
}

#[cfg(test)]
mod test {
    use rstest::rstest;

    use super::*;
    use crate::test_utils;

    #[rstest]
    #[case("2.6.3", HbaseRole::Master, vec!["master", "ui-http"])]
    #[case("2.6.3", HbaseRole::RegionServer, vec!["regionserver", "ui-http"])]
    #[case("2.6.3", HbaseRole::RestServer, vec!["rest-http", "ui-http"])]
    #[case("2.6.4", HbaseRole::Master, vec!["master", "ui-http"])]
    #[case("2.6.4", HbaseRole::RegionServer, vec!["regionserver", "ui-http"])]
    #[case("2.6.4", HbaseRole::RestServer, vec!["rest-http", "ui-http"])]
    fn test_rolegroup_service_ports(
        #[case] hbase_version: &str,
        #[case] role: HbaseRole,
        #[case] expected_ports: Vec<&str>,
    ) {
        let input = format!(
            "
        apiVersion: hbase.stackable.tech/v1alpha1
        kind: HbaseCluster
        metadata:
          name: hbase
          uid: c2e98fc1-6b88-4d11-9381-52530e3f431e
        spec:
          image:
            productVersion: {hbase_version}
          clusterConfig:
            hdfsConfigMapName: simple-hdfs
            zookeeperConfigMapName: simple-znode
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
        "
        );
        let hbase: v1alpha1::HbaseCluster =
            serde_yaml::from_str(&input).expect("illegal test input");

        let cluster = test_utils::validated_cluster();
        let role_group_name = test_utils::role_group_name("default");
        let service = build_rolegroup_service(&hbase, &cluster, &role, &role_group_name);

        assert_eq!(
            expected_ports,
            service
                .spec
                .unwrap()
                .ports
                .unwrap()
                .iter()
                .map(|port| { port.clone().name.unwrap() })
                .collect::<Vec<String>>()
        );
    }
}
