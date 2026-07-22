//! Build the per-rolegroup `Service`s for the HbaseCluster.

use stackable_operator::{
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    v2::builder::service::{Scheme, Scraping, prometheus_annotations, prometheus_labels},
};

use crate::{
    controller::{RoleGroupName, ValidatedCluster},
    crd::HbaseRole,
};

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a
/// certain rolegroup.
///
/// This is mostly useful for internal communication between peers, or for clients that perform
/// client-side load balancing.
pub fn build_rolegroup_service(
    cluster: &ValidatedCluster,
    hbase_role: &HbaseRole,
    role_group_name: &RoleGroupName,
) -> Service {
    let ports = hbase_role
        .ports(cluster.has_https_enabled())
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
                    .role_group_resource_names(hbase_role, role_group_name)
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
                    .role_group_resource_names(hbase_role, role_group_name)
                    .metrics_service_name()
                    .to_string(),
                hbase_role,
                role_group_name,
            )
            .with_labels(prometheus_labels(&Scraping::Enabled))
            .with_annotations(prometheus_annotations(
                &Scraping::Enabled,
                if cluster.has_https_enabled() {
                    &Scheme::Https
                } else {
                    &Scheme::Http
                },
                "/prometheus",
                &hbase_role.metrics_port(),
            ))
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

#[cfg(test)]
mod test {
    use rstest::rstest;

    use super::*;
    use crate::test_utils;

    #[rstest]
    #[case(HbaseRole::Master, vec!["master", "ui-http"])]
    #[case(HbaseRole::RegionServer, vec!["regionserver", "ui-http"])]
    #[case(HbaseRole::RestServer, vec!["rest-http", "ui-http"])]
    fn test_rolegroup_service_ports(#[case] role: HbaseRole, #[case] expected_ports: Vec<&str>) {
        let cluster = test_utils::validated_cluster();
        let role_group_name = test_utils::role_group_name("default");
        let service = build_rolegroup_service(&cluster, &role, &role_group_name);

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
