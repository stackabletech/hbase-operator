//! Ensures that `Pod`s are configured and running for each [`HbaseCluster`]

use crate::discovery::build_discovery_configmap;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hbase_crd::{
    HbaseCluster, HbaseConfig, HbaseRole, APP_NAME, HBASE_ENV_SH, HBASE_MASTER_PORT,
    HBASE_REGIONSERVER_PORT, HBASE_REST_PORT, HBASE_SITE_XML, HBASE_ZOOKEEPER_QUORUM,
};
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder},
    cluster_resources::ClusterResources,
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapVolumeSource, ContainerPort, HTTPGetAction, Probe, Service,
                ServicePort, ServiceSpec, TCPSocketAction,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    kube::{runtime::controller::Action, Resource, ResourceExt},
    labels::{role_group_selector_labels, role_selector_labels},
    logging::controller::ReconcilerError,
    product_config::{types::PropertyNameKind, writer, ProductConfigManager},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::{Role, RoleGroupRef},
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

const CONTROLLER_NAME: &str = "hbase-operator";

const CONFIG_DIR_NAME: &str = "/stackable/conf";
const HDFS_DISCOVERY_TMP_DIR: &str = "/stackable/tmp/hdfs";
const HBASE_CONFIG_TMP_DIR: &str = "/stackable/tmp/hbase";

const ZOOKEEPER_DISCOVERY_CM_ENTRY: &str = "ZOOKEEPER";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object defines no version"))]
    ObjectHasNoVersion,
    #[snafu(display("object defines no master role"))]
    NoMasterRole,
    #[snafu(display("object defines no regionserver role"))]
    NoRegionServerRole,
    #[snafu(display("failed to calculate global service name"))]
    GlobalServiceNameNotFound,
    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply global Service"))]
    ApplyRoleService {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<HbaseCluster>,
    },
    #[snafu(display("failed to apply discovery configmap"))]
    ApplyDiscoveryConfigMap {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build discovery configmap"))]
    BuildDiscoveryConfigMap {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build ConfigMap for {}", rolegroup))]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<HbaseCluster>,
    },
    #[snafu(display("failed to apply ConfigMap for {}", rolegroup))]
    ApplyRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<HbaseCluster>,
    },
    #[snafu(display("failed to apply StatefulSet for {}", rolegroup))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<HbaseCluster>,
    },
    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to retrieve the HDFS configuration"))]
    NoHdfsSiteConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("no configmap_name for {cm_name} discovery is configured"))]
    MissingConfigMap {
        source: stackable_operator::error::Error,
        cm_name: String,
    },
    #[snafu(display("failed to retrieve the entry {entry} for config map {cm_name}"))]
    MissingConfigMapEntry {
        entry: &'static str,
        cm_name: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_hbase(hbase: Arc<HbaseCluster>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let client = &ctx.client;

    let zk_discovery_cm_name = &hbase.spec.zookeeper_config_map_name;
    let zk_connect_string = client
        .get::<ConfigMap>(zk_discovery_cm_name, hbase.namespace().as_deref())
        .await
        .context(MissingConfigMapSnafu {
            cm_name: zk_discovery_cm_name.to_string(),
        })?
        .data
        .and_then(|mut data| data.remove(ZOOKEEPER_DISCOVERY_CM_ENTRY))
        .context(MissingConfigMapEntrySnafu {
            entry: ZOOKEEPER_DISCOVERY_CM_ENTRY,
            cm_name: zk_discovery_cm_name.to_string(),
        })?;

    let roles = build_roles(&hbase)?;

    let validated_config = validate_all_roles_and_groups_config(
        hbase_version(&hbase)?,
        &transform_all_roles_to_config(&*hbase, roles).context(GenerateProductConfigSnafu)?,
        &ctx.product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let mut cluster_resources =
        ClusterResources::new(APP_NAME, CONTROLLER_NAME, &hbase.object_ref(&()))
            .context(CreateClusterResourcesSnafu)?;

    let region_server_role_service = build_region_server_role_service(&hbase)?;
    cluster_resources
        .add(client, &region_server_role_service)
        .await
        .context(ApplyRoleServiceSnafu)?;

    // discovery config map
    let discovery_cm = build_discovery_configmap(&hbase, &zk_connect_string, CONTROLLER_NAME)
        .context(BuildDiscoveryConfigMapSnafu)?;
    cluster_resources
        .add(client, &discovery_cm)
        .await
        .context(ApplyDiscoveryConfigMapSnafu)?;

    for (role_name, group_config) in validated_config.iter() {
        for (rolegroup_name, rolegroup_config) in group_config.iter() {
            let rolegroup = hbase.server_rolegroup_ref(role_name, rolegroup_name);
            let rg_service = build_rolegroup_service(&hbase, &rolegroup, rolegroup_config)?;
            let rg_configmap = build_rolegroup_config_map(
                &hbase,
                &rolegroup,
                rolegroup_config,
                &zk_connect_string,
            )?;
            let rg_statefulset = build_rolegroup_statefulset(&hbase, &rolegroup, rolegroup_config)?;
            cluster_resources
                .add(client, &rg_service)
                .await
                .with_context(|_| ApplyRoleGroupServiceSnafu {
                    rolegroup: rolegroup.clone(),
                })?;
            cluster_resources
                .add(client, &rg_configmap)
                .await
                .with_context(|_| ApplyRoleGroupConfigSnafu {
                    rolegroup: rolegroup.clone(),
                })?;
            cluster_resources
                .add(client, &rg_statefulset)
                .await
                .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                    rolegroup: rolegroup.clone(),
                })?;
        }
    }

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    Ok(Action::await_change())
}

/// The server-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
pub fn build_region_server_role_service(hbase: &HbaseCluster) -> Result<Service> {
    let role = HbaseRole::RegionServer;
    let role_name = role.to_string();
    let role_svc_name = hbase
        .server_role_service_name()
        .context(GlobalServiceNameNotFoundSnafu)?;
    let ports = role
        .port_properties()
        .into_iter()
        .map(|(port_name, port_number, port_protocol)| ServicePort {
            name: Some(port_name.into()),
            port: port_number,
            protocol: Some(port_protocol.into()),
            ..ServicePort::default()
        })
        .collect();

    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hbase)
            .name(&role_svc_name)
            .ownerreference_from_resource(hbase, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                hbase,
                APP_NAME,
                hbase_version(hbase)?,
                CONTROLLER_NAME,
                &role_name,
                "global",
            )
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(ports),
            selector: Some(role_selector_labels(hbase, APP_NAME, &role_name)),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_rolegroup_config_map(
    hbase: &HbaseCluster,
    rolegroup: &RoleGroupRef<HbaseCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    zk_connect_string: &str,
) -> Result<ConfigMap, Error> {
    let mut hbase_site_config = rolegroup_config
        .get(&PropertyNameKind::File(HBASE_SITE_XML.to_string()))
        .cloned()
        .unwrap_or_default();

    hbase_site_config.insert(
        HBASE_ZOOKEEPER_QUORUM.to_string(),
        zk_connect_string.to_string(),
    );

    let hbase_site_config = hbase_site_config
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect::<BTreeMap<_, _>>();

    let hbase_env_config = rolegroup_config
        .get(&PropertyNameKind::File(HBASE_ENV_SH.to_string()))
        .cloned()
        .unwrap_or_default();

    let mut builder = ConfigMapBuilder::new();

    builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(hbase)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(hbase, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(
                    hbase,
                    APP_NAME,
                    hbase_version(hbase)?,
                    CONTROLLER_NAME,
                    &rolegroup.role,
                    &rolegroup.role_group,
                )
                .build(),
        )
        .add_data(
            HBASE_SITE_XML,
            writer::to_hadoop_xml(hbase_site_config.iter()),
        )
        .add_data(HBASE_ENV_SH, write_hbase_env_sh(hbase_env_config.iter()));

    builder.build().map_err(|e| Error::BuildRoleGroupConfig {
        source: e,
        rolegroup: rolegroup.clone(),
    })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_rolegroup_service(
    hbase: &HbaseCluster,
    rolegroup: &RoleGroupRef<HbaseCluster>,
    _rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<Service> {
    let role = serde_yaml::from_str::<HbaseRole>(&rolegroup.role).unwrap();
    let ports = role
        .port_properties()
        .into_iter()
        .map(|(port_name, port_number, port_protocol)| ServicePort {
            name: Some(port_name.into()),
            port: port_number,
            protocol: Some(port_protocol.into()),
            ..ServicePort::default()
        })
        .collect();

    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hbase)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(hbase, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                hbase,
                APP_NAME,
                hbase_version(hbase)?,
                CONTROLLER_NAME,
                &rolegroup.role,
                &rolegroup.role_group,
            )
            .with_label("prometheus.io/scrape", "true")
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(ports),
            selector: Some(role_group_selector_labels(
                hbase,
                APP_NAME,
                &rolegroup.role,
                &rolegroup.role_group,
            )),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding [`Service`] (from [`build_rolegroup_service`]).
fn build_rolegroup_statefulset(
    hbase: &HbaseCluster,
    rolegroup_ref: &RoleGroupRef<HbaseCluster>,
    _rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<StatefulSet> {
    let hbase_version = hbase_version(hbase)?;

    let image = format!("docker.stackable.tech/stackable/hbase:{}", hbase_version);

    let role = serde_yaml::from_str::<HbaseRole>(&rolegroup_ref.role).unwrap();

    let ports = role
        .port_properties()
        .into_iter()
        .map(|(port_name, port_number, port_protocol)| ContainerPort {
            name: Some(port_name.into()),
            container_port: port_number,
            protocol: Some(port_protocol.into()),
            ..ContainerPort::default()
        })
        .collect();

    let probe_template = match role {
        HbaseRole::Master => Probe {
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::Int(HBASE_MASTER_PORT),
                ..TCPSocketAction::default()
            }),
            ..Probe::default()
        },
        HbaseRole::RegionServer => Probe {
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::Int(HBASE_REGIONSERVER_PORT),
                ..TCPSocketAction::default()
            }),
            ..Probe::default()
        },
        HbaseRole::RestServer => Probe {
            http_get: Some(HTTPGetAction {
                port: IntOrString::Int(HBASE_REST_PORT),
                ..HTTPGetAction::default()
            }),
            ..Probe::default()
        },
    };

    let startup_probe = Probe {
        failure_threshold: Some(120),
        initial_delay_seconds: Some(4),
        period_seconds: Some(5),
        timeout_seconds: Some(3),
        ..probe_template.clone()
    };
    let liveness_probe = Probe {
        failure_threshold: Some(3),
        period_seconds: Some(10),
        timeout_seconds: Some(3),
        ..probe_template.clone()
    };
    let readiness_probe = Probe {
        failure_threshold: Some(1),
        period_seconds: Some(10),
        timeout_seconds: Some(2),
        ..probe_template
    };

    let container = ContainerBuilder::new("hbase")
        .image(image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![[
            format!("mkdir -p {}", CONFIG_DIR_NAME),
            format!(
                "cp {}/hdfs-site.xml {}",
                HDFS_DISCOVERY_TMP_DIR, CONFIG_DIR_NAME
            ),
            format!(
                "cp {}/core-site.xml {}",
                HDFS_DISCOVERY_TMP_DIR, CONFIG_DIR_NAME
            ),
            format!("cp {}/* {}", HBASE_CONFIG_TMP_DIR, CONFIG_DIR_NAME),
            format!(
                "bin/hbase {} start",
                match role {
                    HbaseRole::Master => "master",
                    HbaseRole::RegionServer => "regionserver",
                    HbaseRole::RestServer => "rest",
                }
            ),
        ]
        .join(" && ")])
        .add_env_var("HBASE_CONF_DIR", CONFIG_DIR_NAME)
        .add_volume_mount("hbase-config", HBASE_CONFIG_TMP_DIR)
        .add_volume_mount("hdfs-discovery", HDFS_DISCOVERY_TMP_DIR)
        .add_container_ports(ports)
        .startup_probe(startup_probe)
        .liveness_probe(liveness_probe)
        .readiness_probe(readiness_probe)
        .build();

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hbase)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(hbase, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                hbase,
                APP_NAME,
                hbase_version,
                CONTROLLER_NAME,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            )
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: Some(rolegroup_replicas(hbase, rolegroup_ref)?),
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    hbase,
                    APP_NAME,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )),
                ..LabelSelector::default()
            },
            service_name: rolegroup_ref.object_name(),
            template: PodBuilder::new()
                .metadata_builder(|m| {
                    m.with_recommended_labels(
                        hbase,
                        APP_NAME,
                        hbase_version,
                        CONTROLLER_NAME,
                        &rolegroup_ref.role,
                        &rolegroup_ref.role_group,
                    )
                })
                .add_container(container)
                .add_volume(stackable_operator::k8s_openapi::api::core::v1::Volume {
                    name: "hbase-config".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: Some(rolegroup_ref.object_name()),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .add_volume(stackable_operator::k8s_openapi::api::core::v1::Volume {
                    name: "hdfs-discovery".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: Some(hbase.spec.hdfs_config_map_name.clone()),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .build_template(),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

pub fn hbase_version(hbase: &HbaseCluster) -> Result<&str> {
    hbase
        .spec
        .version
        .as_deref()
        .context(ObjectHasNoVersionSnafu)
}

fn rolegroup_replicas(
    hbase: &HbaseCluster,
    rolegroup_ref: &RoleGroupRef<HbaseCluster>,
) -> Result<i32, Error> {
    if hbase.spec.stopped.unwrap_or(false) {
        Ok(0)
    } else {
        let role = serde_yaml::from_str(&rolegroup_ref.role).unwrap();

        let replicas = hbase
            .get_role(role)
            .as_ref()
            .map(|role| &role.role_groups)
            .and_then(|role_group| role_group.get(&rolegroup_ref.role_group))
            .and_then(|rg| rg.replicas)
            .map(i32::from)
            .unwrap_or(0);

        Ok(replicas)
    }
}

type RoleConfig = HashMap<String, (Vec<PropertyNameKind>, Role<HbaseConfig>)>;
fn build_roles(hbase: &HbaseCluster) -> Result<RoleConfig> {
    let config_types = vec![
        PropertyNameKind::File(HBASE_ENV_SH.to_string()),
        PropertyNameKind::File(HBASE_SITE_XML.to_string()),
    ];

    let mut roles: RoleConfig = [
        (
            HbaseRole::Master.to_string(),
            (
                config_types.to_owned(),
                hbase
                    .get_role(HbaseRole::Master)
                    .cloned()
                    .context(NoMasterRoleSnafu)?,
            ),
        ),
        (
            HbaseRole::RegionServer.to_string(),
            (
                config_types.to_owned(),
                hbase
                    .get_role(HbaseRole::RegionServer)
                    .cloned()
                    .context(NoRegionServerRoleSnafu)?,
            ),
        ),
    ]
    .into();

    if let Some(rest_servers) = hbase.get_role(HbaseRole::RestServer) {
        roles.insert(
            HbaseRole::RestServer.to_string(),
            (config_types, rest_servers.to_owned()),
        );
    }

    Ok(roles)
}

fn write_hbase_env_sh<'a, T>(properties: T) -> String
where
    T: Iterator<Item = (&'a String, &'a String)>,
{
    properties
        .map(|(variable, value)| format!("export {variable}={value}\n"))
        .collect()
}

pub fn error_policy(_error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}
