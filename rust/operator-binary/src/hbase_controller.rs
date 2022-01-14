//! Ensures that `Pod`s are configured and running for each [`HbaseCluster`]

use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hbase_crd::{
    HbaseCluster, HbaseRole, APP_NAME, CORE_SITE_XML, FS_DEFAULT_FS, HBASE_SITE_XML,
    HBASE_ZOOKEEPER_QUORUM,
};
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder},
    client::Client,
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{ConfigMap, ConfigMapVolumeSource, Service, ServicePort, ServiceSpec},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::runtime::controller::{Context, ReconcilerAction},
    labels::{role_group_selector_labels, role_selector_labels},
    product_config::{types::PropertyNameKind, writer, ProductConfigManager},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::RoleGroupRef,
};

const FIELD_MANAGER_SCOPE: &str = "hbasecluster";

const CONFIG_DIR_NAME: &str = "/stackable/conf";

const HBASE_MASTER_PORT: i32 = 60000;
const HBASE_REGIONSERVER_PORT: i32 = 60020;

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug)]
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
    #[snafu(display("failed to apply global Service"))]
    ApplyRoleService {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<HbaseCluster>,
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
    #[snafu(display("failed to retrieve the ZooKeeper cluster location"))]
    NoZookeeperUrls {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to retrieve the HDFS cluster location"))]
    NoHdfsUrls {
        source: stackable_operator::error::Error,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn reconcile_hbase(hbase: HbaseCluster, ctx: Context<Ctx>) -> Result<ReconcilerAction> {
    tracing::info!("Starting reconcile");

    let client = &ctx.get_ref().client;

    let mut masters_role = hbase.spec.masters.clone().context(NoMasterRoleSnafu)?;
    let mut region_servers_role = hbase
        .spec
        .region_servers
        .clone()
        .context(NoRegionServerRoleSnafu)?;

    // TODO Make config map optional
    let hdfs_url = get_value_from_config_map(
        client,
        &hbase.spec.hdfs_config_map_name,
        hbase.metadata.namespace.as_deref(),
        "HDFS",
    )
    .await
    .context(NoHdfsUrlsSnafu)?;
    masters_role
        .config
        .config
        .hbase_rootdir
        .get_or_insert(hdfs_url.to_owned());
    region_servers_role
        .config
        .config
        .hbase_rootdir
        .get_or_insert(hdfs_url.to_owned());

    let zookeeper_url = get_value_from_config_map(
        client,
        &hbase.spec.zookeeper_config_map_name,
        hbase.metadata.namespace.as_deref(),
        "ZOOKEEPER",
    )
    .await
    .context(NoZookeeperUrlsSnafu)?;

    let role_config = transform_all_roles_to_config(
        &hbase,
        [
            (
                HbaseRole::Master.to_string(),
                (
                    vec![PropertyNameKind::File(HBASE_SITE_XML.to_string())],
                    masters_role,
                ),
            ),
            (
                HbaseRole::RegionServer.to_string(),
                (
                    vec![PropertyNameKind::File(HBASE_SITE_XML.to_string())],
                    region_servers_role,
                ),
            ),
        ]
        .into(),
    )
    .context(GenerateProductConfigSnafu)?;

    let validated_config = validate_all_roles_and_groups_config(
        hbase_version(&hbase)?,
        &role_config,
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let master_role_service = build_master_role_service(&hbase)?;
    client
        .apply_patch(
            FIELD_MANAGER_SCOPE,
            &master_role_service,
            &master_role_service,
        )
        .await
        .context(ApplyRoleServiceSnafu)?;

    for (role_name, group_config) in validated_config.iter() {
        for (rolegroup_name, rolegroup_config) in group_config.iter() {
            let rolegroup = hbase.server_rolegroup_ref(role_name, rolegroup_name);
            let rg_service = build_rolegroup_service(&hbase, &rolegroup, rolegroup_config)?;
            let rg_configmap =
                build_rolegroup_config_map(&hbase, &rolegroup, rolegroup_config, &zookeeper_url)?;
            let rg_statefulset = build_rolegroup_statefulset(&hbase, &rolegroup, rolegroup_config)?;
            client
                .apply_patch(FIELD_MANAGER_SCOPE, &rg_service, &rg_service)
                .await
                .with_context(|_| ApplyRoleGroupServiceSnafu {
                    rolegroup: rolegroup.clone(),
                })?;
            client
                .apply_patch(FIELD_MANAGER_SCOPE, &rg_configmap, &rg_configmap)
                .await
                .with_context(|_| ApplyRoleGroupConfigSnafu {
                    rolegroup: rolegroup.clone(),
                })?;
            client
                .apply_patch(FIELD_MANAGER_SCOPE, &rg_statefulset, &rg_statefulset)
                .await
                .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                    rolegroup: rolegroup.clone(),
                })?;
        }
    }

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

async fn get_value_from_config_map(
    client: &Client,
    config_map_name: &str,
    namespace: Option<&str>,
    key: &'static str,
) -> Result<String, stackable_operator::error::Error> {
    let config_map = client
        .get::<ConfigMap>(config_map_name, Some(namespace.unwrap_or("default")))
        .await?;
    config_map
        .data
        .as_ref()
        .and_then(|m| m.get(key).cloned())
        .ok_or(stackable_operator::error::Error::MissingObjectKey { key })
}

/// The server-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
pub fn build_master_role_service(hbase: &HbaseCluster) -> Result<Service> {
    let role_name = HbaseRole::Master.to_string();
    let role_svc_name = hbase
        .server_role_service_name()
        .context(GlobalServiceNameNotFoundSnafu)?;
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hbase)
            .name(&role_svc_name)
            .ownerreference_from_resource(hbase, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(hbase, APP_NAME, hbase_version(hbase)?, &role_name, "global")
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("master".to_string()),
                port: HBASE_MASTER_PORT,
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
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
    zookeeper_urls: &str,
) -> Result<ConfigMap, Error> {
    let hbase_site_config = rolegroup_config
        .get(&PropertyNameKind::File(HBASE_SITE_XML.to_string()))
        .cloned()
        .unwrap_or_default();
    let mut hbase_site_config = hbase_site_config
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect::<BTreeMap<_, _>>();
    hbase_site_config.insert(
        HBASE_ZOOKEEPER_QUORUM.to_string(),
        Some(zookeeper_urls.to_string()),
    );

    ConfigMapBuilder::new()
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
                    &rolegroup.role,
                    &rolegroup.role_group,
                )
                .build(),
        )
        .add_data("hbase-env.sh", "export HBASE_MANAGES_ZK=false\n")
        .add_data(
            CORE_SITE_XML,
            writer::to_hadoop_xml(
                [
                    // TODO Set HDFS connection string
                    (
                        &FS_DEFAULT_FS.to_string(),
                        &Some("hdfs://server1:9000".to_string()),
                    ),
                ]
                .into_iter(),
            ),
        )
        .add_data(
            HBASE_SITE_XML,
            writer::to_hadoop_xml(hbase_site_config.iter()),
        )
        .build()
        .map_err(|e| Error::BuildRoleGroupConfig {
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
    let (name, port) = match serde_yaml::from_str(&rolegroup.role).unwrap() {
        HbaseRole::Master => ("master", HBASE_MASTER_PORT),
        HbaseRole::RegionServer => ("regionserver", HBASE_REGIONSERVER_PORT),
    };

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
                &rolegroup.role,
                &rolegroup.role_group,
            )
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(vec![ServicePort {
                name: Some(name.to_string()),
                port: port,
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
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

    let image = format!(
        "docker.stackable.tech/stackable/hbase:{}-stackable0",
        hbase_version
    );

    let container = ContainerBuilder::new("hbase")
        .image(image)
        .command(vec![
            "bin/hbase".into(),
            rolegroup_ref.role.to_owned(),
            "start".into(),
        ])
        .add_env_var("HBASE_CONF_DIR", CONFIG_DIR_NAME)
        .add_volume_mount("config", CONFIG_DIR_NAME)
        // .add_container_port("http", APP_PORT.into())
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
                        &rolegroup_ref.role,
                        &rolegroup_ref.role_group,
                    )
                })
                .add_container(container)
                .add_volume(stackable_operator::k8s_openapi::api::core::v1::Volume {
                    name: "config".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: Some(rolegroup_ref.object_name()),
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

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}

fn rolegroup_replicas(
    hbase: &HbaseCluster,
    rolegroup_ref: &RoleGroupRef<HbaseCluster>,
) -> Result<i32, Error> {
    let replicas = match serde_yaml::from_str(&rolegroup_ref.role).unwrap() {
        HbaseRole::Master => hbase
            .spec
            .masters
            .as_ref()
            .context(NoMasterRoleSnafu)?
            .role_groups
            .get(&rolegroup_ref.role_group)
            .and_then(|rg| rg.replicas)
            .map(i32::from)
            .unwrap_or(0),
        HbaseRole::RegionServer => hbase
            .spec
            .masters
            .as_ref()
            .context(NoRegionServerRoleSnafu)?
            .role_groups
            .get(&rolegroup_ref.role_group)
            .and_then(|rg| rg.replicas)
            .map(i32::from)
            .unwrap_or(0),
    };

    if hbase.spec.stopped.unwrap_or(false) {
        Ok(0)
    } else {
        Ok(replicas)
    }
}
