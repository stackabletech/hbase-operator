//! Ensures that `Pod`s are configured and running for each [`HbaseCluster`]

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hbase_crd::{
    HbaseCluster, HbaseConfig, HbaseRole, APP_NAME, HBASE_ENV_SH, HBASE_SITE_XML, HDFS_SITE_XML,
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
const HBASE_REST_PORT: i32 = 8080;

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

pub async fn reconcile_hbase(
    hbase: Arc<HbaseCluster>,
    ctx: Context<Ctx>,
) -> Result<ReconcilerAction> {
    tracing::info!("Starting reconcile");

    let client = &ctx.get_ref().client;

    let namespace = hbase.metadata.namespace.as_deref().unwrap_or("default");
    let mut hbase = hbase.as_ref().clone();
    if let Some(config) = &mut hbase.spec.config {
        apply_zookeeper_configmap(client, namespace, config).await?;
        apply_hdfs_configmap(client, namespace, config).await?;
    }
    for role in [&mut hbase.spec.masters, &mut hbase.spec.region_servers]
        .into_iter()
        .flatten()
    {
        apply_zookeeper_configmap(client, namespace, &mut role.config.config).await?;
        apply_hdfs_configmap(client, namespace, &mut role.config.config).await?;
    }

    let config_types = vec![
        PropertyNameKind::File(HBASE_ENV_SH.to_string()),
        PropertyNameKind::File(HBASE_SITE_XML.to_string()),
        PropertyNameKind::File(HDFS_SITE_XML.to_string()),
    ];

    let mut roles: HashMap<String, _> = [
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
            (config_types.to_owned(), rest_servers.to_owned()),
        );
    }

    let role_config =
        transform_all_roles_to_config(&hbase, roles).context(GenerateProductConfigSnafu)?;

    let validated_config = validate_all_roles_and_groups_config(
        hbase_version(&hbase)?,
        &role_config,
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    // TODO Really master?
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
            let rg_configmap = build_rolegroup_config_map(&hbase, &rolegroup, rolegroup_config)?;
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

async fn apply_zookeeper_configmap(
    client: &Client,
    namespace: &str,
    config: &mut HbaseConfig,
) -> Result<()> {
    if let Some(config_map_name) = &config.zookeeper_config_map_name {
        let value = get_value_from_config_map(client, config_map_name, namespace, "ZOOKEEPER")
            .await
            .context(NoZookeeperUrlsSnafu)?;
        config.hbase_zookeeper_quorum = Some(value);
    }
    Ok(())
}

async fn apply_hdfs_configmap(
    client: &Client,
    namespace: &str,
    config: &mut HbaseConfig,
) -> Result<()> {
    if let Some(config_map_name) = &config.hdfs_config_map_name {
        let value = get_value_from_config_map(client, config_map_name, namespace, "hdfs-site.xml")
            .await
            .context(NoZookeeperUrlsSnafu)?;
        config.hdfs_config = Some(value);
    }
    Ok(())
}

async fn get_value_from_config_map(
    client: &Client,
    config_map_name: &str,
    namespace: &str,
    key: &'static str,
) -> Result<String, stackable_operator::error::Error> {
    let config_map = client
        .get::<ConfigMap>(config_map_name, Some(namespace))
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
) -> Result<ConfigMap, Error> {
    let hbase_site_config = rolegroup_config
        .get(&PropertyNameKind::File(HBASE_SITE_XML.to_string()))
        .cloned()
        .unwrap_or_default();
    let hbase_site_config = hbase_site_config
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect::<BTreeMap<_, _>>();

    let hbase_env_config = rolegroup_config
        .get(&PropertyNameKind::File(HBASE_ENV_SH.to_string()))
        .cloned()
        .unwrap_or_default();

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
        .add_data(
            HBASE_SITE_XML,
            writer::to_hadoop_xml(hbase_site_config.iter()),
        )
        .add_data(HBASE_ENV_SH, write_hbase_env_sh(hbase_env_config.iter()))
        .add_data(
            HDFS_SITE_XML,
            rolegroup_config
                .get(&PropertyNameKind::File(HDFS_SITE_XML.to_string()))
                .and_then(|m| m.get("content").cloned())
                .unwrap_or_default(),
        )
        .build()
        .map_err(|e| Error::BuildRoleGroupConfig {
            source: e,
            rolegroup: rolegroup.clone(),
        })
}

fn write_hbase_env_sh<'a, T>(properties: T) -> String
where
    T: Iterator<Item = (&'a String, &'a String)>,
{
    properties
        .map(|(variable, value)| format!("export {variable}={value}\n"))
        .collect()
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
        HbaseRole::RestServer => ("rest", HBASE_REST_PORT),
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
                port,
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

    let role = serde_yaml::from_str(&rolegroup_ref.role).unwrap();
    let command = vec![
        "bin/hbase".into(),
        match role {
            HbaseRole::Master => "master".into(),
            HbaseRole::RegionServer => "regionserver".into(),
            HbaseRole::RestServer => "rest".into(),
        },
        "start".into(),
    ];

    let container = ContainerBuilder::new("hbase")
        .image(image)
        .command(command)
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
