//! Ensures that `Pod`s are configured and running for each [`HbaseCluster`]

use crate::{
    discovery::build_discovery_configmap,
    operations::pdb::add_pdbs,
    product_logging::{
        extend_role_group_config_map, resolve_vector_aggregator_address, LOG4J_CONFIG_FILE,
    },
    zookeeper::{self, ZookeeperConnectionInformation},
    OPERATOR_NAME,
};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hbase_crd::{
    Container, HbaseCluster, HbaseClusterStatus, HbaseConfig, HbaseConfigFragment, HbaseRole,
    APP_NAME, CONFIG_DIR_NAME, HBASE_ENV_SH, HBASE_HEAPSIZE, HBASE_MASTER_PORT,
    HBASE_REGIONSERVER_PORT, HBASE_REST_PORT, HBASE_SITE_XML, JVM_HEAP_FACTOR,
    JVM_SECURITY_PROPERTIES_FILE,
};
use stackable_operator::{
    builder::{
        resources::ResourceRequirementsBuilder, ConfigMapBuilder, ContainerBuilder,
        ObjectMetaBuilder, PodBuilder, PodSecurityContextBuilder,
    },
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        product_image_selection::ResolvedProductImage,
        rbac::{build_rbac_resources, service_account_name},
    },
    k8s_openapi::{api::core::v1::Volume, DeepMerge},
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
    kube::{runtime::controller::Action, Resource},
    labels::{role_group_selector_labels, role_selector_labels, ObjectLabels},
    logging::controller::ReconcilerError,
    memory::{BinaryMultiple, MemoryQuantity},
    product_config::{
        types::PropertyNameKind,
        writer::{self, to_java_properties_string},
        ProductConfigManager,
    },
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    product_logging::{
        self,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::{GenericRoleConfig, Role, RoleGroupRef},
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

pub const HBASE_CONTROLLER_NAME: &str = "hbasecluster";
pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
pub const MAX_HBASE_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

const HDFS_DISCOVERY_TMP_DIR: &str = "/stackable/tmp/hdfs";
const HBASE_CONFIG_TMP_DIR: &str = "/stackable/tmp/hbase";
const HBASE_LOG_CONFIG_TMP_DIR: &str = "/stackable/tmp/log_config";

const DOCKER_IMAGE_BASE_NAME: &str = "hbase";
const HBASE_UID: i64 = 1000;

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
    #[snafu(display("object defines no namespace"))]
    ObjectHasNoNamespace,
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
    #[snafu(display("failed to retrieve zookeeper connection information"))]
    RetrieveZookeeperConnectionInformation { source: zookeeper::Error },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
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
    #[snafu(display("failed to patch service account"))]
    ApplyServiceAccount {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to patch role binding"))]
    ApplyRoleBinding {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("could not parse Hbase role [{role}]"))]
    UnidentifiedHbaseRole {
        source: strum::ParseError,
        role: String,
    },
    #[snafu(display("failed to retrieve Hbase role group: {source}"))]
    UnidentifiedHbaseRoleGroup { source: stackable_hbase_crd::Error },
    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: stackable_hbase_crd::Error },
    #[snafu(display("invalid java heap config - missing default or value in crd?"))]
    InvalidJavaHeapConfig,
    #[snafu(display("failed to convert java heap config to unit [{unit}]"))]
    FailedToConvertJavaHeap {
        source: stackable_operator::error::Error,
        unit: String,
    },
    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress {
        source: crate::product_logging::Error,
    },
    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::error::Error,
    },
    #[snafu(display(
        "failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for {}",
        rolegroup
    ))]
    SerializeJvmSecurity {
        source: stackable_operator::product_config::writer::PropertiesWriterError,
        rolegroup: RoleGroupRef<HbaseCluster>,
    },
    #[snafu(display("failed to create PodDisruptionBudget"))]
    FailedToCreatePdb {
        source: crate::operations::pdb::Error,
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

    let resolved_product_image = hbase
        .spec
        .image
        .resolve(DOCKER_IMAGE_BASE_NAME, crate::built_info::CARGO_PKG_VERSION);
    let zookeeper_connection_information = ZookeeperConnectionInformation::retrieve(&hbase, client)
        .await
        .context(RetrieveZookeeperConnectionInformationSnafu)?;

    let vector_aggregator_address = resolve_vector_aggregator_address(&hbase, client)
        .await
        .context(ResolveVectorAggregatorAddressSnafu)?;

    let roles = build_roles(&hbase)?;

    let validated_config = validate_all_roles_and_groups_config(
        &resolved_product_image.app_version_label,
        &transform_all_roles_to_config(hbase.as_ref(), roles)
            .context(GenerateProductConfigSnafu)?,
        &ctx.product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        HBASE_CONTROLLER_NAME,
        &hbase.object_ref(&()),
        ClusterResourceApplyStrategy::from(&hbase.spec.cluster_operation),
    )
    .context(CreateClusterResourcesSnafu)?;

    let region_server_role_service =
        build_region_server_role_service(&hbase, &resolved_product_image)?;
    cluster_resources
        .add(client, region_server_role_service)
        .await
        .context(ApplyRoleServiceSnafu)?;

    // discovery config map
    let discovery_cm = build_discovery_configmap(
        &hbase,
        &zookeeper_connection_information,
        &resolved_product_image,
    )
    .context(BuildDiscoveryConfigMapSnafu)?;
    cluster_resources
        .add(client, discovery_cm)
        .await
        .context(ApplyDiscoveryConfigMapSnafu)?;

    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
        hbase.as_ref(),
        APP_NAME,
        cluster_resources.get_required_labels(),
    )
    .context(BuildRbacResourcesSnafu)?;
    cluster_resources
        .add(client, rbac_sa)
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    for (role_name, group_config) in validated_config.iter() {
        let hbase_role = HbaseRole::from_str(role_name).context(UnidentifiedHbaseRoleSnafu {
            role: role_name.to_string(),
        })?;
        for (rolegroup_name, rolegroup_config) in group_config.iter() {
            let rolegroup = hbase.server_rolegroup_ref(role_name, rolegroup_name);

            let config = hbase
                .merged_config(
                    &hbase_role,
                    &rolegroup.role_group,
                    &hbase.spec.cluster_config.hdfs_config_map_name,
                )
                .context(FailedToResolveConfigSnafu)?;

            let rg_service =
                build_rolegroup_service(&hbase, &hbase_role, &rolegroup, &resolved_product_image)?;
            let rg_configmap = build_rolegroup_config_map(
                &hbase,
                &rolegroup,
                rolegroup_config,
                &zookeeper_connection_information,
                &config,
                &resolved_product_image,
                vector_aggregator_address.as_deref(),
            )?;
            let rg_statefulset = build_rolegroup_statefulset(
                &hbase,
                &hbase_role,
                &rolegroup,
                &config,
                &resolved_product_image,
            )?;
            cluster_resources
                .add(client, rg_service)
                .await
                .with_context(|_| ApplyRoleGroupServiceSnafu {
                    rolegroup: rolegroup.clone(),
                })?;
            cluster_resources
                .add(client, rg_configmap)
                .await
                .with_context(|_| ApplyRoleGroupConfigSnafu {
                    rolegroup: rolegroup.clone(),
                })?;
            ss_cond_builder.add(
                cluster_resources
                    .add(client, rg_statefulset)
                    .await
                    .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                        rolegroup: rolegroup.clone(),
                    })?,
            );
        }

        let role_config = hbase.role_config(&hbase_role);
        if let Some(GenericRoleConfig {
            pod_disruption_budget: pdb,
        }) = role_config
        {
            add_pdbs(pdb, &hbase, &hbase_role, client, &mut cluster_resources)
                .await
                .context(FailedToCreatePdbSnafu)?;
        }
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&hbase.spec.cluster_operation);

    let status = HbaseClusterStatus {
        conditions: compute_conditions(
            hbase.as_ref(),
            &[&ss_cond_builder, &cluster_operation_cond_builder],
        ),
    };

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;
    client
        .apply_patch_status(OPERATOR_NAME, hbase.as_ref(), &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::await_change())
}

/// The server-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
pub fn build_region_server_role_service(
    hbase: &HbaseCluster,
    resolved_product_image: &ResolvedProductImage,
) -> Result<Service> {
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
            .with_recommended_labels(build_recommended_labels(
                hbase,
                &resolved_product_image.app_version_label,
                &role_name,
                "global",
            ))
            .build(),
        spec: Some(ServiceSpec {
            type_: Some(hbase.spec.cluster_config.listener_class.k8s_service_type()),
            ports: Some(ports),
            selector: Some(role_selector_labels(hbase, APP_NAME, &role_name)),
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
    zookeeper_connection_information: &ZookeeperConnectionInformation,
    config: &HbaseConfig,
    resolved_product_image: &ResolvedProductImage,
    vector_aggregator_address: Option<&str>,
) -> Result<ConfigMap, Error> {
    let mut hbase_site_config = rolegroup_config
        .get(&PropertyNameKind::File(HBASE_SITE_XML.to_string()))
        .cloned()
        .unwrap_or_default();

    hbase_site_config.extend(zookeeper_connection_information.as_hbase_settings());

    let mut hbase_env_config = rolegroup_config
        .get(&PropertyNameKind::File(HBASE_ENV_SH.to_string()))
        .cloned()
        .unwrap_or_default();

    let memory_limit = MemoryQuantity::try_from(
        config
            .resources
            .memory
            .limit
            .as_ref()
            .context(InvalidJavaHeapConfigSnafu)?,
    )
    .context(FailedToConvertJavaHeapSnafu {
        unit: BinaryMultiple::Mebi.to_java_memory_unit(),
    })?;
    let heap_in_mebi = (memory_limit * JVM_HEAP_FACTOR)
        .scale_to(BinaryMultiple::Mebi)
        .format_for_java()
        .context(FailedToConvertJavaHeapSnafu {
            unit: BinaryMultiple::Mebi.to_java_memory_unit(),
        })?;

    hbase_env_config.insert(HBASE_HEAPSIZE.to_string(), heap_in_mebi);

    let jvm_sec_props: BTreeMap<String, Option<String>> = rolegroup_config
        .get(&PropertyNameKind::File(
            JVM_SECURITY_PROPERTIES_FILE.to_string(),
        ))
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect();

    let mut builder = ConfigMapBuilder::new();

    builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(hbase)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(hbase, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(build_recommended_labels(
                    hbase,
                    &resolved_product_image.app_version_label,
                    &rolegroup.role,
                    &rolegroup.role_group,
                ))
                .build(),
        )
        .add_data(
            HBASE_SITE_XML,
            writer::to_hadoop_xml(
                hbase_site_config
                    .into_iter()
                    .map(|(k, v)| (k, Some(v)))
                    .collect::<BTreeMap<_, _>>()
                    .iter(),
            ),
        )
        .add_data(HBASE_ENV_SH, write_hbase_env_sh(hbase_env_config.iter()))
        .add_data(
            JVM_SECURITY_PROPERTIES_FILE,
            to_java_properties_string(jvm_sec_props.iter()).with_context(|_| {
                SerializeJvmSecuritySnafu {
                    rolegroup: rolegroup.clone(),
                }
            })?,
        );

    extend_role_group_config_map(
        rolegroup,
        vector_aggregator_address,
        &config.logging,
        &mut builder,
    )
    .context(InvalidLoggingConfigSnafu {
        cm_name: rolegroup.object_name(),
    })?;

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
    hbase_role: &HbaseRole,
    rolegroup: &RoleGroupRef<HbaseCluster>,
    resolved_product_image: &ResolvedProductImage,
) -> Result<Service> {
    let ports = hbase_role
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
            .with_recommended_labels(build_recommended_labels(
                hbase,
                &resolved_product_image.app_version_label,
                &rolegroup.role,
                &rolegroup.role_group,
            ))
            .with_label("prometheus.io/scrape", "true")
            .build(),
        spec: Some(ServiceSpec {
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_string()),
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
    hbase_role: &HbaseRole,
    rolegroup_ref: &RoleGroupRef<HbaseCluster>,
    config: &HbaseConfig,
    resolved_product_image: &ResolvedProductImage,
) -> Result<StatefulSet> {
    let hbase_version = &resolved_product_image.app_version_label;

    // In hbase-op the restserver role is optional :/
    let role = hbase.get_role(hbase_role);
    let role_group = role.and_then(|r| r.role_groups.get(&rolegroup_ref.role_group));

    let ports = hbase_role
        .port_properties()
        .into_iter()
        .map(|(port_name, port_number, port_protocol)| ContainerPort {
            name: Some(port_name.into()),
            container_port: port_number,
            protocol: Some(port_protocol.into()),
            ..ContainerPort::default()
        })
        .collect();

    let probe_template = match hbase_role {
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
        .expect("ContainerBuilder not created")
        .image_from_product_image(resolved_product_image)
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
            format!("cp {HBASE_LOG_CONFIG_TMP_DIR}/{LOG4J_CONFIG_FILE} {CONFIG_DIR_NAME}",),
            format!(
                "bin/hbase {} start",
                match hbase_role {
                    HbaseRole::Master => "master",
                    HbaseRole::RegionServer => "regionserver",
                    HbaseRole::RestServer => "rest",
                }
            ),
        ]
        .join(" && ")])
        .add_env_var("HBASE_CONF_DIR", CONFIG_DIR_NAME)
        // required by phoenix (for cases where Kerberos is enabled): see https://issues.apache.org/jira/browse/PHOENIX-2369
        .add_env_var("HADOOP_CONF_DIR", CONFIG_DIR_NAME)
        .add_volume_mount("hbase-config", HBASE_CONFIG_TMP_DIR)
        .add_volume_mount("hdfs-discovery", HDFS_DISCOVERY_TMP_DIR)
        .add_volume_mount("log-config", HBASE_LOG_CONFIG_TMP_DIR)
        .add_volume_mount("log", STACKABLE_LOG_DIR)
        .add_container_ports(ports)
        .resources(config.resources.clone().into())
        .startup_probe(startup_probe)
        .liveness_probe(liveness_probe)
        .readiness_probe(readiness_probe)
        .build();

    let mut pod_builder = PodBuilder::new();
    pod_builder
        .metadata_builder(|m| {
            m.with_recommended_labels(build_recommended_labels(
                hbase,
                hbase_version,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            ))
        })
        .image_pull_secrets_from_product_image(resolved_product_image)
        .affinity(&config.affinity)
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
                name: Some(hbase.spec.cluster_config.hdfs_config_map_name.clone()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .add_empty_dir_volume(
            "log",
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_HBASE_LOG_FILES_SIZE],
            )),
        )
        .service_account_name(service_account_name(APP_NAME))
        .security_context(
            PodSecurityContextBuilder::new()
                .run_as_user(HBASE_UID)
                .run_as_group(0)
                .fs_group(1000)
                .build(),
        );

    if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = config.logging.containers.get(&Container::Hbase)
    {
        pod_builder.add_volume(Volume {
            name: "log-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(config_map.into()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
    } else {
        pod_builder.add_volume(Volume {
            name: "log-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(rolegroup_ref.object_name()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
    }

    if config.logging.enable_vector_agent {
        pod_builder.add_container(product_logging::framework::vector_container(
            resolved_product_image,
            "hbase-config",
            "log",
            config.logging.containers.get(&Container::Vector),
            ResourceRequirementsBuilder::new()
                .with_cpu_request("250m")
                .with_cpu_limit("500m")
                .with_memory_request("128Mi")
                .with_memory_limit("128Mi")
                .build(),
        ));
    }

    let mut pod_template = pod_builder.build_template();
    if let Some(role) = role {
        pod_template.merge_from(role.config.pod_overrides.clone());
    }
    if let Some(role_group) = role_group {
        pod_template.merge_from(role_group.config.pod_overrides.clone());
    }

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hbase)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(hbase, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                hbase,
                hbase_version,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            ))
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: role_group.and_then(|rg| rg.replicas).map(i32::from),
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
            template: pod_template,
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

// The result type is only defined once, there is no value in extracting it into a type definition.
#[allow(clippy::type_complexity)]
fn build_roles(
    hbase: &HbaseCluster,
) -> Result<HashMap<String, (Vec<PropertyNameKind>, Role<HbaseConfigFragment>)>> {
    let config_types = vec![
        PropertyNameKind::File(HBASE_ENV_SH.to_string()),
        PropertyNameKind::File(HBASE_SITE_XML.to_string()),
        PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
    ];

    let mut roles = HashMap::from([
        (
            HbaseRole::Master.to_string(),
            (
                config_types.to_owned(),
                hbase
                    .get_role(&HbaseRole::Master)
                    .cloned()
                    .context(NoMasterRoleSnafu)?,
            ),
        ),
        (
            HbaseRole::RegionServer.to_string(),
            (
                config_types.to_owned(),
                hbase
                    .get_role(&HbaseRole::RegionServer)
                    .cloned()
                    .context(NoRegionServerRoleSnafu)?,
            ),
        ),
    ]);

    if let Some(rest_servers) = hbase.get_role(&HbaseRole::RestServer) {
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
        .map(|(variable, value)| format!("export {variable}=\"{value}\"\n"))
        .collect()
}

pub fn error_policy(_obj: Arc<HbaseCluster>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

pub fn build_recommended_labels<'a>(
    owner: &'a HbaseCluster,
    app_version: &'a str,
    role: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, HbaseCluster> {
    ObjectLabels {
        owner,
        app_name: APP_NAME,
        app_version,
        operator_name: OPERATOR_NAME,
        controller_name: HBASE_CONTROLLER_NAME,
        role,
        role_group,
    }
}
