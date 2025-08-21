//! Ensures that `Pod`s are configured and running for each [`v1alpha1::HbaseCluster`]

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write,
    str::FromStr,
    sync::Arc,
};

use const_format::concatcp;
use indoc::formatdoc;
use product_config::{
    ProductConfigManager,
    types::PropertyNameKind,
    writer::{PropertiesWriterError, to_hadoop_xml, to_java_properties_string},
};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder, container::ContainerBuilder, resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder,
        },
    },
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        product_image_selection::{self, ResolvedProductImage},
        rbac::build_rbac_resources,
    },
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapVolumeSource, ContainerPort, EnvVar, Probe, Service,
                ServiceAccount, ServicePort, ServiceSpec, TCPSocketAction, Volume,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    kube::{
        Resource, ResourceExt,
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    kvp::{Label, LabelError, Labels, ObjectLabels},
    logging::controller::ReconcilerError,
    memory::{BinaryMultiple, MemoryQuantity},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    product_logging::{
        self,
        framework::LoggingError,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::{GenericRoleConfig, RoleGroupRef},
    shared::time::Duration,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    utils::cluster_info::KubernetesClusterInfo,
};
use strum::{EnumDiscriminants, IntoStaticStr, ParseError};

use crate::{
    OPERATOR_NAME,
    config::jvm::{
        construct_global_jvm_args, construct_hbase_heapsize_env,
        construct_role_specific_non_heap_jvm_args,
    },
    crd::{
        APP_NAME, AnyServiceConfig, Container, HBASE_ENV_SH, HBASE_MASTER_PORT,
        HBASE_MASTER_UI_PORT, HBASE_REGIONSERVER_PORT, HBASE_REGIONSERVER_UI_PORT,
        HBASE_REST_PORT_NAME_HTTP, HBASE_REST_PORT_NAME_HTTPS, HBASE_SITE_XML, HbaseClusterStatus,
        HbaseRole, JVM_SECURITY_PROPERTIES_FILE, LISTENER_VOLUME_DIR, LISTENER_VOLUME_NAME,
        SSL_CLIENT_XML, SSL_SERVER_XML, merged_env, v1alpha1,
    },
    discovery::build_discovery_configmap,
    kerberos::{
        self, add_kerberos_pod_config, kerberos_config_properties, kerberos_ssl_client_settings,
        kerberos_ssl_server_settings,
    },
    operations::{graceful_shutdown::add_graceful_shutdown_config, pdb::add_pdbs},
    product_logging::{
        CONTAINERDEBUG_LOG_DIRECTORY, STACKABLE_LOG_DIR, extend_role_group_config_map,
    },
    security::{self, opa::HbaseOpaConfig},
    zookeeper::{self, ZookeeperConnectionInformation},
};

pub const HBASE_CONTROLLER_NAME: &str = "hbasecluster";
pub const FULL_HBASE_CONTROLLER_NAME: &str = concatcp!(HBASE_CONTROLLER_NAME, '.', OPERATOR_NAME);
pub const MAX_HBASE_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

// These constants are hard coded in hbase-entrypoint.sh
// You need to change them there too.
const HDFS_DISCOVERY_TMP_DIR: &str = "/stackable/tmp/hdfs";
const HBASE_CONFIG_TMP_DIR: &str = "/stackable/tmp/hbase";
const HBASE_LOG_CONFIG_TMP_DIR: &str = "/stackable/tmp/log_config";

const DOCKER_IMAGE_BASE_NAME: &str = "hbase";

const HBASE_MASTER_PORT_NAME: &str = "master";
const HBASE_REGIONSERVER_PORT_NAME: &str = "regionserver";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("invalid role properties"))]
    RoleProperties { source: crate::crd::Error },

    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("object defines no version"))]
    ObjectHasNoVersion,

    #[snafu(display("object defines no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("object defines no master role"))]
    NoMasterRole,

    #[snafu(display("the HBase role [{role}] is missing from spec"))]
    MissingHbaseRole { role: String },

    #[snafu(display("object defines no regionserver role"))]
    NoRegionServerRole,

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::HbaseCluster>,
    },

    #[snafu(display("failed to apply discovery configmap"))]
    ApplyDiscoveryConfigMap {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build discovery configmap"))]
    BuildDiscoveryConfigMap { source: super::discovery::Error },

    #[snafu(display("failed to build ConfigMap for {}", rolegroup))]
    BuildRoleGroupConfig {
        source: stackable_operator::builder::configmap::Error,
        rolegroup: RoleGroupRef<v1alpha1::HbaseCluster>,
    },

    #[snafu(display("failed to apply ConfigMap for {}", rolegroup))]
    ApplyRoleGroupConfig {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::HbaseCluster>,
    },

    #[snafu(display("failed to apply StatefulSet for {}", rolegroup))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::HbaseCluster>,
    },

    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("failed to retrieve zookeeper connection information"))]
    RetrieveZookeeperConnectionInformation { source: zookeeper::Error },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("no configmap_name for {cm_name} discovery is configured"))]
    MissingConfigMap {
        source: stackable_operator::builder::meta::Error,
        cm_name: String,
    },

    #[snafu(display("failed to retrieve the entry {entry} for config map {cm_name}"))]
    MissingConfigMapEntry {
        entry: &'static str,
        cm_name: String,
    },

    #[snafu(display("failed to patch service account"))]
    ApplyServiceAccount {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to patch role binding"))]
    ApplyRoleBinding {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("could not parse Hbase role [{role}]"))]
    UnidentifiedHbaseRole {
        source: strum::ParseError,
        role: String,
    },

    #[snafu(display("failed to retrieve Hbase role group: {source}"))]
    UnidentifiedHbaseRoleGroup { source: crate::crd::Error },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crate::crd::Error },

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,

    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },

    #[snafu(display("failed to add kerberos config"))]
    AddKerberosConfig { source: kerberos::Error },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display(
        "failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for {}",
        rolegroup
    ))]
    SerializeJvmSecurity {
        source: PropertiesWriterError,
        rolegroup: RoleGroupRef<v1alpha1::HbaseCluster>,
    },

    #[snafu(display("failed to create PodDisruptionBudget"))]
    FailedToCreatePdb {
        source: crate::operations::pdb::Error,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::operations::graceful_shutdown::Error,
    },

    #[snafu(display("failed to build label"))]
    BuildLabel { source: LabelError },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("invalid OPA configuration"))]
    InvalidOpaConfig { source: security::opa::Error },

    #[snafu(display("unknown role [{role}]"))]
    UnknownHbaseRole { source: ParseError, role: String },

    #[snafu(display("authorization is only supported from HBase 2.6 onwards"))]
    AuthorizationNotSupported,

    #[snafu(display("failed to configure logging"))]
    ConfigureLogging { source: LoggingError },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("HBaseCluster object is invalid"))]
    InvalidHBaseCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to construct HBASE_HEAPSIZE env variable"))]
    ConstructHbaseHeapsizeEnv { source: crate::config::jvm::Error },

    #[snafu(display("failed to construct JVM arguments"))]
    ConstructJvmArgument { source: crate::config::jvm::Error },

    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("failed to build listener volume"))]
    ListenerVolume { source: crate::crd::Error },

    #[snafu(display("failed to build listener persistent volume claim"))]
    ListenerPersistentVolumeClaim { source: crate::crd::Error },

    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_hbase(
    hbase: Arc<DeserializeGuard<v1alpha1::HbaseCluster>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let hbase = hbase
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidHBaseClusterSnafu)?;

    let client = &ctx.client;

    let resolved_product_image = hbase
        .spec
        .image
        .resolve(DOCKER_IMAGE_BASE_NAME, crate::built_info::PKG_VERSION)
        .context(ResolveProductImageSnafu)?;
    let zookeeper_connection_information = ZookeeperConnectionInformation::retrieve(hbase, client)
        .await
        .context(RetrieveZookeeperConnectionInformationSnafu)?;

    let roles = hbase.build_role_properties().context(RolePropertiesSnafu)?;

    let validated_config = validate_all_roles_and_groups_config(
        &resolved_product_image.app_version_label_value,
        &transform_all_roles_to_config(hbase, roles).context(GenerateProductConfigSnafu)?,
        &ctx.product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let hbase_opa_config = match &hbase.spec.cluster_config.authorization {
        Some(opa_config) => Some(
            HbaseOpaConfig::from_opa_config(client, hbase, opa_config)
                .await
                .context(InvalidOpaConfigSnafu)?,
        ),
        None => None,
    };

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        HBASE_CONTROLLER_NAME,
        &hbase.object_ref(&()),
        ClusterResourceApplyStrategy::from(&hbase.spec.cluster_operation),
    )
    .context(CreateClusterResourcesSnafu)?;

    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
        hbase,
        APP_NAME,
        cluster_resources
            .get_required_labels()
            .context(BuildLabelSnafu)?,
    )
    .context(BuildRbacResourcesSnafu)?;
    cluster_resources
        .add(client, rbac_sa.clone())
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

            let merged_config = hbase
                .merged_config(
                    &hbase_role,
                    &rolegroup.role_group,
                    &hbase.spec.cluster_config.hdfs_config_map_name,
                )
                .context(FailedToResolveConfigSnafu)?;

            let rg_service =
                build_rolegroup_service(hbase, &hbase_role, &rolegroup, &resolved_product_image)?;
            let rg_configmap = build_rolegroup_config_map(
                hbase,
                &client.kubernetes_cluster_info,
                &rolegroup,
                rolegroup_config,
                &zookeeper_connection_information,
                &merged_config,
                &resolved_product_image,
                hbase_opa_config.as_ref(),
            )?;
            let rg_statefulset = build_rolegroup_statefulset(
                hbase,
                &hbase_role,
                &rolegroup,
                rolegroup_config,
                &merged_config,
                &resolved_product_image,
                &rbac_sa,
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
            add_pdbs(pdb, hbase, &hbase_role, client, &mut cluster_resources)
                .await
                .context(FailedToCreatePdbSnafu)?;
        }
    }

    // Discovery CM will fail to build until the rest of the cluster has been deployed, so do it last
    // so that failure won't inhibit the rest of the cluster from booting up.
    let discovery_cm = build_discovery_configmap(
        hbase,
        &client.kubernetes_cluster_info,
        &zookeeper_connection_information,
        &resolved_product_image,
    )
    .context(BuildDiscoveryConfigMapSnafu)?;
    cluster_resources
        .add(client, discovery_cm)
        .await
        .context(ApplyDiscoveryConfigMapSnafu)?;

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&hbase.spec.cluster_operation);

    let status = HbaseClusterStatus {
        conditions: compute_conditions(hbase, &[&ss_cond_builder, &cluster_operation_cond_builder]),
    };

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;
    client
        .apply_patch_status(OPERATOR_NAME, hbase, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::await_change())
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
#[allow(clippy::too_many_arguments)]
fn build_rolegroup_config_map(
    hbase: &v1alpha1::HbaseCluster,
    cluster_info: &KubernetesClusterInfo,
    rolegroup: &RoleGroupRef<v1alpha1::HbaseCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    zookeeper_connection_information: &ZookeeperConnectionInformation,
    merged_config: &AnyServiceConfig,
    resolved_product_image: &ResolvedProductImage,
    hbase_opa_config: Option<&HbaseOpaConfig>,
) -> Result<ConfigMap, Error> {
    let mut hbase_site_xml = String::new();
    let mut hbase_env_sh = String::new();
    let mut ssl_server_xml = String::new();
    let mut ssl_client_xml = String::new();

    let hbase_role =
        HbaseRole::from_str(rolegroup.role.as_ref()).with_context(|_| UnknownHbaseRoleSnafu {
            role: rolegroup.role.clone(),
        })?;

    for (property_name_kind, config) in rolegroup_config {
        match property_name_kind {
            PropertyNameKind::File(file_name) if file_name == HBASE_SITE_XML => {
                let mut hbase_site_config = BTreeMap::new();
                hbase_site_config.extend(zookeeper_connection_information.as_hbase_settings());
                hbase_site_config.extend(
                    kerberos_config_properties(hbase, cluster_info)
                        .context(AddKerberosConfigSnafu)?,
                );
                hbase_site_config
                    .extend(hbase_opa_config.map_or(vec![], |config| config.hbase_site_config()));

                // Set flag to override default behaviour, which is that the
                // RPC client should bind the client address (forcing outgoing
                // RPC traffic to happen from the same network interface that
                // the RPC server is bound on).
                hbase_site_config.insert(
                    "hbase.client.rpc.bind.address".to_string(),
                    "false".to_string(),
                );

                match hbase_role {
                    HbaseRole::Master => {
                        hbase_site_config.insert(
                            "hbase.master.ipc.address".to_string(),
                            "0.0.0.0".to_string(),
                        );
                        hbase_site_config.insert(
                            "hbase.master.ipc.port".to_string(),
                            HBASE_MASTER_PORT.to_string(),
                        );
                        hbase_site_config.insert(
                            "hbase.master.hostname".to_string(),
                            "${HBASE_SERVICE_HOST}".to_string(),
                        );
                        hbase_site_config.insert(
                            "hbase.master.port".to_string(),
                            "${HBASE_SERVICE_PORT}".to_string(),
                        );
                        hbase_site_config.insert(
                            "hbase.master.info.port".to_string(),
                            "${HBASE_INFO_PORT}".to_string(),
                        );
                        hbase_site_config.insert(
                            "hbase.master.bound.info.port".to_string(),
                            HBASE_MASTER_UI_PORT.to_string(),
                        );
                    }
                    HbaseRole::RegionServer => {
                        hbase_site_config.insert(
                            "hbase.regionserver.ipc.address".to_string(),
                            "0.0.0.0".to_string(),
                        );
                        hbase_site_config.insert(
                            "hbase.regionserver.ipc.port".to_string(),
                            HBASE_REGIONSERVER_PORT.to_string(),
                        );
                        hbase_site_config.insert(
                            "hbase.unsafe.regionserver.hostname".to_string(),
                            "${HBASE_SERVICE_HOST}".to_string(),
                        );
                        hbase_site_config.insert(
                            "hbase.regionserver.port".to_string(),
                            "${HBASE_SERVICE_PORT}".to_string(),
                        );
                        hbase_site_config.insert(
                            "hbase.regionserver.info.port".to_string(),
                            "${HBASE_INFO_PORT}".to_string(),
                        );
                        hbase_site_config.insert(
                            "hbase.regionserver.bound.info.port".to_string(),
                            HBASE_REGIONSERVER_UI_PORT.to_string(),
                        );
                    }
                    HbaseRole::RestServer => {}
                };

                // configOverride come last
                hbase_site_config.extend(config.clone());
                hbase_site_xml = to_hadoop_xml(
                    hbase_site_config
                        .into_iter()
                        .map(|(k, v)| (k, Some(v)))
                        .collect::<BTreeMap<_, _>>()
                        .iter(),
                );
            }
            PropertyNameKind::File(file_name) if file_name == HBASE_ENV_SH => {
                let mut hbase_env_config =
                    build_hbase_env_sh(hbase, merged_config, &hbase_role, &rolegroup.role_group)?;

                // configOverride come last
                hbase_env_config.extend(config.clone());
                hbase_env_sh = write_hbase_env_sh(hbase_env_config.iter());
            }
            PropertyNameKind::File(file_name) if file_name == SSL_SERVER_XML => {
                let mut config_opts = BTreeMap::new();
                config_opts.extend(kerberos_ssl_server_settings(hbase));

                // configOverride come last
                config_opts.extend(config.clone());
                ssl_server_xml = to_hadoop_xml(
                    config_opts
                        .into_iter()
                        .map(|(k, v)| (k, Some(v)))
                        .collect::<BTreeMap<_, _>>()
                        .iter(),
                );
            }
            PropertyNameKind::File(file_name) if file_name == SSL_CLIENT_XML => {
                let mut config_opts = BTreeMap::new();
                config_opts.extend(kerberos_ssl_client_settings(hbase));

                // configOverride come last
                config_opts.extend(config.clone());
                ssl_client_xml = to_hadoop_xml(
                    config_opts
                        .into_iter()
                        .map(|(k, v)| (k, Some(v)))
                        .collect::<BTreeMap<_, _>>()
                        .iter(),
                );
            }
            _ => {}
        }
    }

    let jvm_sec_props: BTreeMap<String, Option<String>> = rolegroup_config
        .get(&PropertyNameKind::File(
            JVM_SECURITY_PROPERTIES_FILE.to_string(),
        ))
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect();
    let jvm_sec_props = to_java_properties_string(jvm_sec_props.iter()).with_context(|_| {
        SerializeJvmSecuritySnafu {
            rolegroup: rolegroup.clone(),
        }
    })?;

    let mut builder = ConfigMapBuilder::new();

    let cm_metadata = ObjectMetaBuilder::new()
        .name_and_namespace(hbase)
        .name(rolegroup.object_name())
        .ownerreference_from_resource(hbase, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(build_recommended_labels(
            hbase,
            &resolved_product_image.app_version_label_value,
            &rolegroup.role,
            &rolegroup.role_group,
        ))
        .context(ObjectMetaSnafu)?
        .build();

    builder
        .metadata(cm_metadata)
        .add_data(HBASE_SITE_XML, hbase_site_xml)
        .add_data(HBASE_ENV_SH, hbase_env_sh)
        .add_data(JVM_SECURITY_PROPERTIES_FILE, jvm_sec_props);

    // HBase does not like empty config files:
    // Caused by: com.ctc.wstx.exc.WstxEOFException: Unexpected EOF in prolog at [row,col,system-id]: [1,0,"file:/stackable/conf/ssl-server.xml"]
    if !ssl_server_xml.is_empty() {
        builder.add_data(SSL_SERVER_XML, ssl_server_xml);
    }
    if !ssl_client_xml.is_empty() {
        builder.add_data(SSL_CLIENT_XML, ssl_client_xml);
    }

    extend_role_group_config_map(rolegroup, merged_config.logging(), &mut builder).context(
        InvalidLoggingConfigSnafu {
            cm_name: rolegroup.object_name(),
        },
    )?;

    builder.build().map_err(|e| Error::BuildRoleGroupConfig {
        source: e,
        rolegroup: rolegroup.clone(),
    })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_rolegroup_service(
    hbase: &v1alpha1::HbaseCluster,
    hbase_role: &HbaseRole,
    rolegroup: &RoleGroupRef<v1alpha1::HbaseCluster>,
    resolved_product_image: &ResolvedProductImage,
) -> Result<Service> {
    let ports = hbase
        .ports(hbase_role)
        .into_iter()
        .map(|(name, value)| ServicePort {
            name: Some(name),
            port: i32::from(value),
            protocol: Some("TCP".to_string()),
            ..ServicePort::default()
        })
        .collect();

    let prometheus_label =
        Label::try_from(("prometheus.io/scrape", "true")).context(BuildLabelSnafu)?;

    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(hbase)
        .name(headless_service_name(&rolegroup.object_name()))
        .ownerreference_from_resource(hbase, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(build_recommended_labels(
            hbase,
            &resolved_product_image.app_version_label_value,
            &rolegroup.role,
            &rolegroup.role_group,
        ))
        .context(ObjectMetaSnafu)?
        .with_label(prometheus_label)
        .build();

    let service_selector =
        Labels::role_group_selector(hbase, APP_NAME, &rolegroup.role, &rolegroup.role_group)
            .context(BuildLabelSnafu)?;

    let service_spec = ServiceSpec {
        // Internal communication does not need to be exposed
        type_: Some("ClusterIP".to_string()),
        cluster_ip: Some("None".to_string()),
        ports: Some(ports),
        selector: Some(service_selector.into()),
        publish_not_ready_addresses: Some(true),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata,
        spec: Some(service_spec),
        status: None,
    })
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding [`Service`] (from [`build_rolegroup_service`]).
#[allow(clippy::too_many_arguments)]
fn build_rolegroup_statefulset(
    hbase: &v1alpha1::HbaseCluster,
    hbase_role: &HbaseRole,
    rolegroup_ref: &RoleGroupRef<v1alpha1::HbaseCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    merged_config: &AnyServiceConfig,
    resolved_product_image: &ResolvedProductImage,
    service_account: &ServiceAccount,
) -> Result<StatefulSet> {
    let hbase_version = &resolved_product_image.app_version_label_value;

    let ports = hbase
        .ports(hbase_role)
        .into_iter()
        .map(|(name, value)| ContainerPort {
            name: Some(name),
            container_port: i32::from(value),
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        })
        .collect();

    let probe_template = match hbase_role {
        HbaseRole::Master => Probe {
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::String(HBASE_MASTER_PORT_NAME.to_string()),
                ..TCPSocketAction::default()
            }),
            ..Probe::default()
        },
        HbaseRole::RegionServer => Probe {
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::String(HBASE_REGIONSERVER_PORT_NAME.to_string()),
                ..TCPSocketAction::default()
            }),
            ..Probe::default()
        },
        HbaseRole::RestServer => Probe {
            // We cant use HTTPGetAction, as it returns a 401 in case kerberos is enabled, and there is currently no way
            // to tell Kubernetes an 401 is healthy. As an alternative we run curl ourselves and check the http status
            // code there.
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::String(
                    if hbase.has_https_enabled() {
                        HBASE_REST_PORT_NAME_HTTPS
                    } else {
                        HBASE_REST_PORT_NAME_HTTP
                    }
                    .to_string(),
                ),
                ..TCPSocketAction::default()
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

    let mut merged_env = merged_env(rolegroup_config.get(&PropertyNameKind::Env));
    // This env var is set for all roles to avoid bash's "unbound variable" errors
    merged_env.extend([
        EnvVar {
            name: "REGION_MOVER_OPTS".to_string(),
            value: Some(merged_config.region_mover_args()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "RUN_REGION_MOVER".to_string(),
            value: Some(merged_config.run_region_mover().to_string()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "STACKABLE_LOG_DIR".to_string(),
            value: Some(STACKABLE_LOG_DIR.to_string()),
            ..EnvVar::default()
        },
    ]);

    let role_name = hbase_role.cli_role_name();
    let mut hbase_container = ContainerBuilder::new("hbase").expect("ContainerBuilder not created");

    let rest_http_port_name = if hbase.has_https_enabled() {
        HBASE_REST_PORT_NAME_HTTPS
    } else {
        HBASE_REST_PORT_NAME_HTTP
    };

    hbase_container
        .image_from_product_image(resolved_product_image)
        .command(command())
        .args(vec![formatdoc! {"
            {entrypoint} {role} {port} {port_name} {ui_port_name}",
            entrypoint = "/stackable/hbase/bin/hbase-entrypoint.sh".to_string(),
            role = role_name,
            port = hbase.service_port(hbase_role).to_string(),
            port_name = match hbase_role {
                HbaseRole::Master => HBASE_MASTER_PORT_NAME,
                HbaseRole::RegionServer => HBASE_REGIONSERVER_PORT_NAME,
                HbaseRole::RestServer => rest_http_port_name,
            },
            ui_port_name = hbase.ui_port_name(),
        }])
        .add_env_vars(merged_env)
        // Needed for the `containerdebug` process to log it's tracing information to.
        .add_env_var(
            "CONTAINERDEBUG_LOG_DIRECTORY",
            &*CONTAINERDEBUG_LOG_DIRECTORY,
        )
        .add_volume_mount("hbase-config", HBASE_CONFIG_TMP_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("hdfs-discovery", HDFS_DISCOVERY_TMP_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log-config", HBASE_LOG_CONFIG_TMP_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log", STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(LISTENER_VOLUME_NAME, LISTENER_VOLUME_DIR)
        .context(AddVolumeMountSnafu)?
        .add_container_ports(ports)
        .resources(merged_config.resources().clone().into())
        .startup_probe(startup_probe)
        .liveness_probe(liveness_probe)
        .readiness_probe(readiness_probe);

    let mut pod_builder = PodBuilder::new();

    let recommended_object_labels = build_recommended_labels(
        hbase,
        hbase_version,
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    );
    let recommended_labels =
        Labels::recommended(recommended_object_labels.clone()).context(LabelBuildSnafu)?;

    let pb_metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(recommended_object_labels)
        .context(ObjectMetaSnafu)?
        .build();

    pod_builder
        .metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .affinity(merged_config.affinity())
        .add_volume(stackable_operator::k8s_openapi::api::core::v1::Volume {
            name: "hbase-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: rolegroup_ref.object_name(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .context(AddVolumeSnafu)?
        .add_volume(stackable_operator::k8s_openapi::api::core::v1::Volume {
            name: "hdfs-discovery".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: hbase.spec.cluster_config.hdfs_config_map_name.clone(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .context(AddVolumeSnafu)?
        .add_empty_dir_volume(
            "log",
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_HBASE_LOG_FILES_SIZE],
            )),
        )
        .context(AddVolumeSnafu)?
        .service_account_name(service_account.name_any())
        .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());

    if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = merged_config.logging().containers.get(&Container::Hbase)
    {
        pod_builder
            .add_volume(Volume {
                name: "log-config".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: config_map.into(),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            })
            .context(AddVolumeSnafu)?;
    } else {
        pod_builder
            .add_volume(Volume {
                name: "log-config".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: rolegroup_ref.object_name(),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            })
            .context(AddVolumeSnafu)?;
    }

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;
    if hbase.has_kerberos_enabled() {
        add_kerberos_pod_config(
            hbase,
            &mut hbase_container,
            &mut pod_builder,
            merged_config
                .requested_secret_lifetime()
                .context(MissingSecretLifetimeSnafu)?,
        )
        .context(AddKerberosConfigSnafu)?;
    }
    pod_builder.add_container(hbase_container.build());

    // Vector sidecar shall be the last container in the list
    if merged_config.logging().enable_vector_agent {
        if let Some(vector_aggregator_config_map_name) =
            &hbase.spec.cluster_config.vector_aggregator_config_map_name
        {
            pod_builder.add_container(
                product_logging::framework::vector_container(
                    resolved_product_image,
                    "hbase-config",
                    "log",
                    merged_config.logging().containers.get(&Container::Vector),
                    ResourceRequirementsBuilder::new()
                        .with_cpu_request("250m")
                        .with_cpu_limit("500m")
                        .with_memory_request("128Mi")
                        .with_memory_limit("128Mi")
                        .build(),
                    vector_aggregator_config_map_name,
                )
                .context(ConfigureLoggingSnafu)?,
            );
        } else {
            VectorAggregatorConfigMapMissingSnafu.fail()?;
        }
    }

    let listener_pvc = hbase_role
        .listener_pvc(merged_config, &recommended_labels)
        .context(ListenerPersistentVolumeClaimSnafu)?;

    if let Some(listener_volume) = hbase_role
        .listener_volume(merged_config, &recommended_labels)
        .context(ListenerVolumeSnafu)?
    {
        pod_builder
            .add_volume(listener_volume)
            .context(AddVolumeSnafu)?;
    };

    let mut pod_template = pod_builder.build_template();

    hbase.merge_pod_overrides(&mut pod_template, hbase_role, rolegroup_ref);

    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(hbase)
        .name(rolegroup_ref.object_name())
        .ownerreference_from_resource(hbase, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(build_recommended_labels(
            hbase,
            hbase_version,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        .context(ObjectMetaSnafu)?
        .build();

    let statefulset_match_labels = Labels::role_group_selector(
        hbase,
        APP_NAME,
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    )
    .context(BuildLabelSnafu)?;

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some("Parallel".to_string()),
        replicas: hbase.replicas(hbase_role, rolegroup_ref),
        selector: LabelSelector {
            match_labels: Some(statefulset_match_labels.into()),
            ..LabelSelector::default()
        },
        service_name: Some(headless_service_name(&rolegroup_ref.object_name())),
        template: pod_template,
        volume_claim_templates: listener_pvc,
        ..StatefulSetSpec::default()
    };

    Ok(StatefulSet {
        metadata,
        spec: Some(statefulset_spec),
        status: None,
    })
}

/// Returns the container command.
fn command() -> Vec<String> {
    vec![
        "/bin/bash".to_string(),
        "-x".to_string(),
        "-euo".to_string(),
        "pipefail".to_string(),
        "-c".to_string(),
    ]
}

fn write_hbase_env_sh<'a, T>(properties: T) -> String
where
    T: Iterator<Item = (&'a String, &'a String)>,
{
    properties.fold(String::new(), |mut output, (variable, value)| {
        let _ = writeln!(output, "export {variable}=\"{value}\"");
        output
    })
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::HbaseCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        // root object is invalid, will be requed when modified
        Error::InvalidHBaseCluster { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}

pub fn build_recommended_labels<'a>(
    owner: &'a v1alpha1::HbaseCluster,
    app_version: &'a str,
    role: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, v1alpha1::HbaseCluster> {
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

/// The content of the HBase `hbase-env.sh` file.
fn build_hbase_env_sh(
    hbase: &v1alpha1::HbaseCluster,
    merged_config: &AnyServiceConfig,
    hbase_role: &HbaseRole,
    role_group: &str,
) -> Result<BTreeMap<String, String>, Error> {
    let mut result = BTreeMap::new();

    result.insert("HBASE_MANAGES_ZK".to_string(), "false".to_string());

    result.insert(
        "HBASE_HEAPSIZE".to_owned(),
        construct_hbase_heapsize_env(merged_config).context(ConstructHbaseHeapsizeEnvSnafu)?,
    );
    result.insert(
        "HBASE_OPTS".to_owned(),
        construct_global_jvm_args(hbase.has_kerberos_enabled()),
    );
    let role_specific_non_heap_jvm_args =
        construct_role_specific_non_heap_jvm_args(hbase, hbase_role, role_group)
            .context(ConstructJvmArgumentSnafu)?;

    match hbase_role {
        HbaseRole::Master => {
            result.insert(
                "HBASE_MASTER_OPTS".to_string(),
                role_specific_non_heap_jvm_args,
            );
        }
        HbaseRole::RegionServer => {
            result.insert(
                "HBASE_REGIONSERVER_OPTS".to_string(),
                role_specific_non_heap_jvm_args,
            );
        }
        HbaseRole::RestServer => {
            result.insert(
                "HBASE_REST_OPTS".to_string(),
                role_specific_non_heap_jvm_args,
            );
        }
    }

    Ok(result)
}

fn headless_service_name(role_group_name: &str) -> String {
    format!("{name}-headless", name = role_group_name)
}

#[cfg(test)]
mod test {
    use rstest::rstest;
    use stackable_operator::kube::runtime::reflector::ObjectRef;

    use super::*;

    #[rstest]
    #[case("2.6.1", HbaseRole::Master, vec!["master", "ui-http"])]
    #[case("2.6.1", HbaseRole::RegionServer, vec!["regionserver", "ui-http"])]
    #[case("2.6.1", HbaseRole::RestServer, vec!["rest-http", "ui-http"])]
    #[case("2.6.2", HbaseRole::Master, vec!["master", "ui-http"])]
    #[case("2.6.2", HbaseRole::RegionServer, vec!["regionserver", "ui-http"])]
    #[case("2.6.2", HbaseRole::RestServer, vec!["rest-http", "ui-http"])]
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

        let resolved_image = ResolvedProductImage {
            image: format!("oci.stackable.tech/sdp/hbase:{hbase_version}-stackable0.0.0-dev"),
            app_version_label_value: hbase_version
                .parse()
                .expect("test: hbase version is always valid"),
            product_version: hbase_version.to_string(),
            image_pull_policy: "Never".to_string(),
            pull_secrets: None,
        };

        let role_group_ref = RoleGroupRef {
            cluster: ObjectRef::<v1alpha1::HbaseCluster>::from_obj(&hbase),
            role: role.to_string(),
            role_group: "default".to_string(),
        };
        let service = build_rolegroup_service(&hbase, &role, &role_group_ref, &resolved_image)
            .expect("failed to build service");

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
