mod error;
use crate::error::Error;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use kube::api::{ListParams, ResourceExt};
use kube::Api;
use product_config::types::PropertyNameKind;
use product_config::ProductConfigManager;
use stackable_hbase_crd::commands::{Restart, Start, Stop};
use stackable_hbase_crd::{
    HbaseCluster, HbaseClusterSpec, HbaseRole, HbaseVersion, APP_NAME, CONFIG_MAP_TYPE_DATA,
    CORE_SITE_XML, FS_DEFAULT_FS, HBASE_MASTER_PORT, HBASE_MASTER_WEB_UI_PORT,
    HBASE_REGION_SERVER_PORT, HBASE_REGION_SERVER_WEB_UI_PORT, HBASE_ROOT_DIR, HBASE_SITE_XML,
    HBASE_ZOOKEEPER_QUORUM, HTTP_PORT, METRICS_PORT, RPC_PORT,
};
use stackable_hdfs_crd::discovery::HdfsConnectionInformation;
use stackable_operator::builder::{
    ContainerBuilder, ContainerPortBuilder, ObjectMetaBuilder, PodBuilder,
};
use stackable_operator::client::Client;
use stackable_operator::command::materialize_command;
use stackable_operator::configmap;
use stackable_operator::controller::Controller;
use stackable_operator::controller::{ControllerStrategy, ReconciliationState};
use stackable_operator::error::OperatorResult;
use stackable_operator::identity::{LabeledPodIdentityFactory, PodIdentity, PodToNodeMapping};
use stackable_operator::labels;
use stackable_operator::labels::{
    build_common_labels_for_all_managed_resources, get_recommended_labels,
};
use stackable_operator::name_utils;
use stackable_operator::product_config_utils::{
    config_for_role_and_group, transform_all_roles_to_config, validate_all_roles_and_groups_config,
    ValidatedRoleConfigByPropertyKind,
};
use stackable_operator::reconcile::{
    ContinuationStrategy, ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_operator::role_utils;
use stackable_operator::role_utils::{
    get_role_and_group_labels, list_eligible_nodes_for_role_and_group, EligibleNodesForRoleAndGroup,
};
use stackable_operator::scheduler::{
    K8SUnboundedHistory, RoleGroupEligibleNodes, ScheduleStrategy, Scheduler, StickyScheduler,
};
use stackable_operator::status::HasClusterExecutionStatus;
use stackable_operator::status::{init_status, ClusterExecutionStatus};
use stackable_operator::versioning::{finalize_versioning, init_versioning};
use stackable_zookeeper_crd::discovery::ZookeeperConnectionInformation;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use strum::IntoEnumIterator;
use tracing::error;
use tracing::{debug, info, trace, warn};

const FINALIZER_NAME: &str = "hbase.stackable.tech/cleanup";
const ID_LABEL: &str = "hbase.stackable.tech/id";
const SHOULD_BE_SCRAPED: &str = "monitoring.stackable.tech/should_be_scraped";

const CONFIG_DIR_NAME: &str = "conf";

type HbaseReconcileResult = ReconcileResult<error::Error>;

struct HbaseState {
    context: ReconciliationContext<HbaseCluster>,
    existing_pods: Vec<Pod>,
    eligible_nodes: EligibleNodesForRoleAndGroup,
    validated_role_config: ValidatedRoleConfigByPropertyKind,
    zookeeper_info: Option<ZookeeperConnectionInformation>,
    hdfs_info: Option<HdfsConnectionInformation>,
}

impl HbaseState {
    /// Required labels for pods. Pods without any of these will deleted and/or replaced.
    pub fn get_required_labels(&self) -> BTreeMap<String, Option<Vec<String>>> {
        let roles = HbaseRole::iter()
            .map(|role| role.to_string())
            .collect::<Vec<_>>();
        let mut mandatory_labels = BTreeMap::new();

        mandatory_labels.insert(labels::APP_COMPONENT_LABEL.to_string(), Some(roles));
        mandatory_labels.insert(
            labels::APP_INSTANCE_LABEL.to_string(),
            Some(vec![self.context.name()]),
        );
        mandatory_labels.insert(
            labels::APP_VERSION_LABEL.to_string(),
            Some(vec![self.context.resource.spec.version.to_string()]),
        );
        mandatory_labels.insert(ID_LABEL.to_string(), None);

        mandatory_labels
    }

    async fn get_zookeeper_connection_information(&mut self) -> HbaseReconcileResult {
        let zk_ref: &stackable_zookeeper_crd::discovery::ZookeeperReference =
            &self.context.resource.spec.zookeeper_reference;

        if let Some(chroot) = zk_ref.chroot.as_deref() {
            stackable_zookeeper_crd::discovery::is_valid_zookeeper_path(chroot)?;
        }

        let zookeeper_info = stackable_zookeeper_crd::discovery::get_zk_connection_info(
            &self.context.client,
            zk_ref,
        )
        .await?;

        debug!(
            "Received ZooKeeper connection information: [{}]",
            &zookeeper_info.connection_string
        );

        self.zookeeper_info = Some(zookeeper_info);

        Ok(ReconcileFunctionAction::Continue)
    }

    async fn get_hdfs_connection_information(&mut self) -> HbaseReconcileResult {
        let hdfs_ref: &stackable_hdfs_crd::discovery::HdfsReference =
            &self.context.resource.spec.hdfs_reference;

        let hdfs_info =
            stackable_hdfs_crd::discovery::get_hdfs_connection_info(&self.context.client, hdfs_ref)
                .await?;

        debug!("Received HBase connection information: [{:?}]", &hdfs_info);

        self.hdfs_info = hdfs_info;

        Ok(ReconcileFunctionAction::Continue)
    }

    /// Will initialize the status object if it's never been set.
    async fn init_status(&mut self) -> HbaseReconcileResult {
        // init status with default values if not available yet.
        self.context.resource = init_status(&self.context.client, &self.context.resource).await?;

        let spec_version = self.context.resource.spec.version.clone();

        self.context.resource =
            init_versioning(&self.context.client, &self.context.resource, spec_version).await?;

        // set the cluster status to running
        if self.context.resource.cluster_execution_status().is_none() {
            self.context
                .client
                .merge_patch_status(
                    &self.context.resource,
                    &self
                        .context
                        .resource
                        .cluster_execution_status_patch(&ClusterExecutionStatus::Running),
                )
                .await?;
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    pub async fn create_missing_pods(&mut self) -> HbaseReconcileResult {
        trace!("Starting `create_missing_pods`");
        // The iteration happens in two stages here, to accommodate the way our operators think
        // about roles and role groups.
        // The hierarchy is:
        // - Roles (for HBase there are masters and region_servers)
        //   - Role groups for this role (user defined)
        for hbase_role in HbaseRole::iter() {
            if let Some(nodes_for_role) = self.eligible_nodes.get(&hbase_role.to_string()) {
                for (role_group, eligible_nodes) in nodes_for_role {
                    debug!(
                        "Identify missing pods for [{}] role and group [{}]",
                        hbase_role, role_group
                    );
                    trace!(
                        "candidate_nodes[{}]: [{:?}]",
                        eligible_nodes.nodes.len(),
                        eligible_nodes
                            .nodes
                            .iter()
                            .map(|node| node.metadata.name.as_ref().unwrap())
                            .collect::<Vec<_>>()
                    );
                    trace!(
                        "existing_pods[{}]: [{:?}]",
                        &self.existing_pods.len(),
                        &self
                            .existing_pods
                            .iter()
                            .map(|pod| pod.metadata.name.as_ref().unwrap())
                            .collect::<Vec<_>>()
                    );
                    trace!(
                        "labels: [{:?}]",
                        get_role_and_group_labels(&hbase_role.to_string(), role_group)
                    );

                    let mut history = match self
                        .context
                        .resource
                        .status
                        .as_ref()
                        .and_then(|status| status.history.as_ref())
                    {
                        Some(simple_history) => {
                            // we clone here because we cannot access mut self because we need it later
                            // to create config maps and pods. The `status` history will be out of sync
                            // with the cloned `simple_history` until the next reconcile.
                            // The `status` history should not be used after this method to avoid side
                            // effects.
                            K8SUnboundedHistory::new(&self.context.client, simple_history.clone())
                        }
                        None => K8SUnboundedHistory::new(
                            &self.context.client,
                            PodToNodeMapping::default(),
                        ),
                    };

                    let mut scheduler =
                        StickyScheduler::new(&mut history, ScheduleStrategy::GroupAntiAffinity);

                    let pod_id_factory = LabeledPodIdentityFactory::new(
                        APP_NAME,
                        &self.context.name(),
                        &self.eligible_nodes,
                        ID_LABEL,
                        1,
                    );

                    let state = scheduler.schedule(
                        &pod_id_factory,
                        &RoleGroupEligibleNodes::from(&self.eligible_nodes),
                        &self.existing_pods,
                    )?;

                    let mapping = state.remaining_mapping().filter(
                        APP_NAME,
                        &self.context.name(),
                        &hbase_role.to_string(),
                        role_group,
                    );

                    if let Some((pod_id, node_id)) = mapping.iter().next() {
                        // now we have a node that needs a pod -> get validated config
                        let validated_config = config_for_role_and_group(
                            pod_id.role(),
                            pod_id.group(),
                            &self.validated_role_config,
                        )?;

                        let config_maps = self
                            .create_config_maps(pod_id, validated_config, &state.mapping())
                            .await?;

                        self.create_pod(pod_id, &node_id.name, &config_maps, validated_config)
                            .await?;

                        history.save(&self.context.resource).await?;

                        return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
                    }
                }
            }
        }

        // If we reach here it means all pods must be running on target_version.
        // We can now set current_version to target_version (if target_version was set) and
        // target_version to None
        finalize_versioning(&self.context.client, &self.context.resource).await?;
        Ok(ReconcileFunctionAction::Continue)
    }

    /// Creates the config maps required for a hbase instance (or role, role_group combination):
    /// * The 'hbase-site.xml' properties file
    /// * The 'core-site.xml' properties file
    ///
    /// The 'hbase-site.xml' properties are read from the product_config and/or merged with the
    /// cluster custom resource.
    ///
    /// Labels are automatically adapted from the `recommended_labels` with the type "data".
    /// Names are generated via `name_utils::build_resource_name`.
    ///
    /// Returns a map with a 'type' identifier (e.g. data, id) as key and the corresponding
    /// ConfigMap as value. This is required to set the volume mounts in the pod later on.
    ///
    /// # Arguments
    ///
    /// - `pod_id` - The `PodIdentity` containing app, instance, role, group names and the id.
    /// - `validated_config` - The validated product config.
    /// - `id_mapping` - All id to node mappings required to create config maps
    ///
    async fn create_config_maps(
        &self,
        pod_id: &PodIdentity,
        validated_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
        _id_mapping: &PodToNodeMapping,
    ) -> Result<HashMap<&'static str, ConfigMap>, Error> {
        let mut config_maps = HashMap::new();
        let mut config_maps_data = BTreeMap::new();

        let recommended_labels = get_recommended_labels(
            &self.context.resource,
            pod_id.app(),
            &self.context.resource.spec.version.to_string(),
            pod_id.role(),
            pod_id.group(),
        );

        // zk discovery
        let zk_info = match &self.zookeeper_info {
            Some(zookeeper_info) => zookeeper_info,
            None => return Err(error::Error::ZookeeperConnectionInformationError),
        };

        // hdfs discovery
        let hdfs_info = match &self.hdfs_info {
            Some(hdfs_info) => hdfs_info,
            None => return Err(error::Error::HdfsConnectionInformationError),
        };

        for (property_name_kind, config) in validated_config {
            match property_name_kind {
                PropertyNameKind::File(file_name) if file_name == CORE_SITE_XML => {
                    let mut data = BTreeMap::new();

                    data.insert(
                        FS_DEFAULT_FS.to_string(),
                        Some(hdfs_info.connection_string()),
                    );

                    for (property_name, property_value) in config {
                        data.insert(property_name.to_string(), Some(property_value.to_string()));
                    }

                    config_maps_data.insert(
                        file_name.clone(),
                        product_config::writer::to_hadoop_xml(data.iter()),
                    );
                }
                PropertyNameKind::File(file_name) if file_name == HBASE_SITE_XML => {
                    let mut data = BTreeMap::new();

                    // hdfs discovery
                    data.insert(
                        HBASE_ROOT_DIR.to_string(),
                        Some(hdfs_info.full_connection_string()),
                    );
                    // zk discovery
                    data.insert(
                        HBASE_ZOOKEEPER_QUORUM.to_string(),
                        Some(zk_info.connection_string.clone()),
                    );

                    // // TODO: move to product config properties
                    // data.insert(
                    //     HBASE_CLUSTER_DISTRIBUTED.to_string(),
                    //     Some("true".to_string()),
                    // );

                    for (property_name, property_value) in config {
                        data.insert(property_name.to_string(), Some(property_value.to_string()));
                    }

                    config_maps_data.insert(
                        file_name.clone(),
                        product_config::writer::to_hadoop_xml(data.iter()),
                    );
                }
                _ => {}
            }
        }

        // enhance with config map type label
        let mut cm_config_data_labels = recommended_labels.clone();
        cm_config_data_labels.insert(
            configmap::CONFIGMAP_TYPE_LABEL.to_string(),
            CONFIG_MAP_TYPE_DATA.to_string(),
        );

        let cm_data_name = name_utils::build_resource_name(
            pod_id.app(),
            pod_id.instance(),
            pod_id.role(),
            Some(pod_id.group()),
            None,
            Some(CONFIG_MAP_TYPE_DATA),
        )?;

        let cm = configmap::build_config_map(
            &self.context.resource,
            &cm_data_name,
            &self.context.namespace(),
            cm_config_data_labels,
            config_maps_data,
        )?;

        config_maps.insert(
            CONFIG_MAP_TYPE_DATA,
            configmap::create_config_map(&self.context.client, cm).await?,
        );

        Ok(config_maps)
    }

    /// Creates the pod required for the hbase instance.
    ///
    /// # Arguments
    ///
    /// - `pod_id` - The `PodIdentity` containing app, instance, role, group names and the id.
    /// - `node_name` - The node_name for this pod.
    /// - `config_maps` - The config maps and respective types required for this pod.
    /// - `validated_config` - The validated product config.
    ///
    async fn create_pod(
        &self,
        pod_id: &PodIdentity,
        node_name: &str,
        config_maps: &HashMap<&'static str, ConfigMap>,
        validated_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    ) -> Result<Pod, Error> {
        let version: &HbaseVersion = &self.context.resource.spec.version;

        let mut metrics_port: Option<&String> = None;
        let mut master_rpc_port: Option<&String> = None;
        let mut master_web_ui_port: Option<&String> = None;
        let mut region_server_rpc_port: Option<&String> = None;
        let mut region_server_web_ui_port: Option<&String> = None;

        let pod_name = name_utils::build_resource_name(
            pod_id.app(),
            pod_id.instance(),
            pod_id.role(),
            Some(pod_id.group()),
            Some(node_name),
            None,
        )?;

        let mut container_builder = ContainerBuilder::new(APP_NAME);
        container_builder.image(format!("stackable/hbase:{}", version.to_string()));
        container_builder.command(vec![HbaseRole::from_str(pod_id.role())?.command(version)]);

        for (property_name_kind, config) in validated_config {
            match property_name_kind {
                PropertyNameKind::File(file_name) if file_name == HBASE_SITE_XML => {
                    // we need to extract the master rpc port here to add to container ports later
                    master_rpc_port = config.get(HBASE_MASTER_PORT);
                    // we need to extract the master web ui port here to add to container ports later
                    master_web_ui_port = config.get(HBASE_MASTER_WEB_UI_PORT);
                    // we need to extract the region server rpc port here to add to container ports later
                    region_server_rpc_port = config.get(HBASE_REGION_SERVER_PORT);
                    // we need to extract the region server web ui port here to add to container ports later
                    region_server_web_ui_port = config.get(HBASE_REGION_SERVER_WEB_UI_PORT);
                }
                PropertyNameKind::Env => {
                    for (property_name, property_value) in config {
                        if property_name.is_empty() {
                            warn!("Received empty property_name for ENV... skipping");
                            continue;
                        }
                        // if a metrics port is provided (for now by user, it is not required in
                        // product config to be able to not configure any monitoring / metrics)
                        if property_name == METRICS_PORT {
                            metrics_port = Some(property_value);
                            container_builder.add_env_var(
                                "HBASE_OPTS".to_string(), 
                                format!("-javaagent:{{{{packageroot}}}}/{}/stackable/lib/jmx_prometheus_javaagent-0.16.1.jar={}:{{{{packageroot}}}}/{}/stackable/conf/jmx_exporter.yaml",
                                              version.package_name(), property_value, version.package_name()));
                            continue;
                        }

                        container_builder.add_env_var(property_name, property_value);
                    }
                }
                _ => {}
            }
        }

        // add the config dir
        container_builder.add_env_var(
            "HBASE_CONF_DIR".to_string(),
            format!("{{{{configroot}}}}/{}", CONFIG_DIR_NAME),
        );

        // One mount for the config directory
        if let Some(config_map_data) = config_maps.get(CONFIG_MAP_TYPE_DATA) {
            if let Some(name) = config_map_data.metadata.name.as_ref() {
                container_builder.add_configmapvolume(name, CONFIG_DIR_NAME.to_string());
            } else {
                return Err(error::Error::MissingConfigMapNameError {
                    cm_type: HBASE_SITE_XML,
                });
            }
        } else {
            return Err(error::Error::MissingConfigMapError {
                cm_type: HBASE_SITE_XML,
                pod_name,
            });
        }

        let mut annotations = BTreeMap::new();
        // only add metrics container port and annotation if available
        if let Some(metrics_port) = metrics_port {
            annotations.insert(SHOULD_BE_SCRAPED.to_string(), "true".to_string());
            container_builder.add_container_port(
                ContainerPortBuilder::new(metrics_port.parse()?)
                    .name("metrics")
                    .build(),
            );
        }

        match HbaseRole::from_str(pod_id.role())? {
            // add master container ports
            HbaseRole::Master => {
                if let Some(master_rpc_port) = &master_rpc_port {
                    container_builder.add_container_port(
                        ContainerPortBuilder::new(master_rpc_port.parse()?)
                            .name(RPC_PORT)
                            .build(),
                    );
                }

                // add admin port if available
                if let Some(master_web_ui_port) = master_web_ui_port {
                    container_builder.add_container_port(
                        ContainerPortBuilder::new(master_web_ui_port.parse()?)
                            .name(HTTP_PORT)
                            .build(),
                    );
                }
            }
            // add region server container ports
            HbaseRole::RegionServer => {
                if let Some(region_server_rpc_port) = region_server_rpc_port {
                    container_builder.add_container_port(
                        ContainerPortBuilder::new(region_server_rpc_port.parse()?)
                            .name(RPC_PORT)
                            .build(),
                    );
                }

                // add admin port if available
                if let Some(region_server_web_ui_port) = region_server_web_ui_port {
                    container_builder.add_container_port(
                        ContainerPortBuilder::new(region_server_web_ui_port.parse()?)
                            .name(HTTP_PORT)
                            .build(),
                    );
                }
            }
        }

        let mut pod_labels = get_recommended_labels(
            &self.context.resource,
            pod_id.app(),
            &self.context.resource.spec.version.to_string(),
            pod_id.role(),
            pod_id.group(),
        );

        // we need to add the id to the labels
        pod_labels.insert(ID_LABEL.to_string(), pod_id.id().to_string());

        let pod = PodBuilder::new()
            .metadata(
                ObjectMetaBuilder::new()
                    .generate_name(pod_name)
                    .namespace(&self.context.client.default_namespace)
                    .with_labels(pod_labels)
                    .with_annotations(annotations)
                    .ownerreference_from_resource(&self.context.resource, Some(true), Some(true))?
                    .build()?,
            )
            .add_stackable_agent_tolerations()
            .add_container(container_builder.build())
            .node_name(node_name)
            .build()?;

        Ok(self.context.client.create(&pod).await?)
    }

    async fn delete_all_pods(&self) -> OperatorResult<ReconcileFunctionAction> {
        for pod in &self.existing_pods {
            self.context.client.delete(pod).await?;
        }
        Ok(ReconcileFunctionAction::Done)
    }

    pub async fn process_command(&mut self) -> HbaseReconcileResult {
        match self.context.retrieve_current_command().await? {
            // if there is no new command and the execution status is stopped we stop the
            // reconcile loop here.
            None => match self.context.resource.cluster_execution_status() {
                Some(execution_status) if execution_status == ClusterExecutionStatus::Stopped => {
                    Ok(ReconcileFunctionAction::Done)
                }
                _ => Ok(ReconcileFunctionAction::Continue),
            },
            Some(command_ref) => match command_ref.kind.as_str() {
                "Restart" => {
                    info!("Restarting cluster [{:?}]", command_ref);
                    let mut restart_command: Restart =
                        materialize_command(&self.context.client, &command_ref).await?;
                    Ok(self.context.default_restart(&mut restart_command).await?)
                }
                "Start" => {
                    info!("Starting cluster [{:?}]", command_ref);
                    let mut start_command: Start =
                        materialize_command(&self.context.client, &command_ref).await?;
                    Ok(self.context.default_start(&mut start_command).await?)
                }
                "Stop" => {
                    info!("Stopping cluster [{:?}]", command_ref);
                    let mut stop_command: Stop =
                        materialize_command(&self.context.client, &command_ref).await?;

                    Ok(self.context.default_stop(&mut stop_command).await?)
                }
                _ => {
                    error!("Got unknown type of command: [{:?}]", command_ref);
                    Ok(ReconcileFunctionAction::Done)
                }
            },
        }
    }
}

impl ReconciliationState for HbaseState {
    type Error = error::Error;

    fn reconcile(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + Send + '_>>
    {
        info!("========================= Starting reconciliation =========================");

        Box::pin(async move {
            self.init_status()
                .await?
                .then(self.context.handle_deletion(
                    Box::pin(self.delete_all_pods()),
                    FINALIZER_NAME,
                    true,
                ))
                .await?
                .then(self.get_zookeeper_connection_information())
                .await?
                .then(self.get_hdfs_connection_information())
                .await?
                .then(self.context.delete_illegal_pods(
                    self.existing_pods.as_slice(),
                    &self.get_required_labels(),
                    ContinuationStrategy::OneRequeue,
                ))
                .await?
                .then(
                    self.context
                        .wait_for_terminating_pods(self.existing_pods.as_slice()),
                )
                .await?
                .then(
                    self.context
                        .wait_for_running_and_ready_pods(&self.existing_pods),
                )
                .await?
                .then(self.process_command())
                .await?
                .then(self.context.delete_excess_pods(
                    list_eligible_nodes_for_role_and_group(&self.eligible_nodes).as_slice(),
                    &self.existing_pods,
                    ContinuationStrategy::OneRequeue,
                ))
                .await?
                .then(self.create_missing_pods())
                .await
        })
    }
}

struct HbaseStrategy {
    config: Arc<ProductConfigManager>,
}

impl HbaseStrategy {
    pub fn new(config: ProductConfigManager) -> HbaseStrategy {
        HbaseStrategy {
            config: Arc::new(config),
        }
    }
}

#[async_trait]
impl ControllerStrategy for HbaseStrategy {
    type Item = HbaseCluster;
    type State = HbaseState;
    type Error = Error;

    /// Init the Hbase state. Store all available pods owned by this cluster for later processing.
    /// Retrieve nodes that fit selectors and store them for later processing:
    /// HbaseRole (we only have 'server') -> role group -> list of nodes.
    async fn init_reconcile_state(
        &self,
        context: ReconciliationContext<Self::Item>,
    ) -> Result<Self::State, Self::Error> {
        let existing_pods = context
            .list_owned(build_common_labels_for_all_managed_resources(
                APP_NAME,
                &context.resource.name(),
            ))
            .await?;
        trace!(
            "{}: Found [{}] pods",
            context.log_name(),
            existing_pods.len()
        );

        let hbase_spec: HbaseClusterSpec = context.resource.spec.clone();

        let mut eligible_nodes = HashMap::new();

        eligible_nodes.insert(
            HbaseRole::Master.to_string(),
            role_utils::find_nodes_that_fit_selectors(&context.client, None, &hbase_spec.masters)
                .await?,
        );

        eligible_nodes.insert(
            HbaseRole::RegionServer.to_string(),
            role_utils::find_nodes_that_fit_selectors(
                &context.client,
                None,
                &hbase_spec.region_servers,
            )
            .await?,
        );

        let mut roles = HashMap::new();
        roles.insert(
            HbaseRole::Master.to_string(),
            (
                vec![
                    PropertyNameKind::File(HBASE_SITE_XML.to_string()),
                    PropertyNameKind::File(CORE_SITE_XML.to_string()),
                    PropertyNameKind::Env,
                ],
                context.resource.spec.masters.clone().into(),
            ),
        );
        roles.insert(
            HbaseRole::RegionServer.to_string(),
            (
                vec![
                    PropertyNameKind::File(HBASE_SITE_XML.to_string()),
                    PropertyNameKind::File(CORE_SITE_XML.to_string()),
                    PropertyNameKind::Env,
                ],
                context.resource.spec.region_servers.clone().into(),
            ),
        );

        let role_config = transform_all_roles_to_config(&context.resource, roles);
        let validated_role_config = validate_all_roles_and_groups_config(
            &context.resource.spec.version.to_string(),
            &role_config,
            &self.config,
            false,
            false,
        )?;

        Ok(HbaseState {
            context,
            existing_pods,
            eligible_nodes,
            validated_role_config,
            zookeeper_info: None,
            hdfs_info: None,
        })
    }
}

/// This creates an instance of a [`Controller`] which waits for incoming events and reconciles them.
///
/// This is an async method and the returned future needs to be consumed to make progress.
pub async fn create_controller(client: Client, product_config_path: &str) -> OperatorResult<()> {
    let api: Api<HbaseCluster> = client.get_all_api();
    let pods_api: Api<Pod> = client.get_all_api();
    let config_maps_api: Api<ConfigMap> = client.get_all_api();
    let cmd_restart_api: Api<Restart> = client.get_all_api();
    let cmd_start_api: Api<Start> = client.get_all_api();
    let cmd_stop_api: Api<Stop> = client.get_all_api();

    let controller = Controller::new(api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default())
        .owns(cmd_restart_api, ListParams::default())
        .owns(cmd_start_api, ListParams::default())
        .owns(cmd_stop_api, ListParams::default());

    let product_config = ProductConfigManager::from_yaml_file(product_config_path).unwrap();

    let strategy = HbaseStrategy::new(product_config);

    controller
        .run(client, strategy, Duration::from_secs(10))
        .await;

    Ok(())
}
