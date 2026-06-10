pub mod build;
pub mod dereference;
pub mod validate;

use std::collections::BTreeMap;

use stackable_operator::{
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::{api::core::v1::PodTemplateSpec, apimachinery::pkg::apis::meta::v1::ObjectMeta},
    kube::Resource,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        builder::pod::container::EnvVarSet,
        types::{
            kubernetes::{NamespaceName, Uid},
            operator::ClusterName,
        },
    },
};

use crate::{
    crd::{AnyServiceConfig, HbaseRole, v1alpha1},
    security::opa::HbaseOpaConfig,
    zookeeper::ZookeeperConnectionInformation,
};

/// The validated cluster: proves that config merging and validation succeeded for
/// every role and role group before any resources are created.
#[derive(Clone, Debug)]
pub struct ValidatedCluster {
    /// Backs the [`Resource`] implementation (provides `meta()`/`name_any()`) so the build
    /// functions can derive `ObjectMeta`, owner references and labels without the full
    /// `HbaseCluster` object. Holds only name, namespace and uid.
    metadata: ObjectMeta,
    /// The logical (and Kubernetes object) name of the cluster.
    pub name: ClusterName,
    /// The namespace the cluster lives in. Part of the cluster identity; currently consumed via
    /// the [`Resource`] metadata (`name_and_namespace`) rather than read directly.
    #[allow(dead_code)]
    pub namespace: NamespaceName,
    /// The UID of the `HbaseCluster` object, used to build owner references.
    pub uid: Uid,
    pub image: ResolvedProductImage,
    pub cluster_config: ValidatedClusterConfig,
    pub role_group_configs: BTreeMap<HbaseRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
    pub role_configs: BTreeMap<HbaseRole, ValidatedRoleConfig>,
}

impl ValidatedCluster {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: ClusterName,
        namespace: NamespaceName,
        uid: Uid,
        image: ResolvedProductImage,
        cluster_config: ValidatedClusterConfig,
        role_group_configs: BTreeMap<HbaseRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
        role_configs: BTreeMap<HbaseRole, ValidatedRoleConfig>,
    ) -> Self {
        Self {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                uid: Some(uid.to_string()),
                ..ObjectMeta::default()
            },
            name,
            namespace,
            uid,
            image,
            cluster_config,
            role_group_configs,
            role_configs,
        }
    }
}

impl Resource for ValidatedCluster {
    type DynamicType = <v1alpha1::HbaseCluster as Resource>::DynamicType;
    type Scope = <v1alpha1::HbaseCluster as Resource>::Scope;

    fn group(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HbaseCluster::group(dt)
    }

    fn version(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HbaseCluster::version(dt)
    }

    fn kind(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HbaseCluster::kind(dt)
    }

    fn plural(dt: &Self::DynamicType) -> std::borrow::Cow<'_, str> {
        v1alpha1::HbaseCluster::plural(dt)
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

impl HasName for ValidatedCluster {
    fn to_name(&self) -> String {
        self.name.to_string()
    }
}

impl HasUid for ValidatedCluster {
    fn to_uid(&self) -> Uid {
        self.uid.clone()
    }
}

impl NameIsValidLabelValue for ValidatedCluster {
    fn to_label_value(&self) -> String {
        self.name.to_label_value()
    }
}

/// Cluster-wide settings resolved once during validation.
#[derive(Clone, Debug)]
pub struct ValidatedClusterConfig {
    // Pre-resolved OPA connection configuration.
    pub hbase_opa_config: Option<HbaseOpaConfig>,
    pub kerberos_enabled: bool,
    // Pre-resolved kerberos properties for hbase-site.xml (empty when kerberos is disabled).
    pub hbase_site_kerberos_config: BTreeMap<String, String>,
    // Pre-resolved kerberos properties for the discovery `hbase-site.xml` exposed to clients
    // (empty when kerberos is disabled).
    pub discovery_kerberos_config: BTreeMap<String, String>,
    // Pre-resolved ssl-server.xml settings (empty when HTTPS is disabled).
    pub ssl_server_settings: BTreeMap<String, String>,
    // Pre-resolved ssl-client.xml settings (empty when HTTPS is disabled).
    pub ssl_client_settings: BTreeMap<String, String>,
    // Pre-resolved zookeeper connection settings.
    pub zookeeper_connection_information: ZookeeperConnectionInformation,
}

/// Per-role configuration extracted during validation.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: stackable_operator::commons::pdb::PdbConfig,
}

/// Per-rolegroup configuration: the merged CRD config plus the merged
/// (role <- role group) `configOverrides`, `envOverrides` and `podOverrides`.
///
/// This carries every override channel so that the build step is a pure function of
/// [`ValidatedCluster`] and never has to reach back into the raw `HbaseCluster`.
#[derive(Clone, Debug)]
pub struct ValidatedRoleGroupConfig {
    /// The desired number of replicas (`None` lets Kubernetes default to 1).
    pub replicas: Option<u16>,
    pub merged_config: AnyServiceConfig,
    pub config_overrides: v1alpha1::HbaseConfigOverrides,
    pub env_overrides: EnvVarSet,
    /// Merged (role <- role group) pod template overrides.
    pub pod_overrides: PodTemplateSpec,
    /// Pre-resolved role-specific non-heap JVM args (operator-generated + role/role-group overrides).
    pub non_heap_jvm_args: String,
}
