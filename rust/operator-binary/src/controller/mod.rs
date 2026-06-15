pub mod build;
pub mod dereference;
pub mod validate;
pub mod zookeeper;

use std::{collections::BTreeMap, str::FromStr};

use const_format::concatcp;
pub use stackable_operator::v2::types::operator::RoleGroupName;
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::{api::core::v1::PodTemplateSpec, apimachinery::pkg::apis::meta::v1::ObjectMeta},
    kube::Resource,
    kvp::Labels,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        builder::{meta::ownerreference_from_resource, pod::container::EnvVarSet},
        kvp::label::{recommended_labels, role_group_selector},
        role_group_utils::ResourceNames,
        types::{
            kubernetes::{NamespaceName, Uid},
            operator::{
                ClusterName, ControllerName, OperatorName, ProductName, ProductVersion, RoleName,
            },
        },
    },
};

use crate::{
    controller::{build::opa::HbaseOpaConfig, zookeeper::ZookeeperConnectionInformation},
    crd::{APP_NAME, AnyServiceConfig, HbaseRole, OPERATOR_NAME, v1alpha1},
};

pub const HBASE_CONTROLLER_NAME: &str = "hbasecluster";
pub const FULL_HBASE_CONTROLLER_NAME: &str = concatcp!(HBASE_CONTROLLER_NAME, '.', OPERATOR_NAME);

/// The product name (`hbase`) as a type-safe label value.
pub(crate) fn product_name() -> ProductName {
    ProductName::from_str(APP_NAME).expect("'hbase' is a valid product name")
}

/// The operator name as a type-safe label value.
pub(crate) fn operator_name() -> OperatorName {
    OperatorName::from_str(OPERATOR_NAME).expect("the operator name is a valid label value")
}

/// The controller name as a type-safe label value.
pub(crate) fn controller_name() -> ControllerName {
    ControllerName::from_str(HBASE_CONTROLLER_NAME)
        .expect("the controller name is a valid label value")
}

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
    /// The namespace the cluster lives in.
    pub namespace: NamespaceName,
    /// The UID of the `HbaseCluster` object, used to build owner references.
    pub uid: Uid,
    pub image: ResolvedProductImage,
    /// The product version as a valid label value, used for the recommended
    /// `app.kubernetes.io/version` label. Derived from the resolved image's app version label
    /// value.
    pub product_version: ProductVersion,
    pub cluster_config: ValidatedClusterConfig,
    pub role_group_configs: BTreeMap<HbaseRole, BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>>,
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
        role_group_configs: BTreeMap<HbaseRole, BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>>,
        role_configs: BTreeMap<HbaseRole, ValidatedRoleConfig>,
    ) -> Self {
        // `app_version_label_value` is constructed to be a valid label value, so it is also a
        // valid `ProductVersion`.
        let product_version = ProductVersion::from_str(&image.app_version_label_value)
            .expect("the app version label value is a valid product version");
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
            product_version,
            cluster_config,
            role_group_configs,
            role_configs,
        }
    }

    /// The Kubernetes role name for an [`HbaseRole`] (e.g. `master`, `regionserver`,
    /// `restserver`).
    pub fn role_name(hbase_role: &HbaseRole) -> RoleName {
        RoleName::from_str(&hbase_role.to_string()).expect("an HbaseRole name is a valid role name")
    }

    /// Type-safe names for the resources of a given role group.
    pub(crate) fn resource_names(
        &self,
        hbase_role: &HbaseRole,
        role_group_name: &RoleGroupName,
    ) -> ResourceNames {
        ResourceNames {
            cluster_name: self.name.clone(),
            role_name: Self::role_name(hbase_role),
            role_group_name: role_group_name.clone(),
        }
    }

    /// Recommended labels for a role-group resource.
    pub fn recommended_labels(
        &self,
        hbase_role: &HbaseRole,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        recommended_labels(
            self,
            &product_name(),
            &self.product_version,
            &operator_name(),
            &controller_name(),
            &Self::role_name(hbase_role),
            role_group_name,
        )
    }

    /// Selector labels matching the pods of a role group.
    pub fn role_group_selector(
        &self,
        hbase_role: &HbaseRole,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        role_group_selector(
            self,
            &product_name(),
            &Self::role_name(hbase_role),
            role_group_name,
        )
    }

    /// Returns an [`ObjectMetaBuilder`] pre-filled with the namespace, an owner reference back to
    /// this cluster, and the recommended labels for a resource named `name` in `role_group_name`.
    ///
    /// Consolidates the metadata chain repeated by the child-resource builders. Call sites that
    /// need extra labels/annotations chain them onto the returned builder.
    pub(crate) fn object_meta(
        &self,
        name: impl Into<String>,
        hbase_role: &HbaseRole,
        role_group_name: &RoleGroupName,
    ) -> ObjectMetaBuilder {
        let mut builder = ObjectMetaBuilder::new();
        builder
            .name_and_namespace(self)
            .name(name)
            .ownerreference(ownerreference_from_resource(self, None, Some(true)))
            .with_labels(self.recommended_labels(hbase_role, role_group_name));
        builder
    }

    /// Whether Kerberos is enabled for this cluster.
    ///
    /// Mirrors [`v1alpha1::HbaseCluster::has_kerberos_enabled`], derived here from the validated
    /// config so build steps don't have to re-read the raw cluster.
    pub fn has_kerberos_enabled(&self) -> bool {
        self.cluster_config.kerberos_enabled
    }

    /// Whether HTTPS is enabled for this cluster.
    pub fn has_https_enabled(&self) -> bool {
        self.cluster_config.https_enabled
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
    /// Whether HTTPS is enabled (a TLS `SecretClass` was configured).
    pub https_enabled: bool,
    /// The Kerberos `SecretClass` name, if Kerberos is enabled.
    pub kerberos_secret_class: Option<String>,
    /// The HTTPS/TLS `SecretClass` name, if HTTPS is enabled.
    pub https_secret_class: Option<String>,
    /// The HDFS discovery ConfigMap name the cluster connects to.
    pub hdfs_config_map_name: String,
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
#[derive(Clone, Debug)]
pub struct ValidatedHbaseConfig {
    /// The merged, role-specific product config.
    pub config: AnyServiceConfig,
    /// The validated logging configuration (HBase + optional Vector container), resolved up-front
    /// during validation.
    pub logging: validate::ValidatedLogging,
}

#[derive(Clone, Debug)]
pub struct ValidatedRoleGroupConfig {
    /// The desired number of replicas (defaulted to 1 during validation).
    pub replicas: u16,
    pub config: ValidatedHbaseConfig,
    pub config_overrides: v1alpha1::HbaseConfigOverrides,
    pub env_overrides: EnvVarSet,
    /// Merged (role <- role group) pod template overrides.
    pub pod_overrides: PodTemplateSpec,
    /// Pre-resolved role-specific non-heap JVM args (operator-generated + role/role-group overrides).
    pub non_heap_jvm_args: String,
}
