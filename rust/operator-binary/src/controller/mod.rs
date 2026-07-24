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
    k8s_openapi::{
        api::{
            apps::v1::StatefulSet,
            core::v1::{ConfigMap, Service, ServiceAccount},
            policy::v1::PodDisruptionBudget,
            rbac::v1::RoleBinding,
        },
        apimachinery::pkg::apis::meta::v1::ObjectMeta,
    },
    kube::Resource,
    kvp::Labels,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        builder::meta::ownerreference_from_resource,
        kvp::label::{recommended_labels, role_group_selector},
        role_group_utils::ResourceNames,
        role_utils,
        types::{
            kubernetes::{ConfigMapName, NamespaceName, SecretClassName, Uid},
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

/// The complete set of Kubernetes resources built for a [`ValidatedCluster`], ready to be applied.
///
/// hbase exposes its listeners as volume/PVC sources inside the `StatefulSet` rather than as
/// top-level `Listener` objects, so (unlike some sibling operators) there is no `listeners` field.
pub struct KubernetesResources {
    pub stateful_sets: Vec<StatefulSet>,
    pub services: Vec<Service>,
    pub config_maps: Vec<ConfigMap>,
    pub pod_disruption_budgets: Vec<PodDisruptionBudget>,
    pub service_accounts: Vec<ServiceAccount>,
    pub role_bindings: Vec<RoleBinding>,
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
    pub role_group_configs: BTreeMap<HbaseRole, BTreeMap<RoleGroupName, HbaseRoleGroupConfig>>,
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
        role_group_configs: BTreeMap<HbaseRole, BTreeMap<RoleGroupName, HbaseRoleGroupConfig>>,
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

    /// Type-safe names for the per-cluster RBAC resources: the ServiceAccount shared by all
    /// Pods, its (namespaced) RoleBinding, and the operator-deployed ClusterRole it binds.
    pub fn cluster_resource_names(&self) -> role_utils::ResourceNames {
        role_utils::ResourceNames {
            cluster_name: self.name.clone(),
            product_name: product_name(),
        }
    }

    /// Type-safe names for the resources of a given role group.
    pub(crate) fn role_group_resource_names(
        &self,
        hbase_role: &HbaseRole,
        role_group_name: &RoleGroupName,
    ) -> ResourceNames {
        ResourceNames {
            cluster_name: self.name.clone(),
            role_name: hbase_role.into(),
            role_group_name: role_group_name.clone(),
        }
    }

    /// Recommended labels for a role-group resource.
    pub fn recommended_labels(&self, role: &HbaseRole, role_group_name: &RoleGroupName) -> Labels {
        self.recommended_labels_for(&role.into(), role_group_name)
    }

    /// Recommended labels for a resource that is not tied to a concrete [`HbaseRole`] (e.g. the
    /// Kubernetes executor pod template), using a free-form role/role-group label value.
    pub fn recommended_labels_for(
        &self,
        role_name: &RoleName,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        self.recommended_labels_with(&self.product_version, role_name, role_group_name)
    }

    fn recommended_labels_with(
        &self,
        product_version: &ProductVersion,
        role_name: &RoleName,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        recommended_labels(
            self,
            &product_name(),
            product_version,
            &operator_name(),
            &controller_name(),
            role_name,
            role_group_name,
        )
    }

    /// Selector labels matching the pods of a role group.
    pub fn role_group_selector(
        &self,
        hbase_role: &HbaseRole,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        role_group_selector(self, &product_name(), &hbase_role.into(), role_group_name)
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
    pub fn has_kerberos_enabled(&self) -> bool {
        self.cluster_config.kerberos_secret_class.is_some()
    }

    /// Whether HTTPS is enabled for this cluster.
    ///
    /// Derived from the validated config (a TLS `SecretClass` was configured).
    pub fn has_https_enabled(&self) -> bool {
        self.cluster_config.https_secret_class.is_some()
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
    /// The Kerberos `SecretClass` name, if Kerberos is enabled.
    pub kerberos_secret_class: Option<SecretClassName>,
    /// The HTTPS/TLS `SecretClass` name, if HTTPS is enabled.
    pub https_secret_class: Option<SecretClassName>,
    /// The HDFS discovery ConfigMap name the cluster connects to.
    pub hdfs_config_map_name: ConfigMapName,
    // Pre-resolved zookeeper connection settings.
    pub zookeeper_connection_information: ZookeeperConnectionInformation,
}

/// Per-role configuration extracted during validation.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: stackable_operator::commons::pdb::PdbConfig,
}

/// The validated per-rolegroup product configuration: the merged CRD config and the resolved
/// logging settings. The merged (role <- role group) `configOverrides`, `envOverrides` and
/// `podOverrides` live on the enclosing [`HbaseRoleGroupConfig`] (the `RoleGroupConfig` wrapper),
/// not here.
#[derive(Clone, Debug)]
pub struct ValidatedHbaseConfig {
    /// The merged, role-specific product config.
    pub config: AnyServiceConfig,
    /// The validated logging configuration (HBase + optional Vector container), resolved up-front
    /// during validation.
    pub logging: validate::ValidatedLogging,
}

pub type HbaseRoleGroupConfig = stackable_operator::v2::role_utils::RoleGroupConfig<
    ValidatedHbaseConfig,
    stackable_operator::v2::role_utils::JavaCommonConfig,
    v1alpha1::HbaseConfigOverrides,
>;

#[cfg(test)]
mod tests {
    use stackable_operator::v2::types::operator::RoleName;
    use strum::IntoEnumIterator;

    use crate::crd::HbaseRole;

    /// Locks the invariant behind the `expect` in the `From<HbaseRole> for RoleName` impls:
    /// every `HbaseRole` variant (present and future) must serialise to a valid `RoleName`.
    #[test]
    fn every_hbase_role_serialises_to_a_valid_role_name() {
        for role in HbaseRole::iter() {
            let _: RoleName = (&role).into();
            let _: RoleName = role.into();
        }
    }
}
