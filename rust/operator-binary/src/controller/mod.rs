pub mod apply;
pub mod build;
pub mod dereference;
pub mod update_status;
pub mod validate;
pub mod zookeeper;

use std::{collections::BTreeMap, marker::PhantomData, ops::Deref, str::FromStr};

use const_format::concatcp;
pub use stackable_operator::v2::types::operator::RoleGroupName;
use stackable_operator::{
    commons::product_image_selection::ResolvedProductImage,
    constant,
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
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        builder::pod::container::EnvVarName,
        config_overrides::KeyValueConfigOverrides,
        role_group_utils::ResourceNames,
        role_utils::{self, JavaCommonConfig, RoleGroupConfig},
        types::{
            kubernetes::{ConfigMapName, NamespaceName, SecretClassName, Uid},
            operator::{ClusterName, ControllerName, OperatorName, ProductName, ProductVersion},
        },
    },
};

use crate::{
    controller::{
        build::opa::HbaseOpaConfig, validate::ValidatedLogging,
        zookeeper::ZookeeperConnectionInformation,
    },
    crd::{APP_NAME, HBASE_OPERATOR_NAME, HbaseConfig, HbaseRole, RegionServerConfig, v1alpha1},
};

pub const HBASE_CONTROLLER_NAME: &str = "hbasecluster";
pub const FULL_HBASE_CONTROLLER_NAME: &str =
    concatcp!(HBASE_CONTROLLER_NAME, '.', HBASE_OPERATOR_NAME);

constant!(PRODUCT_NAME: ProductName = APP_NAME);
constant!(OPERATOR_NAME: OperatorName = HBASE_OPERATOR_NAME);
constant!(CONTROLLER_NAME: ControllerName = HBASE_CONTROLLER_NAME);

pub(crate) const DEFAULT_REPLICAS: u16 = 1;

/// Marker for prepared Kubernetes resources which are not applied yet.
pub struct Prepared;

/// Marker for applied Kubernetes resources.
pub struct Applied;

pub(crate) fn replicas_or_default(replicas: Option<u16>) -> u16 {
    replicas.unwrap_or(DEFAULT_REPLICAS)
}

/// The complete set of Kubernetes resources built for a [`ValidatedCluster`], ready to be applied.
///
/// hbase exposes its listeners as volume/PVC sources inside the `StatefulSet` rather than as
/// top-level `Listener` objects, so (unlike some sibling operators) there is no `listeners` field.
///
/// `T` is a marker that indicates if these resources are only [`Prepared`] or already [`Applied`].
/// The marker is useful e.g. to ensure that the cluster status is updated based on the applied
/// resources.
pub struct KubernetesResources<T> {
    pub stateful_sets: Vec<StatefulSet>,
    pub services: Vec<Service>,
    pub config_maps: Vec<ConfigMap>,
    pub pod_disruption_budgets: Vec<PodDisruptionBudget>,
    pub service_accounts: Vec<ServiceAccount>,
    pub role_bindings: Vec<RoleBinding>,
    pub status: PhantomData<T>,
}

/// A master role group's validated config together with its validated logging.
#[derive(Clone, Debug)]
pub struct ValidatedMasterConfig {
    pub config: HbaseConfig,
    pub logging: ValidatedLogging,
}
#[derive(Clone, Debug)]
pub struct ValidatedRegionServerConfig {
    pub config: RegionServerConfig,
    pub logging: ValidatedLogging,
}
#[derive(Clone, Debug)]
pub struct ValidatedRestServerConfig {
    pub config: HbaseConfig,
    pub logging: ValidatedLogging,
}

/// The merged `configOverrides` of a role group, with the `hbase-env.sh` variable names
/// validated. `EnvVarName` is Kubernetes' rule (non-empty, printable ASCII, no `=`), not the
/// shell's: it rejects a name that would corrupt the `export` line, nothing stricter.
#[derive(Clone, Debug)]
pub struct ValidatedHbaseConfigOverrides {
    pub hbase_site_xml: KeyValueConfigOverrides,
    pub hbase_env_sh: BTreeMap<EnvVarName, String>,
    pub ssl_server_xml: KeyValueConfigOverrides,
    pub ssl_client_xml: KeyValueConfigOverrides,
    pub security_properties: KeyValueConfigOverrides,
}

pub type MasterRoleGroupConfig =
    RoleGroupConfig<ValidatedMasterConfig, JavaCommonConfig, ValidatedHbaseConfigOverrides>;
pub type RegionServerRoleGroupConfig =
    RoleGroupConfig<ValidatedRegionServerConfig, JavaCommonConfig, ValidatedHbaseConfigOverrides>;
pub type RestServerRoleGroupConfig =
    RoleGroupConfig<ValidatedRestServerConfig, JavaCommonConfig, ValidatedHbaseConfigOverrides>;

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
    /// The masters' role-level config. Masters are required.
    pub master_config: ValidatedRoleConfig,
    /// The validated config of every master role group, keyed by role group name.
    pub master_role_group_configs: BTreeMap<RoleGroupName, MasterRoleGroupConfig>,
    pub region_server_config: ValidatedRoleConfig,
    pub region_server_role_group_configs: BTreeMap<RoleGroupName, RegionServerRoleGroupConfig>,
    /// The rest servers' role-level config, or `None` if the role is absent.
    pub rest_server_config: Option<ValidatedRoleConfig>,
    /// Empty if the role is absent.
    pub rest_server_role_group_configs: BTreeMap<RoleGroupName, RestServerRoleGroupConfig>,
}

impl ValidatedCluster {
    pub(crate) fn object_meta(
        name: &ClusterName,
        namespace: &NamespaceName,
        uid: &Uid,
    ) -> ObjectMeta {
        ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            uid: Some(uid.to_string()),
            ..ObjectMeta::default()
        }
    }

    pub(crate) fn product_version(image: &ResolvedProductImage) -> ProductVersion {
        // `app_version_label_value` is constructed to be a valid label value, so it is also a
        // valid `ProductVersion`.
        ProductVersion::from_str(&image.app_version_label_value)
            .expect("the app version label value is a valid product version")
    }

    /// Type-safe names for the per-cluster RBAC resources: the ServiceAccount shared by all
    /// Pods, its (namespaced) RoleBinding, and the operator-deployed ClusterRole it binds.
    pub fn cluster_resource_names(&self) -> role_utils::ResourceNames {
        role_utils::ResourceNames {
            cluster_name: self.name.clone(),
            product_name: PRODUCT_NAME.clone(),
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
            role_name: hbase_role.deref().clone(),
            role_group_name: role_group_name.clone(),
        }
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

#[cfg(test)]
mod tests {
    use super::{CONTROLLER_NAME, OPERATOR_NAME, PRODUCT_NAME};

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *PRODUCT_NAME;
        let _ = *OPERATOR_NAME;
        let _ = *CONTROLLER_NAME;
    }
}
