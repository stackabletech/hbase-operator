use std::{collections::BTreeMap, str::FromStr};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            volume::{SecretFormat, SecretOperatorVolumeSourceBuilder, VolumeBuilder},
        },
    },
    commons::secret_class::SecretClassVolumeProvisionParts,
    shared::time::Duration,
    utils::cluster_info::KubernetesClusterInfo,
    v2::types::kubernetes::VolumeName,
};

use crate::{
    controller::ValidatedCluster,
    crd::{TLS_STORE_DIR, TLS_STORE_PASSWORD, TLS_STORE_TYPE, TLS_STORE_VOLUME_NAME},
};

/// Mount path of the Kerberos secret volume (keytab + `krb5.conf`).
pub const STACKABLE_KERBEROS_DIR: &str = "/stackable/kerberos";
/// Path of the `krb5.conf` rendered into the Kerberos secret volume. Referenced both here (the
/// `KRB5_CONFIG` env var) and by the JVM args builder.
pub const KRB5_CONFIG_PATH: &str = const_format::concatcp!(STACKABLE_KERBEROS_DIR, "/krb5.conf");
// Name of the Kerberos secret volume.
stackable_operator::constant!(KERBEROS_VOLUME_NAME: VolumeName = "kerberos");
/// The RPC/data-transfer quality-of-protection level used when Kerberos is enabled.
const PROTECTION_PRIVACY: &str = "privacy";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build Kerberos secret volume"))]
    BuildKerberosSecretVolume {
        source: stackable_operator::builder::pod::volume::SecretOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to build TLS secret volume"))]
    BuildTlsSecretVolume {
        source: stackable_operator::builder::pod::volume::SecretOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },
}

/// The `hbase-site.xml` Kerberos properties for `cluster`, gated on Kerberos being enabled
/// (empty when disabled). Derived in the build step from the validated cluster.
pub fn hbase_site_kerberos_config(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> BTreeMap<String, String> {
    if cluster.has_kerberos_enabled() {
        kerberos_config_properties(
            cluster.name.as_ref(),
            cluster.namespace.as_ref(),
            cluster_info,
        )
    } else {
        BTreeMap::new()
    }
}

/// The Kerberos properties for the discovery `hbase-site.xml` exposed to clients, gated on
/// Kerberos being enabled (empty when disabled).
pub fn discovery_kerberos_config(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
) -> BTreeMap<String, String> {
    if cluster.has_kerberos_enabled() {
        kerberos_discovery_config_properties(
            cluster.name.as_ref(),
            cluster.namespace.as_ref(),
            cluster_info,
        )
    } else {
        BTreeMap::new()
    }
}

/// The `ssl-server.xml` settings for `cluster`, gated on HTTPS being enabled (empty when disabled).
pub fn ssl_server_settings(cluster: &ValidatedCluster) -> BTreeMap<String, String> {
    if cluster.has_https_enabled() {
        kerberos_ssl_server_settings()
    } else {
        BTreeMap::new()
    }
}

/// The `ssl-client.xml` settings for `cluster`, gated on HTTPS being enabled (empty when disabled).
pub fn ssl_client_settings(cluster: &ValidatedCluster) -> BTreeMap<String, String> {
    if cluster.has_https_enabled() {
        kerberos_ssl_client_settings()
    } else {
        BTreeMap::new()
    }
}

pub fn kerberos_config_properties(
    hbase_name: &str,
    hbase_namespace: &str,
    cluster_info: &KubernetesClusterInfo,
) -> BTreeMap<String, String> {
    let principal_host_part = principal_host_part(hbase_name, hbase_namespace, cluster_info);

    let mut config = BTreeMap::from([
        // Kerberos settings
        (
            "hbase.security.authentication".to_string(),
            "kerberos".to_string(),
        ),
        (
            "hbase.security.authorization".to_string(),
            "true".to_string(),
        ),
        (
            "hbase.rpc.protection".to_string(),
            PROTECTION_PRIVACY.to_string(),
        ),
        (
            "dfs.data.transfer.protection".to_string(),
            PROTECTION_PRIVACY.to_string(),
        ),
        (
            "hbase.rpc.engine".to_string(),
            "org.apache.hadoop.hbase.ipc.SecureRpcEngine".to_string(),
        ),
        (
            "hbase.master.keytab.file".to_string(),
            format!("{STACKABLE_KERBEROS_DIR}/keytab"),
        ),
        (
            "hbase.regionserver.keytab.file".to_string(),
            format!("{STACKABLE_KERBEROS_DIR}/keytab"),
        ),
        (
            "hbase.rest.keytab.file".to_string(),
            format!("{STACKABLE_KERBEROS_DIR}/keytab"),
        ),
        (
            "hbase.coprocessor.master.classes".to_string(),
            "org.apache.hadoop.hbase.security.access.AccessController".to_string(),
        ),
        (
            "hbase.coprocessor.region.classes".to_string(),
            "org.apache.hadoop.hbase.security.token.TokenProvider,org.apache.hadoop.hbase.security.access.AccessController".to_string(),
        ),

        // Rest server
        ("hbase.rest.authentication.type".to_string(), "kerberos".to_string()),
        ("hbase.rest.authentication.kerberos.principal".to_string(), format!(
            "HTTP/{principal_host_part}"
        )),
        ("hbase.rest.authentication.kerberos.keytab".to_string(), format!("{STACKABLE_KERBEROS_DIR}/keytab")),

        // Enabled https as well
        ("hbase.ssl.enabled".to_string(), "true".to_string()),
        ("hbase.http.policy".to_string(), "HTTPS_ONLY".to_string()),
        // Recommended by the docs https://hbase.apache.org/book.html#hbase.ui.cache
        ("hbase.http.filter.no-store.enable".to_string(), "true".to_string()),
        // Key- and truststore come from ssl-server.xml and ssl-client.xml

        // Https for rest server
        ("hbase.rest.ssl.enabled".to_string(), "true".to_string()),
        ("hbase.rest.ssl.keystore.store".to_string(), format!("{TLS_STORE_DIR}/keystore.p12")),
        ("hbase.rest.ssl.keystore.password".to_string(), TLS_STORE_PASSWORD.to_string()),
        ("hbase.rest.ssl.keystore.type".to_string(), TLS_STORE_TYPE.to_string()),
    ]);
    config.extend(kerberos_principals(&principal_host_part));
    config
}

pub fn kerberos_discovery_config_properties(
    hbase_name: &str,
    hbase_namespace: &str,
    cluster_info: &KubernetesClusterInfo,
) -> BTreeMap<String, String> {
    let principal_host_part = principal_host_part(hbase_name, hbase_namespace, cluster_info);

    let mut config = BTreeMap::from([
        (
            "hbase.security.authentication".to_string(),
            "kerberos".to_string(),
        ),
        (
            "hbase.rpc.protection".to_string(),
            PROTECTION_PRIVACY.to_string(),
        ),
        ("hbase.ssl.enabled".to_string(), "true".to_string()),
    ]);
    config.extend(kerberos_principals(&principal_host_part));
    config
}

pub fn kerberos_ssl_server_settings() -> BTreeMap<String, String> {
    let mut settings = truststore_settings("server");
    settings.extend([
        (
            "ssl.server.keystore.location".to_string(),
            format!("{TLS_STORE_DIR}/keystore.p12"),
        ),
        (
            "ssl.server.keystore.type".to_string(),
            TLS_STORE_TYPE.to_string(),
        ),
        (
            "ssl.server.keystore.password".to_string(),
            TLS_STORE_PASSWORD.to_string(),
        ),
    ]);
    settings
}

pub fn kerberos_ssl_client_settings() -> BTreeMap<String, String> {
    truststore_settings("client")
}

pub fn add_kerberos_pod_config(
    cluster: &ValidatedCluster,
    metrics_service_name: &str,
    cb: &mut ContainerBuilder,
    pb: &mut PodBuilder,
    requested_secret_lifetime: Duration,
) -> Result<(), Error> {
    if let Some(kerberos_secret_class) = cluster.cluster_config.kerberos_secret_class.clone() {
        // Mount keytab
        let kerberos_secret_operator_volume = SecretOperatorVolumeSourceBuilder::new(
            kerberos_secret_class,
            // We need both public (krb5.conf) and private (keytab) parts.
            SecretClassVolumeProvisionParts::PublicPrivate,
        )
        .with_service_scope(cluster.name.to_string())
        .with_kerberos_service_name(kerberos_service_name())
        .with_kerberos_service_name("HTTP")
        .build()
        .context(BuildKerberosSecretVolumeSnafu)?;
        pb.add_volume(
            VolumeBuilder::new(&*KERBEROS_VOLUME_NAME)
                .ephemeral(kerberos_secret_operator_volume)
                .build(),
        )
        .context(AddVolumeSnafu)?;
        cb.add_volume_mount(&*KERBEROS_VOLUME_NAME, STACKABLE_KERBEROS_DIR)
            .context(AddVolumeMountSnafu)?;

        // Needed env vars
        cb.add_env_var("KRB5_CONFIG", KRB5_CONFIG_PATH);
    }

    if let Some(https_secret_class) = cluster.cluster_config.https_secret_class.clone() {
        // Mount TLS keystore
        pb.add_volume(
            VolumeBuilder::new(&*TLS_STORE_VOLUME_NAME)
                .ephemeral(
                    SecretOperatorVolumeSourceBuilder::new(
                        https_secret_class,
                        // HBase serves its own TLS endpoints, so the Pod needs both the public
                        // certificate and the private key.
                        SecretClassVolumeProvisionParts::PublicPrivate,
                    )
                    .with_pod_scope()
                    .with_node_scope()
                    // We need to add the metrics service for scraping
                    .with_service_scope(metrics_service_name)
                    .with_format(SecretFormat::TlsPkcs12)
                    .with_tls_pkcs12_password(TLS_STORE_PASSWORD)
                    .with_auto_tls_cert_lifetime(requested_secret_lifetime)
                    .build()
                    .context(BuildTlsSecretVolumeSnafu)?,
                )
                .build(),
        )
        .context(AddVolumeSnafu)?;
        cb.add_volume_mount(&*TLS_STORE_VOLUME_NAME, TLS_STORE_DIR)
            .context(AddVolumeMountSnafu)?;
    }
    Ok(())
}

/// The `hbase.{master,regionserver,rest}.kerberos.principal` entries shared by the main
/// and discovery config. All roles use the same `hbase` service principal (see
/// [`kerberos_service_name`]).
fn kerberos_principals(principal_host_part: &str) -> [(String, String); 3] {
    let principal = format!(
        "{service_name}/{principal_host_part}",
        service_name = kerberos_service_name()
    );
    [
        (
            "hbase.master.kerberos.principal".to_string(),
            principal.clone(),
        ),
        (
            "hbase.regionserver.kerberos.principal".to_string(),
            principal.clone(),
        ),
        ("hbase.rest.kerberos.principal".to_string(), principal),
    ]
}

/// The `ssl.{role}.truststore.*` entries (location/type/password) shared by the server and
/// client TLS settings. `role` is either `"server"` or `"client"`.
fn truststore_settings(role: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            format!("ssl.{role}.truststore.location"),
            format!("{TLS_STORE_DIR}/truststore.p12"),
        ),
        (
            format!("ssl.{role}.truststore.type"),
            TLS_STORE_TYPE.to_string(),
        ),
        (
            format!("ssl.{role}.truststore.password"),
            TLS_STORE_PASSWORD.to_string(),
        ),
    ])
}

fn principal_host_part(
    hbase_name: &str,
    hbase_namespace: &str,
    cluster_info: &KubernetesClusterInfo,
) -> String {
    let cluster_domain = &cluster_info.cluster_domain;
    format!("{hbase_name}.{hbase_namespace}.svc.{cluster_domain}@${{env:KERBEROS_REALM}}")
}

/// We could have different service names depended on the role (e.g. "hbase-master", "hbase-regionserver" and
/// "hbase-restserver"). However this produces error messages such as
/// [RpcServer.priority.RWQ.Fifo.write.handler=0,queue=0,port=16020] security.ShellBasedUnixGroupsMapping: unable to return groups for user hbase-master PartialGroupNameException The user name 'hbase-master' is not found. id: 'hbase-master': no such user
/// or
/// Caused by: org.apache.hadoop.hbase.ipc.RemoteWithExtrasException(org.apache.hadoop.hbase.security.AccessDeniedException): org.apache.hadoop.hbase.security.AccessDeniedException: Insufficient permissions (user=hbase-master/hbase-master-default-1.hbase-master-default.kuttl-test-poetic-sunbeam.svc.cluster.local@CLUSTER.LOCAL, scope=hbase:meta, family=table:state, params=[table=hbase:meta,family=table:state],action=WRITE)
///
/// Also the documentation states:
/// > A Kerberos principal has three parts, with the form username/fully.qualified.domain.name@YOUR-REALM.COM. We recommend using hbase as the username portion.
///
/// As a result we use "hbase" everywhere (which e.g. differs from the current hdfs implementation)
fn kerberos_service_name() -> &'static str {
    "hbase"
}
