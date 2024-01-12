use std::collections::BTreeMap;

use indoc::formatdoc;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hbase_crd::{
    HbaseCluster, HbaseRole, CONFIG_DIR_NAME, HBASE_MASTER_UI_PORT_HTTPS,
    HBASE_REGIONSERVER_UI_PORT_HTTPS, TLS_STORE_DIR, TLS_STORE_PASSWORD, TLS_STORE_VOLUME_NAME,
};
use stackable_operator::{
    builder::{
        ContainerBuilder, PodBuilder, SecretFormat, SecretOperatorVolumeSourceBuilder,
        SecretOperatorVolumeSourceBuilderError, VolumeBuilder,
    },
    kube::{runtime::reflector::ObjectRef, ResourceExt},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object {hbase} is missing namespace"))]
    ObjectMissingNamespace { hbase: ObjectRef<HbaseCluster> },

    #[snafu(display("failed to add Kerberos secret volume"))]
    AddKerberosSecretVolume {
        source: SecretOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to add TLS secret volume"))]
    AddTlsSecretVolume {
        source: SecretOperatorVolumeSourceBuilderError,
    },
}

pub fn kerberos_config_properties(hbase: &HbaseCluster) -> Result<BTreeMap<String, String>, Error> {
    if !hbase.has_kerberos_enabled() {
        return Ok(BTreeMap::new());
    }

    let principal_host_part = principal_host_part(hbase)?;

    Ok(BTreeMap::from([
        // Kerberos settings
        (
            "hbase.security.authentication".to_string(),
            "kerberos".to_string(),
        ),
        (
            "hbase.security.authorization".to_string(),
            "true".to_string(),
        ),
        ("hbase.rpc.protection".to_string(), "privacy".to_string()),
        (
            "dfs.data.transfer.protection".to_string(),
            "privacy".to_string(),
        ),
        (
            "hbase.rpc.engine".to_string(),
            "org.apache.hadoop.hbase.ipc.SecureRpcEngine".to_string(),
        ),
        (
            "hbase.master.kerberos.principal".to_string(),
            format!(
                "{service_name}/{principal_host_part}",
                service_name = HbaseRole::Master.kerberos_service_name()
            ),
        ),
        (
            "hbase.regionserver.kerberos.principal".to_string(),
            format!(
                "{service_name}/{principal_host_part}",
                service_name = HbaseRole::RegionServer.kerberos_service_name()
            ),
        ),
        (
            "hbase.rest.kerberos.principal".to_string(),
            format!(
                "{service_name}/{principal_host_part}",
                service_name = HbaseRole::RestServer.kerberos_service_name()
            ),
        ),
        (
            "hbase.master.keytab.file".to_string(),
            "/stackable/kerberos/keytab".to_string(),
        ),
        (
            "hbase.regionserver.keytab.file".to_string(),
            "/stackable/kerberos/keytab".to_string(),
        ),
        (
            "hbase.rest.keytab.file".to_string(),
            "/stackable/kerberos/keytab".to_string(),
        ),
        (
            "hbase.coprocessor.master.classes".to_string(),
            "org.apache.hadoop.hbase.security.access.AccessController".to_string(),
        ),
        (
            "hbase.coprocessor.region.classes".to_string(),
            "org.apache.hadoop.hbase.security.token.TokenProvider,org.apache.hadoop.hbase.security.access.AccessController".to_string(),
        ),

        // Rest
        ("hbase.rest.authentication.type".to_string(), "kerberos".to_string()),
        ("hbase.rest.authentication.kerberos.principal".to_string(), format!(
            "HTTP/{principal_host_part}"
        )),
        ("hbase.rest.authentication.kerberos.keytab".to_string(), "/stackable/kerberos/keytab".to_string()),

        // Enabled https as well
        ("hbase.ssl.enabled".to_string(), "true".to_string()),
        ("hbase.http.policy".to_string(), "HTTPS_ONLY".to_string()),
        // Recommended by the docs https://hbase.apache.org/book.html#hbase.ui.cache
        ("hbase.http.filter.no-store.enable".to_string(), "true".to_string()),
        // key and truststore come from ssl-server.xml and ssl-client.xml

        // Rest
        ("hbase.rest.ssl.enabled".to_string(), "true".to_string()),
        ("hbase.rest.ssl.keystore.store".to_string(), format!("{TLS_STORE_DIR}/keystore.p12")),
        ("hbase.rest.ssl.keystore.password".to_string(), TLS_STORE_PASSWORD.to_string()),
        ("hbase.rest.ssl.keystore.type".to_string(), "pkcs12".to_string()),
        // TODO: Check if needed
        // ("hbase.rest.support.proxyuser".to_string(), "true".to_string()),

        // Set non-default ports when https is enabled
        ("hbase.master.info.port".to_string(), HBASE_MASTER_UI_PORT_HTTPS.to_string()),
        ("hbase.regionserver.info.port".to_string(), HBASE_REGIONSERVER_UI_PORT_HTTPS.to_string()),
    ]))
}

pub fn kerberos_discovery_config_properties(
    hbase: &HbaseCluster,
) -> Result<BTreeMap<String, String>, Error> {
    if !hbase.has_kerberos_enabled() {
        return Ok(BTreeMap::new());
    }

    let principal_host_part = principal_host_part(hbase)?;

    Ok(BTreeMap::from([
        (
            "hbase.security.authentication".to_string(),
            "kerberos".to_string(),
        ),
        ("hbase.rpc.protection".to_string(), "privacy".to_string()),
        ("hbase.ssl.enabled".to_string(), "true".to_string()),
        (
            "hbase.master.kerberos.principal".to_string(),
            format!(
                "{service_name}/{principal_host_part}",
                service_name = HbaseRole::Master.kerberos_service_name()
            ),
        ),
        (
            "hbase.regionserver.kerberos.principal".to_string(),
            format!(
                "{service_name}/{principal_host_part}",
                service_name = HbaseRole::RegionServer.kerberos_service_name()
            ),
        ),
        (
            "hbase.rest.kerberos.principal".to_string(),
            format!(
                "{service_name}/{principal_host_part}",
                service_name = HbaseRole::RestServer.kerberos_service_name()
            ),
        ),
    ]))
}

pub fn kerberos_ssl_server_settings(hbase: &HbaseCluster) -> BTreeMap<String, String> {
    if !hbase.has_https_enabled() {
        return BTreeMap::new();
    }

    BTreeMap::from([
        (
            "ssl.server.truststore.location".to_string(),
            format!("{TLS_STORE_DIR}/truststore.p12"),
        ),
        (
            "ssl.server.truststore.type".to_string(),
            "pkcs12".to_string(),
        ),
        (
            "ssl.server.truststore.password".to_string(),
            TLS_STORE_PASSWORD.to_string(),
        ),
        (
            "ssl.server.keystore.location".to_string(),
            format!("{TLS_STORE_DIR}/keystore.p12"),
        ),
        ("ssl.server.keystore.type".to_string(), "pkcs12".to_string()),
        (
            "ssl.server.keystore.password".to_string(),
            TLS_STORE_PASSWORD.to_string(),
        ),
    ])
}

pub fn kerberos_ssl_client_settings(hbase: &HbaseCluster) -> BTreeMap<String, String> {
    if !hbase.has_https_enabled() {
        return BTreeMap::new();
    }

    BTreeMap::from([
        (
            "ssl.client.truststore.location".to_string(),
            format!("{TLS_STORE_DIR}/truststore.p12"),
        ),
        (
            "ssl.client.truststore.type".to_string(),
            "pkcs12".to_string(),
        ),
        (
            "ssl.client.truststore.password".to_string(),
            TLS_STORE_PASSWORD.to_string(),
        ),
    ])
}

pub fn add_kerberos_pod_config(
    hbase: &HbaseCluster,
    role: &HbaseRole,
    cb: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<(), Error> {
    if let Some(kerberos_secret_class) = hbase.kerberos_secret_class() {
        // Keytab
        let mut kerberos_secret_operator_volume_builder =
            SecretOperatorVolumeSourceBuilder::new(kerberos_secret_class);
        kerberos_secret_operator_volume_builder
            .with_service_scope(hbase.name_any())
            .with_kerberos_service_name(role.kerberos_service_name())
            .with_kerberos_service_name("HTTP");
        if let Some(true) = hbase.kerberos_request_node_principals() {
            kerberos_secret_operator_volume_builder.with_node_scope();
        }
        pb.add_volume(
            VolumeBuilder::new("kerberos")
                .ephemeral(
                    kerberos_secret_operator_volume_builder
                        .build()
                        .context(AddKerberosSecretVolumeSnafu)?,
                )
                .build(),
        );
        cb.add_volume_mount("kerberos", "/stackable/kerberos");

        // Needed env vars
        cb.add_env_var("KRB5_CONFIG", "/stackable/kerberos/krb5.conf");
        // This env var does not only affect the servers, but also the hbase shell
        cb.add_env_var(
            "HBASE_OPTS",
            "-Djava.security.krb5.conf=/stackable/kerberos/krb5.conf",
        );
    }

    if let Some(https_secret_class) = hbase.https_secret_class() {
        // TLS certs
        pb.add_volume(
            VolumeBuilder::new(TLS_STORE_VOLUME_NAME)
                .ephemeral(
                    SecretOperatorVolumeSourceBuilder::new(https_secret_class)
                        .with_pod_scope()
                        .with_node_scope()
                        .with_format(SecretFormat::TlsPkcs12)
                        .with_tls_pkcs12_password(TLS_STORE_PASSWORD)
                        .build()
                        .context(AddTlsSecretVolumeSnafu)?,
                )
                .build(),
        );
        cb.add_volume_mount(TLS_STORE_VOLUME_NAME, TLS_STORE_DIR);
    }
    Ok(())
}

pub fn kerberos_container_start_commands(hbase: &HbaseCluster) -> String {
    if !hbase.has_kerberos_enabled() {
        return String::new();
    }

    formatdoc! {"
        export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' /stackable/kerberos/krb5.conf)
        sed -i -e 's/${{env.KERBEROS_REALM}}/'\"$KERBEROS_REALM/g\" {CONFIG_DIR_NAME}/hbase-site.xml",
    }
}

fn principal_host_part(hbase: &HbaseCluster) -> Result<String, Error> {
    let hbase_name = hbase.name_any();
    let hbase_namespace = hbase.namespace().context(ObjectMissingNamespaceSnafu {
        hbase: ObjectRef::from_obj(hbase),
    })?;
    Ok(format!(
        "{hbase_name}.{hbase_namespace}.svc.cluster.local@${{env.KERBEROS_REALM}}"
    ))
}
