use std::collections::BTreeMap;

use crate::hbase_controller::CONFIG_DIR_NAME;
use stackable_hbase_crd::{HbaseCluster, HbaseRole};
use stackable_operator::builder::{
    ContainerBuilder, PodBuilder, SecretOperatorVolumeSourceBuilder, VolumeBuilder,
};

pub fn kerberos_config(hbase: &HbaseCluster) -> BTreeMap<String, String> {
    let mut config = BTreeMap::new();
    if hbase.has_kerberos_enabled() {
        config.insert(
            "hbase.security.authentication".to_string(),
            "kerberos".to_string(),
        );
        config.insert(
            "hbase.security.authorization".to_string(),
            "true".to_string(),
        );
        config.insert(
            "dfs.data.transfer.protection".to_string(),
            "privacy".to_string(),
        );
        config.insert(
            "hbase.rpc.engine".to_string(),
            "org.apache.hadoop.hbase.ipc.SecureRpcEngine".to_string(),
        );
        config.insert(
            "hbase.master.kerberos.principal".to_string(),
            format!(
                "{service_name}/_HOST@${{env.KERBEROS_REALM}}",
                service_name = HbaseRole::Master.kerberos_service_name()
            ),
        );
        config.insert(
            "hbase.regionserver.kerberos.principal".to_string(),
            format!(
                "{service_name}/_HOST@${{env.KERBEROS_REALM}}",
                service_name = HbaseRole::RegionServer.kerberos_service_name()
            ),
        );
        config.insert(
            "hbase.rest.kerberos.principal".to_string(),
            format!(
                "{service_name}/_HOST@${{env.KERBEROS_REALM}}",
                service_name = HbaseRole::RestServer.kerberos_service_name()
            ),
        );
        config.insert(
            "hbase.master.keytab.file".to_string(),
            "/stackable/kerberos/keytab".to_string(),
        );
        config.insert(
            "hbase.regionserver.keytab.file".to_string(),
            "/stackable/kerberos/keytab".to_string(),
        );
        config.insert(
            "hbase.rest.keytab.file".to_string(),
            "/stackable/kerberos/keytab".to_string(),
        );

        config.insert(
            "hbase.coprocessor.master.classes".to_string(),
            "org.apache.hadoop.hbase.security.access.AccessController".to_string(),
        );
        config.insert(
            "hbase.coprocessor.region.classes".to_string(),
            "org.apache.hadoop.hbase.security.token.TokenProvider,org.apache.hadoop.hbase.security.access.AccessController".to_string(),
        );
    }
    config
}

pub fn add_kerberos_volumes(pb: &mut PodBuilder, hbase: &HbaseCluster, role: &HbaseRole) {
    if let Some(kerberos_secret_class) = hbase.kerberos_secret_class() {
        let mut kerberos_secret_operator_volume_builder =
            SecretOperatorVolumeSourceBuilder::new(kerberos_secret_class);
        kerberos_secret_operator_volume_builder
            .with_pod_scope()
            .with_kerberos_service_name(role.kerberos_service_name())
            .with_kerberos_service_name("HTTP");
        if let Some(true) = hbase.kerberos_request_node_principals() {
            kerberos_secret_operator_volume_builder.with_node_scope();
        }

        pb.add_volume(
            VolumeBuilder::new("kerberos")
                .ephemeral(kerberos_secret_operator_volume_builder.build())
                .build(),
        );
    }
}

pub fn add_kerberos_volume_mounts(cb: &mut ContainerBuilder, hbase: &HbaseCluster) {
    if hbase.kerberos_secret_class().is_some() {
        cb.add_volume_mount("kerberos", "/stackable/kerberos");
    }
}

pub fn add_kerberos_env_vars(cb: &mut ContainerBuilder, hbase: &HbaseCluster) {
    if hbase.has_kerberos_enabled() {
        cb.add_env_var("KRB5_CONFIG", "/stackable/kerberos/krb5.conf");
        // TODO: Add comment that users (e.g. using hbase shell) need to have this config, not only the hbase roles
        cb.add_env_var(
            "HBASE_OPTS",
            "-Djava.security.krb5.conf=/stackable/kerberos/krb5.conf",
        );
    }
}

pub fn kerberos_container_args(hbase: &HbaseCluster) -> String {
    let mut args = String::new();

    if hbase.has_kerberos_enabled() {
        args.push_str(export_kerberos_real_env_var_command());
        args.push('\n');
    }

    // FFS hbase
    args.push_str(
        format!(
            "sed -i -e 's/${{env.KERBEROS_REALM}}/'\"$KERBEROS_REALM/g\" {CONFIG_DIR_NAME}/hbase-site.xml"
        )
        .as_str(),
    );
    args.push('\n');

    args
}

// Command to export `KERBEROS_REALM` env var to default real from krb5.conf, e.g. `CLUSTER.LOCAL`
fn export_kerberos_real_env_var_command() -> &'static str {
    "export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' /stackable/kerberos/krb5.conf)"
}
