use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hbase_crd::{
    HbaseConfig, HbaseConfigFragment, HbaseRole, CONFIG_DIR_NAME, JVM_SECURITY_PROPERTIES_FILE,
    METRICS_PORT,
};
use stackable_operator::{
    memory::{BinaryMultiple, MemoryQuantity},
    role_utils::{self, GenericRoleConfig, JavaCommonConfig, JvmArgumentOverrides, Role},
};

const JAVA_HEAP_FACTOR: f32 = 0.8;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("invalid memory resource configuration - missing default or value in crd?"))]
    MissingMemoryResourceConfig,

    #[snafu(display("invalid memory config"))]
    InvalidMemoryConfig {
        source: stackable_operator::memory::Error,
    },

    #[snafu(display("failed to merge jvm argument overrides"))]
    MergeJvmArgumentOverrides { source: role_utils::Error },
}

// Applies to both the servers and the CLI
pub fn construct_global_jvm_args(kerberos_enabled: bool) -> String {
    let mut jvm_args = Vec::new();

    if kerberos_enabled {
        jvm_args.push("-Djava.security.krb5.conf=/stackable/kerberos/krb5.conf");
    }

    // We do *not* add user overrides to the global JVM args, but only the role specific JVM arguments.
    // This allows users to configure stuff for the server (probably what they want to do), without
    // also influencing e.g. startup scripts.
    //
    // However, this is just an assumption. If it is wrong users can still envOverride the global
    // JVM args.
    //
    // Please feel absolutely free to change this behavior!
    jvm_args.join(" ")
}

/// JVM arguments that specifically for the role (server), so will *not* be used e.g. by CLI tools
fn construct_role_specific_jvm_args(
    hbase_role: &HbaseRole,
    role: &Role<HbaseConfigFragment, GenericRoleConfig, JavaCommonConfig>,
    role_group: &str,
    product_version: &str,
    kerberos_enabled: bool,
) -> Result<Vec<String>, Error> {
    let mut jvm_args = vec![format!(
        "-Djava.security.properties={CONFIG_DIR_NAME}/{JVM_SECURITY_PROPERTIES_FILE}"
    )];

    // Starting with HBase 2.6 the JVM exporter is not needed anymore
    if product_version.starts_with(r"2.4") {
        jvm_args.push(
            format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={METRICS_PORT}:/stackable/jmx/{hbase_role}.yaml")
        );
    }
    if kerberos_enabled {
        jvm_args.push("-Djava.security.krb5.conf=/stackable/kerberos/krb5.conf".to_owned());
    }

    let operator_generated = JvmArgumentOverrides::new_with_only_additions(jvm_args);
    let merged = role
        .get_merged_jvm_argument_overrides(role_group, &operator_generated)
        .context(MergeJvmArgumentOverridesSnafu)?;
    Ok(merged
        .effective_jvm_config_after_merging()
        // Sorry for the clone, that's how operator-rs is currently modelled :P
        .clone())
}

/// Arguments that go into `HBASE_OPTS`, so *not* the heap settings (which go into `HBASE_HEAPSIZE`).
pub fn construct_role_specific_non_heap_jvm_args(
    hbase_role: &HbaseRole,
    role: &Role<HbaseConfigFragment, GenericRoleConfig, JavaCommonConfig>,
    role_group: &str,
    product_version: &str,
    kerberos_enabled: bool,
) -> Result<String, Error> {
    let mut jvm_args = construct_role_specific_jvm_args(
        hbase_role,
        role,
        role_group,
        product_version,
        kerberos_enabled,
    )?;
    jvm_args.retain(|arg| !is_heap_jvm_argument(arg));

    Ok(jvm_args.join(" "))
}

/// This will be put into `HBASE_HEAPSIZE`, which is just the heap size in megabytes (with the `m`
/// unit prepended).
///
/// The `bin/hbase` script will use this to set the needed JVM arguments.
/// Looking at `bin/hbase`, you can actually add the `m` suffix to make the unit more clear, the
/// script will detect this [here](https://github.com/apache/hbase/blob/777010361abb203b8b17673d84acf4f7f1d0283a/bin/hbase#L165)
/// and work correctly.
pub fn construct_hbase_heapsize_env(merged_config: &HbaseConfig) -> Result<String, Error> {
    let heap_size = MemoryQuantity::try_from(
        merged_config
            .resources
            .memory
            .limit
            .as_ref()
            .context(MissingMemoryResourceConfigSnafu)?,
    )
    .context(InvalidMemoryConfigSnafu)?
    .scale_to(BinaryMultiple::Mebi)
        * JAVA_HEAP_FACTOR;

    heap_size
        .format_for_java()
        .context(InvalidMemoryConfigSnafu)
}

fn is_heap_jvm_argument(jvm_argument: &str) -> bool {
    let lowercase = jvm_argument.to_lowercase();

    lowercase.starts_with("-xms") || lowercase.starts_with("-xmx")
}

#[cfg(test)]
mod tests {
    use stackable_hbase_crd::{HbaseCluster, HbaseRole};

    use super::*;

    #[test]
    fn test_construct_jvm_arguments_defaults() {
        let input = r#"
        apiVersion: hbase.stackable.tech/v1alpha1
        kind: HbaseCluster
        metadata:
          name: simple-hbase
        spec:
          image:
            productVersion: 2.6.1
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
        "#;
        let (hbase_role, merged_config, role, role_group, product_version) =
            construct_boilerplate(input);
        let kerberos_enabled = false;

        let global_jvm_args = construct_global_jvm_args(kerberos_enabled);
        let role_specific_non_heap_jvm_args = construct_role_specific_non_heap_jvm_args(
            &hbase_role,
            &role,
            &role_group,
            &product_version,
            kerberos_enabled,
        )
        .unwrap();
        let hbase_heapsize_env = construct_hbase_heapsize_env(&merged_config).unwrap();

        assert_eq!(global_jvm_args, "");
        assert_eq!(
            role_specific_non_heap_jvm_args,
            "-Djava.security.properties=/stackable/conf/security.properties"
        );
        assert_eq!(hbase_heapsize_env, "819m");
    }

    #[test]
    fn test_construct_jvm_argument_overrides() {
        let input = r#"
        apiVersion: hbase.stackable.tech/v1alpha1
        kind: HbaseCluster
        metadata:
          name: simple-hbase
        spec:
          image:
            productVersion: 2.4.18
          clusterConfig:
            hdfsConfigMapName: simple-hdfs
            zookeeperConfigMapName: simple-znode
          masters:
            roleGroups:
              default:
                replicas: 1
          regionServers:
            config:
              resources:
                memory:
                  limit: 42Gi
            jvmArgumentOverrides:
              add:
                - -Dhttps.proxyHost=proxy.my.corp
                - -Dhttps.proxyPort=8080
                - -Djava.net.preferIPv4Stack=true
            roleGroups:
              default:
                replicas: 1
                jvmArgumentOverrides:
                  removeRegex:
                    - -Dhttps.proxyPort=.*
                  add:
                    - -Xmx40000m # This has no effect!
                    - -Dhttps.proxyPort=1234
        "#;
        let (hbase_role, merged_config, role, role_group, product_version) =
            construct_boilerplate(input);
        let kerberos_enabled = true;

        let global_jvm_args = construct_global_jvm_args(kerberos_enabled);
        let role_specific_non_heap_jvm_args = construct_role_specific_non_heap_jvm_args(
            &hbase_role,
            &role,
            &role_group,
            &product_version,
            kerberos_enabled,
        )
        .unwrap();
        let hbase_heapsize_env = construct_hbase_heapsize_env(&merged_config).unwrap();

        assert_eq!(
            global_jvm_args,
            "-Djava.security.krb5.conf=/stackable/kerberos/krb5.conf"
        );
        assert_eq!(
            role_specific_non_heap_jvm_args,
            "-Djava.security.properties=/stackable/conf/security.properties \
            -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar=9100:/stackable/jmx/regionserver.yaml \
            -Djava.security.krb5.conf=/stackable/kerberos/krb5.conf \
            -Dhttps.proxyHost=proxy.my.corp \
            -Djava.net.preferIPv4Stack=true \
            -Dhttps.proxyPort=1234"
        );
        assert_eq!(hbase_heapsize_env, "34406m");
    }

    fn construct_boilerplate(
        hbase_cluster: &str,
    ) -> (
        HbaseRole,
        HbaseConfig,
        Role<HbaseConfigFragment, GenericRoleConfig, JavaCommonConfig>,
        String,
        String,
    ) {
        let hbase: HbaseCluster = serde_yaml::from_str(hbase_cluster).expect("illegal test input");

        let hbase_role = HbaseRole::RegionServer;
        let merged_config = hbase
            .merged_config(&hbase_role, "default", "my-hdfs")
            .unwrap();
        let role: Role<HbaseConfigFragment, GenericRoleConfig, JavaCommonConfig> =
            hbase.spec.region_servers.unwrap();
        let product_version = hbase.spec.image.product_version().to_owned();

        (
            hbase_role,
            merged_config,
            role,
            "default".to_owned(),
            product_version,
        )
    }
}
