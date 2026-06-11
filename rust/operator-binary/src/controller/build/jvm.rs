use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    memory::{BinaryMultiple, MemoryQuantity},
    v2::jvm_argument_overrides::JvmArgumentOverrides,
};

use crate::crd::{AnyServiceConfig, CONFIG_DIR_NAME, JVM_SECURITY_PROPERTIES_FILE, v1alpha1};

const JAVA_HEAP_FACTOR: f32 = 0.8;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("invalid memory resource configuration - missing default or value in crd?"))]
    MissingMemoryResourceConfig,

    #[snafu(display("invalid memory config"))]
    InvalidMemoryConfig {
        source: stackable_operator::memory::Error,
    },
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

/// JVM arguments that are specifically for the role (server), so will *not* be used e.g. by CLI tools.
/// Heap settings are excluded, as they go into `HBASE_HEAPSIZE`.
///
/// `merged_jvm_argument_overrides` is the role <- role-group merged [`JvmArgumentOverrides`]
/// produced by
/// [`with_validated_config`](stackable_operator::v2::role_utils::with_validated_config). The
/// operator-generated arguments below form the base that the user overrides are applied on top of.
pub fn construct_role_specific_non_heap_jvm_args(
    hbase: &v1alpha1::HbaseCluster,
    merged_jvm_argument_overrides: &JvmArgumentOverrides,
) -> String {
    let mut operator_generated = vec![format!(
        "-Djava.security.properties={CONFIG_DIR_NAME}/{JVM_SECURITY_PROPERTIES_FILE}"
    )];

    if hbase.has_kerberos_enabled() {
        operator_generated
            .push("-Djava.security.krb5.conf=/stackable/kerberos/krb5.conf".to_owned());
    }

    let mut jvm_args = merged_jvm_argument_overrides.apply_to(operator_generated);
    jvm_args.retain(|arg| !is_heap_jvm_argument(arg));

    jvm_args.join(" ")
}

/// This will be put into `HBASE_HEAPSIZE`, which is just the heap size in megabytes (with the `m`
/// unit prepended).
///
/// The `bin/hbase` script will use this to set the needed JVM arguments.
/// Looking at `bin/hbase`, you can actually add the `m` suffix to make the unit more clear, the
/// script will detect this [here](https://github.com/apache/hbase/blob/777010361abb203b8b17673d84acf4f7f1d0283a/bin/hbase#L165)
/// and work correctly.
pub fn construct_hbase_heapsize_env(merged_config: &AnyServiceConfig) -> Result<String, Error> {
    let heap_size = MemoryQuantity::try_from(
        merged_config
            .resources()
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
    use super::*;
    use crate::crd::{HbaseRole, v1alpha1};

    #[test]
    fn test_construct_jvm_arguments_defaults() {
        let input = r#"
        apiVersion: hbase.stackable.tech/v1alpha1
        kind: HbaseCluster
        metadata:
          name: simple-hbase
        spec:
          image:
            productVersion: 2.6.4
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
        let (hbase, merged_config, merged_jvm_argument_overrides) = construct_boilerplate(input);

        let global_jvm_args = construct_global_jvm_args(false);
        let role_specific_non_heap_jvm_args =
            construct_role_specific_non_heap_jvm_args(&hbase, &merged_jvm_argument_overrides);
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
            productVersion: 2.6.4
          clusterConfig:
            hdfsConfigMapName: simple-hdfs
            zookeeperConfigMapName: simple-znode
            authentication:
              tlsSecretClass: tls
              kerberos:
                secretClass: kerberos-simple
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
        let (hbase, merged_config, merged_jvm_argument_overrides) = construct_boilerplate(input);

        let global_jvm_args = construct_global_jvm_args(hbase.has_kerberos_enabled());
        let role_specific_non_heap_jvm_args =
            construct_role_specific_non_heap_jvm_args(&hbase, &merged_jvm_argument_overrides);
        let hbase_heapsize_env = construct_hbase_heapsize_env(&merged_config).unwrap();

        assert_eq!(
            global_jvm_args,
            "-Djava.security.krb5.conf=/stackable/kerberos/krb5.conf"
        );
        assert_eq!(
            role_specific_non_heap_jvm_args,
            "-Djava.security.properties=/stackable/conf/security.properties \
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
        v1alpha1::HbaseCluster,
        AnyServiceConfig,
        JvmArgumentOverrides,
    ) {
        let hbase: v1alpha1::HbaseCluster =
            serde_yaml::from_str(hbase_cluster).expect("illegal test input");

        // Merge + validate the region server `default` role group via the real
        // `with_validated_config` path, returning the merged config (for heap sizing) and the
        // merged JVM argument overrides.
        let (merged_config, merged_jvm_argument_overrides) =
            crate::crd::test_helpers::merged_role_group_config(
                &hbase,
                &HbaseRole::RegionServer,
                "default",
                "my-hdfs",
            );

        (hbase, merged_config, merged_jvm_argument_overrides)
    }
}
