use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    memory::{BinaryMultiple, MemoryQuantity},
    role_utils::{self, JvmArgumentOverrides},
};

use crate::crd::{
    AnyServiceConfig, CONFIG_DIR_NAME, HbaseRole, JVM_SECURITY_PROPERTIES_FILE, v1alpha1,
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

    #[snafu(display("the HBase role [{role}] is missing from spec"))]
    MissingHbaseRole { role: String },
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
pub fn construct_role_specific_non_heap_jvm_args(
    hbase: &v1alpha1::HbaseCluster,
    hbase_role: &HbaseRole,
    role_group: &str,
) -> Result<String, Error> {
    let mut jvm_args = vec![format!(
        "-Djava.security.properties={CONFIG_DIR_NAME}/{JVM_SECURITY_PROPERTIES_FILE}"
    )];

    if hbase.has_kerberos_enabled() {
        jvm_args.push("-Djava.security.krb5.conf=/stackable/kerberos/krb5.conf".to_owned());
    }

    let operator_generated = JvmArgumentOverrides::new_with_only_additions(jvm_args);

    let merged = match hbase_role {
        HbaseRole::Master => hbase
            .spec
            .masters
            .as_ref()
            .context(MissingHbaseRoleSnafu {
                role: hbase_role.to_string(),
            })?
            .get_merged_jvm_argument_overrides(role_group, &operator_generated)
            .context(MergeJvmArgumentOverridesSnafu)?,
        HbaseRole::RegionServer => hbase
            .spec
            .region_servers
            .as_ref()
            .context(MissingHbaseRoleSnafu {
                role: hbase_role.to_string(),
            })?
            .get_merged_jvm_argument_overrides(role_group, &operator_generated)
            .context(MergeJvmArgumentOverridesSnafu)?,
        HbaseRole::RestServer => hbase
            .spec
            .rest_servers
            .as_ref()
            .context(MissingHbaseRoleSnafu {
                role: hbase_role.to_string(),
            })?
            .get_merged_jvm_argument_overrides(role_group, &operator_generated)
            .context(MergeJvmArgumentOverridesSnafu)?,
    };
    jvm_args = merged
        .effective_jvm_config_after_merging()
        // Sorry for the clone, that's how operator-rs is currently modelled :P
        .clone();

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
            productVersion: 2.6.3
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
        let (hbase, hbase_role, merged_config, role_group) = construct_boilerplate(input);

        let global_jvm_args = construct_global_jvm_args(false);
        let role_specific_non_heap_jvm_args =
            construct_role_specific_non_heap_jvm_args(&hbase, &hbase_role, &role_group).unwrap();
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
            productVersion: 2.6.3
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
        let (hbase, hbase_role, merged_config, role_group) = construct_boilerplate(input);

        let global_jvm_args = construct_global_jvm_args(hbase.has_kerberos_enabled());
        let role_specific_non_heap_jvm_args =
            construct_role_specific_non_heap_jvm_args(&hbase, &hbase_role, &role_group).unwrap();
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
    ) -> (v1alpha1::HbaseCluster, HbaseRole, AnyServiceConfig, String) {
        let hbase: v1alpha1::HbaseCluster =
            serde_yaml::from_str(hbase_cluster).expect("illegal test input");

        let hbase_role = HbaseRole::RegionServer;
        let merged_config = hbase
            .merged_config(&hbase_role, "default", "my-hdfs")
            .unwrap();

        (hbase, hbase_role, merged_config, "default".to_owned())
    }
}
