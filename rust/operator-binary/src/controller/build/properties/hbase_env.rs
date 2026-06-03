//! Builds the `hbase-env.sh` config file: JVM heap/options env vars + overrides.

use std::{collections::BTreeMap, fmt::Write};

use snafu::{ResultExt, Snafu};
use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

use crate::{
    config::jvm::{
        self, construct_global_jvm_args, construct_hbase_heapsize_env,
        construct_role_specific_non_heap_jvm_args,
    },
    controller::build::properties::resolved_overrides,
    crd::{AnyServiceConfig, HbaseRole, v1alpha1},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to construct the HBASE_HEAPSIZE env variable"))]
    ConstructHbaseHeapsizeEnv { source: jvm::Error },

    #[snafu(display("failed to construct the JVM arguments"))]
    ConstructJvmArgument { source: jvm::Error },
}

/// Renders `hbase-env.sh` as `export VAR="VALUE"` lines.
pub fn build(
    hbase: &v1alpha1::HbaseCluster,
    merged_config: &AnyServiceConfig,
    role: &HbaseRole,
    role_group: &str,
    overrides: KeyValueConfigOverrides,
) -> Result<String, Error> {
    let mut env: BTreeMap<String, String> = BTreeMap::new();

    env.insert("HBASE_MANAGES_ZK".to_string(), "false".to_string());
    env.insert(
        "HBASE_HEAPSIZE".to_string(),
        construct_hbase_heapsize_env(merged_config).context(ConstructHbaseHeapsizeEnvSnafu)?,
    );
    env.insert(
        "HBASE_OPTS".to_string(),
        construct_global_jvm_args(hbase.has_kerberos_enabled()),
    );

    let role_specific_non_heap_jvm_args =
        construct_role_specific_non_heap_jvm_args(hbase, role, role_group)
            .context(ConstructJvmArgumentSnafu)?;
    match role {
        HbaseRole::Master => {
            env.insert(
                "HBASE_MASTER_OPTS".to_string(),
                role_specific_non_heap_jvm_args,
            );
        }
        HbaseRole::RegionServer => {
            env.insert(
                "HBASE_REGIONSERVER_OPTS".to_string(),
                role_specific_non_heap_jvm_args,
            );
        }
        HbaseRole::RestServer => {
            env.insert(
                "HBASE_REST_OPTS".to_string(),
                role_specific_non_heap_jvm_args,
            );
        }
    }

    // configOverride come last
    env.extend(resolved_overrides(overrides));

    Ok(env
        .iter()
        .fold(String::new(), |mut output, (variable, value)| {
            let _ = writeln!(output, "export {variable}=\"{value}\"");
            output
        }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::build::properties::test_support::{config_overrides, minimal_hbase};

    fn master_merged_config(hbase: &v1alpha1::HbaseCluster) -> AnyServiceConfig {
        hbase
            .merged_config(&HbaseRole::Master, "default", "simple-hdfs")
            .expect("merged config for the minimal master group")
    }

    fn region_server_merged_config(hbase: &v1alpha1::HbaseCluster) -> AnyServiceConfig {
        hbase
            .merged_config(&HbaseRole::RegionServer, "default", "simple-hdfs")
            .expect("merged config for the minimal region server group")
    }

    #[test]
    fn renders_operator_defaults() {
        let hbase = minimal_hbase();
        let merged = master_merged_config(&hbase);
        let env = build(
            &hbase,
            &merged,
            &HbaseRole::Master,
            "default",
            config_overrides(&[]),
        )
        .unwrap();
        assert!(env.contains("export HBASE_MANAGES_ZK=\"false\""), "{env}");
        assert!(env.contains("export HBASE_MASTER_OPTS="), "{env}");
    }

    #[test]
    fn renders_region_server_opts() {
        let hbase = minimal_hbase();
        let merged = region_server_merged_config(&hbase);
        let env = build(
            &hbase,
            &merged,
            &HbaseRole::RegionServer,
            "default",
            config_overrides(&[]),
        )
        .unwrap();
        assert!(env.contains("export HBASE_REGIONSERVER_OPTS="), "{env}");
    }

    #[test]
    fn user_override_appears() {
        let hbase = minimal_hbase();
        let merged = master_merged_config(&hbase);
        let env = build(
            &hbase,
            &merged,
            &HbaseRole::Master,
            "default",
            config_overrides(&[("CUSTOM_VAR", "custom_value")]),
        )
        .unwrap();
        assert!(
            env.contains("export CUSTOM_VAR=\"custom_value\""),
            "{env}"
        );
    }
}
