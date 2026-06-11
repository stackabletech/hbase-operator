//! Builds the `hbase-env.sh` config file: JVM heap/options env vars + overrides.

use std::{collections::BTreeMap, fmt::Write};

use snafu::{ResultExt, Snafu};
use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

use crate::{
    config::jvm::{self, construct_global_jvm_args, construct_hbase_heapsize_env},
    crd::{AnyServiceConfig, HbaseRole},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to construct the HBASE_HEAPSIZE env variable"))]
    ConstructHbaseHeapsizeEnv { source: jvm::Error },
}

/// Renders `hbase-env.sh` as `export VAR="VALUE"` lines.
pub fn build(
    merged_config: &AnyServiceConfig,
    role: &HbaseRole,
    kerberos_enabled: bool,
    non_heap_jvm_args: String,
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
        construct_global_jvm_args(kerberos_enabled),
    );

    match role {
        HbaseRole::Master => {
            env.insert("HBASE_MASTER_OPTS".to_string(), non_heap_jvm_args);
        }
        HbaseRole::RegionServer => {
            env.insert("HBASE_REGIONSERVER_OPTS".to_string(), non_heap_jvm_args);
        }
        HbaseRole::RestServer => {
            env.insert("HBASE_REST_OPTS".to_string(), non_heap_jvm_args);
        }
    }

    // configOverride come last
    env.extend(overrides);

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
    use crate::controller::build::properties::test_support::{merged_config, validated_cluster};

    #[test]
    fn renders_operator_defaults() {
        let validated_cluster = validated_cluster();
        let merged = merged_config(&validated_cluster, &HbaseRole::Master);
        let env = build(
            merged,
            &HbaseRole::Master,
            false,
            "-Xtest".to_string(),
            KeyValueConfigOverrides::default(),
        )
        .unwrap();
        assert!(env.contains("export HBASE_MANAGES_ZK=\"false\""), "{env}");
        assert!(env.contains("export HBASE_MASTER_OPTS="), "{env}");
    }

    #[test]
    fn renders_region_server_opts() {
        let validated_cluster = validated_cluster();
        let merged = merged_config(&validated_cluster, &HbaseRole::RegionServer);
        let env = build(
            merged,
            &HbaseRole::RegionServer,
            false,
            "-Xtest".to_string(),
            KeyValueConfigOverrides::default(),
        )
        .unwrap();
        assert!(env.contains("export HBASE_REGIONSERVER_OPTS="), "{env}");
    }

    #[test]
    fn user_override_appears() {
        let validated_cluster = validated_cluster();
        let merged = merged_config(&validated_cluster, &HbaseRole::Master);
        let env = build(
            merged,
            &HbaseRole::Master,
            false,
            "-Xtest".to_string(),
            [("CUSTOM_VAR", "custom_value")].into(),
        )
        .unwrap();
        assert!(env.contains("export CUSTOM_VAR=\"custom_value\""), "{env}");
    }
}
