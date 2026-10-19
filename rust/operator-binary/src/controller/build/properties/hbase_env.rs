//! Builds the `hbase-env.sh` config file: JVM heap/options env vars + overrides.

use std::{collections::BTreeMap, fmt::Write};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::resources::{NoRuntimeLimits, Resources},
    v2::config_overrides::KeyValueConfigOverrides,
};

use crate::{
    controller::build::jvm::{self, construct_global_jvm_args, construct_hbase_heapsize_env},
    crd::HbaseStorageConfig,
};

/// The `hbase-env.sh` variable that carries a role's own JVM options; `bin/hbase` reads the one
/// matching the process it starts.
pub(crate) const HBASE_MASTER_OPTS: &str = "HBASE_MASTER_OPTS";
pub(crate) const HBASE_REGIONSERVER_OPTS: &str = "HBASE_REGIONSERVER_OPTS";
pub(crate) const HBASE_REST_OPTS: &str = "HBASE_REST_OPTS";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to construct the HBASE_HEAPSIZE env variable"))]
    ConstructHbaseHeapsizeEnv { source: jvm::Error },
}

/// Renders `hbase-env.sh` as `export VAR="VALUE"` lines.
pub fn build(
    resources: &Resources<HbaseStorageConfig, NoRuntimeLimits>,
    opts_variable: &str,
    kerberos_enabled: bool,
    non_heap_jvm_args: String,
    overrides: KeyValueConfigOverrides,
) -> Result<String, Error> {
    let mut env: BTreeMap<String, String> = BTreeMap::new();

    env.insert("HBASE_MANAGES_ZK".to_string(), "false".to_string());
    env.insert(
        "HBASE_HEAPSIZE".to_string(),
        construct_hbase_heapsize_env(resources).context(ConstructHbaseHeapsizeEnvSnafu)?,
    );
    env.insert(
        "HBASE_OPTS".to_string(),
        construct_global_jvm_args(kerberos_enabled),
    );
    env.insert(opts_variable.to_string(), non_heap_jvm_args);

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
    use crate::test_utils::{self, validated_cluster};

    #[test]
    fn renders_operator_defaults() {
        let validated_cluster = validated_cluster();
        let merged = &test_utils::master_role_group_config(&validated_cluster, "default")
            .config
            .config
            .resources;
        let env = build(
            merged,
            HBASE_MASTER_OPTS,
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
        let merged = &test_utils::region_server_role_group_config(&validated_cluster, "default")
            .config
            .config
            .resources;
        let env = build(
            merged,
            HBASE_REGIONSERVER_OPTS,
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
        let merged = &test_utils::master_role_group_config(&validated_cluster, "default")
            .config
            .config
            .resources;
        let env = build(
            merged,
            HBASE_MASTER_OPTS,
            false,
            "-Xtest".to_string(),
            [("CUSTOM_VAR", "custom_value")].into(),
        )
        .unwrap();
        assert!(env.contains("export CUSTOM_VAR=\"custom_value\""), "{env}");
    }
}
