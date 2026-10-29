//! Builds the `hbase-env.sh` config file: JVM heap/options env vars + overrides.

use std::{collections::BTreeMap, fmt::Write, str::FromStr};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::resources::{NoRuntimeLimits, Resources},
    constant,
    v2::builder::pod::container::EnvVarName,
};

use crate::{
    controller::build::jvm::{self, construct_global_jvm_args, construct_hbase_heapsize_env},
    crd::HbaseStorageConfig,
};

constant!(pub(crate) HBASE_MASTER_OPTS: EnvVarName = "HBASE_MASTER_OPTS");
constant!(pub(crate) HBASE_REGIONSERVER_OPTS: EnvVarName = "HBASE_REGIONSERVER_OPTS");
constant!(pub(crate) HBASE_REST_OPTS: EnvVarName = "HBASE_REST_OPTS");

constant!(HBASE_MANAGES_ZK: EnvVarName = "HBASE_MANAGES_ZK");
constant!(HBASE_HEAPSIZE: EnvVarName = "HBASE_HEAPSIZE");
constant!(HBASE_OPTS: EnvVarName = "HBASE_OPTS");

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to construct the HBASE_HEAPSIZE env variable"))]
    ConstructHbaseHeapsizeEnv { source: jvm::Error },
}

/// Renders `hbase-env.sh` as `export VAR="VALUE"` lines.
pub fn build(
    resources: &Resources<HbaseStorageConfig, NoRuntimeLimits>,
    opts_variable: &EnvVarName,
    kerberos_enabled: bool,
    non_heap_jvm_args: String,
    overrides: BTreeMap<EnvVarName, String>,
) -> Result<String, Error> {
    let mut env: BTreeMap<EnvVarName, String> = BTreeMap::new();

    env.insert(HBASE_MANAGES_ZK.clone(), "false".to_string());
    env.insert(
        HBASE_HEAPSIZE.clone(),
        construct_hbase_heapsize_env(resources).context(ConstructHbaseHeapsizeEnvSnafu)?,
    );
    env.insert(
        HBASE_OPTS.clone(),
        construct_global_jvm_args(kerberos_enabled),
    );
    env.insert(opts_variable.clone(), non_heap_jvm_args);

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
            .resources;
        let env = build(
            merged,
            &HBASE_MASTER_OPTS,
            false,
            "-Xtest".to_string(),
            BTreeMap::new(),
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
            .resources;
        let env = build(
            merged,
            &HBASE_REGIONSERVER_OPTS,
            false,
            "-Xtest".to_string(),
            BTreeMap::new(),
        )
        .unwrap();
        assert!(env.contains("export HBASE_REGIONSERVER_OPTS="), "{env}");
    }

    #[test]
    fn user_override_appears() {
        let validated_cluster = validated_cluster();
        let merged = &test_utils::master_role_group_config(&validated_cluster, "default")
            .config
            .resources;
        let env = build(
            merged,
            &HBASE_MASTER_OPTS,
            false,
            "-Xtest".to_string(),
            BTreeMap::from([(
                EnvVarName::from_str("CUSTOM_VAR").expect("valid variable name"),
                "custom_value".to_string(),
            )]),
        )
        .unwrap();
        assert!(env.contains("export CUSTOM_VAR=\"custom_value\""), "{env}");
    }
}
