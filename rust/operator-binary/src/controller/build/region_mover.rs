//! Build-side region-mover logic for [`AnyServiceConfig`]: derives the `bin/hbase` region-mover
//! CLI arguments from the merged config. Kept out of the `crd` module so it stays a pure API
//! definition.

use shell_escape::escape;
use stackable_operator::shared::time::Duration;

use crate::crd::AnyServiceConfig;

/// Fallback region-mover timeout, used only when no graceful shutdown timeout is set. In practice
/// the merged config always defaults `graceful_shutdown_timeout` (60m for region servers), so this
/// is a defensive default; it mirrors that 60m default minus [`DEFAULT_REGION_MOVER_DELTA_TO_SHUTDOWN`].
const DEFAULT_REGION_MOVER_TIMEOUT: Duration = Duration::from_minutes_unchecked(59);
/// Time reserved before the graceful shutdown deadline, subtracted from the configured timeout so
/// the region move finishes before the pod is terminated.
const DEFAULT_REGION_MOVER_DELTA_TO_SHUTDOWN: Duration = Duration::from_minutes_unchecked(1);

impl AnyServiceConfig {
    /// Returns command line arguments to pass on to the region mover tool.
    /// The following arguments are excluded because they are already part of the
    /// hbase-entrypoint.sh script.
    /// The most important argument, '--regionserverhost' can only be computed on the Pod
    /// because it contains the pod's hostname.
    ///
    /// Returns an empty string if the region mover is disabled or any other role is "self".
    pub fn region_mover_args(&self) -> String {
        match self {
            AnyServiceConfig::RegionServer(config) => {
                if config.region_mover.run_before_shutdown {
                    let timeout = config
                        .graceful_shutdown_timeout
                        .map(|d| {
                            if d.as_secs() <= DEFAULT_REGION_MOVER_DELTA_TO_SHUTDOWN.as_secs() {
                                d.as_secs()
                            } else {
                                d.as_secs() - DEFAULT_REGION_MOVER_DELTA_TO_SHUTDOWN.as_secs()
                            }
                        })
                        .unwrap_or(DEFAULT_REGION_MOVER_TIMEOUT.as_secs());
                    let mut command = vec![
                        "--maxthreads".to_string(),
                        config.region_mover.max_threads.to_string(),
                        "--timeout".to_string(),
                        timeout.to_string(),
                    ];
                    if !config.region_mover.ack {
                        command.push("--noack".to_string());
                    }

                    command.extend(
                        config
                            .region_mover
                            .cli_opts
                            .iter()
                            .flat_map(|o| o.additional_mover_options.clone())
                            .map(|s| escape(std::borrow::Cow::Borrowed(&s)).to_string()),
                    );
                    command.join(" ")
                } else {
                    "".to_string()
                }
            }
            _ => "".to_string(),
        }
    }

    pub fn run_region_mover(&self) -> bool {
        match self {
            AnyServiceConfig::RegionServer(config) => config.region_mover.run_before_shutdown,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use crate::{crd::HbaseRole, test_utils};

    /// Renders the region-mover args for the default region-server role group of a cluster whose
    /// `regionServers.config` is built from `config_lines`. Each entry is one YAML line nested
    /// under `config:`; indentation relative to `config:` is written as leading spaces in the entry
    /// (e.g. `"  runBeforeShutdown: true"` for a key under `regionMover:`).
    fn region_mover_args_for(config_lines: &[&str]) -> String {
        // `regionServers.config` is placed last so the generated config lines simply extend the
        // document, avoiding any trailing-key indentation juggling.
        const HEADER: &str = indoc! {r#"
            apiVersion: hbase.stackable.tech/v1alpha1
            kind: HbaseCluster
            metadata:
              name: test-hbase
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              image:
                productVersion: 2.6.4
              clusterConfig:
                hdfsConfigMapName: test-hdfs
                zookeeperConfigMapName: test-znode
              masters:
                roleGroups:
                  default:
                    replicas: 1
              restServers:
                roleGroups:
                  default:
                    replicas: 1
              regionServers:
                roleGroups:
                  default:
                    replicas: 1
                config:
        "#};

        let config = config_lines
            .iter()
            .map(|line| format!("      {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        let yaml = format!("{HEADER}{config}\n");

        let hbase = test_utils::hbase_from_yaml(&yaml);
        let validated = test_utils::validated_cluster_from(&hbase);
        test_utils::merged_config_for(&validated, &HbaseRole::RegionServer, "default")
            .region_mover_args()
    }

    #[test]
    fn empty_when_disabled() {
        assert_eq!(
            region_mover_args_for(&["regionMover:", "  runBeforeShutdown: false"]),
            ""
        );
    }

    #[test]
    fn empty_for_non_region_server_role() {
        // The region mover only applies to region servers; every other role returns no args.
        let validated = test_utils::validated_cluster();
        assert_eq!(
            test_utils::merged_config(&validated, &HbaseRole::Master).region_mover_args(),
            ""
        );
    }

    #[test]
    fn uses_default_graceful_shutdown_timeout_minus_delta() {
        // Default region-server graceful shutdown timeout is 60m (3600s); the region mover reserves
        // a 1m (60s) delta, leaving 3540s.
        assert_eq!(
            region_mover_args_for(&["regionMover:", "  runBeforeShutdown: true"]),
            "--maxthreads 1 --timeout 3540"
        );
    }

    #[test]
    fn subtracts_delta_when_timeout_above_delta() {
        // 5m (300s) - 1m delta = 240s.
        assert_eq!(
            region_mover_args_for(&[
                "gracefulShutdownTimeout: 5m",
                "regionMover:",
                "  runBeforeShutdown: true",
            ]),
            "--maxthreads 1 --timeout 240"
        );
    }

    #[test]
    fn keeps_timeout_when_exactly_at_delta() {
        // 1m (60s) is not greater than the 60s delta, so it is used verbatim (no underflow).
        assert_eq!(
            region_mover_args_for(&[
                "gracefulShutdownTimeout: 1m",
                "regionMover:",
                "  runBeforeShutdown: true",
            ]),
            "--maxthreads 1 --timeout 60"
        );
    }

    #[test]
    fn keeps_timeout_when_below_delta() {
        // 30s is below the 60s delta, so it is used verbatim rather than underflowing.
        assert_eq!(
            region_mover_args_for(&[
                "gracefulShutdownTimeout: 30s",
                "regionMover:",
                "  runBeforeShutdown: true",
            ]),
            "--maxthreads 1 --timeout 30"
        );
    }

    #[test]
    fn appends_noack_when_ack_disabled() {
        assert_eq!(
            region_mover_args_for(&["regionMover:", "  runBeforeShutdown: true", "  ack: false",]),
            "--maxthreads 1 --timeout 3540 --noack"
        );
    }

    #[test]
    fn appends_shell_escaped_extra_options() {
        // Extra options are passed through verbatim except for shell escaping of unsafe values.
        assert_eq!(
            region_mover_args_for(&[
                "regionMover:",
                "  runBeforeShutdown: true",
                r#"  additionalMoverOptions: ["--foo", "a b"]"#,
            ]),
            "--maxthreads 1 --timeout 3540 --foo 'a b'"
        );
    }
}
