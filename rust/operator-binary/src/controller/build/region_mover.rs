//! Build-side region-mover logic for [`AnyServiceConfig`]: derives the `bin/hbase` region-mover
//! CLI arguments from the merged config. Kept out of the `crd` module so it stays a pure API
//! definition.

use shell_escape::escape;
use stackable_operator::shared::time::Duration;

use crate::crd::AnyServiceConfig;

const DEFAULT_REGION_MOVER_TIMEOUT: Duration = Duration::from_minutes_unchecked(59);
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
