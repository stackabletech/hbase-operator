use snafu::{ResultExt, Snafu};
use stackable_operator::builder::pod::PodBuilder;

use crate::crd::AnyServiceConfig;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to set terminationGracePeriod"))]
    SetTerminationGracePeriod {
        source: stackable_operator::builder::pod::Error,
    },
}

pub fn add_graceful_shutdown_config(
    merged_config: &AnyServiceConfig,
    pod_builder: &mut PodBuilder,
) -> Result<(), Error> {
    // This must be always set by the merge mechanism, as we provide a default value,
    // users can not disable graceful shutdown.
    if let Some(graceful_shutdown_timeout) = merged_config.graceful_shutdown_timeout() {
        pod_builder
            .termination_grace_period(graceful_shutdown_timeout)
            .context(SetTerminationGracePeriodSnafu)?;
    }

    Ok(())
}
