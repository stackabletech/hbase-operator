use std::sync::Arc;

use clap::Parser;
use futures::StreamExt;
use hbase_controller::FULL_HBASE_CONTROLLER_NAME;
use stackable_operator::{
    YamlSchema,
    cli::{Command, ProductOperatorRun},
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    kube::{
        ResourceExt,
        core::DeserializeGuard,
        runtime::{
            Controller,
            events::{Recorder, Reporter},
            reflector::ObjectRef,
            watcher,
        },
    },
    logging::controller::report_controller_reconciled,
    shared::yaml::SerializeOptions,
    telemetry::{
        Tracing,
        tracing::{RollingPeriod, settings::Settings},
    },
};
use tracing::level_filters::LevelFilter;

use crate::crd::{HbaseCluster, v1alpha1};

mod config;
mod crd;
mod discovery;
mod hbase_controller;
mod kerberos;
mod operations;
mod product_logging;
mod security;
mod zookeeper;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

const OPERATOR_NAME: &str = "hbase.stackable.com";

// TODO (@NickLarsenNZ): Change the variable to `CONSOLE_LOG`
pub const ENV_VAR_CONSOLE_LOG: &str = "HBASE_OPERATOR_LOG";

#[derive(Parser)]
#[clap(about, author)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => {
            HbaseCluster::merged_crd(HbaseCluster::V1Alpha1)?
                .print_yaml_schema(built_info::PKG_VERSION, SerializeOptions::default())?;
        }
        Command::Run(ProductOperatorRun {
            product_config,
            watch_namespace,
            telemetry_arguments,
            cluster_info_opts,
        }) => {
            let _tracing_guard = Tracing::builder()
                // TODO (@Techassi): This should be a constant
                .service_name("hbase-operator")
                .with_console_output((
                    ENV_VAR_CONSOLE_LOG,
                    LevelFilter::INFO,
                    !telemetry_arguments.no_console_output,
                ))
                // NOTE (@Techassi): Before stackable-telemetry was used, the log directory was
                // set via an env: `HBASE_OPERATOR_LOG_DIRECTORY`.
                // See: https://github.com/stackabletech/operator-rs/blob/f035997fca85a54238c8de895389cc50b4d421e2/crates/stackable-operator/src/logging/mod.rs#L40
                // Now it will be `ROLLING_LOGS` (or via `--rolling-logs <DIRECTORY>`).
                .with_file_output(telemetry_arguments.rolling_logs.map(|log_directory| {
                    let rotation_period = telemetry_arguments
                        .rolling_logs_period
                        .unwrap_or(RollingPeriod::Hourly)
                        .clone();

                    Settings::builder()
                        // TODO (@Techassi): Change to CONSOLE_LOG or FILE_LOG, create constant
                        .with_environment_variable(ENV_VAR_CONSOLE_LOG)
                        .with_default_level(LevelFilter::INFO)
                        .file_log_settings_builder(log_directory, "tracing-rs.json")
                        .with_rotation_period(rotation_period)
                        .build()
                }))
                .with_otlp_log_exporter((
                    "OTLP_LOG",
                    LevelFilter::DEBUG,
                    telemetry_arguments.otlp_logs,
                ))
                .with_otlp_trace_exporter((
                    "OTLP_TRACE",
                    LevelFilter::DEBUG,
                    telemetry_arguments.otlp_traces,
                ))
                .build()
                .init()?;

            tracing::info!(
                built_info.pkg_version = built_info::PKG_VERSION,
                built_info.git_version = built_info::GIT_VERSION,
                built_info.target = built_info::TARGET,
                built_info.built_time_utc = built_info::BUILT_TIME_UTC,
                built_info.rustc_version = built_info::RUSTC_VERSION,
                "Starting {description}",
                description = built_info::PKG_DESCRIPTION
            );

            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/hbase-operator/config-spec/properties.yaml",
            ])?;
            let client = stackable_operator::client::initialize_operator(
                Some(OPERATOR_NAME.to_string()),
                &cluster_info_opts,
            )
            .await?;

            let event_recorder = Arc::new(Recorder::new(client.as_kube_client(), Reporter {
                controller: FULL_HBASE_CONTROLLER_NAME.to_string(),
                instance: None,
            }));

            let hbase_controller = Controller::new(
                watch_namespace.get_api::<DeserializeGuard<v1alpha1::HbaseCluster>>(&client),
                watcher::Config::default(),
            );
            let config_map_store = hbase_controller.store();
            hbase_controller
                .owns(
                    watch_namespace.get_api::<Service>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<StatefulSet>(&client),
                    watcher::Config::default(),
                )
                .shutdown_on_signal()
                .watches(
                    watch_namespace.get_api::<DeserializeGuard<ConfigMap>>(&client),
                    watcher::Config::default(),
                    move |config_map| {
                        config_map_store
                            .state()
                            .into_iter()
                            .filter(move |hbase| references_config_map(hbase, &config_map))
                            .map(|hbase| ObjectRef::from_obj(&*hbase))
                    },
                )
                .run(
                    hbase_controller::reconcile_hbase,
                    hbase_controller::error_policy,
                    Arc::new(hbase_controller::Ctx {
                        client: client.clone(),
                        product_config,
                    }),
                )
                .for_each_concurrent(
                    16, // concurrency limit
                    |result| {
                        // The event_recorder needs to be shared across all invocations, so that
                        // events are correctly aggregated
                        let event_recorder = event_recorder.clone();
                        async move {
                            report_controller_reconciled(
                                &event_recorder,
                                FULL_HBASE_CONTROLLER_NAME,
                                &result,
                            )
                            .await;
                        }
                    },
                )
                .await;
        }
    }

    Ok(())
}

fn references_config_map(
    hbase: &DeserializeGuard<v1alpha1::HbaseCluster>,
    config_map: &DeserializeGuard<ConfigMap>,
) -> bool {
    let Ok(hbase) = &hbase.0 else {
        return false;
    };

    hbase.spec.cluster_config.zookeeper_config_map_name == config_map.name_any()
        || hbase.spec.cluster_config.hdfs_config_map_name == config_map.name_any()
        || match &hbase.spec.cluster_config.authorization {
            Some(hbase_authorization) => {
                hbase_authorization.opa.config_map_name == config_map.name_any()
            }
            None => false,
        }
}
