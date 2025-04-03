use std::sync::Arc;

use clap::Parser;
use futures::StreamExt;
use hbase_controller::FULL_HBASE_CONTROLLER_NAME;
use stackable_operator::{
    YamlSchema,
    cli::{Command, ProductOperatorRun},
    k8s_openapi::api::{apps::v1::StatefulSet, core::v1::Service},
    kube::{
        core::DeserializeGuard,
        runtime::{
            Controller,
            events::{Recorder, Reporter},
            watcher,
        },
    },
    logging::controller::report_controller_reconciled,
    shared::yaml::SerializeOptions,
};

use crate::crd::{APP_NAME, HbaseCluster, v1alpha1};

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
            tracing_target,
            cluster_info_opts,
        }) => {
            stackable_operator::logging::initialize_logging(
                "HBASE_OPERATOR_LOG",
                APP_NAME,
                tracing_target,
            );
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

            Controller::new(
                watch_namespace.get_api::<DeserializeGuard<v1alpha1::HbaseCluster>>(&client),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace.get_api::<Service>(&client),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace.get_api::<StatefulSet>(&client),
                watcher::Config::default(),
            )
            .shutdown_on_signal()
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
