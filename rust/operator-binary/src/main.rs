use std::sync::Arc;

use clap::Parser;
use futures::StreamExt;
use hbase_controller::FULL_HBASE_CONTROLLER_NAME;
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    kube::{
        core::DeserializeGuard,
        runtime::{
            events::{Recorder, Reporter},
            reflector::ObjectRef,
            watcher, Controller,
        },
        ResourceExt,
    },
    logging::controller::report_controller_reconciled,
    shared::yaml::SerializeOptions,
    YamlSchema,
};

use crate::crd::{v1alpha1, HbaseCluster, APP_NAME};

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
            stackable_operator::utils::print_startup_string(
                built_info::PKG_DESCRIPTION,
                built_info::PKG_VERSION,
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
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

            let event_recorder = Arc::new(Recorder::new(
                client.as_kube_client(),
                Reporter {
                    controller: FULL_HBASE_CONTROLLER_NAME.to_string(),
                    instance: None,
                },
            ));

            let hbase_controller = Controller::new(
                watch_namespace.get_api::<DeserializeGuard<v1alpha1::HbaseCluster>>(&client),
                watcher::Config::default(),
            );
            let hbase_store_1 = hbase_controller.store();
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
                        hbase_store_1
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
        || match hbase.spec.cluster_config.authorization.to_owned() {
            Some(hbase_authorization) => {
                hbase_authorization.opa.config_map_name == config_map.name_any()
            }
            None => false,
        }
}
