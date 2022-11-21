mod discovery;
mod hbase_controller;
mod rbac;

use crate::hbase_controller::HBASE_CONTROLLER_NAME;

use clap::Parser;
use futures::StreamExt;
use stackable_hbase_crd::{HbaseCluster, APP_NAME};
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    k8s_openapi::api::{apps::v1::StatefulSet, core::v1::Service},
    kube::{api::ListParams, runtime::controller::Controller},
    logging::controller::report_controller_reconciled,
    CustomResourceExt,
};
use std::sync::Arc;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

const OPERATOR_NAME: &str = "hbase.stackable.com";

#[derive(Parser)]
#[clap(about = built_info::PKG_DESCRIPTION, author = stackable_operator::cli::AUTHOR)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => {
            HbaseCluster::print_yaml_schema()?;
        }
        Command::Run(ProductOperatorRun {
            product_config,
            watch_namespace,
            tracing_target,
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
            let client =
                stackable_operator::client::create_client(Some(OPERATOR_NAME.to_string())).await?;

            Controller::new(
                watch_namespace.get_api::<HbaseCluster>(&client),
                ListParams::default(),
            )
            .owns(
                watch_namespace.get_api::<Service>(&client),
                ListParams::default(),
            )
            .owns(
                watch_namespace.get_api::<StatefulSet>(&client),
                ListParams::default(),
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
            .map(|res| {
                report_controller_reconciled(
                    &client,
                    &format!("{HBASE_CONTROLLER_NAME}.{OPERATOR_NAME}"),
                    &res,
                )
            })
            .collect::<()>()
            .await;
        }
    }

    Ok(())
}
