mod hbase_controller;

use clap::Parser;
use futures::StreamExt;
use stackable_hbase_crd::HbaseCluster;
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    k8s_openapi::api::{apps::v1::StatefulSet, core::v1::Service},
    kube::{
        api::ListParams,
        runtime::{controller::Context, Controller},
        CustomResourceExt,
    },
};

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(Parser)]
#[clap(about = built_info::PKG_DESCRIPTION, author = stackable_operator::cli::AUTHOR)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    stackable_operator::logging::initialize_logging("HBASE_OPERATOR_LOG");

    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => println!("{}", serde_yaml::to_string(&HbaseCluster::crd())?,),
        Command::Run(ProductOperatorRun { product_config }) => {
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
                stackable_operator::client::create_client(Some("hbase.stackable.tech".to_string()))
                    .await?;

            Controller::new(client.get_all_api::<HbaseCluster>(), ListParams::default())
                .owns(client.get_all_api::<Service>(), ListParams::default())
                .owns(client.get_all_api::<StatefulSet>(), ListParams::default())
                .shutdown_on_signal()
                .run(
                    hbase_controller::reconcile_hbase,
                    hbase_controller::error_policy,
                    Context::new(hbase_controller::Ctx {
                        client: client.clone(),
                        product_config,
                    }),
                )
                .for_each(|res| async {
                    match res {
                        Ok((obj, _)) => tracing::info!(object = %obj, "Reconciled object"),
                        Err(err) => {
                            tracing::error!(
                                error = &err as &dyn std::error::Error,
                                "Failed to reconcile object",
                            )
                        }
                    }
                })
                .await;
        }
    }

    Ok(())
}
