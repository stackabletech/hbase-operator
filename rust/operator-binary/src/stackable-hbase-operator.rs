use clap::{crate_version, App, AppSettings, SubCommand};
use kube::CustomResourceExt;
use stackable_hbase_crd::commands::{Restart, Start, Stop};
use stackable_hbase_crd::HbaseCluster;
use stackable_operator::{cli, logging};
use stackable_operator::{client, error};
use tracing::error;

mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    logging::initialize_logging("HBASE_OPERATOR_LOG");

    // Handle CLI arguments
    let matches = App::new(built_info::PKG_DESCRIPTION)
        .author("Stackable GmbH - info@stackable.de")
        .about(built_info::PKG_DESCRIPTION)
        .version(crate_version!())
        .arg(cli::generate_productconfig_arg())
        .subcommand(
            SubCommand::with_name("crd")
                .setting(AppSettings::ArgRequiredElseHelp)
                .subcommand(cli::generate_crd_subcommand::<HbaseCluster>())
                .subcommand(cli::generate_crd_subcommand::<Start>())
                .subcommand(cli::generate_crd_subcommand::<Stop>())
                .subcommand(cli::generate_crd_subcommand::<Restart>()),
        )
        .get_matches();

    if let ("crd", Some(subcommand)) = matches.subcommand() {
        if cli::handle_crd_subcommand::<HbaseCluster>(subcommand)? {
            return Ok(());
        };
        if cli::handle_crd_subcommand::<Start>(subcommand)? {
            return Ok(());
        };
        if cli::handle_crd_subcommand::<Stop>(subcommand)? {
            return Ok(());
        };
        if cli::handle_crd_subcommand::<Restart>(subcommand)? {
            return Ok(());
        };
    }

    let paths = vec![
        "deploy/config-spec/properties.yaml",
        "/etc/stackable/hbase-operator/config-spec/properties.yaml",
    ];
    let product_config_path = cli::handle_productconfig_arg(&matches, paths)?;

    stackable_operator::utils::print_startup_string(
        built_info::PKG_DESCRIPTION,
        built_info::PKG_VERSION,
        built_info::GIT_VERSION,
        built_info::TARGET,
        built_info::BUILT_TIME_UTC,
        built_info::RUSTC_VERSION,
    );

    let client = client::create_client(Some("hbase.stackable.tech".to_string())).await?;

    if let Err(error) = stackable_operator::crd::wait_until_crds_present(
        &client,
        vec![
            HbaseCluster::crd_name(),
            Restart::crd_name(),
            Start::crd_name(),
            Stop::crd_name(),
        ],
        None,
    )
    .await
    {
        error!("Required CRDs missing, aborting: {:?}", error);
        return Err(error);
    };

    tokio::try_join!(
        stackable_hbase_operator::create_controller(client.clone(), &product_config_path),
        stackable_operator::command_controller::create_command_controller::<Restart, HbaseCluster>(
            client.clone()
        ),
        stackable_operator::command_controller::create_command_controller::<Start, HbaseCluster>(
            client.clone()
        ),
        stackable_operator::command_controller::create_command_controller::<Stop, HbaseCluster>(
            client.clone()
        )
    )?;

    Ok(())
}
