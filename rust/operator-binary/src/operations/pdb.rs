use snafu::{ResultExt, Snafu};
use stackable_hbase_crd::{HbaseCluster, HbaseRole, APP_NAME};
use stackable_operator::{
    builder::pdb::PodDisruptionBudgetBuilder, client::Client, cluster_resources::ClusterResources,
    commons::pdb::PdbConfig, kube::ResourceExt,
};

use crate::{hbase_controller::HBASE_CONTROLLER_NAME, OPERATOR_NAME};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Cannot create PodDisruptionBudget for role [{role}]"))]
    CreatePdb {
        source: stackable_operator::error::Error,
        role: String,
    },

    #[snafu(display("Cannot apply PodDisruptionBudget [{name}]"))]
    ApplyPdb {
        source: stackable_operator::error::Error,
        name: String,
    },
}

pub async fn add_pdbs(
    pdb: &PdbConfig,
    hbase: &HbaseCluster,
    role: &HbaseRole,
    client: &Client,
    cluster_resources: &mut ClusterResources,
) -> Result<(), Error> {
    if !pdb.enabled {
        return Ok(());
    }
    let max_unavailable = pdb.max_unavailable.unwrap_or(match role {
        HbaseRole::Master => max_unavailable_masters(),
        HbaseRole::RegionServer => max_unavailable_region_servers(),
        HbaseRole::RestServer => max_unavailable_rest_servers(),
    });
    let pdb = PodDisruptionBudgetBuilder::new_with_role(
        hbase,
        APP_NAME,
        &role.to_string(),
        OPERATOR_NAME,
        HBASE_CONTROLLER_NAME,
    )
    .with_context(|_| CreatePdbSnafu {
        role: role.to_string(),
    })?
    .with_max_unavailable(max_unavailable)
    .build();
    let pdb_name = pdb.name_any();
    cluster_resources
        .add(client, pdb)
        .await
        .with_context(|_| ApplyPdbSnafu { name: pdb_name })?;

    Ok(())
}

fn max_unavailable_masters() -> u16 {
    1
}

fn max_unavailable_region_servers() -> u16 {
    1
}

fn max_unavailable_rest_servers() -> u16 {
    // RestServers are stateless, we only need to make sure we have two available, so we don't have a single point of failure.
    // However, users probably deploy multiple rest servers for both - availability and performance. As there is the use-case
    // of having multiple RestServers for availability reasons, we need to be restrictive and stick to our `maxUnavailable: 1`
    // for `Multiple replicas to increase availability` rolegroups guideline.
    1
}
