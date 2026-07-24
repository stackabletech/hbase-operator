//! Build the per-role `PodDisruptionBudget` for the HbaseCluster.

use stackable_operator::{
    commons::pdb::PdbConfig, k8s_openapi::api::policy::v1::PodDisruptionBudget,
    v2::builder::pdb::pod_disruption_budget_builder_with_role,
};

use crate::{
    controller::{ValidatedCluster, controller_name, operator_name, product_name},
    crd::HbaseRole,
};

/// Builds the [`PodDisruptionBudget`] for the given `role`, or `None` if PDBs are disabled.
pub fn build_pdb(
    pdb: &PdbConfig,
    cluster: &ValidatedCluster,
    role: &HbaseRole,
) -> Option<PodDisruptionBudget> {
    if !pdb.enabled {
        return None;
    }
    let max_unavailable = pdb.max_unavailable.unwrap_or(match role {
        HbaseRole::Master => max_unavailable_masters(),
        HbaseRole::RegionServer => max_unavailable_region_servers(),
        HbaseRole::RestServer => max_unavailable_rest_servers(),
    });
    let pdb = pod_disruption_budget_builder_with_role(
        cluster,
        &product_name(),
        &role.into(),
        &operator_name(),
        &controller_name(),
    )
    .with_max_unavailable(max_unavailable)
    .build();

    Some(pdb)
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
