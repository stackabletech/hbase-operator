use snafu::{ResultExt, Snafu};

use crate::{
    controller::{build::opa::HbaseOpaConfig, zookeeper::ZookeeperConnectionInformation},
    crd::v1alpha1,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to retrieve zookeeper connection information"))]
    RetrieveZookeeperConnectionInformation {
        source: crate::controller::zookeeper::Error,
    },

    #[snafu(display("invalid OPA configuration"))]
    InvalidOpaConfig {
        source: crate::controller::build::opa::Error,
    },
}

/// External references resolved during the dereference step.
pub struct DereferencedObjects {
    pub zookeeper_connection_information: ZookeeperConnectionInformation,
    pub hbase_opa_config: Option<HbaseOpaConfig>,
}

pub async fn dereference(
    client: &stackable_operator::client::Client,
    hbase: &v1alpha1::HbaseCluster,
) -> Result<DereferencedObjects, Error> {
    let zookeeper_connection_information = ZookeeperConnectionInformation::retrieve(hbase, client)
        .await
        .context(RetrieveZookeeperConnectionInformationSnafu)?;

    let hbase_opa_config = match &hbase.spec.cluster_config.authorization {
        Some(opa_config) => Some(
            HbaseOpaConfig::from_opa_config(client, hbase, opa_config)
                .await
                .context(InvalidOpaConfigSnafu)?,
        ),
        None => None,
    };

    Ok(DereferencedObjects {
        zookeeper_connection_information,
        hbase_opa_config,
    })
}
