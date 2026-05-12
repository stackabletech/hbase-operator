use snafu::{ResultExt, Snafu};
use stackable_operator::commons::product_image_selection::{self, ResolvedProductImage};

use crate::{
    crd::v1alpha1, security::opa::HbaseOpaConfig, zookeeper::ZookeeperConnectionInformation,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to retrieve zookeeper connection information"))]
    RetrieveZookeeperConnectionInformation { source: crate::zookeeper::Error },

    #[snafu(display("invalid OPA configuration"))]
    InvalidOpaConfig { source: crate::security::opa::Error },
}

/// External references resolved during the dereference step.
pub struct DereferencedObjects {
    pub resolved_product_image: ResolvedProductImage,
    pub zookeeper_connection_information: ZookeeperConnectionInformation,
    pub hbase_opa_config: Option<HbaseOpaConfig>,
}

pub async fn dereference(
    client: &stackable_operator::client::Client,
    hbase: &v1alpha1::HbaseCluster,
    image_base_name: &str,
    image_repository: &str,
    pkg_version: &str,
) -> Result<DereferencedObjects, Error> {
    let resolved_product_image = hbase
        .spec
        .image
        .resolve(image_base_name, image_repository, pkg_version)
        .context(ResolveProductImageSnafu)?;

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
        resolved_product_image,
        zookeeper_connection_information,
        hbase_opa_config,
    })
}
