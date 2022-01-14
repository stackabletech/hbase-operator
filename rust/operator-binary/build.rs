use stackable_hbase_crd::HbaseCluster;
use stackable_operator::crd::CustomResourceExt;

fn main() {
    built::write_built_file().expect("Failed to acquire build-time information");

    HbaseCluster::write_yaml_schema("../../deploy/crd/hbasecluster.crd.yaml").unwrap();
}
