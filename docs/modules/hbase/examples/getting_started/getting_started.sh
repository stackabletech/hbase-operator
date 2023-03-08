#!/usr/bin/env bash
set -euo pipefail

# This script contains all the code snippets from the guide, as well as some assert tests
# to test if the instructions in the guide work. The user *could* use it, but it is intended
# for testing only. It installs an HBase cluster and its dependencies and executes several steps
# to verify that it is working.

if [ $# -eq 0 ]
then
  echo "Installation method argument ('helm' or 'stackablectl') required."
  exit 1
fi

cd "$(dirname "$0")"

case "$1" in
"helm")
echo "Adding 'stackable-dev' Helm Chart repository"
# tag::helm-add-repo[]
helm repo add stackable-dev https://repo.stackable.tech/repository/helm-dev/
# end::helm-add-repo[]
echo "Updating Helm repo"
helm repo update
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait zookeeper-operator stackable-dev/zookeeper-operator --version 0.0.0-dev
helm install --wait hdfs-operator stackable-dev/hdfs-operator --version 0.0.0-dev
helm install --wait commons-operator stackable-dev/commons-operator --version 0.0.0-dev
helm install --wait secret-operator stackable-dev/secret-operator --version 0.0.0-dev
helm install --wait hbase-operator stackable-dev/hbase-operator --version 0.0.0-dev
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=0.0.0-dev \
  secret=0.0.0-dev \
  zookeeper=0.0.0-dev \
  hdfs=0.0.0-dev \
  hbase=0.0.0-dev
# end::stackablectl-install-operators[]
;;
*)
echo "Need to give 'helm' or 'stackablectl' as an argument for which installation method to use!"
exit 1
;;
esac

echo "Creating ZooKeeper cluster"
# tag::install-zk[]
kubectl apply -f zk.yaml
# end::install-zk[]

echo "Creating ZNode"
# tag::install-zk[]
kubectl apply -f znode.yaml
# end::install-zk[]

for (( i=1; i<=15; i++ ))
do
  echo "Waiting for ZookeeperCluster to appear ..."
  if eval kubectl get statefulset simple-zk-server-default; then
    break
  fi

  sleep 1
done

echo "Awaiting ZooKeeper rollout finish"
# tag::watch-zk-rollout[]
kubectl rollout status --watch statefulset/simple-zk-server-default --timeout=300s
# end::watch-zk-rollout[]

echo "Creating HDFS cluster"
# tag::install-hdfs[]
kubectl apply -f hdfs.yaml
# end::install-hdfs[]

for (( i=1; i<=15; i++ ))
do
  echo "Waiting for HdfsCluster to appear ..."
  if eval kubectl get statefulset simple-hdfs-datanode-default; then
    break
  fi

  sleep 1
done

echo "Awaiting HDFS rollout finish"
# tag::watch-hdfs-rollout[]
kubectl rollout status --watch statefulset/simple-hdfs-datanode-default --timeout=300s
kubectl rollout status --watch statefulset/simple-hdfs-namenode-default --timeout=300s
kubectl rollout status --watch statefulset/simple-hdfs-journalnode-default --timeout=300s
# end::watch-hdfs-rollout[]

sleep 5

echo "Creating HBase cluster"
# tag::install-hbase[]
kubectl apply -f hbase.yaml
# end::install-hbase[]

for (( i=1; i<=15; i++ ))
do
  echo "Waiting for HBaseCluster to appear ..."
  if eval kubectl get statefulset simple-hbase-master-default; then
    break
  fi

  sleep 1
done

echo "Awaiting HBase rollout finish"
# tag::watch-hbase-rollout[]
kubectl rollout status --watch statefulset/simple-hbase-master-default --timeout=300s
kubectl rollout status --watch statefulset/simple-hbase-regionserver-default --timeout=300s
kubectl rollout status --watch statefulset/simple-hbase-restserver-default --timeout=300s
# end::watch-hbase-rollout[]

version() {
  # tag::cluster-version[]
  kubectl exec -n default simple-hbase-restserver-default-0 -- \
  curl -s -XGET -H "Accept: application/json" "http://simple-hbase-restserver-default:8080/version/cluster"
  # end::cluster-version[]
}

echo "Check cluster version..."
cluster_version=$(version | jq -r '.Version')

if [ "$cluster_version" == "2.4.12" ]; then
  echo "Cluster version: $cluster_version"
else
  echo "Unexpected version: $cluster_version"
  exit 1
fi

echo "Check cluster status..."
# tag::cluster-status[]
kubectl exec -n default simple-hbase-restserver-default-0 \
-- curl -s -XGET -H "Accept: application/json" "http://simple-hbase-restserver-default:8080/status/cluster" | json_pp
# end::cluster-status[]

echo "Check table via REST API..."
# tag::create-table[]
kubectl exec -n default simple-hbase-restserver-default-0 \
-- curl -s -XPUT -H "Accept: text/xml" -H "Content-Type: text/xml" \
"http://simple-hbase-restserver-default:8080/users/schema" \
-d '<TableSchema name="users"><ColumnSchema name="cf" /></TableSchema>'
# end::create-table[]

# tag::get-table[]
kubectl exec -n default simple-hbase-restserver-default-0 \
-- curl -s -XGET -H "Accept: application/json" "http://simple-hbase-restserver-default:8080/users/schema" | json_pp
# end::get-table[]

get_all() {
  # tag::get-tables[]
  kubectl exec -n default simple-hbase-restserver-default-0 \
  -- curl -s -XGET -H "Accept: application/json" "http://simple-hbase-restserver-default:8080/" |  json_pp
  # end::get-tables[]
}

echo "Checking tables found..."
tables_count=$(get_all | jq -r '.table' | jq '. | length')

if [ "$tables_count" == 1 ]; then
  echo "...the single expected table"
else
  echo "...an unexpected number: $tables_count"
  exit 1
fi

echo "Check table via Phoenix..."
# tag::phoenix-table[]
kubectl exec -n default simple-hbase-restserver-default-0 -- \
/stackable/phoenix/bin/psql.py \
/stackable/phoenix/examples/WEB_STAT.sql \
/stackable/phoenix/examples/WEB_STAT.csv \
/stackable/phoenix/examples/WEB_STAT_QUERIES.sql
# end::phoenix-table[]

echo "Re-checking tables: found..."
tables_count=$(get_all | jq -r '.table' | jq '. | length')

if [ "$tables_count" == 10 ]; then
  echo "...$tables_count tables. Success!"
else
  echo "...an unexpected number: $tables_count"
  exit 1
fi



