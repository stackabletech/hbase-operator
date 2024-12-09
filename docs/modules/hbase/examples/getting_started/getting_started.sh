#!/usr/bin/env bash
set -euo pipefail

# DO NOT EDIT THE SCRIPT
# Instead, update the j2 template, and regenerate it for dev with `make render-docs`.

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
echo "Adding 'stackable-stable' Helm Chart repository"
# tag::helm-add-repo[]
helm repo add stackable-stable https://repo.stackable.tech/repository/helm-stable/
# end::helm-add-repo[]
echo "Updating Helm repo"
helm repo update
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait zookeeper-operator stackable-stable/zookeeper-operator --version 24.11.1-rc1 &
helm install --wait hdfs-operator stackable-stable/hdfs-operator --version 24.11.1-rc1 &
helm install --wait commons-operator stackable-stable/commons-operator --version 24.11.1-rc1 &
helm install --wait secret-operator stackable-stable/secret-operator --version 24.11.1-rc1 &
helm install --wait listener-operator stackable-stable/listener-operator --version 24.11.1-rc1 &
helm install --wait hbase-operator stackable-stable/hbase-operator --version 24.11.1-rc1 &
wait
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=24.11.1-rc1 \
  secret=24.11.1-rc1 \
  listener=24.11.1-rc1 \
  zookeeper=24.11.1-rc1 \
  hdfs=24.11.1-rc1 \
  hbase=24.11.1-rc1
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

if [ "$cluster_version" == "2.4.18" ]; then
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

# There should only be the one table we created
expected_tables=$(echo "
users
" | sort | sed '/^$/d')

expected_count=$(echo "$expected_tables" | wc -l)
if [ "$tables_count" == "$expected_count" ]; then
  echo "...$tables_count expected table(s)"
else
  echo "...an unexpected number: $tables_count instead of $expected_count"
  actual_tables=$(get_all | jq -r '.table[].name' | sort)
  echo "additional tables expected to be present (if any):"
  comm -13 <(echo "$actual_tables") <(echo "$expected_tables")
  echo "additional tables unexpectedly present (if any):"
  comm -23 <(echo "$actual_tables") <(echo "$expected_tables")
  echo
  echo "If you have already run the script, data from the next steps will already exist"
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

# Phoenix sometimes introduces new tables, so we can list the expected tables
# here, and error out if they don't match
expected_tables=$(echo "
SYSTEM.CATALOG
SYSTEM.CHILD_LINK
SYSTEM.FUNCTION
SYSTEM.LOG
SYSTEM.MUTEX
SYSTEM.SEQUENCE
SYSTEM.STATS
SYSTEM.TASK
SYSTEM.TRANSFORM
WEB_STAT
users
" | sort | sed '/^$/d')

expected_count=$(echo "$expected_tables" | wc -l)
if [ "$tables_count" == "$expected_count" ]; then
  echo "...$tables_count expected tables. Success!"
else
  echo "...an unexpected number: $tables_count instead of $expected_count"
  actual_tables=$(get_all | jq -r '.table[].name' | sort)
  echo "additional tables expected to be present (if any):"
  comm -13 <(echo "$actual_tables") <(echo "$expected_tables")
  echo "additional tables unexpectedly present (if any):"
  comm -23 <(echo "$actual_tables") <(echo "$expected_tables")
  exit 1
fi
