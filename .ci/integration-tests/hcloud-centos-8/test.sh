#!/bin/bash
git clone -b "$GIT_BRANCH" https://github.com/stackabletech/hbase-operator.git
(cd hbase-operator/ && ./scripts/run_tests.sh)
exit_code=$?
./operator-logs.sh hbase > /target/hbase-operator.log
./operator-logs.sh hdfs > /target/hdfs-operator.log
./operator-logs.sh zookeeper > /target/zookeeper-operator.log
exit $exit_code
