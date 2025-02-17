#!/usr/bin/env bash
#
# Count the number of regions on server 1.
# It should contain all 15 regions after region server 0 has been restarted.
#
set -euo 'pipefail'
set -x

REGION_COUNT_ON_1=$(echo "list_regions 't1'" | /stackable/hbase/bin/hbase shell --noninteractive | grep -c test-hbase-regionserver-default-1)

test "${REGION_COUNT_ON_1}" -eq 15
