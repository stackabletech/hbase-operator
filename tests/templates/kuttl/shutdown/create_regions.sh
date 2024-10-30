#!/usr/bin/env bash
#
# Create a table with 15 regions and count the number of regions on server 0.
# It should be more than 0.
#
set -euo 'pipefail'
set -x

echo "balance_switch false" | /stackable/hbase/bin/hbase shell --noninteractive

echo "create 't1', 'f1', {NUMREGIONS => 15, SPLITALGO => 'HexStringSplit'}" | /stackable/hbase/bin/hbase shell --noninteractive

REGION_COUNT_ON_0=$(echo "list_regions 't1'" | /stackable/hbase/bin/hbase shell --noninteractive | grep -c test-hbase-regionserver-default-0)

test "${REGION_COUNT_ON_0}" -ge 0
