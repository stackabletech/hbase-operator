#!/usr/bin/env bash
#
# Create a table with 15 regions and count the number of regions on server 0.
# It should be more than 0.
#
set -x

# We need to check if t1 exists first before creating the table.
# The table might already be there if in a previous run, the final test
# for regions on server 0 fails.
# This can happen if Hbase didn't get to assign anything there yet and
# so kuttl re-runs this test step.
T1_EXISTS=$(echo "list" | /stackable/hbase/bin/hbase shell --noninteractive | grep -c t1)
if [ "$T1_EXISTS" == "0" ]; then
  /stackable/hbase/bin/hbase shell --noninteractive <<'EOF'
balance_switch false;
create 't1', 'f1', {NUMREGIONS => 15, SPLITALGO => 'HexStringSplit'};
EOF
fi

REGION_COUNT_ON_0=$(echo "list_regions 't1'" | /stackable/hbase/bin/hbase shell --noninteractive | grep -c test-hbase-regionserver-default-0)

test "${REGION_COUNT_ON_0}" -ge 0
