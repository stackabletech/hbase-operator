#!/usr/bin/env bash
# Usage: test_phoenix.sh

result=$(/stackable/phoenix/bin/psql.py /stackable/phoenix/examples/WEB_STAT.sql /stackable/phoenix/examples/WEB_STAT.csv /stackable/phoenix/examples/WEB_STAT_QUERIES.sql | grep 'EU')
# expected: EU  150
echo "Phoenix query result: $result"

# split into elements
result=($result)
el0=${result[0]}
el1=${result[1]}

if [ "$el1" == '150' ]; then
  echo "[SUCCESS] Selected query result: $el0 -> $el1"
else
  echo "[ERROR] Query failed!"
  exit 1
fi

echo "[SUCCESS] Phoenix test was successful!"
