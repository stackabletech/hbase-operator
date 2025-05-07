#!/usr/bin/env bash
# shellcheck disable=SC2034
set -euxo pipefail

export \
    AWS_ACCESS_KEY_ID=hbaseAccessKey \
    # Used by AWS bundle 1.12.367 via Hadoop 3.3.6
    AWS_SECRET_KEY=hbaseSecretKey \
    # Used by AWS bundle 2.24.6 via Hadoop 3.4.1
    AWS_SECRET_ACCESS_KEY=hbaseSecretKey \
    AWS_ENDPOINT=http://minio:9000/ \
    AWS_SSL_ENABLED=false \
    AWS_PATH_STYLE_ACCESS=true

# Create local snapshot
hbase shell create-snapshot.hbase 2>&1 | \
    grep '=> \["snap"\]' > /dev/null

# Export local snapshot to S3
# --no-checksum-verify is needed for HBase 2.6.1 until this is fixed: https://issues.apache.org/jira/browse/HBASE-28998
export-snapshot-to-s3 \
        --no-checksum-verify \
        --snapshot snap \
        --copy-to s3a://hbase/snap \
        --overwrite 2>&1 | \
    grep 'Export Completed: snap' > /dev/null

# Delete local snapshot
hbase shell delete-snapshot.hbase 2>&1 | \
    grep '=> \[\]' > /dev/null

# Import snapshot from S3
# --no-checksum-verify is needed for HBase 2.6.1 until this is fixed: https://issues.apache.org/jira/browse/HBASE-28998
export-snapshot-to-s3 \
        --no-checksum-verify \
        --snapshot snap \
        --copy-from s3a://hbase/snap \
        --copy-to hdfs://test-hdfs/hbase \
        --overwrite 2>&1 | \
    grep 'Export Completed: snap' > /dev/null

# Restore imported snapshot
hbase shell restore-snapshot.hbase 2>&1 | \
    grep 'value=42' > /dev/null
