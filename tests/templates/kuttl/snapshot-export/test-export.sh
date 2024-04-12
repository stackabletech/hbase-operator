#!/usr/bin/env sh
set -euxo pipefail

export \
    AWS_ACCESS_KEY_ID=hbaseAccessKey \
    AWS_SECRET_KEY=hbaseSecretKey \
    AWS_ENDPOINT=http://minio:9000/ \
    AWS_SSL_ENABLED=false \
    AWS_PATH_STYLE_ACCESS=true

# Create local snapshot
hbase shell create-snapshot.hbase | \
    tee /dev/stderr | \
    grep '=> \["snap"\]' > /dev/null

# Export local snapshot to S3
export-snapshot-to-s3 \
        --snapshot snap \
        --copy-to s3a://hbase/snap \
        --overwrite | \
    tee /dev/stderr | \
    grep 'Export Completed: snap' > /dev/null

# Delete local snapshot
hbase shell delete-snapshot.hbase | \
    tee /dev/stderr | \
    grep '=> \[\]' > /dev/null

# Import snapshot from S3
export-snapshot-to-s3 \
        --snapshot snap \
        --copy-from s3a://hbase/snap \
        --copy-to hdfs://test-hdfs/hbase \
        --overwrite | \
    tee /dev/stderr | \
    grep 'Export Completed: snap' > /dev/null

# Restore imported snapshot
hbase shell restore-snapshot.hbase | \
    tee /dev/stderr | \
    grep 'value=42' > /dev/null
