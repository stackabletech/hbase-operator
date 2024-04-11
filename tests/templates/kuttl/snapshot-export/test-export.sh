#!/usr/bin/env sh
set -euxo pipefail

# The HDFS JARs are not on the CLASSPATH when calling
# `hbase snapshot export` which results in the error
# 'No FileSystem for scheme "hdfs"'. Passsing the argument
# `--internal-classpath` solves this problem.

export HBASE_CLASSPATH=/stackable/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.367.jar:/stackable/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.6.jar

CONF_DIR=`mktemp --directory`
cp /stackable/conf/* "$CONF_DIR"

sed --in-place '/<\/configuration>/{
    i <property><name>fs.s3a.endpoint</name><value>${env.S3A_ENDPOINT}</value></property>
    i <property><name>fs.s3a.connection.ssl.enabled</name><value>${env.S3A_SSL_ENABLED:-true}</value></property>
    i <property><name>fs.s3a.path.style.access</name><value>${env.S3A_PATH_STYLE_ACCESS:-false}</value></property>
}' "$CONF_DIR/core-site.xml"

export \
    AWS_ACCESS_KEY_ID=hbaseAccessKey \
    AWS_SECRET_KEY=hbaseSecretKey \
    S3A_ENDPOINT=http://minio:9000/ \
    S3A_SSL_ENABLED=false \
    S3A_PATH_STYLE_ACCESS=true

# Create local snapshot
hbase shell create-snapshot.hbase | \
    tee /dev/stderr | \
    grep '=> \["snap"\]' > /dev/null

# Export local snapshot to S3
hbase \
    --config "$CONF_DIR" \
    --internal-classpath \
    snapshot export \
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
hbase \
    --config "$CONF_DIR" \
    --internal-classpath \
    snapshot export \
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
