#!/bin/bash
set -e

echo "Installing pymongo..."
pip install pymongo

SYNC_INTERVAL_SECONDS=${SYNC_INTERVAL_SECONDS:-300}

echo "Starting periodic sync loop. Interval: ${SYNC_INTERVAL_SECONDS}s"

while true; do
    echo "=================================================="
    echo "$(date): Running sync job..."
    echo "=================================================="

    /opt/spark/bin/spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1 \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        /opt/spark-job/eth_to_iceberg.py

    echo "$(date): Sync job finished. Sleeping for ${SYNC_INTERVAL_SECONDS}s..."
    sleep "${SYNC_INTERVAL_SECONDS}"
done
