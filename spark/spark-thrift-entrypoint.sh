#!/usr/bin/env bash
set -e

$SPARK_HOME/sbin/start-thriftserver.sh \
  --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
  --hiveconf hive.server2.thrift.port=10000 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.warehouse.dir=/data/delta

# garder le conteneur vivant
tail -f /dev/null
