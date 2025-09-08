#!/bin/bash
# Usage: ./run_spark_sql.sh -e "<COMMAND>"

docker exec -i eventpulse-spark-iceberg spark-sql "$@"
