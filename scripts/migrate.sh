#!/bin/bash
# scripts/migrate.sh
set -e

MIGRATIONS_DIR=tools/migrations
SPARK_SCRIPT=scripts/run_spark_sql.sh

echo "Running Iceberg migrations with log..."

$SPARK_SCRIPT -f "$MIGRATIONS_DIR/V0__create_migration_log_table.sql"

for f in "$MIGRATIONS_DIR"/*.sql; do
    version=$(basename "$f" | cut -d"_" -f1)
    if [[ "$version" == "V0" ]]; then
        continue
    fi

    # Check if migration log table exists
    log_table_exists=$($SPARK_SCRIPT -e "SHOW TABLES IN eventpulse.migration_log LIKE 'migrations'" | grep migrations || true)
    if [[ -z "$log_table_exists" && "$version" != "V0" ]]; then
        echo "Migration log table not found, please run V0 first"
        exit 1
    fi

    # Check if migration already applied
    count=$($SPARK_SCRIPT -e "SELECT COUNT(*) FROM eventpulse.migration_log.migrations WHERE version='$version'" | tail -n1)

    if [[ "$count" == "0" || "$version" == "V0" ]]; then
        echo "Applying migration $f..."
        $SPARK_SCRIPT -f "$f"

        # Insert into migration log (skip for V0)
        if [[ "$version" != "V0" ]]; then
            $SPARK_SCRIPT -e "INSERT INTO eventpulse.migration_log.migrations VALUES('$version','$f',current_timestamp())"
        fi
    else
        echo "Migration $f already applied, skipping."
    fi
done
