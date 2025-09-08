CREATE NAMESPACE IF NOT EXISTS eventpulse.migration_log;

CREATE TABLE IF NOT EXISTS eventpulse.migration_log.migrations (
  version STRING,
  description STRING,
  applied_at TIMESTAMP
) USING iceberg
LOCATION 's3://warehouse/migration_log/migrations/';
