# Observability Data Dictionary

## `observability.run_log`

| Column | Type | Description |
| --- | --- | --- |
| `run_id` | `VARCHAR` | Dagster run id (unique per run). |
| `job_name` | `VARCHAR` | Job name (for example, `daily_prices_job`). |
| `status` | `VARCHAR` | `SUCCESS` or `FAILURE`. |
| `start_time` | `TIMESTAMP` | Run start time (UTC). |
| `end_time` | `TIMESTAMP` | Run end time (UTC). |
| `duration_seconds` | `DOUBLE` | End minus start in seconds. |
| `partition_key` | `VARCHAR` | Partition key from run tags (if partitioned). |
| `tags_json` | `VARCHAR` | JSON-encoded run tags. |
| `assets_materialized_count` | `BIGINT` | Count of materialization events in the run. |
| `row_count` | `BIGINT` | Sum of `row_count` metadata across materializations. |
| `rows_inserted` | `BIGINT` | Sum of `rows_inserted` metadata across materializations. |
| `rows_updated` | `BIGINT` | Sum of `rows_updated` metadata across materializations. |
| `rows_deleted` | `BIGINT` | Sum of `rows_deleted` metadata across materializations. |
| `error_message` | `VARCHAR` | Failure message for failed runs. |
| `logged_ts` | `TIMESTAMP` | Row write timestamp (UTC). |

## `observability.run_asset_log`

| Column | Type | Description |
| --- | --- | --- |
| `run_id` | `VARCHAR` | Dagster run id. |
| `job_name` | `VARCHAR` | Job name. |
| `status` | `VARCHAR` | `SUCCESS` or `FAILURE`. |
| `partition_key` | `VARCHAR` | Partition key from run tags (if partitioned). |
| `asset_key` | `VARCHAR` | Asset key path (slash-delimited). |
| `assets_materialized_count` | `BIGINT` | Materialization event count for this asset/run. |
| `row_count` | `BIGINT` | Sum of `row_count` metadata for this asset/run. |
| `rows_inserted` | `BIGINT` | Sum of inserted-row metadata for this asset/run. |
| `rows_updated` | `BIGINT` | Sum of updated-row metadata for this asset/run. |
| `rows_deleted` | `BIGINT` | Sum of deleted-row metadata for this asset/run. |
| `logged_ts` | `TIMESTAMP` | Row write timestamp (UTC). |

## `observability.data_freshness_checks`

| Column | Type | Description |
| --- | --- | --- |
| `run_id` | `VARCHAR` | Dagster run id. |
| `job_name` | `VARCHAR` | Job name for evaluated check. |
| `partition_key` | `VARCHAR` | Partition key from run tags (if partitioned). |
| `check_name` | `VARCHAR` | Stable freshness check identifier. |
| `severity` | `VARCHAR` | Severity label (`RED` or `YELLOW` in current checks). |
| `status` | `VARCHAR` | `PASS`, `FAIL`, or `SKIPPED`. |
| `measured_value` | `DOUBLE` | Numeric check result value. |
| `threshold_value` | `DOUBLE` | Threshold used by pass/fail semantics. |
| `details_json` | `VARCHAR` | JSON diagnostics payload. |
| `logged_ts` | `TIMESTAMP` | Row write timestamp (UTC). |

## `observability.data_quality_checks`

| Column | Type | Description |
| --- | --- | --- |
| `check_id` | `VARCHAR` | Unique identifier for each check row. |
| `run_id` | `VARCHAR` | Dagster run id. |
| `job_name` | `VARCHAR` | Job name for evaluated check. |
| `partition_key` | `VARCHAR` | Partition key from run tags (if partitioned). |
| `check_name` | `VARCHAR` | Stable data quality check identifier. |
| `severity` | `VARCHAR` | Severity label (for example, `RED`, `YELLOW`). |
| `status` | `VARCHAR` | `PASS`, `FAIL`, `SKIPPED` (and `WARN` where emitted). |
| `measured_value` | `DOUBLE` | Numeric check result value. |
| `threshold_value` | `DOUBLE` | Threshold used by check logic. |
| `details_json` | `VARCHAR` | JSON diagnostics payload. |
| `logged_ts` | `TIMESTAMP` | Row write timestamp (UTC). |
