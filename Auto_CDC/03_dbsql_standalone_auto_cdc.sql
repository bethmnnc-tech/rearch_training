-- =============================================================================
-- 03 – AUTO CDC in Databricks SQL, WITHOUT building a pipeline  (BETA)
-- =============================================================================
-- Run in the SQL editor on a serverless SQL warehouse. Databricks manages the pipeline
-- behind the scenes. One statement = target + flow.
--
-- Good for: an analyst/SQL-only team that needs one or two CDC tables.
-- Use a full pipeline (the transformations/ folder) when you need several tables,
-- expectations, multiple flows into one target, snapshots, or CI/CD via bundles.
--
-- Status (Apr 2026 announcement): Beta. Needs serverless SQL warehouse (or SDP Pro/Advanced)
-- and DBR 17.3+. AUTO CDC FROM SNAPSHOT is not available here.
-- Replace main.auto_cdc_demo with your catalog.schema.
-- =============================================================================

USE CATALOG main;
USE SCHEMA auto_cdc_demo;

-- SCD Type 1 straight from the files in the volume
CREATE OR REFRESH STREAMING TABLE dbsql_customers_current
  FLOW AUTO CDC
  FROM STREAM read_files(
    '/Volumes/main/auto_cdc_demo/landing/customers_cdc/',
    format => 'json',
    schemaHints => 'customer_id INT, sequence_num BIGINT, event_ts TIMESTAMP, last_login_ts TIMESTAMP, changed_columns ARRAY<STRING>'
  )
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = 'DELETE'
  SEQUENCE BY sequence_num
  COLUMNS * EXCEPT (operation, changed_columns, _rescued_data)
  STORED AS SCD TYPE 1;

-- SCD Type 2 from an existing append-only bronze table – only the last lines change
CREATE OR REFRESH STREAMING TABLE dbsql_customers_history
  FLOW AUTO CDC
  FROM STREAM customers_cdc_bronze
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = 'DELETE'
  SEQUENCE BY sequence_num
  COLUMNS * EXCEPT (operation, changed_columns, source_file, ingest_ts, _rescued_data)
  STORED AS SCD TYPE 2
  TRACK HISTORY ON * EXCEPT (last_login_ts, event_ts, sequence_num);

-- Refresh on demand (you can also add a refresh schedule to the streaming table)
REFRESH STREAMING TABLE dbsql_customers_current;

-- NOTE: the bronze table above includes TRUNCATE rows and rows with NULL keys/sequences
-- (batches 3 and 4). In real use, filter them first – e.g. point FROM STREAM at a cleaned
-- streaming table – just like the pipeline version does with its cleaned view.
