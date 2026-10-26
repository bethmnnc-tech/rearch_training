-- =============================================================================
-- AUTO CDC INTO – every option in one Lakeflow Spark Declarative Pipeline (SQL)
-- =============================================================================
-- Same examples as the Python file, in SQL. Use THIS file or the Python file in a
-- pipeline – not both (they create the same table names).
--
-- Pattern you'll see over and over:
--   1. CREATE OR REFRESH STREAMING TABLE <target>;        -- declare an empty target
--   2. CREATE FLOW <name> AS AUTO CDC INTO <target>        -- attach the CDC logic
--        FROM STREAM(<source>) KEYS (...) SEQUENCE BY ... ;
--
-- Clause order inside AUTO CDC (keep this order or it won't parse):
--   KEYS → IGNORE NULL UPDATES → APPLY AS DELETE WHEN → APPLY AS TRUNCATE WHEN →
--   SEQUENCE BY → SYSTEM SEQUENCE BY → COLUMNS → STORED AS → TRACK HISTORY ON →
--   COLUMNS TO UPDATE
--
-- Pipeline configuration needed (Pipeline settings > Configuration):
--   cdc_demo.source_catalog = <catalog used in 00_setup_sample_data>
--   cdc_demo.source_schema  = <schema used in 00_setup_sample_data>
--
-- NOT available in SQL inside a pipeline (use the Python file):
--   * AUTO CDC FROM SNAPSHOT (Examples 9 & 10) – Python only today
--   * Streaming a Delta Change Data Feed as the source (Example 8) and streaming a
--     target's change feed downstream (Example 11b) – shown in Python
-- =============================================================================


-- =============================================================================
-- EXAMPLE 0 – The CDC source: bronze ingest + cleaned views
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE customers_cdc_bronze
COMMENT 'Raw customer CDC events exactly as they landed. Append-only.'
AS SELECT
  *,
  _metadata.file_path AS source_file,           -- lineage
  current_timestamp() AS ingest_ts              -- when WE saw it (system time)
FROM STREAM read_files(
  '/Volumes/${cdc_demo.source_catalog}/${cdc_demo.source_schema}/landing/customers_cdc/',
  format => 'json',
  schemaHints => 'customer_id INT, sequence_num BIGINT, event_ts TIMESTAMP, last_login_ts TIMESTAMP, changed_columns ARRAY<STRING>'
);

-- Expectations drop records AUTO CDC can't handle before they reach any target.
CREATE TEMPORARY VIEW customers_cdc_clean (
  CONSTRAINT has_key        EXPECT (customer_id IS NOT NULL OR operation = 'TRUNCATE') ON VIOLATION DROP ROW,
  CONSTRAINT has_sequence   EXPECT (sequence_num IS NOT NULL)                          ON VIOLATION DROP ROW,
  CONSTRAINT known_operation EXPECT (operation IN ('INSERT','UPDATE','DELETE','TRUNCATE'))  -- warn only
)
COMMENT 'Customer CDC events that are safe to apply.'
AS SELECT * FROM STREAM(customers_cdc_bronze);

-- Targets that don't use APPLY AS TRUNCATE (and every SCD2 target) must not see TRUNCATE rows.
CREATE TEMPORARY VIEW customers_cdc_no_truncate
COMMENT 'Same feed minus TRUNCATE events – for targets that do not truncate.'
AS SELECT * FROM STREAM(customers_cdc_clean) WHERE operation <> 'TRUNCATE';


-- =============================================================================
-- EXAMPLE 1 – SCD Type 1: latest value only, with deletes and truncates
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE customers_current
COMMENT 'SCD1 – one row per customer, latest values only.'
CLUSTER BY (customer_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',                        -- lets others stream our changes
  'pipelines.cdc.tombstoneGCThresholdInSeconds' = '259200'      -- keep delete tombstones 3 days (default 2)
);

CREATE FLOW customers_current_flow AS AUTO CDC INTO customers_current
FROM STREAM(customers_cdc_clean)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
APPLY AS TRUNCATE WHEN operation = 'TRUNCATE'                  -- SCD1 only
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, changed_columns, source_file, ingest_ts, _rescued_data)
STORED AS SCD TYPE 1;                                           -- default; written for clarity


-- =============================================================================
-- EXAMPLE 2 – SCD Type 2: full history (__START_AT / __END_AT added for you)
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE customers_history
COMMENT 'SCD2 – every version of every customer.'
CLUSTER BY (customer_id);

CREATE FLOW customers_history_flow AS AUTO CDC INTO customers_history
FROM STREAM(customers_cdc_no_truncate)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, changed_columns, source_file, ingest_ts, _rescued_data)
STORED AS SCD TYPE 2
-- Changes ONLY to these columns update the current row instead of adding a version:
TRACK HISTORY ON * EXCEPT (last_login_ts, event_ts, sequence_num);
-- Alternative allow-list form:  TRACK HISTORY ON city, state, tier, email


-- =============================================================================
-- EXAMPLE 3 – Pick exactly which columns land (allow-list)
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE customer_contact_current
COMMENT 'SCD1 – a narrow contact table built from the same feed.';

CREATE FLOW customer_contact_flow AS AUTO CDC INTO customer_contact_current
FROM STREAM(customers_cdc_no_truncate)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_num
COLUMNS customer_id, first_name, last_name, email;


-- =============================================================================
-- EXAMPLE 4 – Composite keys + STRUCT sequence to break timestamp ties
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE orders_cdc_bronze
COMMENT 'Raw order-line CDC events.'
AS SELECT * FROM STREAM read_files(
  '/Volumes/${cdc_demo.source_catalog}/${cdc_demo.source_schema}/landing/orders_cdc/',
  format => 'json',
  schemaHints => 'order_id INT, line_num INT, qty INT, event_ts TIMESTAMP, event_id BIGINT'
);

CREATE OR REFRESH STREAMING TABLE order_lines_current
COMMENT 'SCD1 – current order lines.';

CREATE FLOW order_lines_flow AS AUTO CDC INTO order_lines_current
FROM STREAM(orders_cdc_bronze)
KEYS (order_id, line_num)                                      -- composite key
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY STRUCT(event_ts, event_id)                         -- event_ts first, event_id breaks ties
COLUMNS * EXCEPT (op, _rescued_data);


-- =============================================================================
-- EXAMPLE 5 – Partial updates
-- =============================================================================
-- 5a – NULL in an update means "no change"
CREATE OR REFRESH STREAMING TABLE customers_partial_ignore_nulls
COMMENT 'SCD1 – NULLs in updates keep the existing value.';

CREATE FLOW customers_partial_ignore_nulls_flow AS AUTO CDC INTO customers_partial_ignore_nulls
FROM STREAM(customers_cdc_no_truncate)
KEYS (customer_id)
IGNORE NULL UPDATES                                            -- every column
-- Finer control (use instead of the line above):
--   IGNORE NULL UPDATES ON * EXCEPT (last_login_ts)            -- all but these ignore NULLs
--   IGNORE NULL UPDATES ON email, city, state                  -- only these ignore NULLs
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, changed_columns, source_file, ingest_ts, _rescued_data);

-- 5b – each event lists the columns it changed (can also set a column to NULL on purpose)
CREATE OR REFRESH STREAMING TABLE customers_partial_cols_to_update
COMMENT 'SCD1 – only the columns named in changed_columns are written.';

CREATE FLOW customers_partial_cols_to_update_flow AS AUTO CDC INTO customers_partial_cols_to_update
FROM STREAM(customers_cdc_no_truncate)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, changed_columns, source_file, ingest_ts, _rescued_data, sequence_num, event_ts)
COLUMNS TO UPDATE changed_columns;                             -- ARRAY<STRING> column; not with IGNORE NULL UPDATES


-- =============================================================================
-- EXAMPLE 6 – Several flows into ONE target: one-time back-fill + live stream
-- =============================================================================
CREATE TEMPORARY VIEW legacy_customers_shaped
COMMENT 'Legacy rows shaped like the live feed (batch read for the ONCE flow).'
AS SELECT
  customer_id, first_name, last_name, email, city, state, tier,
  CAST(0 AS BIGINT) AS sequence_num,                          -- older than any live event
  'INSERT'          AS operation
FROM ${cdc_demo.source_catalog}.${cdc_demo.source_schema}.legacy_customers;

CREATE OR REFRESH STREAMING TABLE customers_unified
COMMENT 'SCD1 – legacy back-fill merged with the live CDC feed.';

-- Flow A: runs ONE time (and again only on a full refresh). Note: no STREAM keyword.
CREATE FLOW customers_unified_legacy_backfill AS AUTO CDC ONCE INTO customers_unified
FROM legacy_customers_shaped
KEYS (customer_id)
SEQUENCE BY sequence_num
COLUMNS customer_id, first_name, last_name, email, city, state, tier;

-- Flow B: live stream. Same keys, same sequence type, unique flow name.
CREATE FLOW customers_unified_live AS AUTO CDC INTO customers_unified
FROM STREAM(customers_cdc_clean)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
APPLY AS TRUNCATE WHEN operation = 'TRUNCATE'
SEQUENCE BY sequence_num
COLUMNS customer_id, first_name, last_name, email, city, state, tier;


-- =============================================================================
-- EXAMPLE 7 – Bitemporal history (BETA – needs pipeline channel = PREVIEW)
-- =============================================================================
-- Adds __START_AT/__END_AT (business time) AND __SYSTEM_START_AT/__SYSTEM_END_AT (system time).
--
-- CREATE OR REFRESH STREAMING TABLE customers_bitemporal;
--
-- CREATE FLOW customers_bitemporal_flow AS AUTO CDC INTO customers_bitemporal
-- FROM STREAM(customers_cdc_no_truncate)
-- KEYS (customer_id)
-- APPLY AS DELETE WHEN operation = 'DELETE'
-- SEQUENCE BY event_ts                     -- when it happened in the business
-- SYSTEM SEQUENCE BY ingest_ts             -- when we learned about it
-- COLUMNS * EXCEPT (operation, changed_columns, source_file, _rescued_data)
-- STORED AS BITEMPORAL;


-- =============================================================================
-- EXAMPLE 11a – Consume an AUTO CDC target downstream
-- =============================================================================
-- AUTO CDC targets receive updates/deletes, so read them in BATCH (materialized view),
-- not as a plain append-only stream.
CREATE OR REFRESH MATERIALIZED VIEW customer_tier_counts
COMMENT 'Gold – number of current customers per tier.'
AS SELECT tier, COUNT(*) AS customers
FROM customers_history
WHERE __END_AT IS NULL                                         -- current SCD2 rows only
GROUP BY tier;
