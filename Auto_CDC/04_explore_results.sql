-- =============================================================================
-- 04 – Explore the results after each pipeline run
-- =============================================================================
-- Run in the SQL editor. Replace main.auto_cdc_demo with your catalog.schema.
-- Expected results are written next to each query (after batches 1–3).

USE CATALOG main;
USE SCHEMA auto_cdc_demo;

-- 1. SCD1 – one row per customer
SELECT * FROM customers_current ORDER BY customer_id;
--   101 Chicago (the late seq-3 event did NOT overwrite it)
--   103 back again in Portland (deleted in batch 2, re-inserted in batch 3)
--   104 tier = Gold (seq 10 beat seq 9 even though seq 9 arrived later)
--   105 email/city/state are NULL  <- default behaviour for partial updates
--   no row for the bad records (NULL key / NULL sequence)

-- 2. SCD2 – full history; __END_AT IS NULL = current version
SELECT customer_id, city, email, tier, last_login_ts, __START_AT, __END_AT
FROM customers_history
ORDER BY customer_id, __START_AT;
--   101 has 3 versions: Boston/ana@ (1→3), Boston/ana.lopez@ (3→6), Chicago (6→NULL)
--       -> the late event was slotted into the middle of history
--   102 has 1 version with the NEW last_login_ts (login is not history-tracked)
--   103 has a closed version (deleted at 8) and a new open version from 12

-- 3. Current rows only from SCD2
SELECT * FROM customers_history WHERE __END_AT IS NULL;

-- 4. Partial updates – compare customer 105 across the three SCD1 tables
SELECT 'default'           AS approach, customer_id, email, city, tier FROM customers_current               WHERE customer_id = 105
UNION ALL
SELECT 'ignore nulls',                   customer_id, email, city, tier FROM customers_partial_ignore_nulls  WHERE customer_id = 105
UNION ALL
SELECT 'columns to update',              customer_id, email, city, tier FROM customers_partial_cols_to_update WHERE customer_id = 105;
--   default: email/city NULL    others: eli@example.com / Miami kept, tier = Platinum

-- 5. Composite key + struct sequence
SELECT * FROM order_lines_current ORDER BY order_id, line_num;
--   5001/1 qty = 5 (event_id 5 beat event_id 4 at the same timestamp); 5001/2 deleted

-- 6. Back-fill + live flows in one target
SELECT * FROM customers_unified ORDER BY customer_id;
--   90 and 91 come only from legacy; 101 shows live values (live sequence > 0)

-- 7. Delta Change Data Feed source
SELECT * FROM crm_customers_current ORDER BY customer_id;
--   201 Gold, 202 gone (deleted upstream), 203 present

-- 8. Snapshot-derived history
SELECT * FROM customers_snapshot_replayed ORDER BY customer_id, __START_AT;
--   401: Akron (1→2), Toledo Bronze (2→3), Toledo Gold (3→NULL)
--   402: closed at version 2 (missing from snapshot 2 = delete);  403 starts at 2

-- 9. After landing batch 4 (TRUNCATE at seq 20, then customer 108 at seq 21):
--   customers_current and customers_unified contain ONLY 108.
--   customers_history is untouched (SCD2 can't truncate; that's why it reads the no-truncate view).

-- 10. Pipeline event log – see what each flow did (rows upserted/deleted, expectation drops)
SELECT timestamp, origin.flow_name, details:flow_progress.metrics, details:flow_progress.data_quality
FROM event_log(TABLE(customers_current))
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;
