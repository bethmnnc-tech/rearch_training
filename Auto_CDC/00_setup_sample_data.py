# Databricks notebook source
# MAGIC %md
# MAGIC # 00 – Set up sample data for the AUTO CDC examples
# MAGIC
# MAGIC Run this notebook **before** you create the pipeline. It builds everything the pipeline reads:
# MAGIC
# MAGIC | Object | Used by example | What it simulates |
# MAGIC |---|---|---|
# MAGIC | Volume folder `customers_cdc/` (JSON files) | 1–5, 7 | A CDC feed (e.g. Debezium / Fivetran / app events) landing in cloud storage |
# MAGIC | Volume folder `orders_cdc/` (JSON files) | 4 | Order-line CDC with a composite key and timestamp ties |
# MAGIC | Table `legacy_customers` | 6 | An old system you need to back-fill from once |
# MAGIC | Table `crm_customers` (Change Data Feed ON) | 8 | A Delta table you consume through its change feed |
# MAGIC | Table `customers_full_snapshot` | 9 | A source that only gives you a full copy (no change events) |
# MAGIC | Table `customers_snapshot_history` | 10 | Dated/versioned snapshots you need to replay in order |
# MAGIC
# MAGIC **How to use it (the "teaching loop")**
# MAGIC 1. Run cells 1–4 once. They create the schema, volume, tables and land **batch 1**.
# MAGIC 2. Start the pipeline (triggered mode) and look at the results with `04_explore_results.sql`.
# MAGIC 3. Come back, run `land_batch(2)`, run the pipeline again, explore again. Repeat for batch 3.
# MAGIC 4. Batch 4 sends a **TRUNCATE** event. It wipes the SCD Type 1 tables that listen for truncates, so run it last.

# COMMAND ----------

# MAGIC %md ## 1. Parameters – change these to match your workspace

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "auto_cdc_demo", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = "landing"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"Landing zone: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. CDC feed batches (JSON lines)
# MAGIC
# MAGIC Every record carries:
# MAGIC * `operation` – INSERT / UPDATE / DELETE / TRUNCATE
# MAGIC * `sequence_num` – the order the change happened in the source (what `SEQUENCE BY` uses)
# MAGIC * `event_ts` – business time of the change (used by the bitemporal example)
# MAGIC * `changed_columns` – which columns this event actually changed (used by `COLUMNS TO UPDATE`)
# MAGIC
# MAGIC Each batch is designed to show a specific behaviour – see the comments.

# COMMAND ----------

import json

ALL_COLS = ["first_name", "last_name", "email", "city", "state", "tier", "last_login_ts"]


def cust(cid, first, last, email, city, state, tier, login, op, seq, ts, changed=None):
    return {
        "customer_id": cid, "first_name": first, "last_name": last, "email": email,
        "city": city, "state": state, "tier": tier, "last_login_ts": login,
        "operation": op, "sequence_num": seq, "event_ts": ts,
        "changed_columns": changed if changed is not None else ALL_COLS,
    }


CUSTOMER_BATCHES = {
    # ---------- Batch 1: initial load – five brand-new customers ----------
    1: [
        cust(101, "Ana",   "Lopez",  "ana@example.com",   "Boston",  "MA", "Bronze", "2026-10-01T08:00:00", "INSERT", 1, "2026-10-01T08:00:00"),
        cust(102, "Ben",   "Ortiz",  "ben@example.com",   "Austin",  "TX", "Silver", "2026-10-01T09:00:00", "INSERT", 2, "2026-10-01T09:00:00"),
        cust(103, "Cara",  "Nguyen", "cara@example.com",  "Seattle", "WA", "Gold",   "2026-10-01T10:00:00", "INSERT", 3, "2026-10-01T10:00:00"),
        cust(104, "Dev",   "Patel",  "dev@example.com",   "Denver",  "CO", "Bronze", "2026-10-01T11:00:00", "INSERT", 4, "2026-10-01T11:00:00"),
        cust(105, "Eli",   "Kim",    "eli@example.com",   "Miami",   "FL", "Silver", "2026-10-01T12:00:00", "INSERT", 5, "2026-10-01T12:00:00"),
    ],
    # ---------- Batch 2: updates, a delete, out-of-order events, a partial update ----------
    2: [
        # 101 moves city -> SCD2 table gets a NEW history row
        cust(101, "Ana", "Lopez", "ana@example.com", "Chicago", "IL", "Bronze", "2026-10-05T08:00:00", "UPDATE", 6, "2026-10-05T08:00:00", ["city", "state"]),
        # 102 only logs in -> SCD2 table does NOT add a history row (last_login_ts is excluded from history tracking)
        cust(102, "Ben", "Ortiz", "ben@example.com", "Austin", "TX", "Silver", "2026-10-05T09:30:00", "UPDATE", 7, "2026-10-05T09:30:00", ["last_login_ts"]),
        # 103 is deleted
        cust(103, None, None, None, None, None, None, None, "DELETE", 8, "2026-10-05T10:00:00", []),
        # 104 – two updates arrive OUT OF ORDER. seq 10 (Gold) lands before seq 9 (Silver).
        #        AUTO CDC uses sequence_num, so the final answer is Gold.
        cust(104, "Dev", "Patel", "dev@example.com", "Denver", "CO", "Gold",   "2026-10-05T11:00:00", "UPDATE", 10, "2026-10-05T11:10:00", ["tier"]),
        cust(104, "Dev", "Patel", "dev@example.com", "Denver", "CO", "Silver", "2026-10-05T11:00:00", "UPDATE", 9,  "2026-10-05T11:05:00", ["tier"]),
        # 105 – PARTIAL update: the source only sends the changed column (tier) and NULLs for the rest.
        #        Plain SCD1 will overwrite email/city/etc. with NULL. The partial-update examples will not.
        cust(105, None, None, None, None, None, "Platinum", None, "UPDATE", 11, "2026-10-05T12:00:00", ["tier"]),
    ],
    # ---------- Batch 3: late data, a re-insert, and bad records ----------
    3: [
        # LATE event for 101 – it happened (seq 3) BEFORE the Chicago move (seq 6) but arrives now.
        #   SCD1: ignored (seq 6 already applied).  SCD2: slotted into history between seq 1 and seq 6.
        cust(101, "Ana", "Lopez", "ana.lopez@example.com", "Boston", "MA", "Bronze", "2026-10-02T08:00:00", "UPDATE", 3, "2026-10-02T08:00:00", ["email"]),
        # 103 comes back after being deleted
        cust(103, "Cara", "Nguyen", "cara.n@example.com", "Portland", "OR", "Gold", "2026-10-07T10:00:00", "INSERT", 12, "2026-10-07T10:00:00"),
        # Brand-new customer
        cust(106, "Fay", "Reed", "fay@example.com", "Tampa", "FL", "Bronze", "2026-10-07T11:00:00", "INSERT", 13, "2026-10-07T11:00:00"),
        # BAD: no key -> dropped by the expectation in the cleaned view
        cust(None, "No", "Key", "nokey@example.com", "Nowhere", "NA", "Bronze", None, "INSERT", 14, "2026-10-07T12:00:00"),
        # BAD: no sequence value -> dropped (AUTO CDC does not allow NULL sequencing values)
        cust(107, "No", "Seq", "noseq@example.com", "Nowhere", "NA", "Bronze", None, "INSERT", None, "2026-10-07T12:30:00"),
    ],
    # ---------- Batch 4 (run LAST): a TRUNCATE event ----------
    4: [
        # Wipes every SCD Type 1 target that has APPLY AS TRUNCATE WHEN. Rows with a higher
        # sequence than the truncate (seq 21 below) survive.
        cust(None, None, None, None, None, None, None, None, "TRUNCATE", 20, "2026-10-10T00:00:00", []),
        cust(108, "Gus", "Hale", "gus@example.com", "Reno", "NV", "Bronze", "2026-10-10T01:00:00", "INSERT", 21, "2026-10-10T01:00:00"),
    ],
}

# Order lines: composite key (order_id, line_num). Two events for order 5001/line 1 share the SAME
# event_ts, so we break the tie with event_id -> SEQUENCE BY STRUCT(event_ts, event_id).
ORDER_BATCHES = {
    1: [
        {"order_id": 5001, "line_num": 1, "sku": "A-100", "qty": 1, "status": "OPEN", "op": "UPSERT", "event_ts": "2026-10-01T10:00:00", "event_id": 1},
        {"order_id": 5001, "line_num": 2, "sku": "B-200", "qty": 3, "status": "OPEN", "op": "UPSERT", "event_ts": "2026-10-01T10:00:00", "event_id": 2},
        {"order_id": 5002, "line_num": 1, "sku": "C-300", "qty": 2, "status": "OPEN", "op": "UPSERT", "event_ts": "2026-10-01T11:00:00", "event_id": 3},
    ],
    2: [
        # Same timestamp, two changes -> event_id 5 wins over event_id 4
        {"order_id": 5001, "line_num": 1, "sku": "A-100", "qty": 5, "status": "OPEN",    "op": "UPSERT", "event_ts": "2026-10-02T09:00:00", "event_id": 5},
        {"order_id": 5001, "line_num": 1, "sku": "A-100", "qty": 4, "status": "OPEN",    "op": "UPSERT", "event_ts": "2026-10-02T09:00:00", "event_id": 4},
        {"order_id": 5001, "line_num": 2, "sku": "B-200", "qty": 3, "status": "REMOVED", "op": "DELETE", "event_ts": "2026-10-02T09:05:00", "event_id": 6},
        {"order_id": 5002, "line_num": 1, "sku": "C-300", "qty": 2, "status": "SHIPPED", "op": "UPSERT", "event_ts": "2026-10-02T12:00:00", "event_id": 7},
    ],
    3: [],
    4: [],
}


def land_batch(n: int):
    """Write batch n as one JSON-lines file per feed. Auto Loader picks up only new files."""
    for feed, batches in [("customers_cdc", CUSTOMER_BATCHES), ("orders_cdc", ORDER_BATCHES)]:
        rows = batches.get(n, [])
        if not rows:
            continue
        path = f"{VOLUME_PATH}/{feed}/batch_{n:03d}.json"
        dbutils.fs.put(path, "\n".join(json.dumps(r) for r in rows), overwrite=True)
        print(f"Landed {len(rows)} rows -> {path}")

# COMMAND ----------

# MAGIC %md ## 3. Tables for the back-fill, change-feed and snapshot examples

# COMMAND ----------

fq = lambda t: f"{CATALOG}.{SCHEMA}.{t}"

# ---- Example 6: legacy system to back-fill ONCE ----
spark.sql(f"""
CREATE OR REPLACE TABLE {fq('legacy_customers')} AS
SELECT * FROM VALUES
  (90,  'Old', 'Timer', 'old@example.com',  'Albany',  'NY', 'Gold'),
  (91,  'Pat', 'Early', 'pat@example.com',  'Dayton',  'OH', 'Silver'),
  (101, 'Ana', 'Lopez', 'ana@legacy.com',   'Boston',  'MA', 'Bronze')   -- also in the live feed; live wins
AS t(customer_id, first_name, last_name, email, city, state, tier)
""")

# ---- Example 8: a Delta table with Change Data Feed turned on ----
spark.sql(f"""
CREATE OR REPLACE TABLE {fq('crm_customers')} (
  customer_id INT, first_name STRING, last_name STRING, email STRING, city STRING, state STRING, tier STRING
) TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
spark.sql(f"""INSERT INTO {fq('crm_customers')} VALUES
  (201, 'Gia', 'Moss',  'gia@crm.com',  'Omaha', 'NE', 'Bronze'),
  (202, 'Hal', 'Park',  'hal@crm.com',  'Tulsa', 'OK', 'Silver'),
  (203, 'Ivy', 'Stone', 'ivy@crm.com',  'Boise', 'ID', 'Gold')""")
spark.sql(f"UPDATE {fq('crm_customers')} SET tier = 'Gold' WHERE customer_id = 201")
spark.sql(f"DELETE FROM {fq('crm_customers')} WHERE customer_id = 202")

# ---- Example 9: a source that only offers a full snapshot (overwritten each day) ----
spark.sql(f"""
CREATE OR REPLACE TABLE {fq('customers_full_snapshot')} AS
SELECT * FROM VALUES
  (301, 'Jon', 'Ruiz', 'Phoenix', 'Bronze'),
  (302, 'Kai', 'Lee',  'Fresno',  'Silver'),
  (303, 'Liv', 'Shaw', 'Mobile',  'Gold')
AS t(customer_id, first_name, last_name, city, tier)
""")

# ---- Example 10: versioned snapshots to replay in order (e.g. one file/partition per day) ----
spark.sql(f"""
CREATE OR REPLACE TABLE {fq('customers_snapshot_history')} AS
SELECT * FROM VALUES
  -- snapshot 1 (day 1)
  (1, 401, 'Max', 'Akron',  'Bronze'),
  (1, 402, 'Nia', 'Macon',  'Silver'),
  -- snapshot 2 (day 2): 401 moved, 402 disappeared (= delete), 403 is new
  (2, 401, 'Max', 'Toledo', 'Bronze'),
  (2, 403, 'Oto', 'Selma',  'Gold'),
  -- snapshot 3 (day 3): 401 upgraded
  (3, 401, 'Max', 'Toledo', 'Gold'),
  (3, 403, 'Oto', 'Selma',  'Gold')
AS t(snapshot_version, customer_id, first_name, city, tier)
""")
print("Tables created.")

# COMMAND ----------

# MAGIC %md ## 4. Land batch 1, then go run the pipeline

# COMMAND ----------

land_batch(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Later runs – uncomment one at a time, running the pipeline in between
# MAGIC
# MAGIC To simulate a new day of snapshots for Example 9, you can also overwrite `customers_full_snapshot`
# MAGIC (the commented SQL below) before a pipeline run.

# COMMAND ----------

# land_batch(2)
# land_batch(3)
# land_batch(4)   # TRUNCATE – run last

# spark.sql(f"""
# INSERT OVERWRITE {fq('customers_full_snapshot')} VALUES
#   (301, 'Jon', 'Ruiz', 'Phoenix', 'Gold'),   -- changed
#   (303, 'Liv', 'Shaw', 'Mobile',  'Gold'),   -- unchanged; 302 missing = delete
#   (304, 'Mia', 'Fox',  'Ogden',   'Bronze')  -- new
# """)
