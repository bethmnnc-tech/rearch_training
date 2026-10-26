"""
AUTO CDC INTO – every option in one Lakeflow Spark Declarative Pipeline (Python)
================================================================================

What AUTO CDC does
------------------
You give it a stream of change events (insert / update / delete) and tell it:
  * KEYS        – which column(s) identify a row
  * SEQUENCE BY – which column says what order the changes happened in
It then keeps a target streaming table correct for you: it upserts, deletes,
handles events that arrive out of order or late, and (optionally) keeps full
history (SCD Type 2). It replaces the hand-written MERGE INTO + dedupe +
watermark logic you would otherwise maintain.

Release status (checked 2026-10-23 against docs.databricks.com – recheck before reuse)
  * create_auto_cdc_flow / AUTO CDC INTO ....................... GA (replaces apply_changes / APPLY CHANGES INTO)
  * create_auto_cdc_from_snapshot_flow (Python only in SDP) .... GA in docs (no preview badge)
  * Partial updates (ignore_null_updates_* , columns_to_update)  GA (Aug 2026)
  * Bitemporal (stored_as_scd_type="bitemporal") ............... Beta – needs the PREVIEW channel
  * Standalone AUTO CDC in Databricks SQL (no pipeline) ......... Beta – see 03_dbsql_standalone_auto_cdc.sql
  * Needs: serverless pipelines, or the Pro / Advanced pipeline edition.

How to run
----------
1. Run 00_setup_sample_data.py (creates the volume, sample files and tables).
2. Create a pipeline (Jobs & Pipelines > Create > ETL pipeline) with:
     - Default catalog / schema = the catalog / schema you used in setup
     - Source code = THIS file  (use the SQL file instead if you prefer SQL – not both,
       they define the same table names)
     - Configuration:  cdc_demo.source_catalog = <catalog>
                       cdc_demo.source_schema  = <schema>
     - Pipeline mode = Triggered (easiest for learning; also REQUIRED if you ever combine a
       FROM SNAPSHOT flow and AUTO CDC flows into one target)
3. Run it, explore with 04_explore_results.sql, land the next batch, run again.

Examples in this file
---------------------
  0. Bronze ingest (Auto Loader) + a cleaned view with expectations  – the CDC source
  1. SCD Type 1 – latest value only, with deletes and truncates
  2. SCD Type 2 – full history, with "track history on all columns except ..."
  3. Choosing which columns land in the target (column_list / except_column_list)
  4. Composite keys + tie-breaking with a STRUCT sequence
  5. Partial updates – ignore NULLs, or COLUMNS TO UPDATE
  6. Several flows into ONE target – one-time back-fill (once=True) + live stream
  7. Bitemporal history (Beta – commented out)
  8. Source = a Delta table's Change Data Feed
  9. AUTO CDC FROM SNAPSHOT – a table that is a full copy each run
 10. AUTO CDC FROM SNAPSHOT – replay versioned snapshots with a function
 11. Consuming AUTO CDC targets downstream (current-state MV + streaming the change feed)
"""

from typing import Optional, Tuple

from pyspark import pipelines as dp            # the SDP Python module (older code used: import dlt)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Pipeline configuration values (set under Pipeline settings > Configuration)
SRC_CATALOG = spark.conf.get("cdc_demo.source_catalog", "main")
SRC_SCHEMA = spark.conf.get("cdc_demo.source_schema", "auto_cdc_demo")
SRC = f"{SRC_CATALOG}.{SRC_SCHEMA}"
LANDING = f"/Volumes/{SRC_CATALOG}/{SRC_SCHEMA}/landing"

# Columns that exist only to drive CDC / lineage – we usually keep them OUT of the target.
CDC_CONTROL_COLS = ["operation", "changed_columns", "source_file", "ingest_ts", "_rescued_data"]


# =============================================================================
# EXAMPLE 0 – The CDC source: bronze ingest + cleaned view
# =============================================================================
# AUTO CDC needs a STREAMING source. The most common pattern is:
#   files land in cloud storage -> Auto Loader streaming table (bronze) -> AUTO CDC (silver)

@dp.table(
    name="customers_cdc_bronze",
    comment="Raw customer CDC events exactly as they landed. Append-only.",
)
def customers_cdc_bronze() -> DataFrame:
    return (
        spark.readStream.format("cloudFiles")                     # Auto Loader
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        # Pin the types AUTO CDC cares about. SEQUENCE BY types must match across flows.
        .option(
            "cloudFiles.schemaHints",
            "customer_id INT, sequence_num BIGINT, event_ts TIMESTAMP, "
            "last_login_ts TIMESTAMP, changed_columns ARRAY<STRING>",
        )
        .load(f"{LANDING}/customers_cdc/")
        .withColumn("source_file", F.col("_metadata.file_path"))  # lineage
        .withColumn("ingest_ts", F.current_timestamp())           # when WE saw it (system time)
    )


# A temporary view is not stored – it is just a named, reusable step. Expectations here
# drop records AUTO CDC cannot handle BEFORE they reach the target.
@dp.temporary_view(comment="Customer CDC events that are safe to apply.")
@dp.expect_all_or_drop({
    # TRUNCATE events have no key, so let them through
    "has_key": "customer_id IS NOT NULL OR operation = 'TRUNCATE'",
    # AUTO CDC does not support NULL sequencing values
    "has_sequence": "sequence_num IS NOT NULL",
})
@dp.expect("known_operation", "operation IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE')")  # warn only
def customers_cdc_clean() -> DataFrame:
    return spark.readStream.table("customers_cdc_bronze")


# Any target that does NOT set apply_as_truncates must not see TRUNCATE rows – otherwise the
# TRUNCATE row is treated as an upsert for a NULL key. SCD Type 2 targets can't truncate at all.
@dp.temporary_view(comment="Same feed minus TRUNCATE events – for targets that don't truncate.")
def customers_cdc_no_truncate() -> DataFrame:
    return spark.readStream.table("customers_cdc_clean").where("operation <> 'TRUNCATE'")


# =============================================================================
# EXAMPLE 1 – SCD Type 1: keep only the latest version of each customer
# =============================================================================
# Step 1: declare the (empty) target streaming table.
# Step 2: attach an AUTO CDC flow to it.
# That two-step pattern is the same for every example below.

dp.create_streaming_table(
    name="customers_current",
    comment="SCD1 – one row per customer, latest values only.",
    cluster_by=["customer_id"],                       # liquid clustering on the key = fast merges
    table_properties={
        # Lets downstream pipelines stream this table's changes (see Example 11)
        "delta.enableChangeDataFeed": "true",
        # Deletes are kept as hidden "tombstones" so a late, older event can't resurrect a row.
        # Default retention is 2 days; tune it if your late data can be later than that.
        "pipelines.cdc.tombstoneGCThresholdInSeconds": str(3 * 24 * 60 * 60),
    },
)

dp.create_auto_cdc_flow(
    name="customers_current_flow",                    # optional; defaults to the target name
    target="customers_current",
    source="customers_cdc_clean",
    keys=["customer_id"],                             # what makes a row unique
    sequence_by=F.col("sequence_num"),                # what order changes happened in
    apply_as_deletes=F.expr("operation = 'DELETE'"),  # these rows delete instead of upsert
    apply_as_truncates=F.expr("operation = 'TRUNCATE'"),  # SCD1 ONLY – empties the table
    except_column_list=CDC_CONTROL_COLS,              # don't land control columns
    stored_as_scd_type=1,                             # 1 is the default; written here for clarity
)


# =============================================================================
# EXAMPLE 2 – SCD Type 2: keep full history
# =============================================================================
# AUTO CDC adds two columns:
#   __START_AT – sequence value when this version became true
#   __END_AT   – sequence value when it stopped being true (NULL = current row)
# Their data type = the SEQUENCE BY column's type (BIGINT here).

dp.create_streaming_table(
    name="customers_history",
    comment="SCD2 – every version of every customer.",
    cluster_by=["customer_id"],
)

dp.create_auto_cdc_flow(
    target="customers_history",
    source="customers_cdc_no_truncate",               # no truncates allowed on SCD2
    keys=["customer_id"],
    sequence_by=F.col("sequence_num"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),  # closes the current version (__END_AT set)
    except_column_list=CDC_CONTROL_COLS,
    stored_as_scd_type=2,
    # A login timestamp changes constantly. Excluding it from history tracking means a login
    # UPDATES the current row in place instead of creating a new version.
    track_history_except_column_list=["last_login_ts", "event_ts", "sequence_num"],
    # Alternative: only track the columns you list
    # track_history_column_list=["city", "state", "tier", "email"],
)


# =============================================================================
# EXAMPLE 3 – Pick exactly which columns land in the target
# =============================================================================
# column_list      = allow-list   (SQL: COLUMNS customer_id, email, tier)
# except_column_list = deny-list  (SQL: COLUMNS * EXCEPT (operation, ...))
# Use one or the other, never both. Keys must be included.

dp.create_streaming_table(
    name="customer_contact_current",
    comment="SCD1 – a narrow contact table built from the same feed.",
)

dp.create_auto_cdc_flow(
    target="customer_contact_current",
    source="customers_cdc_no_truncate",
    keys=["customer_id"],
    sequence_by="sequence_num",                       # a plain string column name also works
    apply_as_deletes="operation = 'DELETE'",          # ...and a plain SQL string condition
    column_list=["customer_id", "first_name", "last_name", "email"],
)


# =============================================================================
# EXAMPLE 4 – Composite keys + tie-breaking with a STRUCT sequence
# =============================================================================
# Order lines are unique by (order_id, line_num). Two events can share the same event_ts,
# so we sequence by STRUCT(event_ts, event_id): compare event_ts first, then event_id.

@dp.table(name="orders_cdc_bronze", comment="Raw order-line CDC events.")
def orders_cdc_bronze() -> DataFrame:
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints",
                "order_id INT, line_num INT, qty INT, event_ts TIMESTAMP, event_id BIGINT")
        .load(f"{LANDING}/orders_cdc/")
    )

dp.create_streaming_table(name="order_lines_current", comment="SCD1 – current order lines.")

dp.create_auto_cdc_flow(
    target="order_lines_current",
    source="orders_cdc_bronze",
    keys=["order_id", "line_num"],                    # composite key
    sequence_by=F.struct("event_ts", "event_id"),     # first field wins; later fields break ties
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op", "_rescued_data"],
)


# =============================================================================
# EXAMPLE 5 – Partial updates (source sends only the changed columns)
# =============================================================================
# Customer 105's batch-2 update only sets tier; every other column is NULL.
#   customers_current (Example 1)        -> email, city, ... become NULL   (default behaviour)
#   customers_partial_ignore_nulls (5a)  -> NULLs are ignored, old values kept
#   customers_partial_cols_to_update (5b)-> only the columns listed in changed_columns change

# 5a – ignore NULLs
dp.create_streaming_table(name="customers_partial_ignore_nulls",
                          comment="SCD1 – NULLs in updates keep the existing value.")

dp.create_auto_cdc_flow(
    target="customers_partial_ignore_nulls",
    source="customers_cdc_no_truncate",
    keys=["customer_id"],
    sequence_by=F.col("sequence_num"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=CDC_CONTROL_COLS,
    ignore_null_updates=True,                         # every column: NULL = "no change"
    # Finer control – use ONE of these instead of ignore_null_updates=True:
    # ignore_null_updates_column_list=["email", "city", "state"],         # only these ignore NULLs
    # ignore_null_updates_except_column_list=["last_login_ts"],           # all but these ignore NULLs
)

# 5b – COLUMNS TO UPDATE: each event names the columns it changed, so you can ALSO
#      deliberately set a column to NULL (which ignore_null_updates can't do).
#      Can't be combined with any ignore_null_updates* option or with bitemporal.
dp.create_streaming_table(name="customers_partial_cols_to_update",
                          comment="SCD1 – only the columns named in changed_columns are written.")

dp.create_auto_cdc_flow(
    target="customers_partial_cols_to_update",
    source="customers_cdc_no_truncate",
    keys=["customer_id"],
    sequence_by=F.col("sequence_num"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    # sequence_num / event_ts aren't listed in changed_columns, so leave them out of this target
    except_column_list=CDC_CONTROL_COLS + ["sequence_num", "event_ts"],
    columns_to_update="changed_columns",             # an ARRAY<STRING> column in the source
)


# =============================================================================
# EXAMPLE 6 – Several flows into ONE target: one-time back-fill + live stream
# =============================================================================
# Rules: every flow needs a unique `name`, the same keys (same order) and the same
# sequence data type. once=True flows read a BATCH DataFrame and run one time
# (again only on a full refresh).

@dp.temporary_view(comment="Legacy rows shaped like the live feed. BATCH read for the once-flow.")
def legacy_customers_shaped() -> DataFrame:
    return (
        spark.read.table(f"{SRC}.legacy_customers")       # spark.read (batch), not readStream
        .withColumn("sequence_num", F.lit(0).cast("bigint"))  # 0 = older than any live event
        .withColumn("operation", F.lit("INSERT"))
    )

UNIFIED_COLS = ["customer_id", "first_name", "last_name", "email", "city", "state", "tier"]

dp.create_streaming_table(name="customers_unified",
                          comment="SCD1 – legacy back-fill merged with the live CDC feed.")

# Flow A – back-fill once
dp.create_auto_cdc_flow(
    name="customers_unified_legacy_backfill",
    target="customers_unified",
    source="legacy_customers_shaped",
    keys=["customer_id"],
    sequence_by=F.col("sequence_num"),
    column_list=UNIFIED_COLS,
    once=True,
)

# Flow B – live stream (higher sequence numbers, so live data wins for customer 101)
dp.create_auto_cdc_flow(
    name="customers_unified_live",
    target="customers_unified",
    source="customers_cdc_clean",
    keys=["customer_id"],
    sequence_by=F.col("sequence_num"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    apply_as_truncates=F.expr("operation = 'TRUNCATE'"),
    column_list=UNIFIED_COLS,
)


# =============================================================================
# EXAMPLE 7 – Bitemporal history  (BETA – uncomment to try)
# =============================================================================
# Tracks two timelines:
#   business time (sequence_by)       -> __START_AT / __END_AT
#   system time   (system_sequence_by)-> __SYSTEM_START_AT / __SYSTEM_END_AT
# Lets you answer "what did we BELIEVE was true on date X about date Y?" – useful for
# audit and reproducible ML training sets. Requires the pipeline channel = PREVIEW.
#
# dp.create_streaming_table(name="customers_bitemporal")
#
# dp.create_auto_cdc_flow(
#     target="customers_bitemporal",
#     source="customers_cdc_no_truncate",
#     keys=["customer_id"],
#     sequence_by=F.col("event_ts"),               # when it happened in the business
#     system_sequence_by=F.col("ingest_ts"),       # when we learned about it
#     apply_as_deletes=F.expr("operation = 'DELETE'"),
#     except_column_list=["operation", "changed_columns", "source_file", "_rescued_data"],
#     stored_as_scd_type="bitemporal",
# )


# =============================================================================
# EXAMPLE 8 – Source = a Delta table's Change Data Feed (CDF)
# =============================================================================
# When the upstream is already a Delta table with CDF on, read its change feed and map
# the CDF metadata columns to AUTO CDC options. Updates produce a pre-image and a
# post-image row – keep only the post-image.

@dp.temporary_view(comment="Change feed from crm_customers, ready for AUTO CDC.")
def crm_customers_changes() -> DataFrame:
    return (
        spark.readStream.option("readChangeFeed", "true")
        .option("startingVersion", 0)                 # replay all changes since CDF was enabled
        .table(f"{SRC}.crm_customers")
        .where("_change_type <> 'update_preimage'")
    )

dp.create_streaming_table(name="crm_customers_current", comment="SCD1 – mirror of crm_customers.")

dp.create_auto_cdc_flow(
    target="crm_customers_current",
    source="crm_customers_changes",
    keys=["customer_id"],
    sequence_by=F.col("_commit_version"),             # Delta commit order
    apply_as_deletes=F.expr("_change_type = 'delete'"),
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
)


# =============================================================================
# EXAMPLE 9 – AUTO CDC FROM SNAPSHOT: source is a full copy, no change events
# =============================================================================
# Each pipeline update reads the whole table, compares it to the last snapshot, and works
# out inserts / updates / deletes (a key that disappears = delete). Python only in SDP.

dp.create_streaming_table(name="customers_from_snapshot",
                          comment="SCD2 history derived by diffing full snapshots.")

dp.create_auto_cdc_from_snapshot_flow(
    target="customers_from_snapshot",
    source=f"{SRC}.customers_full_snapshot",          # a table/view name
    keys=["customer_id"],
    stored_as_scd_type=2,                             # SCD2 = see every change between snapshots
    # track_history_column_list=["city", "tier"],     # same history options as Example 2
)


# =============================================================================
# EXAMPLE 10 – AUTO CDC FROM SNAPSHOT: replay versioned snapshots with a function
# =============================================================================
# Use this when snapshots pile up (one per day/file/partition) and must be applied IN ORDER.
# Contract for the function:
#   input : the last snapshot version processed (None on the very first run)
#   output: (snapshot_dataframe, version)  -> process it, then the function is called again
#           None                           -> nothing new; the update finishes
#   The FIRST call must return a snapshot, or the update fails.

SNAPSHOT_HISTORY = f"{SRC}.customers_snapshot_history"

def next_customer_snapshot(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
    history = spark.read.table(SNAPSHOT_HISTORY)
    if latest_version is not None:
        history = history.where(F.col("snapshot_version") > latest_version)

    next_version = history.agg(F.min("snapshot_version")).first()[0]
    if next_version is None:
        return None                                    # caught up

    snapshot_df = history.where(F.col("snapshot_version") == next_version).drop("snapshot_version")
    return (snapshot_df, next_version)

dp.create_streaming_table(name="customers_snapshot_replayed",
                          comment="SCD2 built by replaying snapshots 1, 2, 3 ... in order.")

dp.create_auto_cdc_from_snapshot_flow(
    target="customers_snapshot_replayed",
    source=next_customer_snapshot,                     # pass the function itself (no parentheses)
    keys=["customer_id"],
    stored_as_scd_type=2,
)


# =============================================================================
# EXAMPLE 11 – Consuming AUTO CDC targets downstream
# =============================================================================
# AUTO CDC targets get updates and deletes, so you can NOT read them as a plain append-only
# stream. Two safe options:

# 11a – Materialized view over the current SCD2 rows (recomputed incrementally when possible)
@dp.materialized_view(comment="Gold – number of current customers per tier.")
def customer_tier_counts() -> DataFrame:
    return (
        spark.read.table("customers_history")
        .where("__END_AT IS NULL")                     # current version only
        .groupBy("tier")
        .agg(F.count("*").alias("customers"))
    )

# 11b – Stream the SCD1 target's change feed (needs delta.enableChangeDataFeed, set in Example 1)
@dp.table(name="customers_current_change_log",
          comment="Every insert/update/delete applied to customers_current, as an append-only log.")
def customers_current_change_log() -> DataFrame:
    return (
        spark.readStream.option("readChangeFeed", "true")
        .table("customers_current")
        .where("_change_type <> 'update_preimage'")
    )
