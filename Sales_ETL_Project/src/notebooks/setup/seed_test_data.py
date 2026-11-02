# Databricks notebook source
# Test setup (dev + uat only): copy the static test files in test_data/ into the
# landing volume the pipeline reads (source_path). Runs as task 2 of sales_integration_tests,
# after unit tests and before the pipeline refresh.
# Safe on every run: files are only copied if missing, so the pipeline never sees duplicates.

# COMMAND ----------
import os
import shutil

dbutils.widgets.text("source_path", "")
source_path = dbutils.widgets.get("source_path")          # e.g. /Volumes/dev_sales/raw/landing
parts = source_path.strip("/").split("/")
assert len(parts) >= 4 and parts[0] == "Volumes", f"source_path must be /Volumes/<catalog>/<schema>/<volume>/..., got {source_path}"
catalog, schema, volume = parts[1], parts[2], parts[3]
assert not catalog.startswith("prod"), "Refusing to load test data into a prod catalog"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

# The notebook runs from <bundle root>/src/notebooks/setup; test data lives in <bundle root>/test_data
bundle_root = os.path.abspath(os.path.join(os.getcwd(), "..", "..", ".."))
copied = []
for folder in ("orders", "customers"):
    src_dir = os.path.join(bundle_root, "test_data", folder)
    dst_dir = os.path.join(source_path, folder)
    os.makedirs(dst_dir, exist_ok=True)
    for name in sorted(os.listdir(src_dir)):
        dst = os.path.join(dst_dir, name)
        if not os.path.exists(dst):
            shutil.copy(os.path.join(src_dir, name), dst)
            copied.append(dst)

print(f"Copied {len(copied)} file(s):", *copied, sep="\n  ")
