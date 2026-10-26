# Databricks notebook source
# Runs the repo's pytest suite ON Databricks (deployed by the bundle).
# Used by resources/test_jobs/test_jobs.yml after `databricks bundle deploy`.
# The final assert fails the task, so `databricks bundle run unit_tests` exits non-zero in CI.

# COMMAND ----------
# MAGIC %pip install pytest

# COMMAND ----------
import os
import sys

import pytest

dbutils.widgets.text("markers", "")         # e.g. "integration" or "not integration"; empty = all
dbutils.widgets.text("test_catalog", "")    # throwaway catalog for integration tests

os.environ["TEST_CATALOG"] = dbutils.widgets.get("test_catalog")

# The notebook runs from <bundle root>/src/notebooks; tests live in <bundle root>/tests
bundle_root = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
sys.path.insert(0, os.path.join(bundle_root, "src"))
sys.dont_write_bytecode = True               # workspace files are read-only for .pyc

args = [os.path.join(bundle_root, "tests"), "-v", "-p", "no:cacheprovider", "-rA"]
if dbutils.widgets.get("markers"):
    args += ["-m", dbutils.widgets.get("markers")]

retcode = pytest.main(args)
assert retcode == 0, f"pytest failed with exit code {retcode}. See the output above."
