# Databricks notebook source
# Gold publish step. Reads job parameters catalog/schema.
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
print(f"Publishing gold tables in {catalog}.{schema}")
