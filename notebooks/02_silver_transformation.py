# Databricks notebook source
# ============================================================
# NOTEBOOK: 02 - Silver Layer Transformation
# PURPOSE : Clean, validate and enrich Bronze data into Silver
#           layer applying business rules and data quality checks.
# AUTHOR  : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Layer — Data Cleaning & Validation
# MAGIC Reads from Bronze Delta table, applies data quality rules,
# MAGIC deduplicates, and writes clean records to Silver layer.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, when, isnan, isnull,
    current_timestamp, lit, coalesce, round as spark_round,
    row_number, desc
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SilverTransformation")

# COMMAND ----------
# ── Configuration ────────────────────────────────────────────
STORAGE_ACCOUNT  = "your_storage_account"
CONTAINER_BRONZE = "bronze"
CONTAINER_SILVER = "silver"

BRONZE_PATH = f"abfss://{CONTAINER_BRONZE}@{STORAGE_ACCOUNT}.dfs.core.windows.net/crm_data/"
SILVER_PATH = f"abfss://{CONTAINER_SILVER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/crm_data/"

BATCH_ID = dbutils.widgets.get("batch_id") if "dbutils" in dir() else "batch_001"

# COMMAND ----------
# ── Read Bronze Data (Incremental) ───────────────────────────
logger.info(f"[SILVER] Reading Bronze data for batch: {BATCH_ID}")

df_bronze = spark.read \
    .format("delta") \
    .load(BRONZE_PATH) \
    .filter(col("batch_id") == BATCH_ID)

bronze_count = df_bronze.count()
logger.info(f"[SILVER] Records from Bronze: {bronze_count:,}")

# COMMAND ----------
# ── Data Quality Checks ──────────────────────────────────────
def check_data_quality(df):
    """Run data quality checks and log results."""
    total = df.count()

    null_customer  = df.filter(isnull("customer_id")).count()
    null_region    = df.filter(isnull("region")).count()
    null_sale_amt  = df.filter(isnull("sale_amount") | isnan("sale_amount")).count()
    negative_sales = df.filter(col("sale_amount") < 0).count()
    null_date      = df.filter(isnull("sale_date_parsed")).count()

    logger.info(f"""
    [SILVER] Data Quality Report — Batch {BATCH_ID}
    ─────────────────────────────────────────
    Total Records      : {total:,}
    Null customer_id   : {null_customer:,}
    Null region        : {null_region:,}
    Null sale_amount   : {null_sale_amt:,}
    Negative sales     : {negative_sales:,}
    Invalid dates      : {null_date:,}
    ─────────────────────────────────────────
    """)
    return {
        "total": total,
        "null_customer": null_customer,
        "invalid_records": null_customer + null_sale_amt + null_date
    }

quality_report = check_data_quality(df_bronze)

# COMMAND ----------
# ── Cleaning & Standardization ───────────────────────────────
df_silver = df_bronze \
    .filter(
        col("customer_id").isNotNull() &
        col("sale_date_parsed").isNotNull() &
        col("sale_amount").isNotNull() &
        (col("sale_amount") >= 0)
    ) \
    .withColumn("region",       upper(trim(col("region")))) \
    .withColumn("channel",      upper(trim(col("channel")))) \
    .withColumn("status",       upper(trim(col("status")))) \
    .withColumn("sale_amount",  spark_round(col("sale_amount"), 2)) \
    .withColumn("return_amount",
        coalesce(spark_round(col("return_amount"), 2), lit(0.0))
    ) \
    .withColumn("net_amount",
        spark_round(col("sale_amount") - col("return_amount"), 2)
    ) \
    .withColumn("is_returned",
        when(col("return_amount") > 0, True).otherwise(False)
    ) \
    .withColumn("silver_processed_at", current_timestamp())

# COMMAND ----------
# ── Deduplication ────────────────────────────────────────────
window_spec = Window \
    .partitionBy("customer_id", "sale_date_parsed", "product_id") \
    .orderBy(desc("ingestion_timestamp"))

df_silver = df_silver \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

silver_count = df_silver.count()
dedup_removed = bronze_count - quality_report["invalid_records"] - silver_count
logger.info(f"[SILVER] After cleaning: {silver_count:,} | Duplicates removed: {dedup_removed:,}")

# COMMAND ----------
# ── Write to Silver Delta Table ──────────────────────────────
logger.info(f"[SILVER] Writing to Silver layer: {SILVER_PATH}")

df_silver.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("region", "partition_year", "partition_month") \
    .option("mergeSchema", "false") \
    .save(SILVER_PATH)

print(f"""
╔══════════════════════════════════════╗
║   SILVER TRANSFORMATION COMPLETE     ║
╠══════════════════════════════════════╣
║  Batch ID        : {BATCH_ID:<18}║
║  Bronze Input    : {bronze_count:<18,}║
║  Invalid Removed : {quality_report['invalid_records']:<18,}║
║  Dupes Removed   : {dedup_removed:<18,}║
║  Silver Output   : {silver_count:<18,}║
╚══════════════════════════════════════╝
""")
