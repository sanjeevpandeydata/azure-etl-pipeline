# Databricks notebook source
# ============================================================
# NOTEBOOK: 01 - Bronze Layer Ingestion
# PURPOSE : Ingest raw CRM data from ADLS Gen2 into Delta Lake
#           Bronze layer with audit columns and schema checks.
# AUTHOR  : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Layer — Raw CRM Data Ingestion
# MAGIC Reads raw CSV files from ADLS Gen2 landing zone and writes
# MAGIC them as Delta tables with audit metadata added.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, lit, col, input_file_name,
    to_date, year, month
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, DateType
)
from delta.tables import DeltaTable
import logging

# ── Logging Setup ────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BronzeIngestion")

# COMMAND ----------
# ── Configuration ────────────────────────────────────────────
# Update these paths to match your ADLS account
STORAGE_ACCOUNT  = "your_storage_account"
CONTAINER_RAW    = "raw"
CONTAINER_BRONZE = "bronze"

RAW_PATH    = f"abfss://{CONTAINER_RAW}@{STORAGE_ACCOUNT}.dfs.core.windows.net/crm/"
BRONZE_PATH = f"abfss://{CONTAINER_BRONZE}@{STORAGE_ACCOUNT}.dfs.core.windows.net/crm_data/"

# For local/sample testing, use relative paths:
# RAW_PATH    = "/data/sample_crm_data.csv"
# BRONZE_PATH = "/delta/bronze/crm_data"

BATCH_ID = dbutils.widgets.get("batch_id") if "dbutils" in dir() else "batch_001"

# COMMAND ----------
# ── Schema Definition ────────────────────────────────────────
# Explicit schema prevents schema inference cost on large files
crm_schema = StructType([
    StructField("customer_id",    StringType(),  nullable=False),
    StructField("region",         StringType(),  nullable=True),
    StructField("product_id",     StringType(),  nullable=True),
    StructField("product_name",   StringType(),  nullable=True),
    StructField("sale_amount",    DoubleType(),  nullable=True),
    StructField("return_amount",  DoubleType(),  nullable=True),
    StructField("sale_date",      StringType(),  nullable=True),
    StructField("sales_rep_id",   StringType(),  nullable=True),
    StructField("channel",        StringType(),  nullable=True),
    StructField("status",         StringType(),  nullable=True),
])

# COMMAND ----------
# ── Read Raw Data ────────────────────────────────────────────
logger.info(f"[BRONZE] Reading raw CRM data from: {RAW_PATH}")

try:
    df_raw = spark.read \
        .format("csv") \
        .schema(crm_schema) \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .load(RAW_PATH)

    raw_count = df_raw.count()
    logger.info(f"[BRONZE] Records read from source: {raw_count:,}")

except Exception as e:
    logger.error(f"[BRONZE] Failed to read raw data: {str(e)}")
    raise

# COMMAND ----------
# ── Add Audit Columns ────────────────────────────────────────
df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file",         input_file_name()) \
    .withColumn("batch_id",            lit(BATCH_ID)) \
    .withColumn("source_system",       lit("CRM")) \
    .withColumn("sale_date_parsed",    to_date(col("sale_date"), "yyyy-MM-dd")) \
    .withColumn("partition_year",      year(col("sale_date_parsed"))) \
    .withColumn("partition_month",     month(col("sale_date_parsed")))

# COMMAND ----------
# ── Write to Delta Lake (Bronze) ─────────────────────────────
logger.info(f"[BRONZE] Writing to Delta Lake: {BRONZE_PATH}")

try:
    df_bronze.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("region", "partition_year", "partition_month") \
        .option("mergeSchema", "true") \
        .save(BRONZE_PATH)

    logger.info(f"[BRONZE] Successfully wrote {raw_count:,} records.")

except Exception as e:
    logger.error(f"[BRONZE] Write failed: {str(e)}")
    raise

# COMMAND ----------
# ── Validate Write ───────────────────────────────────────────
df_validate = spark.read.format("delta").load(BRONZE_PATH)
written_count = df_validate \
    .filter(col("batch_id") == BATCH_ID) \
    .count()

logger.info(f"[BRONZE] Validation — records written for batch {BATCH_ID}: {written_count:,}")

assert written_count == raw_count, \
    f"Count mismatch! Source: {raw_count}, Written: {written_count}"

print(f"""
╔══════════════════════════════════════╗
║   BRONZE INGESTION COMPLETE          ║
╠══════════════════════════════════════╣
║  Batch ID      : {BATCH_ID:<20}║
║  Records Read  : {raw_count:<20,}║
║  Records Written: {written_count:<19,}║
╚══════════════════════════════════════╝
""")
