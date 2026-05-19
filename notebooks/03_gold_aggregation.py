# Databricks notebook source
# ============================================================
# NOTEBOOK: 03 - Gold Layer Aggregation
# PURPOSE : Aggregate Silver data into business-level Gold tables
#           for leadership reporting — region-wise sales & returns.
# AUTHOR  : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Layer — Business Aggregations
# MAGIC Produces region-wise monthly sales & returns summary
# MAGIC for leadership performance tracking dashboards.

# COMMAND ----------
from pyspark.sql.functions import (
    col, sum as spark_sum, count, countDistinct,
    avg, round as spark_round, max as spark_max,
    current_timestamp, lit
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldAggregation")

# COMMAND ----------
STORAGE_ACCOUNT = "your_storage_account"
SILVER_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/crm_data/"
GOLD_PATH   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/"

REPORT_YEAR  = int(dbutils.widgets.get("report_year"))  if "dbutils" in dir() else 2024
REPORT_MONTH = int(dbutils.widgets.get("report_month")) if "dbutils" in dir() else 1

# COMMAND ----------
# ── Read Silver (filtered to reporting period) ───────────────
df_silver = spark.read.format("delta").load(SILVER_PATH) \
    .filter(
        (col("partition_year")  == REPORT_YEAR) &
        (col("partition_month") == REPORT_MONTH)
    )

logger.info(f"[GOLD] Silver records for {REPORT_YEAR}-{REPORT_MONTH:02d}: {df_silver.count():,}")

# COMMAND ----------
# ── Gold Table 1: Region-wise Monthly Sales Summary ──────────
df_region_summary = df_silver.groupBy(
    "region", "partition_year", "partition_month", "channel"
).agg(
    spark_round(spark_sum("sale_amount"),   2).alias("total_sales"),
    spark_round(spark_sum("return_amount"), 2).alias("total_returns"),
    spark_round(spark_sum("net_amount"),    2).alias("total_net_revenue"),
    count("customer_id").alias("total_transactions"),
    countDistinct("customer_id").alias("unique_customers"),
    spark_round(avg("sale_amount"), 2).alias("avg_order_value"),
    count(
        col("is_returned").cast("int")
    ).alias("return_count"),
) \
.withColumn("return_rate_pct",
    spark_round((col("total_returns") / col("total_sales")) * 100, 2)
) \
.withColumn("gold_created_at", current_timestamp()) \
.withColumn("report_period", lit(f"{REPORT_YEAR}-{REPORT_MONTH:02d}"))

# COMMAND ----------
# ── Gold Table 2: Product Performance Summary ────────────────
df_product_summary = df_silver.groupBy(
    "product_id", "product_name", "region", "partition_year", "partition_month"
).agg(
    spark_round(spark_sum("sale_amount"), 2).alias("product_revenue"),
    count("customer_id").alias("units_sold"),
    spark_round(avg("sale_amount"), 2).alias("avg_selling_price"),
    spark_round(spark_sum("return_amount"), 2).alias("product_returns"),
) \
.withColumn("gold_created_at", current_timestamp())

# COMMAND ----------
# ── Write Gold Tables ────────────────────────────────────────
df_region_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere",
        f"partition_year={REPORT_YEAR} AND partition_month={REPORT_MONTH}"
    ) \
    .partitionBy("partition_year", "partition_month") \
    .save(f"{GOLD_PATH}region_sales_summary/")

df_product_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere",
        f"partition_year={REPORT_YEAR} AND partition_month={REPORT_MONTH}"
    ) \
    .partitionBy("partition_year", "partition_month") \
    .save(f"{GOLD_PATH}product_performance/")

# COMMAND ----------
# ── Preview Results ──────────────────────────────────────────
print(f"\n=== Region-wise Sales Summary ({REPORT_YEAR}-{REPORT_MONTH:02d}) ===")
df_region_summary.select(
    "region", "channel", "total_sales",
    "total_returns", "total_net_revenue",
    "unique_customers", "return_rate_pct"
).orderBy(col("total_net_revenue").desc()).show(10, truncate=False)

print(f"\n=== Top 10 Products by Revenue ===")
df_product_summary.orderBy(
    col("product_revenue").desc()
).show(10, truncate=False)

print(f"\n✅ Gold layer updated for {REPORT_YEAR}-{REPORT_MONTH:02d}")
