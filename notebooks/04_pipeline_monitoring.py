# Databricks notebook source
# ============================================================
# NOTEBOOK: 04 - Pipeline Monitoring & Alerting
# PURPOSE : Centralized logging, exception tracking, and alert
#           mechanism for the CRM ETL pipeline.
# AUTHOR  : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
from pyspark.sql.functions import col, current_timestamp, lit, datediff, now
from delta.tables import DeltaTable
from datetime import datetime
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PipelineMonitor")

# COMMAND ----------
# ── Configuration ────────────────────────────────────────────
STORAGE_ACCOUNT = "your_storage_account"
AUDIT_LOG_PATH  = f"abfss://audit@{STORAGE_ACCOUNT}.dfs.core.windows.net/pipeline_logs/"

# Alert thresholds
THRESHOLDS = {
    "max_runtime_minutes":    60,
    "min_records_expected":   100_000,
    "max_error_rate_pct":     2.0,
    "max_null_rate_pct":      5.0,
}

# COMMAND ----------
class PipelineLogger:
    """Centralized logging utility for pipeline runs."""

    def __init__(self, pipeline_name: str, batch_id: str):
        self.pipeline_name = pipeline_name
        self.batch_id      = batch_id
        self.start_time    = datetime.now()
        self.logs          = []

    def log(self, stage: str, status: str, records: int = 0, message: str = ""):
        entry = {
            "pipeline_name": self.pipeline_name,
            "batch_id":      self.batch_id,
            "stage":         stage,
            "status":        status,
            "records":       records,
            "message":       message,
            "timestamp":     datetime.now().isoformat(),
        }
        self.logs.append(entry)
        logger.info(f"[{status}] {stage}: {message} | records={records:,}")
        return entry

    def write_audit_log(self, spark):
        """Persist all logs to Delta audit table."""
        if not self.logs:
            return
        df_logs = spark.createDataFrame(self.logs)
        df_logs.withColumn("logged_at", current_timestamp()) \
            .write \
            .format("delta") \
            .mode("append") \
            .save(AUDIT_LOG_PATH)
        logger.info(f"[AUDIT] {len(self.logs)} log entries written.")

    def get_runtime_minutes(self) -> float:
        return (datetime.now() - self.start_time).seconds / 60

# COMMAND ----------
class AlertManager:
    """Sends alerts when pipeline thresholds are breached."""

    def __init__(self, thresholds: dict):
        self.thresholds = thresholds
        self.alerts     = []

    def check_runtime(self, runtime_minutes: float):
        if runtime_minutes > self.thresholds["max_runtime_minutes"]:
            self._raise_alert(
                "RUNTIME_EXCEEDED",
                f"Pipeline ran for {runtime_minutes:.1f} min "
                f"(threshold: {self.thresholds['max_runtime_minutes']} min)"
            )

    def check_record_count(self, actual: int):
        expected = self.thresholds["min_records_expected"]
        if actual < expected:
            self._raise_alert(
                "LOW_RECORD_COUNT",
                f"Only {actual:,} records processed "
                f"(minimum expected: {expected:,})"
            )

    def check_error_rate(self, total: int, errors: int):
        if total == 0:
            return
        error_rate = (errors / total) * 100
        if error_rate > self.thresholds["max_error_rate_pct"]:
            self._raise_alert(
                "HIGH_ERROR_RATE",
                f"Error rate {error_rate:.2f}% "
                f"(threshold: {self.thresholds['max_error_rate_pct']}%)"
            )

    def _raise_alert(self, alert_type: str, message: str):
        alert = {
            "type":      alert_type,
            "message":   message,
            "timestamp": datetime.now().isoformat(),
            "severity":  "HIGH"
        }
        self.alerts.append(alert)
        logger.warning(f"[ALERT] {alert_type}: {message}")
        # In production: integrate with Azure Monitor / Teams / Email
        # send_teams_notification(alert)
        # send_email_alert(alert)

    def summary(self):
        if not self.alerts:
            logger.info("[ALERT] No alerts triggered. Pipeline healthy.")
        else:
            logger.warning(f"[ALERT] {len(self.alerts)} alert(s) triggered:")
            for a in self.alerts:
                logger.warning(f"  ⚠ {a['type']}: {a['message']}")
        return self.alerts

# COMMAND ----------
# ── Run Monitoring Check ─────────────────────────────────────
# (This section would be called at end of each pipeline run)

pipeline_logger = PipelineLogger(
    pipeline_name="CRM_ETL_Pipeline",
    batch_id="batch_001"
)
alert_manager = AlertManager(THRESHOLDS)

# Simulate checking pipeline metrics (in prod: pull from Delta logs)
pipeline_stats = {
    "bronze_records":  50_200_000,
    "silver_records":  49_850_000,
    "gold_records":    1_200,
    "error_records":   350_000,
    "runtime_minutes": 35,
}

pipeline_logger.log("BRONZE", "SUCCESS", pipeline_stats["bronze_records"], "Raw ingestion complete")
pipeline_logger.log("SILVER", "SUCCESS", pipeline_stats["silver_records"], "Transformation complete")
pipeline_logger.log("GOLD",   "SUCCESS", pipeline_stats["gold_records"],   "Aggregation complete")

alert_manager.check_runtime(pipeline_stats["runtime_minutes"])
alert_manager.check_record_count(pipeline_stats["silver_records"])
alert_manager.check_error_rate(
    pipeline_stats["bronze_records"],
    pipeline_stats["error_records"]
)

alerts = alert_manager.summary()

# pipeline_logger.write_audit_log(spark)  # Uncomment in Databricks

print(f"""
╔══════════════════════════════════════════╗
║   PIPELINE MONITORING SUMMARY            ║
╠══════════════════════════════════════════╣
║  Bronze Records : {pipeline_stats['bronze_records']:<22,}║
║  Silver Records : {pipeline_stats['silver_records']:<22,}║
║  Runtime        : {pipeline_stats['runtime_minutes']:<22} min ║
║  Alerts Raised  : {len(alerts):<22}║
║  Status         : {"✅ HEALTHY" if not alerts else "⚠ NEEDS ATTENTION":<22}║
╚══════════════════════════════════════════╝
""")
