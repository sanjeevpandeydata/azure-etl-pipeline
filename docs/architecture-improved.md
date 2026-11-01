# Architecture — Azure ETL Pipeline (CRM Data Processing)

## Purpose

This document describes how the CRM ETL pipeline works, why it is layered the way it is, and where it can fail. It is written so a reviewer can check each claim against the four notebooks in `notebooks/`, the Azure Data Factory definition in `adf_pipelines/crm_pipeline_config.json`, and the sample data in `data/sample_crm_data.csv`.

The pipeline takes raw CRM sales data from ADLS Gen2, cleans and validates it through a Medallion (Bronze → Silver → Gold) design on Databricks, and produces region-wise and product-wise aggregates for leadership reporting.

> **Status of this document.** This document did not exist in the repository before this change, even though the README's project structure listed `docs/architecture.md`. It is a new document, written from the notebooks and ADF definition as they stand in the repository. No notebook, ADF definition, or data file was changed to write it. Where a notebook's behaviour differs from the README's summary, this document follows the code and says so (see §7).

---

## 1. System overview

```
CRM SOURCE (raw CSV files)
        │
        ▼
ADLS Gen2 — Raw landing zone (container: raw)
        │
        ▼  Azure Data Factory (CRM_ETL_Master_Pipeline, daily 08:00 IST)
        │  Set batch_id → Bronze → Silver → Gold → Monitoring
        ▼
AZURE DATABRICKS (PySpark / Delta Lake)
        │
        ├── Bronze: raw Delta tables, audit columns added
        │     (container: bronze, partitioned by region / year / month)
        │
        ├── Silver: cleaned, validated, deduplicated Delta tables
        │     (container: silver, same partitioning)
        │
        ├── Gold: aggregated Delta tables for reporting
        │     (container: gold, region_sales_summary + product_performance)
        │
        └── Audit: pipeline log entries (container: audit)
              (see §7.4 — audit writing is commented out as supplied)
        │
        ▼
REPORTING CONSUMERS (dashboards / downstream queries on Gold)
```

**Reading the diagram.** There is no separate serving database in this design. The Gold Delta tables *are* the reporting layer. ADF is the orchestrator; Databricks is the only compute. Storage is ADLS Gen2 throughout, with one container per layer plus raw and audit.

---

## 2. End-to-end flow

One `batch_id` ties a run together. ADF generates it as `batch_YYYYMMDD_HHMMSS`, passes it to each notebook, and each notebook filters or stamps its work with it. The notebooks run in order, each depending on the previous one succeeding — except Monitoring, which runs after Gold whether Gold succeeded or failed (see §2.5).

### 2.1 Bronze — `01_bronze_ingestion.py`: ingest raw, add audit columns

1. **Read.** Raw CSV files are read from `abfss://raw@{storage_account}/crm/` with an explicit 10-column schema (`customer_id`, `region`, `product_id`, `product_name`, `sale_amount`, `return_amount`, `sale_date`, `sales_rep_id`, `channel`, `status`), header on, `PERMISSIVE` mode, corrupt records captured in `_corrupt_record`. An explicit schema avoids the cost of schema inference on large files.
2. **Audit columns.** Each row gains `ingestion_timestamp`, `source_file` (the input file name), `batch_id`, `source_system` (`CRM`), `sale_date_parsed` (parsed from `yyyy-MM-dd`), and `partition_year` / `partition_month` derived from it.
3. **Write.** Append to Delta at `abfss://bronze@{storage_account}/crm_data/`, partitioned by `region`, `partition_year`, `partition_month`, with `mergeSchema=true`.
4. **Validate.** The notebook re-reads the Bronze table, counts rows for this `batch_id`, and asserts the written count equals the read count. A mismatch raises an error and fails the notebook.

### 2.2 Silver — `02_silver_transformation.py`: clean, validate, deduplicate

1. **Read (incremental by batch).** Reads only Bronze rows for this `batch_id`. "Incremental" here means batch-scoped, not change-data-capture: the notebook sees exactly the batch Bronze just wrote.
2. **Data-quality report (logged, not enforced as a gate).** Counts null `customer_id`, null `region`, null/NaN `sale_amount`, negative sales, and invalid (unparsable) dates, and logs the report. The report itself does not stop the pipeline; the filter in the next step is what removes bad rows.
3. **Clean and standardise.** Drops rows with null `customer_id`, null `sale_date_parsed`, null `sale_amount`, or negative `sale_amount`. Trims and upper-cases `region`, `channel`, and `status`. Rounds `sale_amount` and `return_amount` to 2 decimals (null returns become `0.0`), derives `net_amount = sale_amount − return_amount` and `is_returned = return_amount > 0`, and stamps `silver_processed_at`.
4. **Deduplicate (within the batch).** A `ROW_NUMBER()` window partitioned by `customer_id, sale_date_parsed, product_id`, ordered by `ingestion_timestamp` descending, keeps only the latest row per key. This dedup is batch-scoped — see §7.2.
5. **Write.** Append to Delta at `abfss://silver@{storage_account}/crm_data/`, with the same partitioning as Bronze and `mergeSchema=false`.

### 2.3 Gold — `03_gold_aggregation.py`: aggregate for a reporting month

1. **Read.** Reads Silver rows for one reporting month, selected by the `report_year` / `report_month` widgets (in ADF, these are the current year and month at run time). Only that month's partition is read.
2. **Aggregate — Table 1: `region_sales_summary`.** Grouped by `region, partition_year, partition_month, channel`: total sales, total returns, total net revenue, transaction count, distinct customers, average order value, return count, and `return_rate_pct` (returns ÷ sales × 100).
3. **Aggregate — Table 2: `product_performance`.** Grouped by `product_id, product_name, region, partition_year, partition_month`: product revenue, units sold (count of `customer_id`), average selling price, and product returns.
4. **Write.** Each table is written with `mode("overwrite")` plus `replaceWhere` for exactly that year/month, partitioned by `partition_year` / `partition_month`. Re-running Gold for the same month replaces that month's aggregates rather than duplicating them.

### 2.4 Orchestration — ADF `CRM_ETL_Master_Pipeline`

| Activity | Type | Depends on | What it does |
|---|---|---|---|
| `Set_Batch_ID` | SetVariable | — | Generates `batch_id` from the current UTC time. |
| `Bronze_Ingestion` | DatabricksNotebook | Set_Batch_ID (Succeeded) | Runs `01_bronze_ingestion`, passing `batch_id`. Notebook path in the JSON is `/Shared/crm-pipeline/01_bronze_ingestion` — see §7.5. |
| `Silver_Transformation` | DatabricksNotebook | Bronze (Succeeded) | Runs `02_silver_transformation`, passing `batch_id`. |
| `Gold_Aggregation` | DatabricksNotebook | Silver (Succeeded) | Runs `03_gold_aggregation`, passing `batch_id` plus the current year/month. |
| `Pipeline_Monitoring` | DatabricksNotebook | Gold (Succeeded **or** Failed) | Runs `04_pipeline_monitoring`, passing `batch_id`. It runs even when Gold fails, so a failed run still gets a monitoring summary. |
| `On_Failure_Alert` | WebActivity | Gold (Failed) | POSTs a failure message to a Teams webhook. The URL in the JSON is the placeholder `YOUR_TEAMS_WEBHOOK_URL` — see §7.4. |

**Trigger.** A daily schedule trigger, `Daily_8AM_Trigger`, at 08:00 India Standard Time, every day. There is no trigger condition on data arrival; the pipeline runs on the clock whether or not new raw files have landed.

### 2.5 Monitoring — `04_pipeline_monitoring.py`: logging, thresholds, alerts

The notebook defines two utilities and then runs a check with them:

- **`PipelineLogger`** — collects log entries (pipeline, batch, stage, status, record count, message, timestamp) and can persist them as a Delta audit table at `abfss://audit@{storage_account}/pipeline_logs/`.
- **`AlertManager`** — checks three thresholds: runtime over 60 minutes, record count under 100,000, and error rate over 2.0% (a fourth threshold, max null rate 5.0%, is defined but has no check function). Breaches are collected as alerts and logged with `HIGH` severity.

**Read §7.4 before relying on this notebook.** As supplied, the monitoring check runs against hardcoded example figures, the audit-table write is commented out, and the Teams/email send calls are commented out. The utilities are real; the wiring to live metrics and to a notification channel is not.

---

## 3. Data and schema

### 3.1 Source schema (Bronze, explicit)

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `customer_id` | String | No | Key identifier; nulls are dropped in Silver. |
| `region` | String | Yes | Upper-cased and trimmed in Silver; a partition column. |
| `product_id`, `product_name` | String | Yes | |
| `sale_amount`, `return_amount` | Double | Yes | Rounded to 2 decimals in Silver; negative sales dropped. |
| `sale_date` | String | Yes | Parsed to `sale_date_parsed` (`yyyy-MM-dd`); unparsable dates are dropped in Silver. |
| `sales_rep_id`, `channel`, `status` | String | Yes | `channel` and `status` are upper-cased and trimmed in Silver. |

Audit columns added in Bronze: `ingestion_timestamp`, `source_file`, `batch_id`, `source_system`, `sale_date_parsed`, `partition_year`, `partition_month`. Silver adds `net_amount`, `is_returned`, `silver_processed_at`. Gold tables carry `gold_created_at` (region summary also carries `report_period`).

### 3.2 Sample data

`data/sample_crm_data.csv` contains 20 rows with the 10 source columns, dated 5–14 January 2024, across four regions. It is test data for checking the schema and the Bronze read, not a volume test — 20 rows cannot demonstrate anything about 50M-row behaviour.

### 3.3 Partitioning

Bronze and Silver are partitioned by `region, partition_year, partition_month`; Gold by `partition_year, partition_month`. Partitioning is by region and by the *sale* date, not by ingestion date, so a late-arriving sale for an old month lands in that month's partition — which is what makes Gold's `replaceWhere` per month correct, and also what makes a re-run of Gold for an old month necessary if late data arrives.

---

## 4. Design decisions

### 4.1 Medallion layers: Bronze / Silver / Gold

**Decision.** Three Delta layers with different contracts, instead of one cleaned table.

**Why.** Bronze preserves the raw record plus audit metadata, so any cleaning decision can be revisited and re-derived. Silver is the single cleaned, deduplicated, business-rule-applied dataset. Gold is pre-aggregated for the specific reporting questions leadership asks (region and product), so dashboards query small aggregate tables instead of scanning all sales.

**Trade-off.** Three copies of the data and three writes per run. Storage and write cost are the price of re-derivability and fast reporting reads. If a reporting question falls outside the two Gold aggregates, it must be answered from Silver — Gold is not a general query layer.

### 4.2 Explicit schema at ingestion, `PERMISSIVE` mode

**Decision.** Declare the 10-column schema in code and read with `PERMISSIVE` mode plus `_corrupt_record`.

**Why.** Schema inference over 50M+ rows is expensive and fragile; an explicit schema makes the read deterministic and cheap, and `PERMISSIVE` keeps a malformed row from failing the whole batch.

**Trade-off.** `PERMISSIVE` with an explicit schema fills mismatched fields with nulls and captures the raw record in `_corrupt_record` — but nothing in the notebooks then checks `_corrupt_record` or quarantines those rows (see §7.1). Corrupt rows are handled only indirectly, by Silver dropping null-key rows.

### 4.3 Batch-scoped processing via `batch_id`

**Decision.** Every notebook filters or stamps by the `batch_id` ADF generates for the run.

**Why.** A run's work is traceable end to end, Bronze validation can assert "what I read is what I wrote for this batch", and a failed run's batch can be identified in logs and in the data.

**Trade-off.** Batch-scoping is not the same as incremental loading in the change-data sense. Silver only sees the current batch, so dedup and quality logic cannot see history (§7.2), and Gold only rebuilds the current month. A batch that is re-run with the same `batch_id` appends Bronze/Silver rows again — see §7.3.

### 4.4 Data-quality reporting in Silver, enforced by filtering

**Decision.** Log a quality report (nulls, negatives, invalid dates), then filter the bad rows out, rather than failing the notebook on bad data.

**Why.** A small number of bad source rows should not block a 50M-row run. The report makes the loss visible in logs, and the filter keeps Silver clean.

**Trade-off.** Bad rows are silently absent from Silver (apart from the log counts). There is no quarantine table and no per-row reason recorded, so a systematic source problem is visible only as a pattern in the logged counts across runs.

### 4.5 `replaceWhere` overwrites in Gold

**Decision.** Gold writes use `overwrite` with `replaceWhere` scoped to the reporting year/month.

**Why.** Aggregates are fully derived from Silver for that month, so replacing the month is correct and makes Gold re-runs idempotent for that month: re-running Gold for January replaces January, it does not double-count it.

**Trade-off.** The safety of this pattern depends entirely on the `replaceWhere` predicate matching the partition filter. A mismatch between the two would either leave stale partitions or replace the wrong ones. Both are `partition_year` / `partition_month` in the code as supplied, so they match — verify this again before changing either.

### 4.6 ADF orchestration with a clock trigger, not event-based

**Decision.** ADF runs the notebooks in sequence on a daily 08:00 IST schedule.

**Why.** The source is a daily CRM extract, and a fixed morning run matches the reporting rhythm the README describes (data available well within the business day, rather than two days later).

**Trade-off.** The pipeline does not wait for data arrival and does not react to late files. If the extract is late, the run processes whatever is present and Gold rebuilds the current month from an incomplete Silver — a late file then needs a manual re-run or the next day's run to be reflected.

### 4.7 Monitoring as a separate notebook, running on failure too

**Decision.** Monitoring is a notebook in the same pipeline, with a dependency of "Succeeded or Failed" on Gold, plus a Teams webhook activity on Gold failure.

**Why.** A failed Gold run still produces a monitoring summary, and the failure alert does not depend on the monitoring notebook succeeding.

**Trade-off.** As supplied, the monitoring notebook's live wiring is incomplete (§7.4). The *structure* — monitor even on failure, alert on failure — is sound; the *implementation* needs the example figures replaced with real metrics and the notification calls enabled before it can be relied on.

---

## 5. Configuration

| Setting | Where | Value as supplied | Notes |
|---|---|---|---|
| Storage account | All notebooks, as `STORAGE_ACCOUNT` | `"your_storage_account"` | Placeholder — replace in every notebook before running. There is no central config file; the same placeholder appears in all four notebooks. |
| Containers | Notebooks | `raw`, `bronze`, `silver`, `gold`, `audit` | One container per layer, plus raw and audit. |
| `batch_id` | ADF variable / notebook widget | Generated per run (`batch_YYYYMMDD_HHMMSS`); default `"batch_001"` in notebooks | The notebook default is a fallback for manual runs only. Never run two manual runs with the default — see §7.3. |
| `report_year` / `report_month` | ADF expressions / Gold widgets | Current year/month; defaults 2024 / 1 in the notebook | Gold rebuilds only the selected month. |
| Thresholds | Monitoring notebook | Runtime 60 min, min records 100,000, max error rate 2.0%, max null rate 5.0% (defined, unchecked) | Tune to the real volume before relying on alerts. |
| Trigger | ADF JSON | Daily, 08:00, India Standard Time | Clock-based; see §4.6. |
| Teams webhook | ADF JSON, `On_Failure_Alert` | `YOUR_TEAMS_WEBHOOK_URL` | Placeholder — replace or remove before importing. |
| Notebook paths | ADF JSON | `/Shared/crm-pipeline/01_…` … `04_…` | Differ from the README's `/Workspace/projects/` guidance — see §7.5. Confirm the actual workspace paths before importing. |
| Secrets / credentials | — | None in the repository | No Secret Scope, key, or credential appears in these files. Storage access is assumed to come from the Databricks/ADF environment configuration, which is not described in this repository — confirm how the cluster authenticates to ADLS Gen2 before the first run. |

---

## 6. Results — what is claimed, and what the code shows

The README reports, for this pipeline: **50M+ CRM records per month, data availability from T+2 days to under 4 hours, pipeline runtime from 2 hours to 35 minutes, and ~40% compute cost reduction.**

Those figures are reported in the README as project results. The code in this repository does not independently evidence them: there are no run logs, timings, or cost records in the repo, and the only 35-minute / 50M-row figures in the notebooks are the hardcoded example values in the monitoring notebook (§7.4), which are explicitly simulated, not measured. This document therefore repeats the README figures as *reported results*, not as verified measurements, and does not add any new figure of its own.

**How to evidence them.** Keep the ADF run history and Databricks cluster metrics for a representative month: records read per layer, wall-clock time per notebook, cluster size and duration (for cost), and the time from extract landing to Gold availability. Only with those records can the before/after table be presented as measured rather than reported.

---

## 7. Failure modes, gaps, and limitations

Read this section before operating the pipeline. Each item is visible in the supplied files.

1. **Corrupt records are captured but never checked.** Bronze uses `PERMISSIVE` mode with `_corrupt_record`, but no notebook filters, counts, or quarantines corrupt records by that column. A malformed row becomes a row of nulls, which Silver drops only if a key field is null. Add an explicit corrupt-record check and quarantine write if malformed source data needs to be visible.
2. **Silver dedup is batch-scoped, not global.** The `ROW_NUMBER()` dedup partitions within the current batch only, because Silver reads only `batch_id` rows. A duplicate of a row from an earlier batch — same customer, date, and product — is not deduplicated against history. If cross-batch duplicates are possible in the source, dedup needs to read Silver history, not just the batch.
3. **Re-running a batch appends, it does not replace.** Bronze and Silver use `mode("append")`. Re-running Bronze and Silver with the same `batch_id` writes the same rows a second time; only Bronze's count assertion would catch a *partial* write, and it compares counts, not identity. Gold is the only layer that is idempotent for a re-run, via `replaceWhere`. Do not re-run Bronze/Silver for a batch without first removing that batch's rows, or use a fresh `batch_id` and accept the duplication as a known re-run cost.
4. **Monitoring is not yet wired to live data or to notifications.** The monitoring notebook's `pipeline_stats` are hardcoded example figures (50,200,000 bronze records, 49,850,000 silver records, 1,200 gold records, 350,000 error records, 35 minutes), introduced with the comment "Simulate checking pipeline metrics (in prod: pull from Delta logs)". `write_audit_log` is commented out, and the Teams/email send calls in `AlertManager` are commented out. As supplied, the notebook always reports against those fixed figures. Replace them with reads from the Bronze/Silver/Gold tables and the audit log, and enable a notification channel, before treating any monitoring summary as real.
5. **Notebook paths are only stated in the ADF JSON.** The README says only to import the notebooks into the Databricks workspace, without giving a workspace path, while the ADF JSON points at `/Shared/crm-pipeline/01_bronze_ingestion` and so on. Neither path is created by this repository. Confirm the real workspace paths and make the JSON and the run instructions agree before importing.
6. **Placeholders must be replaced in five places.** `your_storage_account` appears in all four notebooks, and `YOUR_TEAMS_WEBHOOK_URL` in the ADF JSON. The pipeline cannot run as supplied without those replacements, and the storage-account placeholder is duplicated rather than centralised — a missed replacement in one notebook fails only that notebook's stage.
7. **No credential or access description.** No Secret Scope, service principal, managed identity, or ADLS access configuration is described in this repository. How the Databricks cluster and ADF linked service authenticate to ADLS Gen2 must be established in the target environment; it cannot be inferred from these files.
8. **Gold reads only the current month.** If late-arriving data belongs to an earlier month, Gold for that earlier month must be re-run explicitly with that month's widgets. The daily ADF run rebuilds only the current month.
9. **No tests in this repository.** There are no unit or integration tests for the cleaning rules, the dedup window, or the Gold aggregations. The sample CSV (20 rows) exercises the Bronze schema, not the business rules at volume.
10. **The null-rate threshold is defined but unused.** `THRESHOLDS` includes `max_null_rate_pct: 5.0`, but `AlertManager` has no check that uses it. Either implement the check or remove the threshold, so the monitoring configuration does not imply a guard that does not exist.

---

## 8. Performance — what is known and what is not

**Known from the code.** The design choices that affect performance are: an explicit schema (no inference cost), partitioning by region/year/month (so Gold reads one month, and queries can prune by region), batch-scoped Silver reads (no full-table scans in Silver), and `replaceWhere` Gold writes (one month replaced, not a full rewrite).

**Not known from the supplied files.** No measured runtime, record count, cluster configuration, or cost record was supplied beyond the README's reported figures (§6) and the monitoring notebook's simulated figures (§7.4). This document claims no additional performance figure.

**How to measure it.** Record, for a representative daily run: raw rows read, Bronze/Silver/Gold rows written, wall-clock time per notebook, cluster type and worker count, and the time from raw-file landing to Gold availability. Compare those with the pre-pipeline baseline on the same definitions before quoting a speed-up or cost reduction as measured.

---

## 9. Deployment and operations checklist

- [ ] `your_storage_account` replaced in all four notebooks; containers `raw`, `bronze`, `silver`, `gold`, `audit` exist and are accessible from the Databricks workspace.
- [ ] How the cluster and ADF authenticate to ADLS Gen2 is established and documented (§7.7).
- [ ] Notebooks uploaded to the workspace paths the ADF JSON actually uses, and the README run instructions updated to match (§7.5).
- [ ] `YOUR_TEAMS_WEBHOOK_URL` replaced with a real webhook, or the `On_Failure_Alert` activity removed.
- [ ] Monitoring notebook wired to real metrics and notifications, or explicitly marked as a template not yet in use (§7.4).
- [ ] First run is against the 20-row sample (or a small extract), with Bronze counts, Silver quality-report counts, and Gold aggregates checked by hand before the daily trigger is enabled.
- [ ] A re-run procedure is agreed: how a failed or duplicated batch is removed before Bronze/Silver are re-run (§7.3).
- [ ] Late-arriving data procedure is agreed: which Gold months are rebuilt, and when (§7.8).
- [ ] Gold aggregates are spot-checked against Silver for one month before dashboards are pointed at Gold.

---

## 10. File inventory

| Path | What it is | Changed in this repository? |
|---|---|---|
| `notebooks/01_bronze_ingestion.py` | Bronze ingestion | No — as supplied |
| `notebooks/02_silver_transformation.py` | Silver cleaning, validation, dedup | No — as supplied |
| `notebooks/03_gold_aggregation.py` | Gold aggregations | No — as supplied |
| `notebooks/04_pipeline_monitoring.py` | Monitoring utilities and (simulated) check | No — as supplied |
| `adf_pipelines/crm_pipeline_config.json` | ADF master pipeline definition | No — as supplied, including placeholders |
| `data/sample_crm_data.csv` | 20-row sample CRM data | No — as supplied |
| `docs/architecture.md` | This document | **Yes — newly created** at the author's request; the README already listed this path, but the file did not exist |
| `README.md` | Project README | No change in this step |

---

## Author

**Sanjeev Kumar Pandey** — Data Engineer → AI Engineer | Azure Databricks | PySpark | RAG | Agentic AI

[LinkedIn](https://www.linkedin.com/in/sanjeev-pandey-7a45831ba) · [GitHub](https://github.com/sanjeevpandeydata)
