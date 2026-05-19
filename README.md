# Azure ETL Pipeline — CRM Data Processing at Scale

![Azure](https://img.shields.io/badge/Azure-Data%20Factory-0078D4)
![Databricks](https://img.shields.io/badge/Databricks-Spark-FF3621)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-00ADD8)
![Python](https://img.shields.io/badge/Python-3.9-blue)

## Problem Statement
Processing **50M+ CRM records monthly** with a T+2 day data availability lag was too slow for business reporting needs. Leadership needed region-wise sales and returns insights on a near-real-time basis.

## Solution
Built a scalable **ELT pipeline** using Azure Data Factory for orchestration and Azure Databricks for transformation, following **Medallion Architecture** (Bronze → Silver → Gold layers).

## Architecture
```
CRM Source System
      │
      ▼
Azure Data Factory (Orchestration + Triggers)
      │
      ▼
ADLS Gen2 (Raw Landing Zone)
      │
      ▼
Azure Databricks (PySpark Transformations)
      │
      ├──▶ Bronze Layer (Raw Delta Tables)
      │
      ├──▶ Silver Layer (Cleaned + Validated)
      │
      └──▶ Gold Layer (Aggregated Business Reports)
```

## Tech Stack
| Layer | Technology |
|---|---|
| Orchestration | Azure Data Factory |
| Processing | Azure Databricks, Apache Spark, PySpark |
| Storage | Azure Data Lake Storage Gen2 |
| Table Format | Delta Lake (ACID transactions) |
| Language | Python, Spark SQL |
| Monitoring | Azure Monitor, Custom Logging |

## Results Achieved
| Metric | Before | After |
|---|---|---|
| Data Availability | T+2 days | Under 4 hours |
| Pipeline Runtime | 2 hours | 35 minutes (70% reduction) |
| Compute Cost | Baseline | ~40% reduction |
| Records/Month | 50M+ | 50M+ (stable) |

## Project Structure
```
azure-etl-pipeline/
├── notebooks/
│   ├── 01_bronze_ingestion.py       # Raw data ingestion to Delta
│   ├── 02_silver_transformation.py  # Cleaning & validation
│   ├── 03_gold_aggregation.py       # Business-level aggregations
│   └── 04_pipeline_monitoring.py    # Logging & alerting
├── adf_pipelines/
│   └── crm_pipeline_config.json     # ADF pipeline definition
├── data/
│   └── sample_crm_data.csv          # Sample dataset for testing
├── docs/
│   └── architecture.md              # Detailed architecture notes
└── README.md
```

## How to Run
1. Clone this repository
   ```bash
   git clone https://github.com/sanjeevpandeydata/azure-etl-pipeline.git
   ```
2. Upload `data/sample_crm_data.csv` to your ADLS Gen2 container
3. Import notebooks from `notebooks/` folder into your Databricks workspace
4. Update the storage paths in each notebook to match your ADLS account
5. Run notebooks in order: 01 → 02 → 03 → 04
6. (Optional) Import `adf_pipelines/crm_pipeline_config.json` into Azure Data Factory

## Key Concepts Demonstrated
- **Medallion Architecture** — Bronze / Silver / Gold layered design
- **Incremental Loads** — Processing only new/changed records
- **Schema Enforcement** — Data quality at ingestion
- **Partitioning Strategy** — Partitioned by region and date for query performance
- **Audit Logging** — Full traceability of every pipeline run

## Author
**Sanjeev Kumar Pandey**  
Data Engineer | Azure | Spark | Databricks  
[LinkedIn](https://www.linkedin.com/in/sanjeev-pandey-7a45831ba/) | sanjeevpandey640@gmail.com
