# Retail Lakehouse Demo

End-to-end retail analytics on Databricks, from TPC-H data generation through
Medallion Architecture, ML models, AI agents, and interactive applications.

Packaged as a **Databricks Asset Bundle** for one-command deployment.

---

## What's in the box

This project builds a complete analytics stack on a single Databricks workspace
using TPC-H benchmark data as the source. It covers the full lifecycle:
data engineering, ML, real-time serving, and user-facing applications.

| Layer | What it does |
|---|---|
| Data generation | Synthetic TPC-H data (1 GB to 100 GB), pure PySpark |
| Medallion pipeline | Bronze → Silver → Gold with Delta Lake, liquid clustering, quality constraints |
| Lakeflow pipeline | Silver → Gold via Spark Declarative Pipeline (7 materialized views) |
| SQL analytics | 12 production-ready queries on Gold tables (window functions, CTEs, pivots) |
| Document AI | Parse invoice/catalog PDFs with `ai_parse_document`, extract fields with `ai_extract` |
| ML models | Customer churn prediction + demand forecasting, logged to MLflow |
| Online serving | Lakebase (PostgreSQL), Feature Store, Vector Search |
| AI agent | Tool-calling LLM backed by UC SQL functions |
| Knowledge Assistant | Q&A over invoice and product catalog PDFs (RAG) |
| BI | Lakeview dashboard, Genie Space for self-serve analytics |
| App | Gradio web app deployed as a Databricks App |

### Databricks App -- Retail Analytics Dashboard

![Retail Analytics Dashboard](docs/DatabricksApp.png)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA GENERATION                                  │
│  00_generate_data  →  TPC-H benchmark (1 GB → 100 GB, pure PySpark)     │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────────┐
│                     MEDALLION ARCHITECTURE                              │
│  01_bronze  →  02_silver  →  03_gold  (notebooks)                       │
│                                ↕                                        │
│  Lakeflow SDP:  silver_to_gold (7 materialized views)                   │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
          ┌─────────────────────┼──────────────────────┐
          │                     │                      │
┌─────────▼──────────┐ ┌────────▼───────┐ ┌───────────▼──────────┐
│  05  LAKEBASE      │ │  06  AI / ML   │ │  04  SQL ANALYTICS   │
│  Online serving    │ │  Churn model   │ │  12 analytical       │
│  Feature Store     │ │  Demand        │ │  queries on Gold     │
│  Vector Search     │ │  forecasting   │ │                      │
└─────────┬──────────┘ └────────┬───────┘ └──────────────────────┘
          │                     │
┌─────────▼─────────────────────▼────────────────────────────────┐
│  Document AI        ai_parse_document / ai_extract             │
│  Knowledge Asst.    Genie Space (Gold tables)                  │
├────────────────────────────────────────────────────────────────┤
│  07  AI AGENT         08  DASHBOARD          09  APP           │
│  6 UC SQL tools       Lakeview dashboard     Gradio web app    │
│  Llama 3.3 70B        Self-serve Q&A         Customer 360      │
└────────────────────────────────────────────────────────────────┘
```

---

## Repository layout

```
retail-lakehouse-demo/
├── databricks.yml               # Asset Bundle config (jobs, pipelines, targets)
├── README.md
├── CONTRIBUTING.md
├── CHANGELOG.md
├── .gitignore
├── pipelines/
│   └── silver_to_gold/          # Lakeflow SDP pipeline (SQL materialized views)
│       ├── gold_daily_sales.sql
│       ├── gold_monthly_sales.sql
│       ├── gold_executive_summary.sql
│       ├── gold_product_performance.sql
│       ├── gold_customer_rfm.sql
│       ├── gold_shipping_analysis.sql
│       └── gold_supplier_scorecard.sql
└── src/
    ├── notebooks/
    │   ├── 00_generate_data.ipynb       # TPC-H data gen (pure PySpark)
    │   ├── 01_bronze_layer.ipynb        # Raw ingestion + audit columns
    │   ├── 02_silver_layer.ipynb        # Dim/fact tables, conformance
    │   ├── 03_gold_layer.ipynb          # Aggregations, RFM, scorecards
    │   ├── 05_lakebase_serving.ipynb    # Lakebase, Feature Store, VS
    │   ├── 06_ai_ml_models.ipynb        # Churn + demand forecasting
    │   ├── 07_ai_agents.ipynb           # Tool-calling AI agent
    │   ├── 08_ai_bi_dashboard.ipynb     # Lakeview dashboard + Genie
    │   └── 09_databricks_app.ipynb      # Gradio app deployment
    ├── sql/
    │   └── 04_gold_analytics.sql        # 12 analytical SQL queries
    └── app/
        ├── app.py                       # Standalone Gradio application
        ├── app.yaml                     # App runtime config
        └── requirements.txt             # App Python dependencies
```

---

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Serverless compute (all notebooks are serverless-compatible)
- A SQL Warehouse (Pro or Serverless) for Genie and the app
- Databricks CLI v0.218+ ([install instructions](https://docs.databricks.com/en/dev-tools/cli/install.html))

---

## Getting started

### Deploy with Asset Bundles

```bash
git clone https://github.com/anagilla/retail-lakehouse-demo.git && cd retail-lakehouse-demo

databricks configure --profile retail-demo
databricks bundle validate -t dev
databricks bundle deploy -t dev

# Run pipelines in order
databricks bundle run retail_data_pipeline -t dev          # generate + bronze + silver + gold
databricks bundle run retail_silver_to_gold -t dev         # Lakeflow SDP (7 materialized views)
databricks bundle run retail_ml_pipeline -t dev
databricks bundle run retail_ai_experiences -t dev
```

### Or run notebooks interactively

Import `src/notebooks/` into your workspace and run in order (00 → 09).
Each notebook is self-contained with inline `%pip install` commands.

| Step | Notebook | ~Time (SF=1) | Purpose |
|------|----------|-------------|---------|
| 0 | `00_generate_data` | 3 min | TPC-H data generation |
| 1 | `01_bronze_layer` | 2 min | Raw ingestion with audit columns |
| 2 | `02_silver_layer` | 3 min | Dimension/fact modelling |
| 3 | `03_gold_layer` | 3 min | KPI aggregations |
| 4 | `04_gold_analytics.sql` | 1 min | Run in SQL editor |
| 5 | `05_lakebase_serving` | 5 min | Lakebase + Feature Store |
| 6 | `06_ai_ml_models` | 5 min | Churn + forecasting models |
| 7 | `07_ai_agents` | 2 min | AI agent with tool-calling |
| 8 | `08_ai_bi_dashboard` | 1 min | Lakeview dashboard + Genie |
| 9 | `09_databricks_app` | 3 min | Deploy Gradio web app |

---

## Configuration

Notebooks auto-detect the current catalog via `spark.catalog.currentCatalog()`.
Schemas created:

| Schema | Purpose |
|---|---|
| `tpch` | Raw TPC-H tables |
| `retail_bronze` | Bronze layer (raw + audit) |
| `retail_silver` | Silver layer (dim/fact) |
| `retail_gold` | Gold layer (aggregations) |
| `retail_serving` | Online serving tables |
| `retail_agent_tools` | UC SQL functions for the AI agent |

To change the data volume, edit `SCALE_FACTOR` in `00_generate_data.ipynb`:

```python
SCALE_FACTOR = 1     # 1 GB  (~8M rows in lineitem)
SCALE_FACTOR = 10    # 10 GB (~60M rows)
SCALE_FACTOR = 100   # 100 GB (~600M rows)
```

---

## Lakebase setup

Notebook 05 pushes Gold tables to Lakebase for low-latency serving.
Before running:

1. Go to **Catalog > Database Instances > Create**
2. Create a sandbox instance
3. Copy the connection string into notebook 05 when prompted

---

## Lakeflow pipeline (Silver → Gold)

`pipelines/silver_to_gold/` has 7 SQL materialized views that aggregate the
Silver fact/dimension tables into the Gold layer. Runs as a serverless Spark
Declarative Pipeline.

| View | Description |
|---|---|
| `gold_daily_sales` | Revenue, orders, margin by date / region / segment |
| `gold_monthly_sales` | Rolls up daily sales; adds MoM and YoY growth |
| `gold_executive_summary` | Quarterly KPIs for leadership (orders, customers, revenue) |
| `gold_product_performance` | Brand-level revenue, margin, return rate |
| `gold_customer_rfm` | RFM segmentation (recency / frequency / monetary quintiles) |
| `gold_shipping_analysis` | On-time rate and delays per ship mode and region |
| `gold_supplier_scorecard` | Delivery, quality, and profitability per supplier |

```bash
databricks bundle run retail_silver_to_gold -t dev
```

---

## Document AI

A set of retail invoice and product catalog PDFs live in the
`retail_gold.gold_documentation` volume. You can parse them with
`ai_parse_document` and pull structured fields out with `ai_extract`:

```sql
-- read PDFs as binary, parse to text
SELECT path, ai_parse_document(content) AS parsed
FROM READ_FILES('/Volumes/<catalog>/retail_gold/gold_documentation/*.pdf',
               format => 'binaryFile');

-- extract invoice fields from the parsed text
SELECT ai_extract(full_text,
         array('invoice_number', 'customer_name', 'total_amount'))
FROM <catalog>.retail_gold.parsed_documents;
```

A Knowledge Assistant is also set up over these PDFs for Q&A.

---

## Genie Space

A Genie Space ("Retail Sales Analytics") sits on top of the Gold tables.
Try questions like:

- "Total sales by region last quarter?"
- "Which brands have the highest margins?"
- "Top 10 suppliers by on-time delivery"

---

## Cleanup

```bash
databricks bundle destroy -t dev
```

Or drop schemas manually:
```sql
DROP SCHEMA <catalog>.tpch CASCADE;
DROP SCHEMA <catalog>.retail_bronze CASCADE;
DROP SCHEMA <catalog>.retail_silver CASCADE;
DROP SCHEMA <catalog>.retail_gold CASCADE;
DROP SCHEMA <catalog>.retail_serving CASCADE;
DROP SCHEMA <catalog>.retail_agent_tools CASCADE;
```

---

## Scaling

The demo runs at SF=1 (1 GB) for fast iteration. For load testing:

1. Set `SCALE_FACTOR = 100` in notebook 00
2. Use a multi-node cluster (the bundle config includes a 2-worker cluster)
3. Enable Predictive Optimization on Gold tables
4. Schedule the data pipeline job on a cron trigger

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
