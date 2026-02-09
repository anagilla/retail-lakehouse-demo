# Retail Lakehouse Demo — Databricks Platform Showcase

End-to-end retail analytics on Databricks, from raw TPC-H data generation through
Medallion Architecture, ML models, AI agents, and interactive applications.

Built as a **Databricks Asset Bundle** for one-command deployment to any workspace.

---

## Why This Matters

Retail organisations sit on enormous transactional datasets but struggle to move
from raw data to business outcomes. This demo closes that gap by showing how the
Databricks Lakehouse Platform handles every step of the journey in a single,
governed environment — no separate ETL tool, no external BI server, no third-party
ML platform.

| Business question | What this demo shows |
|---|---|
| "How do I unify my data estate?" | Medallion Architecture with Delta Lake and Unity Catalog |
| "Can I trust the numbers?" | Data quality constraints, liquid clustering, lineage tracking |
| "What about advanced analytics?" | Customer churn prediction (scikit-learn) + demand forecasting (Prophet) |
| "How do business users get answers?" | AI/BI Genie for natural-language Q&A, Lakeview Dashboards |
| "What about real-time serving?" | Lakebase (PostgreSQL-compatible) + Feature Store + Vector Search |
| "Can I build apps on top of this?" | Databricks Apps with Gradio — zero external infrastructure |
| "What about AI agents?" | Tool-calling LLM agent backed by Unity Catalog SQL functions |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA GENERATION                                  │
│  00_generate_data  →  TPC-H benchmark (1 GB → 100 GB, pure PySpark)   │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────────┐
│                     MEDALLION ARCHITECTURE                              │
│  01_bronze_layer  →  02_silver_layer  →  03_gold_layer                 │
│  (raw + audit)       (dim/fact, SCD)     (KPIs, RFM, scorecards)       │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
          ┌─────────────────────┼──────────────────────┐
          │                     │                      │
┌─────────▼──────────┐ ┌───────▼────────┐ ┌───────────▼──────────┐
│  05  LAKEBASE      │ │  06  AI / ML   │ │  04  SQL ANALYTICS   │
│  Online serving    │ │  Churn model   │ │  12 analytical       │
│  Feature Store     │ │  Demand        │ │  queries on Gold     │
│  Vector Search     │ │  forecasting   │ │                      │
└─────────┬──────────┘ └───────┬────────┘ └──────────────────────┘
          │                     │
┌─────────▼─────────────────────▼────────────────────────────────┐
│  07  AI AGENT         08  AI/BI DASHBOARD    09  DATABRICKS APP│
│  6 UC SQL tools       Lakeview + Genie       Gradio web app    │
│  Llama 3.3 70B        Self-serve Q&A         Customer 360      │
│  tool-calling loop                           Revenue Explorer   │
└────────────────────────────────────────────────────────────────┘
```

---

## Repository Structure

```
retail-lakehouse-demo/
├── databricks.yml               # Asset Bundle config (jobs, targets)
├── README.md
├── CONTRIBUTING.md
├── CHANGELOG.md
├── .gitignore
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

## Databricks Features Demonstrated

| Category | Features |
|---|---|
| **Data Engineering** | Delta Lake, Liquid Clustering, Unity Catalog, Medallion Architecture |
| **Data Quality** | CHECK constraints, audit columns, deterministic data generation |
| **SQL Analytics** | Window functions, PIVOT, NTILE, PERCENTILE, CTEs |
| **Machine Learning** | scikit-learn, Prophet, MLflow experiment tracking, UC Model Registry |
| **Online Serving** | Lakebase (PostgreSQL-compatible), Feature Store, Vector Search |
| **AI Agents** | Foundation Model API, OpenAI-compatible tool-calling, UC SQL functions |
| **BI & Self-Serve** | Lakeview Dashboards, Genie Spaces (natural-language analytics) |
| **Applications** | Databricks Apps (Gradio), serverless compute |
| **DevOps** | Databricks Asset Bundles, multi-target deployment (dev/staging/prod) |

---

## Prerequisites

- Databricks workspace with **Unity Catalog** enabled
- **Serverless compute** available (all notebooks are serverless-compatible)
- A **SQL Warehouse** (Pro or Serverless) for Genie and the Databricks App
- Databricks CLI v0.218+ installed locally (`pip install databricks-cli`)

---

## Quick Start

### Option A — Deploy with Asset Bundles (recommended)

```bash
# 1. Clone the repo
git clone <repo-url> && cd retail-lakehouse-demo

# 2. Configure authentication
databricks configure --profile retail-demo

# 3. Validate the bundle
databricks bundle validate -t dev

# 4. Deploy to your dev workspace
databricks bundle deploy -t dev

# 5. Run the data pipeline
databricks bundle run retail_data_pipeline -t dev

# 6. Run the ML pipeline (after data pipeline completes)
databricks bundle run retail_ml_pipeline -t dev

# 7. Run the AI experiences (agent, dashboard, app)
databricks bundle run retail_ai_experiences -t dev
```

### Option B — Run notebooks interactively

Import the `src/notebooks/` folder into your Databricks workspace and run them
in order (00 → 09). Each notebook is self-contained and includes all necessary
`%pip install` commands.

| Step | Notebook | Time (SF=1) | What it does |
|------|----------|-------------|--------------|
| 0 | `00_generate_data` | ~3 min | TPC-H data generation |
| 1 | `01_bronze_layer` | ~2 min | Raw ingestion with audit columns |
| 2 | `02_silver_layer` | ~3 min | Dimension/fact modelling |
| 3 | `03_gold_layer` | ~3 min | KPI aggregations |
| 4 | `04_gold_analytics.sql` | ~1 min | Run in SQL editor |
| 5 | `05_lakebase_serving` | ~5 min | Lakebase + Feature Store |
| 6 | `06_ai_ml_models` | ~5 min | Churn + forecasting models |
| 7 | `07_ai_agents` | ~2 min | AI agent with tool-calling |
| 8 | `08_ai_bi_dashboard` | ~1 min | Lakeview dashboard + Genie |
| 9 | `09_databricks_app` | ~3 min | Deploy Gradio web app |

---

## Configuration

All notebooks auto-detect the current Unity Catalog catalog using
`spark.catalog.currentCatalog()`. The schemas created are:

| Schema | Purpose |
|---|---|
| `retail_tpch` | Raw TPC-H generated tables |
| `retail_bronze` | Bronze layer (raw + audit) |
| `retail_silver` | Silver layer (dim/fact) |
| `retail_gold` | Gold layer (aggregations) |
| `retail_serving` | Online serving tables |
| `retail_agent_tools` | UC SQL functions for the AI agent |

To change the **scale factor** (data volume), edit `SCALE_FACTOR` in
`00_generate_data.ipynb`:

```python
SCALE_FACTOR = 1     # 1 GB  (~8M rows in lineitem)
SCALE_FACTOR = 10    # 10 GB (~60M rows)
SCALE_FACTOR = 100   # 100 GB (~600M rows)
```

---

## Lakebase Setup

Notebook 05 pushes Gold tables to a Lakebase PostgreSQL instance for
low-latency serving. Before running it:

1. Go to **Catalog → Database Instances → Create**
2. Create a sandbox Lakebase instance
3. Copy the connection string into notebook 05 when prompted

The notebook handles authentication using the workspace OAuth token
and includes retry logic for idempotent re-runs.

---

## Genie Space Setup

The Genie Space (notebook 08) requires a brief manual step:

1. Navigate to **AI/BI → Genie Spaces → New Space**
2. Title it "Retail Analytics Genie"
3. Attach a SQL Warehouse and add the 8 Gold tables
4. Paste the instructions printed by the notebook
5. Add the sample questions

This takes about 2 minutes and gives business users natural-language
access to the entire Gold layer.

---

## Cleanup

```bash
# Remove all deployed resources
databricks bundle destroy -t dev

# Or manually drop the schemas
DROP SCHEMA <catalog>.retail_tpch CASCADE;
DROP SCHEMA <catalog>.retail_bronze CASCADE;
DROP SCHEMA <catalog>.retail_silver CASCADE;
DROP SCHEMA <catalog>.retail_gold CASCADE;
DROP SCHEMA <catalog>.retail_serving CASCADE;
DROP SCHEMA <catalog>.retail_agent_tools CASCADE;
```

---

## Scaling to Production

This demo runs at TPC-H SF=1 (1 GB) for fast iteration. For production-scale
testing:

1. Set `SCALE_FACTOR = 100` in notebook 00 (generates ~100 GB)
2. Use a multi-node cluster (the DAB config includes a 2-worker cluster)
3. Enable **Predictive Optimization** on the Gold tables
4. Schedule the data pipeline job on a cron trigger
5. Set up **Quality Monitors** on the Gold tables for drift detection

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on extending this demo.
