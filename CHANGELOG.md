# Changelog

## v1.0.0 — 2026-02-09

Initial release of the Retail Lakehouse Demo.

### Data platform
- TPC-H data generation (pure PySpark, serverless-compatible)
- Full Medallion Architecture: Bronze → Silver → Gold
- 12 analytical SQL queries on Gold layer tables
- Liquid clustering on high-volume tables
- Data quality constraints (CHECK) on Silver layer

### Machine Learning
- Customer churn prediction (scikit-learn GradientBoosting)
- Demand forecasting by region (Prophet)
- MLflow experiment tracking and Unity Catalog model registry

### Online serving
- Lakebase (PostgreSQL) integration for low-latency lookups
- Feature Store registration for ML features
- Vector Search index for product catalog

### AI / BI
- AI Agent with 6 Unity Catalog SQL tool functions
- Foundation Model API integration (Llama 3.3 70B)
- AI/BI Lakeview Dashboard (programmatic creation)
- Genie Space configuration for self-serve analytics

### Application
- Gradio-based Databricks App with 4 interactive tabs
- REST API deployment automation

### DevOps
- Databricks Asset Bundle configuration
- Multi-target support (dev / staging / prod)
- Structured project layout
