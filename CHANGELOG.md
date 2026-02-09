# Changelog

## v1.0.0 — 2026-02-09

First version with full Medallion pipeline, ML models, and AI/BI integration.

### Data platform
- TPC-H data generation (pure PySpark, serverless-compatible)
- Medallion Architecture: Bronze, Silver, Gold
- 12 analytical SQL queries on Gold tables
- Liquid clustering on high-volume tables
- Data quality constraints on Silver layer

### Machine learning
- Customer churn prediction (scikit-learn GradientBoosting)
- Demand forecasting by region (Prophet)
- MLflow experiment tracking, Unity Catalog model registry

### Online serving
- Lakebase (PostgreSQL) for low-latency lookups
- Feature Store registration
- Vector Search index for product catalog

### AI and BI
- AI Agent with 6 Unity Catalog SQL tool functions
- Foundation Model API (Llama 3.3 70B) with tool-calling
- Lakeview Dashboard (programmatic)
- Genie Space configuration for self-serve analytics

### Application
- Gradio-based Databricks App (4 tabs)
- REST API deployment automation

### Infrastructure
- Databricks Asset Bundle config
- Multi-target support (dev / staging / prod)
