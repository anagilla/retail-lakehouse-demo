"""Retail Analytics Dashboard — Gradio app backed by Gold-layer Delta tables."""

import os
import logging

import gradio as gr
import pandas as pd
from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CATALOG = os.getenv("CATALOG", "main")
GOLD = f"{CATALOG}.retail_gold"
SILVER = f"{CATALOG}.retail_silver"
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")


def run_query(sql: str) -> pd.DataFrame:
    """Execute SQL via Databricks SDK (uses app service principal auth)."""
    try:
        w = WorkspaceClient()
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=sql,
            wait_timeout="30s",
        )
        if response.result and response.manifest:
            columns = [col.name for col in response.manifest.schema.columns]
            rows = response.result.data_array or []
            return pd.DataFrame(rows, columns=columns)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Query failed: {e}")
        return pd.DataFrame({"error": [str(e)]})


# ---------------------------------------------------------------------------
# Tab 1 — Customer 360
# ---------------------------------------------------------------------------
def customer_lookup(customer_id: str):
    try:
        cid = int(customer_id)
    except (ValueError, TypeError):
        return "Enter a valid customer ID (integer).", None

    profile = run_query(f"""
        SELECT c.customer_key, c.customer_name, c.market_segment,
               c.nation_name, c.region_name, c.balance_tier,
               r.rfm_segment, r.rfm_score,
               ROUND(r.monetary, 2) AS lifetime_value,
               r.frequency AS total_orders,
               r.recency_days,
               ROUND(r.avg_order_value, 2) AS avg_order_value
        FROM {SILVER}.dim_customer c
        LEFT JOIN {GOLD}.gold_customer_rfm r
            ON c.customer_key = r.customer_key
        WHERE c.customer_key = {cid}
    """)

    if "error" in profile.columns:
        return f"Query error: {profile['error'].iloc[0]}", None
    if profile.empty:
        return f"Customer {cid} not found.", None

    row = profile.iloc[0]
    md = f"## Customer #{row['customer_key']} - {row['customer_name']}\n\n"
    md += f"| Attribute | Value |\n|---|---|\n"
    md += f"| Segment | {row['market_segment']} |\n"
    md += f"| Region | {row['region_name']} ({row['nation_name']}) |\n"
    md += f"| RFM Segment | {row['rfm_segment']} |\n"
    md += f"| Lifetime Value | ${float(row['lifetime_value']):,.2f} |\n"
    md += f"| Total Orders | {row['total_orders']} |\n"
    md += f"| Avg Order Value | ${float(row['avg_order_value']):,.2f} |\n"
    md += f"| Recency (days) | {row['recency_days']} |\n"
    return md, profile


# ---------------------------------------------------------------------------
# Tab 2 — Revenue Explorer
# ---------------------------------------------------------------------------
def revenue_explorer(region: str, start_month: str, end_month: str):
    where = "WHERE 1=1"
    if region != "ALL":
        where += f" AND region = '{region}'"
    if start_month:
        where += f" AND year_month >= '{start_month}'"
    if end_month:
        where += f" AND year_month <= '{end_month}'"

    df = run_query(f"""
        SELECT year_month, region,
               ROUND(SUM(net_revenue), 0) AS net_revenue,
               SUM(num_orders) AS orders,
               ROUND(AVG(profit_margin_pct), 1) AS margin_pct
        FROM {GOLD}.gold_monthly_sales
        {where}
        GROUP BY year_month, region
        ORDER BY year_month, region
    """)

    if "error" in df.columns:
        return f"Query error: {df['error'].iloc[0]}", df

    total = pd.to_numeric(df["net_revenue"], errors="coerce").sum()
    return f"**Total Revenue**: ${total:,.0f}  |  **Rows**: {len(df)}", df


# ---------------------------------------------------------------------------
# Tab 3 — Product Analytics
# ---------------------------------------------------------------------------
def product_analytics(sort_by: str, top_n: int):
    return run_query(f"""
        SELECT brand, price_band,
               ROUND(SUM(net_revenue), 0) AS net_revenue,
               ROUND(AVG(profit_margin_pct), 1) AS margin_pct,
               ROUND(AVG(return_rate_pct), 1) AS return_rate_pct,
               SUM(num_orders) AS orders
        FROM {GOLD}.gold_product_performance
        GROUP BY brand, price_band
        ORDER BY {sort_by} DESC
        LIMIT {int(top_n)}
    """)


# ---------------------------------------------------------------------------
# Tab 4 — Executive KPIs
# ---------------------------------------------------------------------------
def executive_kpis():
    return run_query(f"""
        SELECT year_quarter, total_orders, active_customers,
               ROUND(gross_order_value, 0) AS gross_order_value,
               ROUND(avg_order_value, 0) AS avg_order_value,
               ROUND(revenue_per_customer, 0) AS rev_per_customer,
               qoq_revenue_growth_pct
        FROM {GOLD}.gold_executive_summary
        ORDER BY year_quarter
    """)


# ---------------------------------------------------------------------------
# Gradio UI
# ---------------------------------------------------------------------------
with gr.Blocks(title="Retail Analytics", theme=gr.themes.Soft()) as app:
    gr.Markdown("# Retail Analytics Dashboard")
    gr.Markdown("Gold-layer Delta tables · Databricks SQL")

    with gr.Tab("Customer 360"):
        gr.Markdown("### Look up any customer by ID")
        with gr.Row():
            cust_input = gr.Textbox(label="Customer ID", placeholder="e.g. 42", scale=1)
            cust_btn = gr.Button("Look Up", variant="primary", scale=1)
        cust_md = gr.Markdown()
        cust_table = gr.Dataframe(label="Raw Profile")
        cust_btn.click(customer_lookup, inputs=cust_input, outputs=[cust_md, cust_table])

    with gr.Tab("Revenue Explorer"):
        gr.Markdown("### Monthly revenue by region")
        with gr.Row():
            region_dd = gr.Dropdown(
                choices=["ALL", "AMERICA", "EUROPE", "ASIA", "AFRICA", "MIDDLE EAST"],
                value="ALL", label="Region",
            )
            start_m = gr.Textbox(label="Start (yyyy-MM)", value="1995-01")
            end_m = gr.Textbox(label="End (yyyy-MM)", value="1997-12")
            rev_btn = gr.Button("Query", variant="primary")
        rev_summary = gr.Markdown()
        rev_table = gr.Dataframe(label="Revenue Data")
        rev_btn.click(revenue_explorer, inputs=[region_dd, start_m, end_m], outputs=[rev_summary, rev_table])

    with gr.Tab("Product Analytics"):
        gr.Markdown("### Product performance by brand")
        with gr.Row():
            sort_dd = gr.Dropdown(
                choices=["net_revenue", "margin_pct", "return_rate_pct", "orders"],
                value="net_revenue", label="Sort By",
            )
            topn_slider = gr.Slider(minimum=5, maximum=50, value=20, step=5, label="Top N")
            prod_btn = gr.Button("Query", variant="primary")
        prod_table = gr.Dataframe(label="Product Performance")
        prod_btn.click(product_analytics, inputs=[sort_dd, topn_slider], outputs=prod_table)

    with gr.Tab("Executive KPIs"):
        gr.Markdown("### Quarterly executive summary")
        exec_btn = gr.Button("Load KPIs", variant="primary")
        exec_table = gr.Dataframe(label="Quarterly KPIs")
        exec_btn.click(executive_kpis, outputs=exec_table)

if __name__ == "__main__":
    logger.info(f"Starting Retail Analytics App (catalog={CATALOG}, warehouse={WAREHOUSE_ID})")
    app.launch(
        server_name="0.0.0.0",
        server_port=int(os.getenv("PORT", "8000")),
    )
