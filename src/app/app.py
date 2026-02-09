"""Retail Analytics Dashboard -- Gradio app backed by Gold-layer Delta tables."""

import os
import logging

import gradio as gr
import pandas as pd
from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -- Configuration ----------------------------------------------------------------
CATALOG      = os.getenv("CATALOG", "main")
GOLD         = f"{CATALOG}.retail_gold"
SILVER       = f"{CATALOG}.retail_silver"
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")

assert WAREHOUSE_ID, "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

w = WorkspaceClient()


def run_query(sql: str) -> pd.DataFrame:
    """Execute a SQL query via the Statement Execution API."""
    try:
        resp = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID, statement=sql, wait_timeout="30s"
        )
        if resp.result and resp.manifest:
            cols = [c.name for c in resp.manifest.schema.columns]
            rows = resp.result.data_array or []
            df = pd.DataFrame(rows, columns=cols)
            for c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="ignore")
            return df
        return pd.DataFrame()
    except Exception as exc:
        logger.error("Query failed: %s", exc)
        return pd.DataFrame({"error": [str(exc)]})


# -- Tab: Customer 360 ------------------------------------------------------------
def customer_lookup(customer_id):
    """Look up a single customer by key."""
    if not customer_id or not str(customer_id).strip():
        return "Enter a customer ID to look up.", None
    try:
        cid = int(customer_id)
    except (ValueError, TypeError):
        return "Enter a valid integer customer ID.", None

    df = run_query(f"""
        SELECT c.customer_key, c.customer_name, c.market_segment,
               c.nation_name, c.region_name, c.balance_tier,
               r.rfm_segment, r.rfm_score,
               ROUND(r.monetary, 2) AS lifetime_value,
               r.frequency AS total_orders, r.recency_days,
               ROUND(r.avg_order_value, 2) AS avg_order_value
        FROM {SILVER}.dim_customer c
        LEFT JOIN {GOLD}.gold_customer_rfm r ON c.customer_key = r.customer_key
        WHERE c.customer_key = {cid}
    """)

    if "error" in df.columns:
        return f"Query error: {df['error'].iloc[0]}", None
    if df.empty:
        return f"No customer found with ID {cid}.", None

    r = df.iloc[0]
    summary = f"""## Customer #{r['customer_key']} -- {r['customer_name']}

| Attribute | Value |
|---|---|
| Segment | {r['market_segment']} |
| Region | {r['region_name']} ({r['nation_name']}) |
| Balance Tier | {r.get('balance_tier', 'N/A')} |
| RFM Segment | {r['rfm_segment']} |
| RFM Score | {r['rfm_score']} |
| Lifetime Value | ${float(r['lifetime_value']):,.2f} |
| Total Orders | {r['total_orders']} |
| Avg Order Value | ${float(r['avg_order_value']):,.2f} |
| Recency (days) | {r['recency_days']} |
"""
    return summary, df


# -- Tab: Revenue ------------------------------------------------------------------
def revenue_data(region, start_month, end_month):
    """Monthly revenue by region with optional filters."""
    clauses = []
    if region and region != "ALL":
        clauses.append(f"region = '{region}'")
    if start_month:
        clauses.append(f"year_month >= '{start_month}'")
    if end_month:
        clauses.append(f"year_month <= '{end_month}'")
    where = "WHERE " + " AND ".join(clauses) if clauses else ""

    df = run_query(f"""
        SELECT year_month, region,
               ROUND(SUM(net_revenue), 0) AS revenue,
               SUM(num_orders) AS orders,
               ROUND(AVG(profit_margin_pct), 1) AS margin_pct
        FROM {GOLD}.gold_monthly_sales
        {where}
        GROUP BY year_month, region ORDER BY year_month, region
    """)

    if "error" in df.columns:
        return f"Query error: {df['error'].iloc[0]}", df

    total = df["revenue"].sum()
    header = f"**Total Revenue**: ${total:,.0f}  |  **Rows**: {len(df)}"
    return header, df


# -- Tab: Products -----------------------------------------------------------------
def product_data(sort_by, top_n):
    """Top products by brand and price band."""
    return run_query(f"""
        SELECT brand, price_band,
               ROUND(SUM(net_revenue), 0) AS revenue,
               ROUND(AVG(profit_margin_pct), 1) AS margin_pct,
               ROUND(AVG(return_rate_pct), 1) AS return_rate,
               SUM(num_orders) AS orders
        FROM {GOLD}.gold_product_performance
        GROUP BY brand, price_band ORDER BY {sort_by} DESC LIMIT {int(top_n)}
    """)


# -- Tab: Churn Risk ---------------------------------------------------------------
def churn_data():
    """Churn risk tier breakdown and the 50 highest-risk customers."""
    summary_df = run_query(f"""
        SELECT risk_tier, COUNT(*) AS customers,
               ROUND(AVG(churn_probability), 3) AS avg_probability,
               ROUND(AVG(lifetime_value), 0) AS avg_ltv
        FROM {GOLD}.gold_churn_scores
        GROUP BY risk_tier
        ORDER BY CASE risk_tier
            WHEN 'Critical' THEN 1 WHEN 'High' THEN 2
            WHEN 'Medium' THEN 3 ELSE 4 END
    """)

    detail_df = run_query(f"""
        SELECT customer_key, risk_tier,
               ROUND(churn_probability, 3) AS churn_prob,
               ROUND(lifetime_value, 0) AS ltv, market_segment
        FROM {GOLD}.gold_churn_scores
        ORDER BY churn_probability DESC LIMIT 50
    """)

    return summary_df, detail_df


# -- Tab: Executive Summary --------------------------------------------------------
def exec_data():
    """Quarterly KPI roll-up with latest-quarter highlight."""
    df = run_query(f"""
        SELECT year_quarter, total_orders, active_customers,
               ROUND(gross_order_value, 0) AS gross_revenue,
               ROUND(avg_order_value, 0) AS avg_order_value,
               ROUND(revenue_per_customer, 0) AS rev_per_customer,
               qoq_revenue_growth_pct AS qoq_growth
        FROM {GOLD}.gold_executive_summary ORDER BY year_quarter
    """)

    if "error" in df.columns:
        return "Error loading data.", df

    if not df.empty:
        q = df.iloc[-1]
        header = f"""### Latest Quarter: {q['year_quarter']}

| KPI | Value |
|---|---|
| Orders | {int(q['total_orders']):,} |
| Active Customers | {int(q['active_customers']):,} |
| Gross Revenue | ${float(q['gross_revenue']):,.0f} |
| Avg Order Value | ${float(q['avg_order_value']):,.0f} |
| Rev / Customer | ${float(q['rev_per_customer']):,.0f} |
| QoQ Growth | {q['qoq_growth']}% |
"""
    else:
        header = "No data available."

    return header, df


# -- Gradio UI ---------------------------------------------------------------------
with gr.Blocks(title="Retail Analytics", theme=gr.themes.Soft()) as demo:
    gr.Markdown("# Retail Analytics Dashboard")
    gr.Markdown("Gold-layer Delta tables served through Databricks SQL")

    with gr.Tab("Customer 360"):
        gr.Markdown(
            "Look up any customer to see their profile, RFM segment, and lifetime value."
        )
        with gr.Row():
            c_id = gr.Textbox(label="Customer ID", placeholder="e.g. 42", scale=2)
            c_btn = gr.Button("Look Up", variant="primary", scale=1)
        c_md = gr.Markdown()
        c_df = gr.Dataframe(label="Profile")
        c_btn.click(customer_lookup, inputs=[c_id], outputs=[c_md, c_df])

    with gr.Tab("Revenue"):
        gr.Markdown("Monthly revenue trends. Filter by region and date range.")
        with gr.Row():
            r_reg = gr.Dropdown(
                ["ALL", "AMERICA", "EUROPE", "ASIA", "AFRICA", "MIDDLE EAST"],
                value="ALL", label="Region",
            )
            r_s = gr.Textbox(label="Start (yyyy-MM)", value="1995-01")
            r_e = gr.Textbox(label="End (yyyy-MM)", value="1997-12")
            r_btn = gr.Button("Query", variant="primary")
        r_hdr = gr.Markdown()
        r_tbl = gr.Dataframe(label="Revenue Data")
        r_btn.click(revenue_data, inputs=[r_reg, r_s, r_e], outputs=[r_hdr, r_tbl])

    with gr.Tab("Products"):
        gr.Markdown("Product performance aggregated by brand and price band.")
        with gr.Row():
            p_sort = gr.Dropdown(
                ["revenue", "margin_pct", "return_rate", "orders"],
                value="revenue", label="Sort By",
            )
            p_n = gr.Slider(5, 50, value=20, step=5, label="Top N")
            p_btn = gr.Button("Query", variant="primary")
        p_tbl = gr.Dataframe(label="Product Data")
        p_btn.click(product_data, inputs=[p_sort, p_n], outputs=[p_tbl])

    with gr.Tab("Churn Risk"):
        gr.Markdown(
            "ML-predicted churn risk tiers and the 50 highest-risk customers."
        )
        ch_btn = gr.Button("Load Churn Data", variant="primary")
        ch_summary = gr.Dataframe(label="Risk Tier Summary")
        ch_detail = gr.Dataframe(label="Top 50 At-Risk Customers")
        ch_btn.click(churn_data, outputs=[ch_summary, ch_detail])

    with gr.Tab("Executive Summary"):
        gr.Markdown("Quarterly KPI roll-up across the full business.")
        ex_btn = gr.Button("Load KPIs", variant="primary")
        ex_md = gr.Markdown()
        ex_tbl = gr.Dataframe(label="All Quarters")
        ex_btn.click(exec_data, outputs=[ex_md, ex_tbl])

demo.queue(default_concurrency_limit=100)

if __name__ == "__main__":
    logger.info("Starting Retail Analytics (catalog=%s)", CATALOG)
    demo.launch(server_name="0.0.0.0", server_port=int(os.getenv("PORT", "8000")))
