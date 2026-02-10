-- Monthly revenue with MoM and YoY growth, partitioned by region + segment.
-- Builds on gold_daily_sales (pipeline-internal dependency).

CREATE OR REFRESH MATERIALIZED VIEW gold_monthly_sales
COMMENT 'Monthly sales with MoM and YoY growth rates by region and segment'
CLUSTER BY (year, month)
AS
WITH monthly AS (
    SELECT
        d.year_month,
        d.year,
        d.month,
        ds.region,
        ds.segment,
        SUM(ds.num_orders)          AS num_orders,
        SUM(ds.total_quantity)      AS total_quantity,
        SUM(ds.net_revenue)         AS net_revenue,
        SUM(ds.gross_revenue)       AS gross_revenue,
        SUM(ds.total_cost)          AS total_cost,
        SUM(ds.total_profit)        AS total_profit,
        ROUND(AVG(ds.avg_discount), 4) AS avg_discount,
        ROUND(SUM(ds.total_profit) / NULLIF(SUM(ds.net_revenue), 0) * 100, 2) AS profit_margin_pct
    FROM gold_daily_sales ds
    JOIN retail_silver.dim_date d ON ds.sale_date = d.date_key
    GROUP BY d.year_month, d.year, d.month, ds.region, ds.segment
)
SELECT
    m.*,
    ROUND(
        (m.net_revenue - LAG(m.net_revenue) OVER w)
        / NULLIF(LAG(m.net_revenue) OVER w, 0) * 100,
    2) AS mom_growth_pct,
    ROUND(
        (m.net_revenue - LAG(m.net_revenue, 12) OVER w)
        / NULLIF(LAG(m.net_revenue, 12) OVER w, 0) * 100,
    2) AS yoy_growth_pct
FROM monthly m
WINDOW w AS (PARTITION BY m.region, m.segment ORDER BY m.year_month);
