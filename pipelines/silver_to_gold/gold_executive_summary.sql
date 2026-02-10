-- Quarterly KPIs for leadership review: orders, revenue, customer counts.

CREATE OR REFRESH MATERIALIZED VIEW gold_executive_summary
COMMENT 'Quarterly executive KPI summary'
AS
WITH quarterly AS (
    SELECT
        CONCAT(o.order_year, '-Q', o.order_quarter) AS year_quarter,
        o.order_year,
        o.order_quarter,
        COUNT(DISTINCT o.order_key)    AS total_orders,
        COUNT(DISTINCT o.customer_key) AS active_customers,
        SUM(o.total_price)             AS gross_order_value,
        ROUND(AVG(o.total_price), 2)   AS avg_order_value
    FROM retail_silver.fact_orders o
    GROUP BY o.order_year, o.order_quarter
)
SELECT
    q.*,
    ROUND(
        (q.gross_order_value - LAG(q.gross_order_value) OVER w)
        / NULLIF(LAG(q.gross_order_value) OVER w, 0) * 100,
    2) AS qoq_revenue_growth_pct,
    ROUND(
        (q.active_customers - LAG(q.active_customers) OVER w)
        / NULLIF(CAST(LAG(q.active_customers) OVER w AS DOUBLE), 0) * 100,
    2) AS qoq_customer_growth_pct,
    ROUND(q.gross_order_value / NULLIF(q.active_customers, 0), 2) AS revenue_per_customer
FROM quarterly q
WINDOW w AS (ORDER BY q.order_year, q.order_quarter);
