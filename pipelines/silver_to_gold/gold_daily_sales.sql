-- Daily sales rolled up by region and market segment.
-- Feeds into gold_monthly_sales downstream.

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_sales
COMMENT 'Daily sales aggregation by region and market segment'
CLUSTER BY (sale_date)
AS
SELECT
    o.order_date                    AS sale_date,
    o.customer_region               AS region,
    o.market_segment                AS segment,
    COUNT(DISTINCT o.order_key)     AS num_orders,
    SUM(li.quantity)                AS total_quantity,
    SUM(li.net_revenue)             AS net_revenue,
    SUM(li.gross_revenue)           AS gross_revenue,
    SUM(li.supply_cost)             AS total_cost,
    SUM(li.profit)                  AS total_profit,
    ROUND(AVG(li.discount), 4)      AS avg_discount,
    COUNT(DISTINCT li.part_key)     AS distinct_products,
    ROUND(SUM(li.profit) / NULLIF(SUM(li.net_revenue), 0) * 100, 2) AS profit_margin_pct
FROM retail_silver.fact_lineitem li
JOIN retail_silver.fact_orders o ON li.order_key = o.order_key
GROUP BY o.order_date, o.customer_region, o.market_segment;
