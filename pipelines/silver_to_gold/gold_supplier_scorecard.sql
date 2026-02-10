-- Per-supplier delivery, quality, and profitability metrics.

CREATE OR REFRESH MATERIALIZED VIEW gold_supplier_scorecard
COMMENT 'Supplier scorecard'
CLUSTER BY (supplier_region)
AS
SELECT
    li.supplier_key,
    li.supplier_name,
    li.supplier_nation,
    li.supplier_region,
    COUNT(*)                       AS total_line_items,
    COUNT(DISTINCT li.order_key)   AS num_orders,
    SUM(li.quantity)               AS total_quantity,
    SUM(li.net_revenue)            AS net_revenue,
    SUM(li.profit)                 AS total_profit,
    ROUND(AVG(li.delivery_delay_days), 1) AS avg_delivery_delay_days,
    ROUND(AVG(li.ship_delay_days), 1)     AS avg_ship_delay_days,
    ROUND(
        SUM(CASE WHEN li.ship_delay_days <= 0 THEN 1 ELSE 0 END)
        / CAST(COUNT(*) AS DOUBLE) * 100, 2
    ) AS on_time_delivery_pct,
    ROUND(
        SUM(CASE WHEN li.return_flag = 'R' THEN 1 ELSE 0 END)
        / CAST(COUNT(*) AS DOUBLE) * 100, 2
    ) AS return_rate_pct,
    ROUND(SUM(li.profit) / NULLIF(SUM(li.net_revenue), 0) * 100, 2) AS profit_margin_pct
FROM retail_silver.fact_lineitem li
GROUP BY li.supplier_key, li.supplier_name, li.supplier_nation, li.supplier_region;
