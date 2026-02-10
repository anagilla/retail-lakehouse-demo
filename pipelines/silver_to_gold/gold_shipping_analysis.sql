-- How each shipping mode performs across regions: delays, on-time %, returns.

CREATE OR REFRESH MATERIALIZED VIEW gold_shipping_analysis
COMMENT 'Shipping mode performance by region'
AS
SELECT
    li.ship_mode,
    o.customer_region,
    COUNT(*)                       AS total_shipments,
    SUM(li.quantity)               AS total_quantity,
    SUM(li.net_revenue)            AS net_revenue,
    ROUND(AVG(li.delivery_delay_days), 1) AS avg_delivery_delay_days,
    ROUND(AVG(li.ship_delay_days), 1)     AS avg_ship_delay_days,
    ROUND(
        SUM(CASE WHEN li.ship_delay_days <= 0 THEN 1 ELSE 0 END)
        / CAST(COUNT(*) AS DOUBLE) * 100, 2
    ) AS on_time_pct,
    ROUND(
        SUM(CASE WHEN li.return_flag = 'R' THEN 1 ELSE 0 END)
        / CAST(COUNT(*) AS DOUBLE) * 100, 2
    ) AS return_rate_pct
FROM retail_silver.fact_lineitem li
JOIN retail_silver.fact_orders o ON li.order_key = o.order_key
GROUP BY li.ship_mode, o.customer_region;
