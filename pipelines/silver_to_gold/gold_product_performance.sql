-- Revenue, margin, and return rate per brand/type.

CREATE OR REFRESH MATERIALIZED VIEW gold_product_performance
COMMENT 'Product performance by brand and type'
CLUSTER BY (brand)
AS
SELECT
    li.brand,
    li.part_type,
    li.price_band,
    li.manufacturer,
    SUM(li.quantity)               AS total_quantity_sold,
    SUM(li.net_revenue)            AS net_revenue,
    SUM(li.profit)                 AS total_profit,
    SUM(li.supply_cost)            AS total_cost,
    COUNT(DISTINCT li.order_key)   AS num_orders,
    ROUND(AVG(li.discount), 4)     AS avg_discount,
    ROUND(
        SUM(CASE WHEN li.return_flag = 'R' THEN 1 ELSE 0 END)
        / CAST(COUNT(*) AS DOUBLE) * 100, 2
    ) AS return_rate_pct,
    ROUND(SUM(li.profit) / NULLIF(SUM(li.net_revenue), 0) * 100, 2) AS profit_margin_pct,
    ROUND(SUM(li.net_revenue) / NULLIF(COUNT(DISTINCT li.order_key), 0), 2) AS avg_revenue_per_order
FROM retail_silver.fact_lineitem li
GROUP BY li.brand, li.part_type, li.price_band, li.manufacturer;
