-- ============================================================================
-- Gold Layer Analytics — Retail Demo
-- ============================================================================
-- Run in the SQL Editor, against a SQL Warehouse, or via %sql in a notebook.
-- All queries target the retail_gold schema.
-- ============================================================================

USE CATALOG ${catalog};  -- parameterize: e.g. SET catalog = 'main';
USE SCHEMA retail_gold;

-- ============================================================================
-- Q1: Revenue Trend — Monthly revenue with YoY comparison
-- CFO monthly review
-- ============================================================================
SELECT
    year_month,
    region,
    net_revenue,
    yoy_growth_pct,
    mom_growth_pct,
    profit_margin_pct,
    num_orders,
    -- Running total within region
    SUM(net_revenue) OVER (
        PARTITION BY region ORDER BY year_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue
FROM gold_monthly_sales
WHERE segment = 'AUTOMOBILE'
ORDER BY region, year_month;

-- ============================================================================
-- Q2: Top 20 Customers by Lifetime Value
-- VIP program / account management
-- ============================================================================
SELECT
    customer_key,
    rfm_segment,
    market_segment,
    customer_nation,
    customer_region,
    monetary              AS lifetime_value,
    frequency             AS total_orders,
    avg_order_value,
    recency_days,
    customer_lifetime_days,
    r_score, f_score, m_score, rfm_score
FROM gold_customer_rfm
ORDER BY monetary DESC
LIMIT 20;

-- ============================================================================
-- Q3: Customer Segment Distribution
-- Marketing / retention planning
-- ============================================================================
SELECT
    rfm_segment,
    COUNT(*)                                           AS num_customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total,
    ROUND(AVG(monetary), 2)                            AS avg_lifetime_value,
    ROUND(AVG(frequency), 1)                           AS avg_orders,
    ROUND(AVG(recency_days), 0)                        AS avg_recency_days,
    ROUND(SUM(monetary), 2)                            AS total_revenue
FROM gold_customer_rfm
GROUP BY rfm_segment
ORDER BY total_revenue DESC;

-- ============================================================================
-- Q4: Regional Revenue — Quarterly by region (pivot)
-- Regional P&L / expansion planning
-- ============================================================================
WITH quarterly AS (
    SELECT
        CONCAT(YEAR(sale_date), '-Q', QUARTER(sale_date)) AS year_quarter,
        region,
        SUM(net_revenue) AS net_revenue
    FROM gold_daily_sales
    GROUP BY 1, 2
)
SELECT *
FROM quarterly
PIVOT (
    ROUND(SUM(net_revenue), 0) AS revenue
    FOR region IN ('AMERICA', 'EUROPE', 'ASIA', 'AFRICA', 'MIDDLE EAST')
)
ORDER BY year_quarter;

-- ============================================================================
-- Q5: Top 15 Product Segments by Profit Margin
-- Uses NTILE-based volume filter so thresholds adapt to any SF
-- ============================================================================
WITH ranked AS (
    SELECT
        brand,
        manufacturer,
        price_band,
        net_revenue,
        total_profit,
        profit_margin_pct,
        total_quantity_sold,
        num_orders,
        return_rate_pct,
        avg_discount,
        avg_revenue_per_order,
        NTILE(4) OVER (ORDER BY num_orders) AS volume_quartile
    FROM gold_product_performance
)
SELECT *
FROM ranked
WHERE volume_quartile >= 3   -- top half by order volume (adapts to any SF)
ORDER BY profit_margin_pct DESC
LIMIT 15;

-- ============================================================================
-- Q6: Worst Performing Products — high return rate or low margin
-- Percentile-based cutoffs so it adapts to any SF
-- ============================================================================
WITH stats AS (
    SELECT
        PERCENTILE(return_rate_pct, 0.75) AS p75_return,
        PERCENTILE(profit_margin_pct, 0.25) AS p25_margin
    FROM gold_product_performance
    WHERE num_orders >= 10
)
SELECT
    p.brand,
    p.part_type,
    p.price_band,
    p.return_rate_pct,
    p.profit_margin_pct,
    p.net_revenue,
    p.num_orders
FROM gold_product_performance p
CROSS JOIN stats s
WHERE p.num_orders >= 10
  AND (p.return_rate_pct > s.p75_return OR p.profit_margin_pct < s.p25_margin)
ORDER BY p.return_rate_pct DESC, p.profit_margin_pct ASC
LIMIT 20;

-- ============================================================================
-- Q7: Supplier Reliability Ranking
-- Procurement / supplier tiering
-- ============================================================================
SELECT
    supplier_name,
    supplier_nation,
    supplier_region,
    total_line_items,
    net_revenue,
    total_profit,
    profit_margin_pct,
    on_time_delivery_pct,
    avg_delivery_delay_days,
    return_rate_pct,
    -- Composite score: higher is better
    ROUND(
        on_time_delivery_pct * 0.4
        + (100 - return_rate_pct) * 0.3
        + profit_margin_pct * 0.3,
        2
    ) AS supplier_score
FROM gold_supplier_scorecard
WHERE total_line_items >= 100
ORDER BY supplier_score DESC
LIMIT 25;

-- ============================================================================
-- Q8: Shipping Mode Comparison
-- Logistics / carrier negotiations
-- ============================================================================
SELECT
    ship_mode,
    SUM(total_shipments)                                       AS total_shipments,
    ROUND(SUM(net_revenue), 2)                                 AS total_revenue,
    ROUND(AVG(avg_delivery_delay_days), 1)                     AS avg_delay_days,
    ROUND(SUM(total_shipments * on_time_pct) / SUM(total_shipments), 2) AS weighted_on_time_pct,
    ROUND(SUM(total_shipments * return_rate_pct) / SUM(total_shipments), 2) AS weighted_return_pct
FROM gold_shipping_analysis
GROUP BY ship_mode
ORDER BY total_revenue DESC;

-- ============================================================================
-- Q9: Executive Dashboard — QoQ trend
-- Board / investor reporting
-- ============================================================================
SELECT
    year_quarter,
    total_orders,
    active_customers,
    gross_order_value,
    avg_order_value,
    revenue_per_customer,
    qoq_revenue_growth_pct,
    qoq_customer_growth_pct
FROM gold_executive_summary
ORDER BY year_quarter;

-- ============================================================================
-- Q10: Seasonality — best and worst months across all years
-- Inventory planning / promo calendar
-- ============================================================================
WITH monthly_totals AS (
    SELECT
        month,
        year,
        year_month,
        SUM(net_revenue)  AS net_revenue,
        SUM(num_orders)   AS num_orders
    FROM gold_monthly_sales
    GROUP BY month, year, year_month
),
month_avg AS (
    SELECT
        month,
        ROUND(AVG(net_revenue), 2)  AS avg_monthly_revenue,
        ROUND(AVG(num_orders), 0)   AS avg_monthly_orders,
        MIN(net_revenue)            AS min_revenue,
        MAX(net_revenue)            AS max_revenue
    FROM monthly_totals
    GROUP BY month
)
SELECT
    month,
    avg_monthly_revenue,
    avg_monthly_orders,
    min_revenue,
    max_revenue,
    ROUND((max_revenue - min_revenue) / avg_monthly_revenue * 100, 2) AS volatility_pct,
    RANK() OVER (ORDER BY avg_monthly_revenue DESC) AS revenue_rank
FROM month_avg
ORDER BY month;

-- ============================================================================
-- Q11: Customer Acquisition Cohort — orders by first-order quarter
-- Cohort retention analysis
-- ============================================================================
WITH cohorts AS (
    SELECT
        customer_key,
        CONCAT(YEAR(first_order_date), '-Q', QUARTER(first_order_date)) AS cohort_quarter,
        frequency,
        monetary
    FROM gold_customer_rfm
)
SELECT
    cohort_quarter,
    COUNT(*)                         AS cohort_size,
    ROUND(AVG(frequency), 1)         AS avg_orders,
    ROUND(AVG(monetary), 2)          AS avg_ltv,
    ROUND(PERCENTILE(monetary, 0.5), 2) AS median_ltv,
    ROUND(PERCENTILE(monetary, 0.9), 2) AS p90_ltv
FROM cohorts
GROUP BY cohort_quarter
ORDER BY cohort_quarter;

-- ============================================================================
-- Q12: Revenue Concentration — segment × region
-- Store format planning / regional assortment
-- ============================================================================
SELECT
    segment,
    region,
    SUM(net_revenue)                                                     AS net_revenue,
    SUM(num_orders)                                                      AS num_orders,
    ROUND(SUM(net_revenue) * 100.0 / SUM(SUM(net_revenue)) OVER (), 2)  AS pct_of_total_revenue,
    ROUND(SUM(total_profit), 2)                                          AS total_profit,
    ROUND(SUM(total_profit) / SUM(net_revenue) * 100, 2)                 AS margin_pct
FROM gold_daily_sales
GROUP BY segment, region
ORDER BY net_revenue DESC;
