-- RFM (recency, frequency, monetary) segmentation.
-- Uses NTILE(5) to bucket customers into quintiles, then labels segments.

CREATE OR REFRESH MATERIALIZED VIEW gold_customer_rfm
COMMENT 'Customer RFM segmentation'
CLUSTER BY (customer_region, rfm_segment)
AS
WITH customer_metrics AS (
    SELECT
        o.customer_key,
        c.market_segment,
        c.nation_name       AS customer_nation,
        c.region_name       AS customer_region,
        DATEDIFF(DATE('1998-12-31'), MAX(o.order_date)) AS recency_days,
        COUNT(DISTINCT o.order_key)  AS frequency,
        SUM(o.total_price)           AS monetary,
        MIN(o.order_date)            AS first_order_date,
        MAX(o.order_date)            AS last_order_date,
        ROUND(AVG(o.total_price), 2) AS avg_order_value,
        DATEDIFF(MAX(o.order_date), MIN(o.order_date)) AS customer_lifetime_days
    FROM retail_silver.fact_orders o
    JOIN retail_silver.dim_customer c ON o.customer_key = c.customer_key
    GROUP BY o.customer_key, c.market_segment, c.nation_name, c.region_name
),
scored AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)      AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)        AS m_score
    FROM customer_metrics
)
SELECT
    s.*,
    (s.r_score + s.f_score + s.m_score) AS rfm_score,
    CASE
        WHEN s.r_score >= 4 AND s.f_score >= 4 AND s.m_score >= 4 THEN 'Champions'
        WHEN s.r_score >= 3 AND s.f_score >= 3 AND s.m_score >= 3 THEN 'Loyal Customers'
        WHEN s.r_score >= 3 AND s.f_score >= 2 THEN 'Potential Loyalists'
        WHEN s.r_score >= 2 AND s.f_score >= 2 THEN 'Needs Attention'
        WHEN s.r_score >= 2                     THEN 'At Risk'
        ELSE 'Lost'
    END AS rfm_segment
FROM scored s;
