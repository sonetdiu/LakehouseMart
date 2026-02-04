USE CATALOG lakehouse;
USE SCHEMA gold;

-- Basic exploration
SELECT * FROM fact_orders LIMIT 100;

-- Revenue by country
SELECT
  country,
  SUM(quantity * unit_price) AS total_revenue
FROM fact_orders
GROUP BY country
ORDER BY total_revenue DESC;

-- Daily revenue by country (from aggregate table)
SELECT * FROM agg_revenue_by_country ORDER BY order_date DESC, country;
