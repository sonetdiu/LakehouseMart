-- Databricks notebook source
-- 03_gold_sql_exploration.sql

USE CATALOG lakehousemart;
USE SCHEMA gold;

-- Quick sanity checks
SELECT * FROM fact_orders LIMIT 20;
SELECT * FROM gold_daily_sales ORDER BY order_date DESC LIMIT 20;

-- Example: Top products by revenue
CREATE OR REPLACE VIEW v_top_products_by_revenue AS
SELECT
  p.product_key,
  p.product_name,
  p.category,
  SUM(f.order_amount) AS total_revenue,
  SUM(f.quantity) AS total_quantity
FROM fact_orders f
LEFT JOIN dim_product p
  ON f.product_key = p.product_key
GROUP BY p.product_key, p.product_name, p.category
ORDER BY total_revenue DESC;

-- Example: Customer lifetime value
CREATE OR REPLACE VIEW v_customer_lifetime_value AS
SELECT
  c.customer_key,
  c.first_name,
  c.last_name,
  c.country,
  SUM(f.order_amount) AS lifetime_value,
  COUNT(DISTINCT f.order_id) AS order_count
FROM fact_orders f
LEFT JOIN dim_customer c
  ON f.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name, c.country
ORDER BY lifetime_value DESC;
