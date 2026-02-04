# 🏗️ LakehouseMart – End-to-End Databricks Medallion Architecture Pipeline

A full production-style data engineering project built on Databricks, showcasing Auto Loader, Delta Lake, Delta Live Tables (DLT), and a complete Bronze → Silver → Gold Medallion architecture. Designed for real-world e-commerce analytics at scale.

---

## 🚀 Project Overview

LakehouseMart simulates an e-commerce company that processes customer, product, and order data.  
This project demonstrates how to build a modern data platform that:

- Ingests raw JSON data from cloud storage  
- Processes it incrementally using Auto Loader  
- Cleans and conforms data using DLT  
- Models analytics-ready fact/dimension tables  
- Produces Gold-layer aggregates for BI and ML  

The entire pipeline is automated, scalable, and production-ready.

---

## 🧱 Architecture (Medallion)

![Architecture Diagram](architecture_diagram.png)

---

## 📁 Repository Structure

- notebooks
  - 01_bronze_auto_loader_ingestion.py
  - 02_dlt_silver_gold_pipeline.py
  - 03_gold_sql_exploration.sql

- configs
  - dlt_pipeline_config.json

- data
  - customers_small.json
  - products_small.json
  - orders_small.json
  - customers_large.json
  - products_large.json
  - orders_large.json

- docs
  - architecture_diagram.md


---

## 📥 Data Sources

This project includes **small (10-row)** and **large (10,000-row)** JSON datasets for:

- Customers  
- Products  
- Orders  

Use small datasets for development and large datasets for performance testing.

---

## 🔄 Pipeline Components

### **Bronze Layer – Ingestion**
- Auto Loader streams JSON files from `/raw/...`
- Schema inference + ingestion metadata
- Delta tables:
  - `bronze.customers`
  - `bronze.products`
  - `bronze.orders`

### **Silver Layer – Cleansing & Conformance**
- Type casting, deduplication, timestamp normalization  
- Data quality expectations via DLT:
  - `expect_or_drop`
  - `expect_or_fail`
- Cleaned tables:
  - `silver.customers`
  - `silver.products`
  - `silver.orders`

### **Gold Layer – Star Schema & Aggregates**
- Dimensions:
  - `dim_customer`
  - `dim_product`
- Fact:
  - `fact_orders`
- Aggregates:
  - `gold_daily_sales`
  - Additional BI views in SQL notebook

---

## 🧪 Data Quality

DLT expectations enforce:

- Non-null keys  
- Valid timestamps  
- Positive quantities  
- Deduplication  
- Referential integrity checks  

Failed rows are tracked in DLT metrics.

---

## ⚙️ DLT Pipeline Configuration

The pipeline is configured via:

/configs/dlt_pipeline_config.json


Includes:

- Target catalog: `lakehousemart`
- Storage location for DLT logs
- Continuous or triggered mode
- Notebook library reference

---

## 📊 BI & Analytics

Gold tables feed downstream analytics:

- Daily revenue trends  
- Customer lifetime value  
- Product performance  
- Country-level sales metrics  

Views are defined in:

03_gold_sql_exploration.sql


---

## 🧪 How to Run

1. Upload sample data to your cloud storage under `/raw/...`
2. Run **Notebook 01** to start Auto Loader ingestion  
3. Create a **DLT pipeline** pointing to Notebook 02  
4. Run the pipeline in **continuous** or **triggered** mode  
5. Explore Gold tables using Notebook 03 or Databricks SQL  

---

## 🎯 Skills Demonstrated

- Databricks Auto Loader  
- Delta Lake (ACID, schema evolution, time travel)  
- Delta Live Tables (DLT)  
- Medallion architecture  
- Data modeling (star schema)  
- Data quality expectations  
- Incremental processing  
- BI-ready data engineering  

---

