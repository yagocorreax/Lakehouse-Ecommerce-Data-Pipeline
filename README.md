# Lakehouse-Ecommerce-Data-Pipeline
> Overview

This project implements a modern __Lakehouse Data Architecture__ using __Databricks, Apache Spark, and Delta Lake__ to process and analyze an e-commerce dataset.

The pipeline follows the __Medallion Architecture (Bronze → Silver → Gold)__, transforming raw CSV files into structured analytical datasets ready for business intelligence and reporting.

The goal of this project is to simulate a real-world __Data Engineering pipeline__, covering ingestion, data cleaning, transformations, and analytics-ready tables.

---
## Architecture

The pipeline follows the __Medallion Architecture__ pattern:
```
Raw CSV Files
      │
      ▼
Bronze Layer (Raw Delta Tables)
      │
      ▼
Silver Layer (Cleaned & Structured Data)
      │
      ▼
Gold Layer (Analytics & Business KPIs)
```

---
### Bronze Layer

Stores raw ingested data with minimal transformations.

Tables created:

- bronze_orders
- bronze_order_items
- bronze_products
- bronze_payments

### Silver Layer

Applies data cleaning and normalization.

Transformations performed:

- Schema enforcement
- Duplicate removal
- Null value filtering
- Feature engineering (delivery time calculation)
- Data integrity validation

__Tables created:__

- silver_orders
- silver_order_items
- silver_products
- silver_payments
- silver_customers

### Gold Layer

Creates analytics-ready datasets used for reporting and insights.

__Tables created:__

- gold_fact_sales
- gold_daily_revenue
- gold_top_categories
- gold_customer_ltv
- gold_delivery_metric

