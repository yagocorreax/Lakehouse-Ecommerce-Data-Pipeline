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


