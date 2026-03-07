# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS lakehouse_ecommerce")

# COMMAND ----------

def ingest_csv_to_bronze(file_name, table_name):

    path = f"/Volumes/workspace/default/olist_raw_data/2/{file_name}.csv"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"lakehouse_ecommerce.{table_name}")

    print(f"Tabela {table_name} criada com sucesso!")

# COMMAND ----------

ingest_csv_to_bronze("olist_customers_dataset", "bronze_customers")
ingest_csv_to_bronze("olist_products_dataset", "bronze_products")
ingest_csv_to_bronze("olist_order_items_dataset", "bronze_order_items")
ingest_csv_to_bronze("olist_order_payments_dataset", "bronze_payments")

# COMMAND ----------

spark.sql("SHOW TABLES IN lakehouse_ecommerce").display()