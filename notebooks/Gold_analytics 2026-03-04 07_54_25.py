import spark
# Databricks notebook source
spark.sql("USE lakehouse_ecommerce")

# COMMAND ----------

orders = spark.table("silver_orders")
items = spark.table("silver_order_items")
payments = spark.table("silver_payments")
products = spark.table("silver_products")
customers = spark.table("silver_customers")

gold_sales = (
    orders
    .join(items, "order_id")
    .join(payments, "order_id")
    .join(products, "product_id")
    .join(customers, "customer_id")
)

# COMMAND ----------

gold_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("lakehouse_ecommerce.gold_fact_sales")

# COMMAND ----------

spark.sql("SHOW TABLES IN lakehouse_ecommerce").display()

# COMMAND ----------

from pyspark.sql.functions import col, to_date, sum as _sum, countDistinct, avg

fact = spark.table("lakehouse_ecommerce.gold_fact_sales")

gold_daily_revenue = (
    fact
    .withColumn("order_date", to_date(col("order_purchase_timestamp")))
    .groupBy("order_date")
    .agg(
        _sum(col("price") + col("freight_value")).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        avg(col("price") + col("freight_value")).alias("avg_ticket")
    )
)

gold_daily_revenue.write.format("delta").mode("overwrite") \
    .saveAsTable("lakehouse_ecommerce.gold_daily_revenue")

# COMMAND ----------

from pyspark.sql.functions import desc

gold_top_categories = (
    fact
    .groupBy("product_category_name")
    .agg(
        _sum(col("price") + col("freight_value")).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders")
    )
    .orderBy(desc("total_revenue"))
)

gold_top_categories.write.format("delta").mode("overwrite") \
    .saveAsTable("lakehouse_ecommerce.gold_top_categories")

# COMMAND ----------

gold_customer_ltv = (
    fact
    .groupBy("customer_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        _sum(col("price") + col("freight_value")).alias("total_spent"),
        avg(col("price") + col("freight_value")).alias("avg_order_value")
    )
    .orderBy(desc("total_spent"))
)

gold_customer_ltv.write.format("delta").mode("overwrite") \
    .saveAsTable("lakehouse_ecommerce.gold_customer_ltv")

# COMMAND ----------

from pyspark.sql.functions import avg as _avg

gold_delivery_metrics = (
    fact
    .groupBy("customer_state")
    .agg(
        _avg(col("delivery_days")).alias("avg_delivery_days")
    )
    .orderBy(desc("avg_delivery_days"))
)

gold_delivery_metrics.write.format("delta").mode("overwrite") \
    .saveAsTable("lakehouse_ecommerce.gold_delivery_metrics")

# COMMAND ----------

spark.sql("SHOW TABLES IN lakehouse_ecommerce").display()

# COMMAND ----------

spark.sql("""
SELECT * FROM lakehouse_ecommerce.gold_top_categories
LIMIT 10
""").display()

# COMMAND ----------

spark.sql("""
SELECT * FROM lakehouse_ecommerce.gold_daily_revenue
ORDER BY order_date DESC
LIMIT 10
""").display()

# COMMAND ----------

spark.sql("""
SELECT * FROM lakehouse_ecommerce.gold_delivery_metrics
LIMIT 10
""").display()

# COMMAND ----------

spark.sql("SELECT COUNT(*) AS rows FROM lakehouse_ecommerce.gold_fact_sales").display()
spark.sql("SELECT * FROM lakehouse_ecommerce.gold_top_categories LIMIT 10").display()
spark.sql("SELECT * FROM lakehouse_ecommerce.gold_daily_revenue ORDER BY order_date DESC LIMIT 10").display()
spark.sql("SELECT * FROM lakehouse_ecommerce.gold_delivery_metrics LIMIT 10").display()