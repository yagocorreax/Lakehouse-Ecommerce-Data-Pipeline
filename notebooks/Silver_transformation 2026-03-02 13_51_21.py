# Databricks notebook source
spark.sql("USE lakehouse_ecommerce")

# COMMAND ----------

bronze_orders = spark.table("lakehouse_ecommerce.bronze_orders")

display(bronze_orders)

# COMMAND ----------

bronze_orders.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

silver_orders = (
    bronze_orders
    .filter(col("order_status").isNotNull())
)

# COMMAND ----------

from pyspark.sql.functions import datediff

silver_orders = silver_orders.withColumn(
    "delivery_days",
    datediff(
        col("order_delivered_customer_date"),
        col("order_purchase_timestamp")
    )
)

# COMMAND ----------

silver_orders = silver_orders.dropDuplicates(["order_id"])

# COMMAND ----------

silver_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("lakehouse_ecommerce.silver_orders")

# COMMAND ----------

spark.sql("SHOW TABLES IN lakehouse_ecommerce").display()

# COMMAND ----------

from pyspark.sql.functions import col

bronze_order_items = spark.table("lakehouse_ecommerce.bronze_order_items")

silver_order_items = (
    bronze_order_items
    # regras básicas de integridade
    .dropna(subset=["order_id", "product_id"])
    # tipos corretos (algumas colunas podem vir como string dependendo do inferSchema)
    .withColumn("order_item_id", col("order_item_id").cast("int"))
    .withColumn("price", col("price").cast("double"))
    .withColumn("freight_value", col("freight_value").cast("double"))
    # evitar valores negativos (dados ruins)
    .filter(col("price").isNotNull() & (col("price") >= 0))
    .filter(col("freight_value").isNotNull() & (col("freight_value") >= 0))
    # dedup por chave natural do dataset
    .dropDuplicates(["order_id", "order_item_id"])
)

silver_order_items.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("lakehouse_ecommerce.silver_order_items")

# COMMAND ----------

spark.sql("SELECT COUNT(*) AS rows FROM lakehouse_ecommerce.silver_order_items").display()
spark.sql("SHOW TABLES IN lakehouse_ecommerce").display()

# COMMAND ----------

from pyspark.sql.functions import col

bronze_payments = spark.table("lakehouse_ecommerce.bronze_payments")

silver_payments = (
    bronze_payments
    .dropna(subset=["order_id"])
    .withColumn("payment_sequential", col("payment_sequential").cast("int"))
    .withColumn("payment_installments", col("payment_installments").cast("int"))
    .withColumn("payment_value", col("payment_value").cast("double"))
    .filter(col("payment_value").isNotNull() & (col("payment_value") >= 0))
    # dedup por chave natural
    .dropDuplicates(["order_id", "payment_sequential"])
)

silver_payments.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("lakehouse_ecommerce.silver_payments")

# COMMAND ----------

spark.sql("SELECT COUNT(*) AS rows FROM lakehouse_ecommerce.silver_payments").display()
spark.sql("SHOW TABLES IN lakehouse_ecommerce").display()

# COMMAND ----------

from pyspark.sql.functions import col

bronze_products = spark.table("lakehouse_ecommerce.bronze_products")

silver_products = (
    bronze_products
    .dropna(subset=["product_id"])
    .withColumn("product_name_lenght", col("product_name_lenght").cast("int"))
    .withColumn("product_description_lenght", col("product_description_lenght").cast("int"))
    .withColumn("product_photos_qty", col("product_photos_qty").cast("int"))
    .withColumn("product_weight_g", col("product_weight_g").cast("double"))
    .withColumn("product_length_cm", col("product_length_cm").cast("double"))
    .withColumn("product_height_cm", col("product_height_cm").cast("double"))
    .withColumn("product_width_cm", col("product_width_cm").cast("double"))
    .dropDuplicates(["product_id"])
)

silver_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("lakehouse_ecommerce.silver_products")

# COMMAND ----------

spark.sql("SHOW TABLES IN lakehouse_ecommerce").display()

# COMMAND ----------

from pyspark.sql.functions import col

bronze_customers = spark.table("lakehouse_ecommerce.bronze_customers")

silver_customers = (
    bronze_customers
    .dropna(subset=["customer_id"])
    .dropDuplicates(["customer_id"])
)

silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("lakehouse_ecommerce.silver_customers")

# COMMAND ----------

spark.sql("SHOW TABLES IN lakehouse_ecommerce").display()