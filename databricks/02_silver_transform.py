# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 - Silver transformation: Sanctions Screening
# MAGIC
# MAGIC Reads Bronze Delta tables, applies basic data-quality checks and standardization,
# MAGIC removes exact duplicate business records, and writes clean Silver Delta tables.
# MAGIC
# MAGIC **Design:** Silver cleans and validates data. Fuzzy matching is handled later in Gold.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("storage_account", "stsanctionsdev001")
dbutils.widgets.text("bronze_container", "bronze")
dbutils.widgets.text("silver_container", "silver")

storage_account = dbutils.widgets.get("storage_account")
bronze_container = dbutils.widgets.get("bronze_container")
silver_container = dbutils.widgets.get("silver_container")

bronze_base = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/sanctions_screening"
silver_base = f"abfss://{silver_container}@{storage_account}.dfs.core.windows.net/sanctions_screening"

print(f"Bronze path: {bronze_base}")
print(f"Silver path: {silver_base}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Bronze Delta tables

# COMMAND ----------

customers_bronze = spark.read.format("delta").load(f"{bronze_base}/customers")
watchlist_bronze = spark.read.format("delta").load(f"{bronze_base}/watchlist")

print(f"Bronze customers: {customers_bronze.count()}")
print(f"Bronze watchlist: {watchlist_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Customer data-quality checks
# MAGIC Required business fields: customer_id, full_name, date_of_birth and country.

# COMMAND ----------

from pyspark.sql import functions as F

customer_dq = (
    customers_bronze
      .withColumn("_dq_missing_customer_id", F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == ""))
      .withColumn("_dq_missing_name", F.col("full_name").isNull() | (F.trim(F.col("full_name")) == ""))
      .withColumn("_dq_missing_dob", F.col("date_of_birth").isNull() | (F.trim(F.col("date_of_birth")) == ""))
      .withColumn("_dq_missing_country", F.col("country").isNull() | (F.trim(F.col("country")) == ""))
      .withColumn(
          "_dq_status",
          F.when(
              F.col("_dq_missing_customer_id")
              | F.col("_dq_missing_name")
              | F.col("_dq_missing_dob")
              | F.col("_dq_missing_country"),
              F.lit("REJECT")
          ).otherwise(F.lit("PASS"))
      )
)

customer_dq.groupBy("_dq_status").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Standardize customer data
# MAGIC Preserve the original Bronze columns and create normalized Silver fields.

# COMMAND ----------

from pyspark.sql import functions as F

customers_silver = (
    customer_dq
      .filter(F.col("_dq_status") == "PASS")
      .withColumn("customer_id", F.trim(F.col("customer_id")))
      .withColumn("full_name", F.trim(F.col("full_name")))
      .withColumn("full_name_normalized", F.upper(F.regexp_replace(F.trim(F.col("full_name")), r"\s+", " ")))
      .withColumn("date_of_birth", F.to_date(F.trim(F.col("date_of_birth")), "yyyy-MM-dd"))
      .withColumn("country", F.upper(F.trim(F.col("country"))))
      .withColumn("customer_type", F.upper(F.trim(F.col("customer_type"))))
)

# Keep only records with a valid parsed DOB.
customers_silver = (
    customers_silver
      .withColumn(
          "_dq_status",
          F.when(F.col("date_of_birth").isNull(), F.lit("REJECT"))
           .otherwise(F.lit("PASS"))
      )
      .filter(F.col("_dq_status") == "PASS")
      .dropDuplicates(["customer_id"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Watchlist data-quality checks and standardization

# COMMAND ----------

watchlist_dq = (
    watchlist_bronze
      .withColumn("_dq_missing_watchlist_id", F.col("watchlist_id").isNull() | (F.trim(F.col("watchlist_id")) == ""))
      .withColumn("_dq_missing_name", F.col("full_name").isNull() | (F.trim(F.col("full_name")) == ""))
      .withColumn("_dq_missing_dob", F.col("date_of_birth").isNull() | (F.trim(F.col("date_of_birth")) == ""))
      .withColumn("_dq_missing_country", F.col("country").isNull() | (F.trim(F.col("country")) == ""))
      .withColumn(
          "_dq_status",
          F.when(
              F.col("_dq_missing_watchlist_id")
              | F.col("_dq_missing_name")
              | F.col("_dq_missing_dob")
              | F.col("_dq_missing_country"),
              F.lit("REJECT")
          ).otherwise(F.lit("PASS"))
      )
)

watchlist_dq.groupBy("_dq_status").count().show()

# COMMAND ----------

watchlist_silver = (
    watchlist_dq
      .filter(F.col("_dq_status") == "PASS")
      .withColumn("watchlist_id", F.trim(F.col("watchlist_id")))
      .withColumn("full_name", F.trim(F.col("full_name")))
      .withColumn("full_name_normalized", F.upper(F.regexp_replace(F.trim(F.col("full_name")), r"\s+", " ")))
      .withColumn("date_of_birth", F.to_date(F.trim(F.col("date_of_birth")), "yyyy-MM-dd"))
      .withColumn("country", F.upper(F.trim(F.col("country"))))
      .withColumn("list_source", F.upper(F.trim(F.col("list_source"))))
      .withColumn("risk_level", F.upper(F.trim(F.col("risk_level"))))
      .withColumn(
          "_dq_status",
          F.when(F.col("date_of_birth").isNull(), F.lit("REJECT"))
           .otherwise(F.lit("PASS"))
      )
      .filter(F.col("_dq_status") == "PASS")
      .dropDuplicates(["watchlist_id"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write Silver Delta tables

# COMMAND ----------

(
    customers_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{silver_base}/customers")
)

(
    watchlist_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{silver_base}/watchlist")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validate Silver layer

# COMMAND ----------

silver_customer_count = spark.read.format("delta").load(f"{silver_base}/customers").count()
silver_watchlist_count = spark.read.format("delta").load(f"{silver_base}/watchlist").count()

print(f"Silver customer count: {silver_customer_count}")
print(f"Silver watchlist count: {silver_watchlist_count}")

display(spark.read.format("delta").load(f"{silver_base}/customers").limit(10))
display(spark.read.format("delta").load(f"{silver_base}/watchlist").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Next steps
# MAGIC - Add explicit quarantine tables for rejected records.
# MAGIC - Add stronger country/date validation rules.
# MAGIC - Add incremental processing and batch IDs.
# MAGIC - Build the sanctions matching logic in the Gold layer.