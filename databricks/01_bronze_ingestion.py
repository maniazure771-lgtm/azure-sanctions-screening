# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 - Bronze ingestion: Sanctions Screening
# MAGIC
# MAGIC Reads synthetic CSV files from ADLS Gen2 Landing, adds ingestion metadata,
# MAGIC and writes append-only Delta tables to the Bronze layer.
# MAGIC
# MAGIC **Design:** Bronze preserves source values. Do not cleanse, deduplicate, or fuzzy-match here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters
# MAGIC Set these values to match your Azure storage account and Unity Catalog external locations.

# COMMAND ----------

dbutils.widgets.text("storage_account", "stsanctionsdev001")
dbutils.widgets.text("landing_container", "landing")
dbutils.widgets.text("bronze_container", "bronze")
dbutils.widgets.text("source_folder", "sanctions_screening")
dbutils.widgets.text("file_name", "")

storage_account = dbutils.widgets.get("storage_account")
landing_container = dbutils.widgets.get("landing_container")
bronze_container = dbutils.widgets.get("bronze_container")
source_folder = dbutils.widgets.get("source_folder")
file_name = dbutils.widgets.get("file_name")


landing_base = f"abfss://{landing_container}@{storage_account}.dfs.core.windows.net/{source_folder}"
bronze_base = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/sanctions_screening"

print(f"Landing path: {landing_base}")
print(f"Bronze path: {bronze_base}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read customer CSV
# MAGIC Use an explicit schema to avoid relying on schema inference.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("country", StringType(), True),
    StructField("customer_type", StringType(), True)
])

customers_raw = (
    spark.read
         .format("csv")
         .option("header", "true")
         .option("mode", "PERMISSIVE")
         .schema(customer_schema)
         .load(f"{landing_base}/customers.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Add Bronze ingestion metadata
# MAGIC Keep source columns as strings in Bronze. Add metadata without changing source values.

# COMMAND ----------

customers_bronze = (
    customers_raw
      .withColumn("_source_file", F.col("_metadata.file_path"))
      .withColumn("_ingestion_timestamp", F.current_timestamp())
      .withColumn("_ingestion_date", F.current_date())
      .withColumn("_source_system", F.lit("SYNTHETIC_CUSTOMER_SOURCE"))
      .withColumn("_pipeline_name", F.lit("pl_sanctions_bronze_ingestion"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Read sanctions watchlist CSV

# COMMAND ----------

watchlist_schema = StructType([
    StructField("watchlist_id", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("country", StringType(), True),
    StructField("list_source", StringType(), True),
    StructField("risk_level", StringType(), True)
])

watchlist_raw = (
    spark.read
         .format("csv")
         .option("header", "true")
         .option("mode", "PERMISSIVE")
         .schema(watchlist_schema)
         .load(f"{landing_base}/sanctions_watchlist.csv")
)

watchlist_bronze = (
    watchlist_raw
      .withColumn("_source_file", F.col("_metadata.file_path"))
      .withColumn("_ingestion_timestamp", F.current_timestamp())
      .withColumn("_ingestion_date", F.current_date())
      .withColumn("_source_system", F.lit("SYNTHETIC_WATCHLIST_SOURCE"))
      .withColumn("_pipeline_name", F.lit("pl_sanctions_bronze_ingestion"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write Bronze Delta tables
# MAGIC This starter notebook uses overwrite for the first controlled demo run.
# MAGIC For recurring production ingestion, implement an idempotent batch strategy or append
# MAGIC with a batch identifier and deduplication/reconciliation rules.

# COMMAND ----------

(
    customers_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{bronze_base}/customers")
)

(
    watchlist_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{bronze_base}/watchlist")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate ingestion

# COMMAND ----------

customer_count = spark.read.format("delta").load(f"{bronze_base}/customers").count()
watchlist_count = spark.read.format("delta").load(f"{bronze_base}/watchlist").count()

print(f"Bronze customer count: {customer_count}")
print(f"Bronze watchlist count: {watchlist_count}")

assert customer_count == 1000, f"Expected 1000 customers; found {customer_count}"
assert watchlist_count == 100, f"Expected 100 watchlist records; found {watchlist_count}"

display(spark.read.format("delta").load(f"{bronze_base}/customers").limit(10))
display(spark.read.format("delta").load(f"{bronze_base}/watchlist").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Next steps
# MAGIC - Move paths into Unity Catalog external locations/volumes where appropriate.
# MAGIC - Add batch IDs, file manifests, and idempotent incremental ingestion.
# MAGIC - Add malformed-record quarantine and quality metrics in the Silver layer.