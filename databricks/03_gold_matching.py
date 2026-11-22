# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 - Gold Matching: Sanctions Screening
# MAGIC
# MAGIC Reads Silver customer and watchlist data, creates candidate pairs,
# MAGIC calculates simple name/DOB/country matching scores, assigns a screening
# MAGIC decision, and writes the Gold Delta result.
# MAGIC
# MAGIC **Design:** Gold contains the screening/matching business logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("storage_account", "stsanctionsdev001")
dbutils.widgets.text("silver_container", "silver")
dbutils.widgets.text("gold_container", "gold")

storage_account = dbutils.widgets.get("storage_account")
silver_container = dbutils.widgets.get("silver_container")
gold_container = dbutils.widgets.get("gold_container")

silver_base = f"abfss://{silver_container}@{storage_account}.dfs.core.windows.net/sanctions_screening"
gold_base = f"abfss://{gold_container}@{storage_account}.dfs.core.windows.net/sanctions_screening"

print(f"Silver path: {silver_base}")
print(f"Gold path: {gold_base}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read Silver Delta tables

# COMMAND ----------

customers_silver = spark.read.format("delta").load(f"{silver_base}/customers")
watchlist_silver = spark.read.format("delta").load(f"{silver_base}/watchlist")

print(f"Silver customers: {customers_silver.count()}")
print(f"Silver watchlist: {watchlist_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prepare matching fields
# MAGIC
# MAGIC Standardize names again for matching and create simple phonetic keys.
# MAGIC Soundex is used as a blocking key to reduce unnecessary comparisons.

# COMMAND ----------

customers_match = (
    customers_silver
      .withColumn(
          "name_clean",
          F.upper(
              F.regexp_replace(
                  F.regexp_replace(F.col("full_name"), r"[^A-Za-z0-9 ]", ""),
                  r"\s+",
                  " "
              )
          )
      )
      .withColumn("name_soundex", F.soundex(F.col("name_clean")))
      .select(
          "customer_id",
          "full_name",
          "name_clean",
          "name_soundex",
          "date_of_birth",
          "country",
          "customer_type"
      )
)

watchlist_match = (
    watchlist_silver
      .withColumn(
          "name_clean",
          F.upper(
              F.regexp_replace(
                  F.regexp_replace(F.col("full_name"), r"[^A-Za-z0-9 ]", ""),
                  r"\s+",
                  " "
              )
          )
      )
      .withColumn("name_soundex", F.soundex(F.col("name_clean")))
      .select(
          "watchlist_id",
          "full_name",
          "name_clean",
          "name_soundex",
          "date_of_birth",
          "country",
          "list_source",
          "risk_level"
      )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate candidate pairs
# MAGIC
# MAGIC Candidate pairs are created when the phonetic name key matches.
# MAGIC This is a simple blocking step before detailed scoring.

# COMMAND ----------

candidates = (
    customers_match.alias("c")
      .join(
          watchlist_match.alias("w"),
          F.col("c.name_soundex") == F.col("w.name_soundex"),
          "inner"
      )
      .select(
          F.col("c.customer_id"),
          F.col("c.full_name").alias("customer_name"),
          F.col("c.name_clean").alias("customer_name_clean"),
          F.col("c.date_of_birth").alias("customer_dob"),
          F.col("c.country").alias("customer_country"),
          F.col("c.customer_type"),
          F.col("w.watchlist_id"),
          F.col("w.full_name").alias("watchlist_name"),
          F.col("w.name_clean").alias("watchlist_name_clean"),
          F.col("w.date_of_birth").alias("watchlist_dob"),
          F.col("w.country").alias("watchlist_country"),
          F.col("w.list_source"),
          F.col("w.risk_level")
      )
)

candidate_count = candidates.count()
print(f"Candidate pairs: {candidate_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Calculate matching features
# MAGIC
# MAGIC Score:
# MAGIC - Name similarity: 60 points
# MAGIC - DOB exact match: 25 points
# MAGIC - Country exact match: 15 points
# MAGIC
# MAGIC Name similarity uses Spark's Levenshtein distance converted to a
# MAGIC percentage-style similarity score.

# COMMAND ----------

max_name_length = F.greatest(
    F.length(F.col("customer_name_clean")),
    F.length(F.col("watchlist_name_clean"))
)

name_distance = F.levenshtein(
    F.col("customer_name_clean"),
    F.col("watchlist_name_clean")
)

name_similarity = F.when(
    max_name_length == 0,
    F.lit(0.0)
).otherwise(
    F.greatest(
        F.lit(0.0),
        F.lit(1.0) - (name_distance.cast("double") / max_name_length.cast("double"))
    )
)

scored = (
    candidates
      .withColumn("name_similarity", F.round(name_similarity, 4))
      .withColumn(
          "name_score",
          F.round(F.col("name_similarity") * F.lit(60), 2)
      )
      .withColumn(
          "dob_score",
          F.when(
              F.col("customer_dob") == F.col("watchlist_dob"),
              F.lit(25.0)
          ).otherwise(F.lit(0.0))
      )
      .withColumn(
          "country_score",
          F.when(
              F.col("customer_country") == F.col("watchlist_country"),
              F.lit(15.0)
          ).otherwise(F.lit(0.0))
      )
      .withColumn(
          "match_score",
          F.round(
              F.col("name_score")
              + F.col("dob_score")
              + F.col("country_score"),
              2
          )
      )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Assign screening decision
# MAGIC
# MAGIC - MATCH: score >= 85
# MAGIC - REVIEW: score >= 60 and < 85
# MAGIC - CLEAR: score < 60

# COMMAND ----------

screened = (
    scored
      .withColumn(
          "screening_decision",
          F.when(F.col("match_score") >= 85, F.lit("MATCH"))
           .when(F.col("match_score") >= 60, F.lit("REVIEW"))
           .otherwise(F.lit("CLEAR"))
      )
      .withColumn("screening_timestamp", F.current_timestamp())
      .withColumn("screening_date", F.current_date())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Select Gold output columns

# COMMAND ----------

gold_results = screened.select(
    "customer_id",
    "customer_name",
    "customer_dob",
    "customer_country",
    "watchlist_id",
    "watchlist_name",
    "watchlist_dob",
    "watchlist_country",
    "list_source",
    "risk_level",
    "name_similarity",
    "name_score",
    "dob_score",
    "country_score",
    "match_score",
    "screening_decision",
    "screening_timestamp",
    "screening_date"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write Gold Delta table

# COMMAND ----------

(
    gold_results.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{gold_base}/screening_results")
)

print(f"Gold output written to: {gold_base}/screening_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Validate Gold results

# COMMAND ----------

gold_read = spark.read.format("delta").load(f"{gold_base}/screening_results")

print(f"Gold result count: {gold_read.count()}")

display(
    gold_read
      .groupBy("screening_decision")
      .count()
      .orderBy("screening_decision")
)

display(
    gold_read
      .orderBy(F.col("match_score").desc())
      .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Summary
# MAGIC
# MAGIC Gold screening flow:
# MAGIC 1. Read Silver data
# MAGIC 2. Create phonetic name blocking keys
# MAGIC 3. Generate candidate customer/watchlist pairs
# MAGIC 4. Calculate name, DOB and country scores
# MAGIC 5. Calculate total match score out of 100
# MAGIC 6. Assign MATCH / REVIEW / CLEAR
# MAGIC 7. Store results as Gold Delta