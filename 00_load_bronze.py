# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sh cd "/Workspace/Users/sweta.mohapatra@ascendion.com/Agent Delivery Framework" && python generate_fhir_bronze.py

# COMMAND ----------

CATALOG = "agentops"
BRONZE_SCHEMA = "bronze"
VOLUME = "landing"
LOCAL_DIR = "/Workspace/Users/sweta.mohapatra@ascendion.com/Agent Delivery Framework"  

RESOURCES = ["Organization", "Practitioner", "Patient", "Encounter",
             "Condition", "Observation", "Procedure", "Claim"]

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{VOLUME}")

VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME}"
print("Landing volume:", VOLUME_PATH)

# COMMAND ----------

for r in RESOURCES:
    dbutils.fs.cp(f"file:{LOCAL_DIR}/fhir_bronze/{r}.ndjson", f"{VOLUME_PATH}/{r}.ndjson")
    print("copied", r)

# COMMAND ----------

from pyspark.sql.functions import get_json_object, input_file_name, current_timestamp, col

for r in RESOURCES:
    path = f"{VOLUME_PATH}/{r}.ndjson"
    df = (
        spark.read.text(path)
        .withColumnRenamed("value", "raw_json")
        .withColumn("resource_id", get_json_object(col("raw_json"), "$.id"))
        .withColumn("resource_type", get_json_object(col("raw_json"), "$.resourceType"))
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )
    target = f"{CATALOG}.{BRONZE_SCHEMA}.{r.lower()}"
    df.write.format("delta").mode("overwrite").saveAsTable(target)
    print(f"{target}: {df.count()} rows")

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.encounter")
    .select(
        "resource_id",
        get_json_object(col("raw_json"), "$.status").alias("status"),
        get_json_object(col("raw_json"), "$.subject.reference").alias("subject_ref"),
        get_json_object(col("raw_json"), "$.period.start").alias("period_start"),
        get_json_object(col("raw_json"), "$.period.end").alias("period_end"),
    )
    .limit(10)
)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.qa")
dbutils.fs.cp(f"file:{LOCAL_DIR}/fhir_bronze/anomaly_manifest.csv", f"{VOLUME_PATH}/anomaly_manifest.csv")
(spark.read.option("header", True).csv(f"{VOLUME_PATH}/anomaly_manifest.csv")
 .write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.qa.fhir_anomaly_manifest"))
print(f"{CATALOG}.qa.fhir_anomaly_manifest loaded — reference only")