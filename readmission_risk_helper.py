# Databricks notebook source
# MAGIC %md
# MAGIC # Readmission Risk Helper - Monthly Condition Encounter Join
# MAGIC Joins encounters with conditions to identify patients with frequent readmissions.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "agentops"

# Bronze source tables
encounter_df = spark.table(f"{CATALOG}.bronze.encounter")
condition_df = spark.table(f"{CATALOG}.bronze.condition")
patient_df = spark.table(f"{CATALOG}.bronze.patient")

# Parse encounter basics
enc_parsed = encounter_df.select(
    F.col("resource_id").alias("encounter_id"),
    F.regexp_replace(F.get_json_object("raw_json", "$.subject.reference"), "Patient/", "").alias("patient_id"),
    F.get_json_object("raw_json", "$.class.code").alias("encounter_class"),
    F.get_json_object("raw_json", "$.period.start").alias("period_start"),
    F.get_json_object("raw_json", "$.status").alias("status"),
)

# Parse conditions
cond_parsed = condition_df.select(
    F.regexp_replace(F.get_json_object("raw_json", "$.subject.reference"), "Patient/", "").alias("patient_id"),
    F.get_json_object("raw_json", "$.code.coding[0].display").alias("condition_name"),
    F.get_json_object("raw_json", "$.clinicalStatus.coding[0].code").alias("clinical_status"),
)

# Join encounters with conditions
joined = enc_parsed.join(cond_parsed, "patient_id", "inner")

# Monthly readmission counts by condition
readmission_monthly = joined.groupBy(
    F.date_trunc("month", F.try_to_timestamp("period_start")).alias("month"),
    "condition_name",
    "encounter_class",
).agg(
    F.count("encounter_id").alias("encounter_count"),
    F.countDistinct("patient_id").alias("unique_patients"),
)

# This could feed agentops.gold.readmission_risk_monthly
readmission_monthly.display()
