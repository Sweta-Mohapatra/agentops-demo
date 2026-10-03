# Databricks notebook source
# FUNCTIONAL TESTS — DBXCOE-29: data rules on the sandbox output + reconciliation against the independent reference SQL
# (approved with plan 36033837cdfe8efb). Runs on the real bronze data.

# COMMAND ----------

import json, re
from pyspark.sql import functions as F, types as T
dbutils.widgets.text("sandbox_catalog", "agentops")
dbutils.widgets.text("sandbox_schema", "sandbox_dbxcoe_29")
SB = dbutils.widgets.get("sandbox_catalog") + "." + dbutils.widgets.get("sandbox_schema")
CONTRACT = json.loads("{\"entry_point\": \"build(spark, sources: dict) -> dict[str, DataFrame]\", \"sources\": {\"encounter\": \"agentops.bronze.encounter\", \"claim\": \"agentops.bronze.claim\", \"patient\": \"agentops.bronze.patient\", \"organization\": \"agentops.bronze.organization\", \"condition\": \"agentops.bronze.condition\"}, \"targets\": {\"ed_utilization_monthly\": {\"table\": \"agentops.gold.ed_utilization_monthly\", \"grain\": [\"facility_id\", \"facility_name\", \"month_start\"], \"columns\": [\"facility_id\", \"facility_name\", \"month_start\", \"top_condition\"], \"metrics\": [\"ed_visit_count\", \"unique_patients\", \"avg_claim_amount\", \"total_billed\", \"admits_from_ed\", \"avg_conditions_per_visit\", \"top_condition_count\"]}}}")
SOURCES = json.loads("{\"encounter\": \"agentops.bronze.encounter\", \"claim\": \"agentops.bronze.claim\", \"patient\": \"agentops.bronze.patient\", \"organization\": \"agentops.bronze.organization\", \"condition\": \"agentops.bronze.condition\"}")
RECON = json.loads("{\"ed_utilization_monthly\": \"WITH ed_encounters AS (\\n  SELECT\\n    get_json_object(e.raw_json, '$.id') as encounter_id,\\n    get_json_object(e.raw_json, '$.class.code') as encounter_class,\\n    try_to_timestamp(get_json_object(e.raw_json, '$.period.start')) as encounter_start,\\n    try_to_timestamp(get_json_object(e.raw_json, '$.period.end')) as encounter_end,\\n    regexp_replace(get_json_object(e.raw_json, '$.subject.reference'), '^Patient/', '') as patient_id,\\n    regexp_replace(get_json_object(e.raw_json, '$.serviceProvider.reference'), '^Organization/', '') as org_id\\n  FROM {src:encounter} e\\n  WHERE get_json_object(e.raw_json, '$.class.code') = 'EMER'\\n),\\nclaims_with_encounters AS (\\n  SELECT\\n    regexp_replace(get_json_object(c.raw_json, '$.item[0].encounter[0].reference'), '^Encounter/', '') as encounter_id,\\n    get_json_object(c.raw_json, '$.total.value') as claim_amount,\\n    regexp_replace(get_json_object(c.raw_json, '$.patient.reference'), '^Patient/', '') as patient_id\\n  FROM {src:claim} c\\n  WHERE get_json_object(c.raw_json, '$.item[0].encounter[0].reference') IS NOT NULL\\n),\\nactive_patients AS (\\n  SELECT resource_id\\n  FROM {src:patient}\\n  WHERE get_json_object(raw_json, '$.active') = true\\n),\\ned_with_claims AS (\\n  SELECT\\n    e.encounter_id,\\n    e.encounter_class,\\n    e.encounter_start,\\n    e.encounter_end,\\n    e.patient_id,\\n    e.org_id,\\n    c.claim_amount\\n  FROM ed_encounters e\\n  INNER JOIN claims_with_encounters c ON e.encounter_id = c.encounter_id\\n  INNER JOIN active_patients p ON e.patient_id = p.resource_id\\n),\\nencounter_conditions AS (\\n  SELECT\\n    regexp_replace(get_json_object(cond.raw_json, '$.encounter.reference'), '^Encounter/', '') as encounter_id,\\n    get_json_object(cond.raw_json, '$.code.coding[0].code') as condition_code\\n  FROM {src:condition} cond\\n  WHERE get_json_object(cond.raw_json, '$.encounter.reference') IS NOT NULL\\n),\\nencounter_condition_count AS (\\n  SELECT\\n    encounter_id,\\n    COUNT(*) as condition_count\\n  FROM encounter_conditions\\n  GROUP BY encounter_id\\n),\\ntop_condition_per_facility_month AS (\\n  SELECT\\n    e.org_id,\\n    date_trunc('MONTH', e.encounter_start) as month_start,\\n    ec.condition_code,\\n    COUNT(*) as cond_count,\\n    ROW_NUMBER() OVER (PARTITION BY e.org_id, date_trunc('MONTH', e.encounter_start) ORDER BY COUNT(*) DESC) as cond_rank\\n  FROM ed_with_claims e\\n  LEFT JOIN encounter_conditions ec ON e.encounter_id = ec.encounter_id\\n  GROUP BY e.org_id, date_trunc('MONTH', e.encounter_start), ec.condition_code\\n),\\ninpatient_admits AS (\\n  SELECT\\n    e.patient_id,\\n    e.org_id,\\n    e.encounter_id,\\n    e.encounter_start\\n  FROM ed_with_claims e\\n  INNER JOIN ed_encounters imp ON e.patient_id = imp.patient_id\\n    AND imp.org_id = e.org_id\\n    AND imp.encounter_class = 'IMP'\\n    AND imp.encounter_start > e.encounter_start\\n    AND imp.encounter_start <= e.encounter_end + INTERVAL 24 HOUR\\n),\\norg_names AS (\\n  SELECT\\n    resource_id,\\n    get_json_object(raw_json, '$.name') as org_name\\n  FROM {src:organization}\\n)\\nSELECT\\n  e.org_id as facility_id,\\n  o.org_name as facility_name,\\n  date_trunc('MONTH', e.encounter_start) as month_start,\\n  COALESCE(tc.condition_code, 'Unknown') as top_condition,\\n  COUNT(DISTINCT e.encounter_id) as ed_visit_count,\\n  COUNT(DISTINCT e.patient_id) as unique_patients,\\n  ROUND(AVG(e.claim_amount), 2) as avg_claim_amount,\\n  ROUND(SUM(e.claim_amount), 2) as total_billed,\\n  COUNT(DISTINCT CASE WHEN ia.encounter_id IS NOT NULL THEN ia.encounter_id END) as admits_from_ed,\\n  ROUND(AVG(COALESCE(ecc.condition_count, 0)), 2) as avg_conditions_per_visit,\\n  tc.cond_count as top_condition_count\\nFROM ed_with_claims e\\nLEFT JOIN encounter_condition_count ecc ON e.encounter_id = ecc.encounter_id\\nLEFT JOIN inpatient_admits ia ON e.encounter_id = ia.encounter_id\\nLEFT JOIN top_condition_per_facility_month tc ON e.org_id = tc.org_id\\n  AND date_trunc('MONTH', e.encounter_start) = tc.month_start\\n  AND tc.cond_rank = 1\\nLEFT JOIN org_names o ON e.org_id = o.resource_id\\nGROUP BY e.org_id, o.org_name, date_trunc('MONTH', e.encounter_start), tc.condition_code, tc.cond_count\"}")
EXISTING = json.loads("[\"agentops.gold.ed_utilization_monthly\"]")
COUNTLIKE = ("count", "total", "amount", "num", "sum", "avg", "days", "per_", "rate")
results = []
def add(name, checks, ok, detail, kind):
    results.append({"name": name, "checks": checks, "passed": bool(ok), "detail": str(detail)[:700], "kind": kind})
def render(sql):
    for _s, _full in SOURCES.items():
        sql = sql.replace("{src:" + _s + "}", _full)
    return sql
def keycol(df, c):
    dt = df.schema[c].dataType
    col = F.date_format(F.col(c), "yyyy-MM-dd") if isinstance(dt, (T.DateType, T.TimestampType)) else F.col(c).cast("string")
    return col.alias(c)

# COMMAND ----------

for tgt, spec in CONTRACT["targets"].items():
    tbl = f"{SB}.{tgt}"
    if not spark.catalog.tableExists(tbl):
        add(f"{tgt}: table_written", "the implementation wrote the table", False, f"{tbl} not found", "rule")
        continue
    df = spark.table(tbl)
    df = df.toDF(*[c.lower() for c in df.columns])
    n = df.count()
    add(f"{tgt}: has_rows", "output is not empty", n > 0, f"{n:,} rows", "rule")
    keys = [k for k in spec["grain"] if k in df.columns]
    miss_keys = [k for k in spec["grain"] if k not in df.columns]
    if not keys:
        keys = [c for c in df.columns if c not in spec["metrics"]]
        miss_keys = []
    if spec["grain"]:
        add(f"{tgt}: grain_columns", "grain columns exist", not miss_keys, f"missing {miss_keys}" if miss_keys else "ok", "rule")
    if keys and not miss_keys:
        d = df.groupBy(*keys).count().where("count > 1").count()
        add(f"{tgt}: grain_unique", "one row per " + ", ".join(keys), d == 0, f"{d} duplicate key(s)", "rule")
        nk = df.where(" OR ".join(f"`{k}` IS NULL" for k in keys)).count()
        add(f"{tgt}: keys_not_null", "grain columns are never null", nk == 0, f"{nk} row(s) with a null key", "rule")
    for m in spec["metrics"]:
        if m not in df.columns:
            add(f"{tgt}: {m}_present", f"metric {m} exists", False, "missing", "rule")
        elif any(w in m for w in COUNTLIKE):
            neg = df.where(F.col(m) < 0).count()
            add(f"{tgt}: {m}_non_negative", f"{m} is never negative", neg == 0, f"{neg} negative value(s)", "rule")
    if spec["table"] in EXISTING:
        old = {f.name.lower(): f.dataType.simpleString() for f in spark.table(spec["table"]).schema.fields}
        new = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        dropped = [c for c in old if c not in new]
        changed = [f"{c}: {old[c]} -> {new[c]}" for c in old if c in new and old[c] != new[c]]
        add(f"{tgt}: backward_compatible_schema", "no existing column is dropped or retyped", not dropped and not changed,
            f"dropped={dropped} retyped={changed}", "rule")
    if tgt in RECON and keys and not miss_keys and spec["metrics"]:
        try:
            ref = spark.sql(render(RECON[tgt]))
            ref = ref.toDF(*[c.lower() for c in ref.columns])
            mets = [m for m in spec["metrics"] if m in df.columns and m in ref.columns]
            o = df.select(*[keycol(df, k) for k in keys], *[F.col(m).cast("double").alias("o_" + m) for m in mets]).withColumn("_o", F.lit(1))
            r = ref.select(*[keycol(ref, k) for k in keys], *[F.col(m).cast("double").alias("r_" + m) for m in mets]).withColumn("_r", F.lit(1))
            j = o.join(r, keys, "full_outer")
            only_o, only_r = j.where(F.col("_r").isNull()).count(), j.where(F.col("_o").isNull()).count()
            add(f"{tgt}: reconcile_keys", "same grain keys as the independent reference query", only_o == 0 and only_r == 0,
                f"{only_o} key(s) only in output, {only_r} only in reference", "reconciliation")
            both = j.where(F.col("_o").isNotNull() & F.col("_r").isNotNull())
            for m in mets:
                oc, rc = F.col("o_" + m), F.col("r_" + m)
                bad = both.where((oc.isNull() != rc.isNull()) | (F.abs(oc - rc) > F.greatest(F.lit(0.01), F.abs(rc) * F.lit(1e-6))))
                nb = bad.count()
                sample = [x.asDict() for x in bad.select(*keys, "o_" + m, "r_" + m).limit(3).collect()] if nb else []
                add(f"{tgt}: reconcile_{m}", f"{m} equals the independent reference calculation from bronze", nb == 0,
                    f"{nb} mismatch(es), e.g. {sample}" if nb else "all keys match", "reconciliation")
            for m in [m for m in spec["metrics"] if m not in mets]:
                add(f"{tgt}: reconcile_{m}", f"{m} can be reconciled", False, "column missing in output or reference", "reconciliation")
        except Exception as e:
            add(f"{tgt}: reconciliation", "the reference query runs and can be compared", False, f"{type(e).__name__}: {str(e)[:500]}", "reconciliation")

# COMMAND ----------

for r in results:
    print(("PASS " if r["passed"] else "FAIL ") + r["name"] + " — " + r["detail"])
dbutils.notebook.exit(json.dumps({"kind": "functional", "results": results}))