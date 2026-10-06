# Databricks notebook source
# FUNCTIONAL TESTS — DBXCOE-33: data rules on the sandbox output + reconciliation against the independent reference SQL
# (approved with plan af8236763d6e0157). Runs on the real bronze data.

# COMMAND ----------

import json, re
from pyspark.sql import functions as F, types as T
dbutils.widgets.text("sandbox_catalog", "agentops")
dbutils.widgets.text("sandbox_schema", "sandbox_dbxcoe_33")
SB = dbutils.widgets.get("sandbox_catalog") + "." + dbutils.widgets.get("sandbox_schema")
CONTRACT = json.loads("{\"entry_point\": \"build(spark, sources: dict) -> dict[str, DataFrame]\", \"sources\": {\"claim\": \"agentops.bronze.claim\", \"patient\": \"agentops.bronze.patient\"}, \"targets\": {\"rejected_claims_patient_summary\": {\"table\": \"agentops.gold.rejected_claims_patient_summary\", \"grain\": [], \"columns\": [\"patient_id\"], \"metrics\": [\"rejected_claim_count\"]}}}")
SOURCES = json.loads("{\"claim\": \"agentops.bronze.claim\", \"patient\": \"agentops.bronze.patient\"}")
RECON = json.loads("{\"rejected_claims_patient_summary\": \"SELECT c.patient_id, COUNT(*) AS rejected_claim_count FROM {src:claim} c WHERE c.status = 'rejected' AND c.patient_id IS NOT NULL GROUP BY c.patient_id\"}")
EXISTING = json.loads("[\"agentops.gold.rejected_claims_patient_summary\"]")
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