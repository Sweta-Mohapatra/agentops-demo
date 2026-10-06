# Databricks notebook source
# DRY RUN — DBXCOE-33: plans build() on the real sources without processing data. Every column and function must resolve,
# and every output must match the approved contract. Fails the task (so nothing else runs) if anything is wrong.

# COMMAND ----------

AGENTOPS_LIBRARY_MODE = True   # the implementation skips its write / @dlt.table cell when imported by tests

# COMMAND ----------

# MAGIC %run ../rejected_claims_patient_summary $run_mode="library"

# COMMAND ----------

import json
from pyspark.sql import types as T
CONTRACT = json.loads("{\"entry_point\": \"build(spark, sources: dict) -> dict[str, DataFrame]\", \"sources\": {\"claim\": \"agentops.bronze.claim\", \"patient\": \"agentops.bronze.patient\"}, \"targets\": {\"rejected_claims_patient_summary\": {\"table\": \"agentops.gold.rejected_claims_patient_summary\", \"grain\": [], \"columns\": [\"patient_id\"], \"metrics\": [\"rejected_claim_count\"]}}}")
results = []
def add(name, checks, ok, detail):
    results.append({"name": name, "checks": checks, "passed": bool(ok), "detail": str(detail)[:800], "kind": "dry_run"})
outs = None
try:
    outs = build(spark, load_sources(spark))
    add("build_called", "build(spark, load_sources(spark)) returns without error", isinstance(outs, dict), type(outs).__name__)
except Exception as e:
    add("build_called", "build(spark, load_sources(spark)) returns without error", False, f"{type(e).__name__}: {e}")
if isinstance(outs, dict):
    for tgt, spec in CONTRACT["targets"].items():
        if tgt not in outs:
            add(f"{tgt}: output_present", f"build() returns '{tgt}'", False, f"keys returned: {list(outs)}")
            continue
        try:
            sch = outs[tgt].schema
            cols = {f.name.lower(): f.dataType for f in sch.fields}
            need = spec["columns"] + spec["metrics"]
            miss = [c for c in need if c not in cols]
            add(f"{tgt}: resolves", "the query plan resolves on serverless", True, f"{len(cols)} columns")
            add(f"{tgt}: contract_columns", "all contract columns exist", not miss, f"missing {miss}" if miss else "ok")
            nonnum = [m for m in spec["metrics"] if m in cols and not isinstance(cols[m], T.NumericType)]
            add(f"{tgt}: metrics_numeric", "metric columns are numeric types", not nonnum, f"non-numeric {nonnum}" if nonnum else "ok")
        except Exception as e:
            add(f"{tgt}: resolves", "the query plan resolves on serverless", False, f"{type(e).__name__}: {e}")
payload = json.dumps({"kind": "dry_run", "passed": all(r["passed"] for r in results), "results": results})
print(payload)
if not all(r["passed"] for r in results):
    raise Exception("AGENTOPS_RESULT:" + payload)
dbutils.notebook.exit(payload)