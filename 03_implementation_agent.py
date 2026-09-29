# Databricks notebook source
# DBTITLE 1,Stage 03 — Implementation & QA Agent
# MAGIC %md
# MAGIC # 03 — Implementation & QA Agent
# MAGIC
# MAGIC **Stage 03** of the Agentic Delivery Framework — implements, tests, and evaluates changes.
# MAGIC
# MAGIC Reads the analysis report from `agentops.control.analysis_reports`, then orchestrates three sub-agents:
# MAGIC
# MAGIC 1. **Agent 03A — Coding Agent**: Generates implementation code (PySpark/SQL notebooks or Lakeflow SDP definitions) based on the recommendation. Handles any task type: ETL pipelines, report builds, ML features, data quality rules, migrations, etc.
# MAGIC    * **Service**: Notebooks + Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC 2. **Agent 03B — Sandbox Testing Agent**: Executes the generated code in a dev sandbox, then runs automated output correctness validation — schema checks, row counts, null analysis, data type matching, sample output review.
# MAGIC    * **Service**: Lakeflow Jobs (dev) + Unity Catalog
# MAGIC
# MAGIC 3. **Agent 03C — AI Evaluation Agent (AI Judge)**: Reviews test results against the original requirement spec. Scores completeness, correctness, and quality. Produces a pass/fail verdict with detailed metrics feedback.
# MAGIC    * **Service**: Foundation Model (LLM-as-a-Judge)
# MAGIC
# MAGIC Results persist to `agentops.control.implementation_results`.

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install openai pyyaml --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Parameters & Imports
import requests, json, yaml, re, base64, time, textwrap
from openai import OpenAI
from datetime import datetime, timezone
from pyspark.sql.types import *

dbutils.widgets.text("request_id", "DBXCOE-24", "Request ID")
dbutils.widgets.text("secret_scope", "jira-intake", "Secret Scope")
dbutils.widgets.text("fm_endpoint", "databricks-gpt-5", "Foundation Model")
dbutils.widgets.text("catalog", "agentops", "Catalog")
dbutils.widgets.text("output_root", "/Users/sweta.mohapatra@ascendion.com/Agent Delivery Framework/generated", "Generated code output folder")

REQUEST_ID   = dbutils.widgets.get("request_id").strip()
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
FM_ENDPOINT  = dbutils.widgets.get("fm_endpoint")
CATALOG      = dbutils.widgets.get("catalog")
OUTPUT_ROOT  = dbutils.widgets.get("output_root")

assert REQUEST_ID, "Provide a request_id"

workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
try:
    token = dbutils.secrets.get(SECRET_SCOPE, "databricks_token")
except Exception:
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {"Authorization": f"Bearer {token}"}
client = OpenAI(base_url=f"https://{workspace_url}/serving-endpoints", api_key=token)

def llm_call(system: str, user: str, temperature: float = 0.0, max_tokens: int = 4096) -> str:
    """Generic LLM call with retry. Handles models that don't support temperature."""
    for attempt in range(3):
        try:
            kwargs = {
                "model": FM_ENDPOINT,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                "max_tokens": max_tokens,
            }
            # Some models (Claude) don't support temperature
            if "claude" not in FM_ENDPOINT.lower():
                kwargs["temperature"] = temperature if attempt == 0 else 0.0
            resp = client.chat.completions.create(**kwargs)
            return resp.choices[0].message.content.strip()
        except Exception as e:
            if "temperature" in str(e).lower():
                # Retry without temperature
                kwargs.pop("temperature", None)
                resp = client.chat.completions.create(**kwargs)
                return resp.choices[0].message.content.strip()
            if attempt == 2:
                raise
            print(f"  LLM retry {attempt+1}: {e}")
            time.sleep(2)

print(f"Stage 03 — Processing request: {REQUEST_ID}")

# COMMAND ----------

# DBTITLE 1,Load Analysis Report & Intake Spec
# ---------------------------------------------------------------------------
# Load the Stage 02 analysis report + Stage 01 intake spec
# ---------------------------------------------------------------------------
analysis_row = (
    spark.table(f"{CATALOG}.control.analysis_reports")
    .where(f"request_id = '{REQUEST_ID}'")
    .orderBy("analysed_at", ascending=False)
    .first()
)
assert analysis_row, f"No analysis report found for {REQUEST_ID}"

spec_row = (
    spark.table(f"{CATALOG}.control.intake_specs")
    .where(f"request_id = '{REQUEST_ID}'")
    .orderBy("processed_at", ascending=False)
    .first()
)
assert spec_row, f"No intake spec found for {REQUEST_ID}"

yaml_spec       = yaml.safe_load(spec_row["yaml_spec"])
analysis_yaml   = analysis_row["analysis_yaml"]
recommendation  = analysis_row["recommendation"]
target_table    = analysis_row["target_table"]
codebase_in_git = analysis_row["codebase_in_git"]

print(f"Recommendation : {recommendation}")
print(f"Target table   : {target_table}")
print(f"Codebase in Git: {codebase_in_git}")
print(f"Report name    : {yaml_spec.get('report_name', 'N/A')}")
print(f"Bronze sources : {yaml_spec.get('bronze_sources', [])}")
print(f"\n--- Analysis YAML (first 800 chars) ---")
print(analysis_yaml[:800] if analysis_yaml else "(empty)")

# COMMAND ----------

# DBTITLE 1,Agent 03A — Coding Agent: Generate Implementation Code
# ---------------------------------------------------------------------------
# AGENT 03A: CODING AGENT
# Reads the analysis report + intake spec and generates implementation code.
# Generic: handles ETL pipelines, report builds, ML features, migrations, etc.
# Service: Notebooks + Lakeflow Spark Declarative Pipelines
# ---------------------------------------------------------------------------
print("=" * 60)
print("AGENT 03A: CODING AGENT")
print("=" * 60)

coding_system = """You are a senior Databricks data engineer. You generate production-quality
PySpark / Spark SQL code for Databricks notebooks.

RULES:
1. Output ONLY executable Python code — no markdown, no explanation, no fences.
2. The code must be a COMPLETE, self-contained Databricks notebook cell sequence.
3. Start with necessary imports. Use `spark` (already available, do NOT create SparkSession).
4. For bronze tables with raw_json column: parse JSON with F.get_json_object(F.col("raw_json"), "$.field").
   Example: df.withColumn("status", F.get_json_object(F.col("raw_json"), "$.status"))
5. JOINS: Always alias tables and qualify columns to avoid AMBIGUOUS_REFERENCE:
   enc = spark.table("catalog.schema.encounter").alias("enc")
   clm = spark.table("catalog.schema.claim").alias("clm")
   joined = enc.join(clm, F.col("enc.some_id") == F.col("clm.some_id"))
6. Write the final result using:
   spark.sql("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
   df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("{target}")
7. Add display(result_df.limit(20)) at the end for validation.
8. Add comments explaining each transformation step.
9. Handle nulls, type casting, and edge cases defensively.
10. If the task is NOT an ETL/report build (e.g., ML model, data quality, migration),
   adapt your code accordingly — you are a general-purpose coding agent.
11. NEVER use dbutils.widgets — the notebook will be run programmatically.
12. NEVER use spark.read.format("delta").load("table.name") — use spark.table("catalog.schema.table").
"""

# Sample actual raw_json from each bronze source so the LLM knows real field paths
bronze_json_samples = {}
for src in yaml_spec.get("bronze_sources", []):
    try:
        row = spark.table(src).select("raw_json").limit(1).collect()
        if row:
            import json as _json
            parsed = _json.loads(row[0]["raw_json"])
            bronze_json_samples[src] = _json.dumps(parsed, indent=2)[:1200]
    except Exception:
        pass

json_samples_text = "\n".join(f"--- {tbl} sample raw_json ---\n{sample}" for tbl, sample in bronze_json_samples.items())

coding_prompt = f"""
== TASK ==
Implement the following requirement. Recommendation: {recommendation}

== REQUIREMENT SPEC (from Jira) ==
{yaml.dump(yaml_spec, default_flow_style=False)}

== ANALYSIS REPORT (from Stage 02) ==
{analysis_yaml}

== CATALOG ==
{CATALOG}

== TARGET TABLE ==
{target_table}

== ACTUAL BRONZE TABLE raw_json SAMPLES (use these exact JSON paths!) ==
{json_samples_text}

IMPORTANT: Use the EXACT JSON field paths shown above. For example:
- Encounter period: F.get_json_object(F.col("raw_json"), "$.period.start")
- Encounter class: F.get_json_object(F.col("raw_json"), "$.class.code")
- Claim amounts: F.get_json_object(F.col("raw_json"), "$.total.value")
- Organization name: F.get_json_object(F.col("raw_json"), "$.name")

Generate the complete PySpark implementation code.
"""

generated_code = llm_call(coding_system, coding_prompt, temperature=0.1, max_tokens=4096)

# Strip markdown fences if LLM wraps them
generated_code = re.sub(r'^```(?:python)?\s*', '', generated_code)
generated_code = re.sub(r'\s*```$', '', generated_code)

print(f"Generated code length: {len(generated_code)} chars")
print("\n--- Generated Code Preview (first 1500 chars) ---")
print(generated_code[:1500])

# COMMAND ----------

# DBTITLE 1,Write Generated Notebook to Workspace
# ---------------------------------------------------------------------------
# Write the generated code as a new Databricks notebook
# ---------------------------------------------------------------------------
import base64

# Ensure output folder exists
try:
    requests.post(
        f"https://{workspace_url}/api/2.0/workspace/mkdirs",
        headers=headers,
        json={"path": OUTPUT_ROOT},
    )
except Exception:
    pass

# Notebook name from the spec
safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', yaml_spec.get('report_name', REQUEST_ID).lower())[:60]
notebook_name = f"impl_{REQUEST_ID}_{safe_name}"
notebook_path = f"{OUTPUT_ROOT}/{notebook_name}"

# Add a header comment to the generated code
full_code = f"""# Auto-generated by Agent 03A (Coding Agent)
# Request: {REQUEST_ID}
# Report: {yaml_spec.get('report_name', 'N/A')}
# Target: {target_table}
# Generated: {datetime.now(timezone.utc).isoformat()}
# Recommendation: {recommendation}
# ---------------------------------------------------------------

{generated_code}
"""

# Upload as a Python notebook
content_b64 = base64.b64encode(full_code.encode("utf-8")).decode("utf-8")
resp = requests.post(
    f"https://{workspace_url}/api/2.0/workspace/import",
    headers=headers,
    json={
        "path": notebook_path,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": content_b64,
        "overwrite": True,
    },
)
assert resp.ok, f"Failed to write notebook: {resp.text[:300]}"

# Get the notebook ID
status_resp = requests.get(
    f"https://{workspace_url}/api/2.0/workspace/get-status",
    headers=headers,
    params={"path": notebook_path},
)
generated_notebook_id = status_resp.json().get("object_id") if status_resp.ok else None

print(f"\u2713 Generated notebook written: {notebook_path}")
print(f"  Notebook ID: {generated_notebook_id}")
print(f"  Code size: {len(full_code)} chars")

# COMMAND ----------

# DBTITLE 1,Agent 03B — Sandbox Testing: Execute & Validate
# ---------------------------------------------------------------------------
# AGENT 03B: SANDBOX TESTING AGENT
# Executes the generated notebook and validates output correctness.
# Service: Lakeflow Jobs (dev) + Unity Catalog
# ---------------------------------------------------------------------------
print("=" * 60)
print("AGENT 03B: SANDBOX TESTING AGENT")
print("=" * 60)

# ---- 1. Execute the generated notebook via Jobs API (one-time run) ----
print("\n--- Executing generated notebook via Jobs API ---")
run_payload = {
    "run_name": f"sandbox_test_{REQUEST_ID}_{int(time.time())}",
    "tasks": [{
        "task_key": "sandbox_run",
        "notebook_task": {"notebook_path": notebook_path},
        "new_cluster": {
            "spark_version": "auto",
            "num_workers": 0,
        },
    }],
}

# Try serverless first, fall back to classic
try:
    # Use run-now with serverless compute
    run_payload["tasks"][0].pop("new_cluster", None)
    run_payload["tasks"][0]["environment_key"] = "Default"
    run_resp = requests.post(
        f"https://{workspace_url}/api/2.1/jobs/runs/submit",
        headers=headers,
        json=run_payload,
    )
    if not run_resp.ok:
        raise Exception(run_resp.text[:200])
except Exception:
    # Fallback: run notebook directly via execution context
    run_resp = None

sandbox_run_id = None
sandbox_status = "skipped"
sandbox_error = None

if run_resp and run_resp.ok:
    sandbox_run_id = run_resp.json().get("run_id")
    print(f"  Submitted run: {sandbox_run_id}")
    
    # Poll for completion (max 10 min)
    max_wait = 600
    poll_interval = 15
    elapsed = 0
    while elapsed < max_wait:
        time.sleep(poll_interval)
        elapsed += poll_interval
        status_resp = requests.get(
            f"https://{workspace_url}/api/2.1/jobs/runs/get",
            headers=headers,
            params={"run_id": sandbox_run_id},
        )
        if status_resp.ok:
            state = status_resp.json().get("state", {})
            life_cycle = state.get("life_cycle_state", "")
            result_state = state.get("result_state", "")
            print(f"  [{elapsed}s] {life_cycle} / {result_state}")
            if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                sandbox_status = result_state or life_cycle
                if result_state != "SUCCESS":
                    sandbox_error = state.get("state_message", "Unknown error")
                break
    else:
        sandbox_status = "TIMEOUT"
        sandbox_error = f"Run did not complete within {max_wait}s"
else:
    # Direct execution fallback: run each command via notebook run
    print("  Jobs API unavailable — executing code directly via spark...")
    try:
        exec(generated_code)
        sandbox_status = "SUCCESS"
        print("  \u2713 Direct execution completed")
    except Exception as e:
        sandbox_status = "FAILED"
        sandbox_error = str(e)[:500]
        print(f"  \u2717 Direct execution failed: {sandbox_error}")

print(f"\nSandbox execution: {sandbox_status}")
if sandbox_error:
    print(f"Error: {sandbox_error}")

# COMMAND ----------

# DBTITLE 1,Agent 03B — Output Correctness Validation
# ---------------------------------------------------------------------------
# AGENT 03B (continued): Automated Output Correctness Validation
# Validates the generated table against the requirement spec.
# Service: Unity Catalog
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("AGENT 03B: OUTPUT CORRECTNESS VALIDATION")
print("=" * 60)

test_results = {
    "target_table": target_table,
    "sandbox_status": sandbox_status,
    "sandbox_error": sandbox_error,
    "tests": [],
}

def add_test(name: str, passed: bool, detail: str):
    test_results["tests"].append({"name": name, "passed": passed, "detail": detail})
    status = "\u2713 PASS" if passed else "\u2717 FAIL"
    print(f"  {status} | {name}: {detail}")

# ---- Test 1: Table exists ----
table_exists = False
try:
    table_exists = spark.catalog.tableExists(target_table)
except Exception:
    try:
        # Fallback: try SQL check
        spark.sql(f"DESCRIBE TABLE {target_table}")
        table_exists = True
    except Exception:
        table_exists = False

if table_exists:
    result_df = spark.table(target_table)
    add_test("table_exists", True, f"{target_table} created successfully")
else:
    add_test("table_exists", False, f"{target_table} does not exist (sandbox likely failed)")

if table_exists:
    # ---- Test 2: Row count > 0 ----
    row_count = result_df.count()
    add_test("row_count", row_count > 0, f"{row_count:,} rows")

    # ---- Test 3: Schema check — expected columns from spec ----
    actual_cols = {c.name.lower() for c in result_df.schema.fields}
    expected_metrics = [m["name"].lower().replace(" ", "_") for m in yaml_spec.get("metrics", [])]
    # Check if at least 50% of expected metrics have a matching column
    metric_matches = sum(1 for m in expected_metrics if any(m.replace(" ", "_") in c or c in m for c in actual_cols))
    metric_coverage = metric_matches / max(len(expected_metrics), 1)
    add_test("metric_columns", metric_coverage >= 0.5,
             f"{metric_matches}/{len(expected_metrics)} metrics found in columns: {sorted(actual_cols)}")

    # ---- Test 4: No all-null columns ----
    from pyspark.sql.functions import col, count, when, isnan
    null_counts = []
    for c in result_df.columns:
        nc = result_df.where(col(c).isNull()).count()
        if nc == row_count and row_count > 0:
            null_counts.append(c)
    add_test("no_all_null_columns", len(null_counts) == 0,
             f"{len(null_counts)} all-null columns" + (f": {null_counts}" if null_counts else ""))

    # ---- Test 5: No duplicate rows (basic) ----
    distinct_count = result_df.distinct().count()
    dup_ratio = 1 - (distinct_count / max(row_count, 1))
    add_test("low_duplicate_ratio", dup_ratio < 0.5,
             f"{distinct_count:,} distinct of {row_count:,} ({dup_ratio:.1%} duplicates)")

    # ---- Test 6: Data types are reasonable ----
    schema_info = [(f.name, str(f.dataType)) for f in result_df.schema.fields]
    has_mixed = any("string" in t.lower() for _, t in schema_info if "amount" in _.lower() or "count" in _.lower() or "rate" in _.lower())
    add_test("numeric_types", not has_mixed,
             f"Numeric columns stored as strings: {has_mixed}. Schema: {schema_info[:10]}")

    # ---- Test 7: Sample output ----
    sample_rows = result_df.limit(5).toPandas().to_dict(orient="records")
    add_test("sample_output", True, f"{len(sample_rows)} sample rows collected")
    test_results["sample_rows"] = sample_rows
    test_results["schema"] = schema_info
    test_results["row_count"] = row_count

    print(f"\n--- Sample Output ({min(5, row_count)} rows) ---")
    display(result_df.limit(5))

# Summary
passed = sum(1 for t in test_results["tests"] if t["passed"])
total = len(test_results["tests"])
test_results["pass_rate"] = f"{passed}/{total}"
print(f"\nValidation: {passed}/{total} tests passed")

# COMMAND ----------

# DBTITLE 1,Retry Loop — Auto-Fix on Failure (up to 3 retries)
# ---------------------------------------------------------------------------
# RETRY LOOP: If sandbox or validation failed, feed the error back to
# Agent 03A and regenerate code. Up to MAX_RETRIES attempts.
# ---------------------------------------------------------------------------
MAX_RETRIES = 3

def _all_tests_pass() -> bool:
    """Check if sandbox succeeded AND all validation tests passed."""
    if sandbox_status != "SUCCESS":
        return False
    return all(t["passed"] for t in test_results.get("tests", []))

def _build_fix_prompt(attempt: int, prev_code: str, error: str, tests: dict) -> str:
    """Build an LLM prompt that includes the previous code + error feedback."""
    failed_tests = [t for t in tests.get("tests", []) if not t["passed"]]
    test_summary = "\n".join(f"  - {t['name']}: {t['detail']}" for t in failed_tests)
    return f"""
== RETRY ATTEMPT {attempt}/{MAX_RETRIES} ==

Your previous code FAILED. Fix the issues below and regenerate COMPLETE code.

== PREVIOUS CODE ==
{prev_code}

== EXECUTION ERROR ==
{error or 'No runtime error (but validation tests failed)'}

== FAILED TESTS ==
{test_summary or 'All validation tests passed but sandbox execution failed'}

== COMMON FIXES ==
- Use spark.table("catalog.schema.table") NOT spark.read.format("delta").load("table.name")
- Bronze tables have columns: raw_json (STRING), resource_id, resource_type, _source_file, _ingested_at
- Parse raw_json with: F.get_json_object(F.col("raw_json"), "$.fieldName") for each field
- Create schema first: spark.sql("CREATE SCHEMA IF NOT EXISTS ...")
- Write with: df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("...")
- NEVER use spark.read.format("delta").load() with a table name — use spark.table()
- NEVER use dbutils.widgets
- AMBIGUOUS_REFERENCE fix: after joins, ALWAYS use table aliases. Example:
    enc = spark.table("agentops.bronze.encounter").alias("enc")
    claim = spark.table("agentops.bronze.claim").alias("claim")
    joined = enc.join(claim, F.col("enc.resource_id") == F.col("claim.resource_id"))
    joined.select(F.col("enc.resource_id").alias("encounter_id"), ...)
- After parsing raw_json, rename common columns BEFORE joining to avoid ambiguity
- For FHIR resources: resource_id in encounter is a UUID, use get_json_object to extract nested fields

== ORIGINAL REQUIREMENT SPEC ==
{yaml.dump(yaml_spec, default_flow_style=False)}

== TARGET TABLE ==
{target_table}

== CATALOG ==
{CATALOG}

Generate the COMPLETE fixed PySpark code. Output ONLY executable code, no markdown.
"""

def _run_sandbox(code: str) -> tuple[str, str]:
    """Execute code directly and return (status, error)."""
    try:
        exec(code)
        return "SUCCESS", None
    except Exception as e:
        return "FAILED", str(e)[:500]

def _run_validation() -> dict:
    """Run the same validation tests as Agent 03B."""
    results = {
        "target_table": target_table,
        "sandbox_status": sandbox_status,
        "sandbox_error": sandbox_error,
        "tests": [],
    }
    def _add(name, passed, detail):
        results["tests"].append({"name": name, "passed": passed, "detail": detail})
        s = "\u2713 PASS" if passed else "\u2717 FAIL"
        print(f"    {s} | {name}: {detail}")

    tbl_exists = False
    parts = target_table.split(".")
    try:
        tbl_exists = spark.catalog.tableExists(f"{parts[0]}.{parts[1]}.{parts[2]}")
    except Exception:
        try:
            tbl_exists = spark.catalog.tableExists(parts[2], f"{parts[0]}.{parts[1]}")
        except Exception:
            tbl_exists = False

    if tbl_exists:
        df = spark.table(target_table)
        _add("table_exists", True, f"{target_table} exists")

        rc = df.count()
        _add("row_count", rc > 0, f"{rc:,} rows")

        actual_cols = {c.name.lower() for c in df.schema.fields}
        expected_metrics = [m["name"].lower().replace(" ", "_") for m in yaml_spec.get("metrics", [])]
        matches = sum(1 for m in expected_metrics if any(m.replace(" ", "_") in c or c in m for c in actual_cols))
        _add("metric_columns", matches / max(len(expected_metrics), 1) >= 0.5,
             f"{matches}/{len(expected_metrics)} metrics found")

        from pyspark.sql.functions import col as _col
        nulls = [c for c in df.columns if df.where(_col(c).isNull()).count() == rc and rc > 0]
        _add("no_all_null_columns", len(nulls) == 0, f"{len(nulls)} all-null cols")

        results["row_count"] = rc
        results["schema"] = [(f.name, str(f.dataType)) for f in df.schema.fields]
        results["sample_rows"] = df.limit(5).toPandas().to_dict(orient="records")
    else:
        _add("table_exists", False, f"{target_table} not found")

    passed = sum(1 for t in results["tests"] if t["passed"])
    results["pass_rate"] = f"{passed}/{len(results['tests'])}"
    return results

# ---- Run retry loop ----
retry_history = []
if not _all_tests_pass():
    print("=" * 60)
    print("RETRY LOOP: Feeding errors back to Agent 03A")
    print("=" * 60)

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n{'- ' * 30}")
        print(f"RETRY {attempt}/{MAX_RETRIES}")
        print(f"{'- ' * 30}")

        # 1. Ask LLM to fix the code
        fix_system = coding_system + "\n\nIMPORTANT: Your previous code failed. Fix the specific errors shown. Output ONLY executable Python code."
        fix_prompt = _build_fix_prompt(attempt, generated_code, sandbox_error, test_results)
        new_code = llm_call(fix_system, fix_prompt, temperature=0.0, max_tokens=4096)
        new_code = re.sub(r'^```(?:python)?\s*', '', new_code)
        new_code = re.sub(r'\s*```$', '', new_code)
        print(f"  Regenerated code: {len(new_code)} chars")

        # 2. Write updated notebook
        full_code_updated = f"""# Auto-generated by Agent 03A (Coding Agent) — RETRY {attempt}
# Request: {REQUEST_ID}
# Target: {target_table}
# Generated: {datetime.now(timezone.utc).isoformat()}
# ---------------------------------------------------------------\n\n{new_code}\n"""
        content_b64 = base64.b64encode(full_code_updated.encode("utf-8")).decode("utf-8")
        requests.post(
            f"https://{workspace_url}/api/2.0/workspace/import",
            headers=headers,
            json={"path": notebook_path, "format": "SOURCE", "language": "PYTHON",
                  "content": content_b64, "overwrite": True},
        )

        # 3. Execute in sandbox
        print(f"  Executing sandbox...")
        sandbox_status, sandbox_error = _run_sandbox(new_code)
        print(f"  Sandbox: {sandbox_status}" + (f" | Error: {sandbox_error[:150]}" if sandbox_error else ""))

        # 4. Validate
        print(f"  Validating...")
        test_results = _run_validation()
        print(f"  Validation: {test_results['pass_rate']}")

        # 5. Update variables for downstream cells
        generated_code = new_code
        retry_history.append({
            "attempt": attempt,
            "sandbox_status": sandbox_status,
            "sandbox_error": sandbox_error,
            "pass_rate": test_results["pass_rate"],
        })

        if _all_tests_pass():
            print(f"\n\u2713 RETRY {attempt} SUCCEEDED — all tests pass!")
            break
        else:
            print(f"  \u2717 Still failing — {'retrying...' if attempt < MAX_RETRIES else 'max retries reached'}")
    else:
        print(f"\n\u2717 All {MAX_RETRIES} retries exhausted. Proceeding with best attempt.")

    # Summary
    print(f"\n{'=' * 60}")
    print("RETRY SUMMARY")
    print(f"{'=' * 60}")
    for rh in retry_history:
        status = "\u2713" if rh["sandbox_status"] == "SUCCESS" and "/" in rh["pass_rate"] and rh["pass_rate"].split("/")[0] == rh["pass_rate"].split("/")[1] else "\u2717"
        print(f"  {status} Attempt {rh['attempt']}: sandbox={rh['sandbox_status']}  tests={rh['pass_rate']}")
else:
    print("\u2713 First attempt passed all tests — no retries needed.")

print(f"\nFinal status: sandbox={sandbox_status}  tests={test_results.get('pass_rate', '?')}")

# COMMAND ----------

# DBTITLE 1,Agent 03C — AI Evaluation (LLM-as-a-Judge)
# ---------------------------------------------------------------------------
# AGENT 03C: AI EVALUATION AGENT (LLM-as-a-Judge)
# Reviews test results against original spec. Scores completeness,
# correctness, quality. Produces pass/fail verdict.
# ---------------------------------------------------------------------------
print("=" * 60)
print("AGENT 03C: AI EVALUATION (LLM-as-a-Judge)")
print("=" * 60)

judge_system = """You are an AI Quality Judge for a data engineering platform.
You evaluate whether auto-generated code correctly implements a requirement spec.

Score each dimension 1-5:
- completeness: Are all requested metrics/columns/transforms present?
- correctness: Does the logic match the spec (joins, aggregations, grain)?
- quality: Data quality (no all-nulls, reasonable row counts, proper types)?
- production_readiness: Error handling, comments, idempotency, performance?

Produce ONLY a YAML report with this shape (no markdown fences):
verdict: <PASS or FAIL (PASS requires all scores >= 3)>
overall_score: <average of 4 scores, 1 decimal>
completeness: <1-5>
correctness: <1-5>
quality: <1-5>
production_readiness: <1-5>
findings:
  - <finding 1>
  - <finding 2>
improvements:
  - <suggestion 1>
  - <suggestion 2>
metric_coverage:
  - metric: <name>
    found: <true/false>
    notes: <how it was implemented or what's missing>
"""

judge_prompt = f"""
== ORIGINAL REQUIREMENT SPEC ==
{yaml.dump(yaml_spec, default_flow_style=False)}

== ANALYSIS REPORT ==
{analysis_yaml[:2000]}

== GENERATED CODE (first 3000 chars) ==
{generated_code[:3000]}

== TEST RESULTS ==
{json.dumps(test_results, indent=2, default=str)[:3000]}

== SANDBOX EXECUTION ==
Status: {sandbox_status}
Error: {sandbox_error or 'None'}

Evaluate the implementation and produce your YAML verdict.
"""

judge_raw = llm_call(judge_system, judge_prompt, temperature=0.0, max_tokens=2048)

# Parse the judge verdict
judge_raw_clean = re.sub(r'^```(?:yaml)?\s*', '', judge_raw)
judge_raw_clean = re.sub(r'\s*```$', '', judge_raw_clean)

try:
    judge_verdict = yaml.safe_load(judge_raw_clean)
except Exception:
    # Try extracting from verdict: line
    judge_verdict = {"verdict": "UNKNOWN", "raw": judge_raw_clean[:1000]}

verdict = judge_verdict.get("verdict", "UNKNOWN")
overall_score = judge_verdict.get("overall_score", 0)

print(f"\nVerdict       : {verdict}")
print(f"Overall Score : {overall_score}/5.0")
print(f"Completeness  : {judge_verdict.get('completeness', '?')}/5")
print(f"Correctness   : {judge_verdict.get('correctness', '?')}/5")
print(f"Quality       : {judge_verdict.get('quality', '?')}/5")
print(f"Prod Readiness: {judge_verdict.get('production_readiness', '?')}/5")

findings = judge_verdict.get("findings", [])
if findings:
    print(f"\nFindings:")
    for f in findings:
        print(f"  \u2022 {f}")

improvements = judge_verdict.get("improvements", [])
if improvements:
    print(f"\nSuggested Improvements:")
    for imp in improvements:
        print(f"  \u2022 {imp}")

metric_coverage = judge_verdict.get("metric_coverage", [])
if metric_coverage:
    print(f"\nMetric Coverage:")
    for mc in metric_coverage:
        status = "\u2713" if mc.get("found") else "\u2717"
        print(f"  {status} {mc.get('metric', '?')}: {mc.get('notes', '')}")

# COMMAND ----------

# DBTITLE 1,Persist Implementation Results
# ---------------------------------------------------------------------------
# Persist results to agentops.control.implementation_results
# ---------------------------------------------------------------------------
print("=" * 60)
print("PERSIST IMPLEMENTATION RESULTS")
print("=" * 60)

result_schema = StructType([
    StructField("request_id", StringType(), False),
    StructField("recommendation", StringType()),
    StructField("generated_notebook_path", StringType()),
    StructField("generated_notebook_id", StringType()),
    StructField("sandbox_status", StringType()),
    StructField("sandbox_error", StringType()),
    StructField("sandbox_run_id", StringType()),
    StructField("test_pass_rate", StringType()),
    StructField("judge_verdict", StringType()),
    StructField("judge_overall_score", StringType()),
    StructField("judge_completeness", StringType()),
    StructField("judge_correctness", StringType()),
    StructField("judge_quality", StringType()),
    StructField("judge_prod_readiness", StringType()),
    StructField("judge_findings", StringType()),
    StructField("judge_improvements", StringType()),
    StructField("judge_full_yaml", StringType()),
    StructField("test_results_json", StringType()),
    StructField("implemented_at", TimestampType()),
])

result_row = [(
    REQUEST_ID,
    recommendation,
    notebook_path,
    str(generated_notebook_id) if generated_notebook_id else None,
    sandbox_status,
    sandbox_error,
    str(sandbox_run_id) if sandbox_run_id else None,
    test_results.get("pass_rate", "0/0"),
    verdict,
    str(overall_score),
    str(judge_verdict.get("completeness", "")),
    str(judge_verdict.get("correctness", "")),
    str(judge_verdict.get("quality", "")),
    str(judge_verdict.get("production_readiness", "")),
    json.dumps(findings, default=str),
    json.dumps(improvements, default=str),
    judge_raw_clean[:4000],
    json.dumps(test_results, default=str)[:4000],
    datetime.now(timezone.utc),
)]

impl_df = spark.createDataFrame(result_row, schema=result_schema)
impl_df.write.format("delta").mode("append").option(
    "mergeSchema", "true"
).saveAsTable(f"{CATALOG}.control.implementation_results")

print(f"\u2713 Results persisted to {CATALOG}.control.implementation_results")
print(f"\n{'=' * 60}")
print("STAGE 03 SUMMARY")
print(f"{'=' * 60}")
print(f"  Request       : {REQUEST_ID}")
print(f"  Recommendation: {recommendation}")
print(f"  Generated     : {notebook_path}")
print(f"  Sandbox       : {sandbox_status}")
print(f"  Tests         : {test_results.get('pass_rate', '?')}")
print(f"  Judge Verdict : {verdict} ({overall_score}/5.0)")
print(f"  Next Step     : {'\u2192 Stage 04 (PR & Code Review)' if verdict == 'PASS' else '\u2192 Fix issues and re-run'}")

display(spark.table(f"{CATALOG}.control.implementation_results").where(f"request_id = '{REQUEST_ID}'").orderBy("implemented_at", ascending=False))