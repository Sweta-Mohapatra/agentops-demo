# Databricks notebook source
# DBTITLE 1,AI Analysis Agent — Stage 02 of Agentic Delivery Framework
# MAGIC %md
# MAGIC # 02 — AI Analysis Agent
# MAGIC
# MAGIC **Stage 02: AI Analysis** of the Agentic Delivery Framework.
# MAGIC
# MAGIC Reads a requirement spec from `agentops.control.intake_specs`, then:
# MAGIC 1. **Git Repo analysis** — scans Git folders (Repos) and workspace notebooks for overlapping pipelines, branches, and existing code
# MAGIC 2. **Unity Catalog lineage** — traces upstream writers and downstream consumers for every bronze source via the UC lineage API
# MAGIC 3. **Dependency analysis** — discovers existing Lakeflow pipelines, jobs, and notebook-to-table data flows
# MAGIC 4. **Profiles** bronze source tables (row counts, schemas, key columns)
# MAGIC 5. **Recommendation**: new pipeline vs. changes to existing, backed by lineage + repo evidence
# MAGIC 6. **Structured analysis report** persisted to `agentops.control.analysis_reports`
# MAGIC
# MAGIC ### Service: Git folders (Repos) + Unity Catalog lineage

# COMMAND ----------

# DBTITLE 1,Install & Config
# MAGIC %pip install openai pyyaml --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Parameters & Imports
import requests, json, yaml, re
from openai import OpenAI
from datetime import datetime, timezone

dbutils.widgets.text("request_id", "DBXCOE-24", "Request ID from Stage 01")
dbutils.widgets.text("secret_scope", "jira-intake", "Databricks Secret Scope")
dbutils.widgets.text("fm_endpoint", "databricks-meta-llama-3-3-70b-instruct", "Foundation Model Endpoint")
dbutils.widgets.text("catalog", "agentops", "Target Catalog")
dbutils.widgets.text("codebase_root", "/Workspace/Repos/sweta.mohapatra@ascendion.com/agentops-demo", "Codebase root folder (Git repo)")

REQUEST_ID     = dbutils.widgets.get("request_id").strip()
SECRET_SCOPE   = dbutils.widgets.get("secret_scope")
FM_ENDPOINT    = dbutils.widgets.get("fm_endpoint")
CATALOG        = dbutils.widgets.get("catalog")
CODEBASE_ROOT  = dbutils.widgets.get("codebase_root")

assert REQUEST_ID, "Provide a request_id from the intake_specs table."

workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
try:
    workspace_token = dbutils.secrets.get(SECRET_SCOPE, "databricks_token")
except Exception:
    workspace_token = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook().getContext().apiToken().get()
    )

client = OpenAI(
    base_url=f"https://{workspace_url}/serving-endpoints",
    api_key=workspace_token,
)
print(f"Processing request: {REQUEST_ID}")

# COMMAND ----------

# DBTITLE 1,Load Intake Spec from Stage 01
# ---------------------------------------------------------------------------
# Read the YAML spec produced by Stage 01
# ---------------------------------------------------------------------------
spec_row = (
    spark.table(f"{CATALOG}.control.intake_specs")
    .where(f"request_id = '{REQUEST_ID}'")
    .orderBy("processed_at", ascending=False)
    .first()
)
assert spec_row, f"No intake spec found for request_id={REQUEST_ID}"

yaml_spec = yaml.safe_load(spec_row["yaml_spec"])

report_name    = yaml_spec.get("report_name", "")
grain          = yaml_spec.get("grain", "")
metrics        = yaml_spec.get("metrics", [])
dimensions     = yaml_spec.get("dimensions", [])
filters_list   = yaml_spec.get("filters", [])
bronze_sources = yaml_spec.get("bronze_sources", [])
target_table   = yaml_spec.get("target_table", "")
open_questions = yaml_spec.get("open_questions", [])

print(f"Report  : {report_name}")
print(f"Grain   : {grain}")
print(f"Metrics : {[m['name'] for m in metrics]}")
print(f"Sources : {bronze_sources}")
print(f"Target  : {target_table}")
print(f"Open Qs : {len(open_questions)}")

# COMMAND ----------

# DBTITLE 1,Profile Bronze Source Tables
# ---------------------------------------------------------------------------
# Profile each bronze table: row count, columns, sample values
# ---------------------------------------------------------------------------
table_profiles = {}
for fqn in bronze_sources:
    try:
        df = spark.table(fqn)
        cols = [(c.name, str(c.dataType)) for c in df.schema.fields]
        count = df.count()
        sample = df.limit(3).toPandas().to_dict(orient="records")
        table_profiles[fqn] = {
            "row_count": count,
            "columns": cols,
            "sample_rows": sample,
        }
        print(f"\n{fqn}: {count:,} rows, {len(cols)} columns")
        for name, dtype in cols:
            print(f"  - {name} ({dtype})")
    except Exception as e:
        table_profiles[fqn] = {"error": str(e)}
        print(f"\n{fqn}: ERROR - {e}")

# COMMAND ----------

# DBTITLE 1,Git Repo Analysis — Scan Repos, Branches, Pipeline Code
# ---------------------------------------------------------------------------
# Git Repo Analysis: scan repos, branches, existing pipeline definitions
# ---------------------------------------------------------------------------
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType
import base64

w = WorkspaceClient()
headers = {"Authorization": f"Bearer {workspace_token}"}

# ---- 1. Discover all Git folders (Repos) in workspace ----
print("=" * 60)
print("GIT REPO ANALYSIS")
print("=" * 60)

# Normalize paths: strip /Workspace prefix for consistent comparison
def _norm(p: str) -> str:
    return p.replace("/Workspace", "") if p.startswith("/Workspace") else p

git_repos = []
try:
    for r in w.repos.list():
        git_repos.append({
            "id": r.id, "path": r.path, "url": r.url, "branch": r.branch,
            "provider": str(r.provider) if r.provider else None,
        })
except Exception as e:
    print(f"  Warning: cannot list repos: {e}")

# Also discover the codebase repo via REST is_git_folder check
codebase_norm = _norm(CODEBASE_ROOT)
try:
    gs_resp = requests.get(
        f"https://{workspace_url}/api/2.0/workspace/get-status",
        headers=headers,
        params={"path": codebase_norm},
    )
    if gs_resp.ok:
        gs_data = gs_resp.json()
        if gs_data.get("directory_info", {}).get("is_git_folder", False):
            oid = gs_data["object_id"]
            rr = requests.get(f"https://{workspace_url}/api/2.0/repos/{oid}", headers=headers)
            if rr.ok:
                rd = rr.json()
                if not any(g.get("id") == rd["id"] for g in git_repos):
                    git_repos.append({
                        "id": rd["id"], "path": rd["path"], "url": rd.get("url", ""),
                        "branch": rd.get("branch", ""), "provider": rd.get("provider", ""),
                    })
                    print(f"  Discovered codebase Git folder: {rd['path']} -> {rd.get('url')} [{rd.get('branch')}]")
except Exception as e:
    print(f"  Warning (codebase git check): {e}")

print(f"\nGit folders in workspace: {len(git_repos)}")
for repo in git_repos:
    print(f"  {repo['path']}")
    print(f"    \u2192 {repo['url']}  [{repo['branch']}]")

# ---- 2. Check if codebase root is inside a Git folder ----
codebase_in_git = False
codebase_git_repo = None

for repo in git_repos:
    repo_norm = _norm(repo["path"])
    if codebase_norm == repo_norm or codebase_norm.startswith(repo_norm + "/"):
        codebase_in_git = True
        codebase_git_repo = repo
        break

if codebase_in_git:
    print(f"\n✓ Codebase root IS in Git folder: {codebase_git_repo['path']}")
    print(f"  Remote: {codebase_git_repo['url']}  [{codebase_git_repo['branch']}]")
else:
    print(f"\n⚠ Codebase root ({CODEBASE_ROOT}) is NOT in a Git folder")
    print("  Recommendation: move to a Git repo for version control and CI/CD")

# ---- 3. Scan each Git repo for relevant code ----
def scan_repo_for_tables(repo: dict, search_terms: list[str]) -> list[dict]:
    """Scan files in a Git repo for references to our bronze/gold tables."""
    hits = []
    repo_path = repo["path"]
    try:
        for obj in w.workspace.list(repo_path):
            if obj.object_type == ObjectType.NOTEBOOK:
                try:
                    resp = requests.get(
                        f"https://{workspace_url}/api/2.0/workspace/export",
                        headers=headers,
                        params={"path": obj.path, "format": "SOURCE"},
                    )
                    if resp.ok:
                        src = base64.b64decode(resp.json()["content"]).decode("utf-8", errors="ignore")
                        matches = [t for t in search_terms if t.lower() in src.lower()]
                        if matches:
                            hits.append({"path": obj.path, "matched": matches, "preview": src[:400]})
                except Exception:
                    pass
    except Exception:
        pass
    return hits

search_terms = [t.split(".")[-1] for t in bronze_sources]
if target_table:
    search_terms.append(target_table.split(".")[-1])

repo_scan_results = {}
for repo in git_repos:
    hits = scan_repo_for_tables(repo, search_terms)
    if hits:
        repo_scan_results[repo["url"]] = hits
        print(f"\n  Repo {repo['url']} — {len(hits)} file(s) reference our tables:")
        for h in hits:
            print(f"    {h['path']}  →  {h['matched']}")

if not repo_scan_results:
    print("\n  No Git repo notebooks reference the required tables.")

# ---- 4. Scan workspace codebase folder too ----
def scan_folder_recursive(path: str, depth: int = 0, max_depth: int = 4) -> list[dict]:
    results = []
    if depth > max_depth:
        return results
    try:
        for obj in w.workspace.list(path):
            # Skip .git internals
            if obj.path and "/.git" in obj.path:
                continue
            if obj.object_type in (ObjectType.NOTEBOOK, ObjectType.FILE):
                results.append({"path": obj.path, "type": str(obj.object_type), "language": str(obj.language) if obj.language else None})
            elif obj.object_type == ObjectType.DIRECTORY:
                results.extend(scan_folder_recursive(obj.path, depth + 1, max_depth))
    except Exception:
        pass
    return results

codebase_files = scan_folder_recursive(CODEBASE_ROOT)
print(f"\nWorkspace codebase files: {len(codebase_files)}")
for f in codebase_files:
    print(f"  {f['type']:10s} {f['path']}")

# COMMAND ----------

# DBTITLE 1,UC Lineage — Trace Upstream Writers & Downstream Consumers
# ---------------------------------------------------------------------------
# Unity Catalog Lineage: trace upstream writers & downstream consumers
# for every bronze source table via the UC lineage REST API
# ---------------------------------------------------------------------------
print("=" * 60)
print("UNITY CATALOG LINEAGE")
print("=" * 60)

def get_table_lineage(table_fqn: str) -> dict:
    """Call the UC lineage API for a single table."""
    resp = requests.get(
        f"https://{workspace_url}/api/2.0/lineage-tracking/table-lineage",
        headers=headers,
        params={"table_name": table_fqn, "include_entity_lineage": "true"},
    )
    if resp.ok:
        return resp.json()
    return {"error": resp.text[:200]}

lineage_graph = {}   # table_fqn -> {upstream_writers, upstream_sources, downstream_consumers}

for fqn in bronze_sources:
    lineage = get_table_lineage(fqn)
    upstreams = lineage.get("upstreams", [])
    downstreams = lineage.get("downstreams", [])

    writers = []
    sources = []
    consumers = []

    for up in upstreams:
        # Notebook writers
        for nb in up.get("notebookInfos", []):
            writers.append({"type": "notebook", "id": nb.get("notebook_id"), "url": nb.get("url", "")})
        # Volume / file sources
        fi = up.get("fileInfo")
        if fi:
            sources.append({"path": fi.get("path", ""), "type": fi.get("securable_type", "")})
        # Pipeline writers
        for pl in up.get("pipelineInfos", []):
            writers.append({"type": "pipeline", "id": pl.get("pipeline_id"), "name": pl.get("pipeline_name", "")})
        # Job writers
        for jb in up.get("jobInfos", []):
            writers.append({"type": "job", "id": jb.get("job_id"), "name": jb.get("job_name", "")})

    for down in downstreams:
        for nb in down.get("notebookInfos", []):
            consumers.append({"type": "notebook", "id": nb.get("notebook_id"), "url": nb.get("url", "")})
        for pl in down.get("pipelineInfos", []):
            consumers.append({"type": "pipeline", "id": pl.get("pipeline_id"), "name": pl.get("pipeline_name", "")})
        for jb in down.get("jobInfos", []):
            consumers.append({"type": "job", "id": jb.get("job_id"), "name": jb.get("job_name", "")})

    lineage_graph[fqn] = {
        "upstream_writers": writers,
        "upstream_sources": sources,
        "downstream_consumers": consumers,
    }

    print(f"\n{fqn}:")
    print(f"  Upstream writers   : {len(writers)}")
    for wr in writers:
        print(f"    [{wr['type']}] id={wr.get('id')} {wr.get('name','')}")
    print(f"  Upstream sources   : {len(sources)}")
    for sr in sources:
        print(f"    [{sr['type']}] {sr['path']}")
    print(f"  Downstream readers : {len(consumers)}")
    for cn in consumers:
        print(f"    [{cn['type']}] id={cn.get('id')} {cn.get('name','')}")

# ---- Identify the primary pipeline notebook (the one that writes all bronze) ----
writer_ids = set()
for fqn, lg in lineage_graph.items():
    for wr in lg["upstream_writers"]:
        if wr["type"] == "notebook":
            writer_ids.add(wr["id"])

print(f"\n→ Unique upstream writer notebook(s): {writer_ids}")

# COMMAND ----------

# DBTITLE 1,Dependency Analysis — Pipelines, Jobs, Notebook Overlaps, Gold Status
# ---------------------------------------------------------------------------
# Dependency Analysis: pipelines, jobs, notebook overlaps, gold status
# ---------------------------------------------------------------------------
print("=" * 60)
print("DEPENDENCY ANALYSIS")
print("=" * 60)

# ---- 1. Existing Lakeflow pipelines ----
print("\n--- Lakeflow Pipelines ---")
existing_pipelines = []
try:
    for p in w.pipelines.list_pipelines():
        existing_pipelines.append({"id": p.pipeline_id, "name": p.name, "state": str(p.state)})
except Exception as e:
    print(f"  Warning: {e}")

print(f"Total pipelines in workspace: {len(existing_pipelines)}")
for p in existing_pipelines:
    print(f"  {p['name']} [{p['state']}] — {p['id']}")

# ---- 2. Existing Jobs ----
print("\n--- Lakeflow Jobs ---")
existing_jobs = []
try:
    for j in w.jobs.list(limit=50):
        existing_jobs.append({"id": j.job_id, "name": j.settings.name if j.settings else "unnamed"})
except Exception as e:
    print(f"  Warning: {e}")

print(f"Total jobs in workspace: {len(existing_jobs)}")
for j in existing_jobs[:15]:
    print(f"  {j['name']} — {j['id']}")
if len(existing_jobs) > 15:
    print(f"  ... and {len(existing_jobs) - 15} more")

# ---- 3. Notebook-to-table overlap (workspace codebase) ----
print("\n--- Notebook Table Overlaps (workspace) ---")
def read_notebook_source(path: str) -> str:
    try:
        resp = requests.get(
            f"https://{workspace_url}/api/2.0/workspace/export",
            headers=headers,
            params={"path": path, "format": "SOURCE"},
        )
        if resp.ok:
            return base64.b64decode(resp.json()["content"]).decode("utf-8", errors="ignore")
    except Exception:
        pass
    return ""

overlapping_notebooks = []
for f in codebase_files:
    if f["type"] != "ObjectType.NOTEBOOK":
        continue
    src = read_notebook_source(f["path"])
    matches = [t for t in search_terms if t.lower() in src.lower()]
    if matches:
        overlapping_notebooks.append({
            "path": f["path"],
            "matched_tables": matches,
            "source_preview": src[:500],
        })

print(f"Notebooks referencing the same tables: {len(overlapping_notebooks)}")
for nb in overlapping_notebooks:
    print(f"  {nb['path']}  →  {nb['matched_tables']}")

# ---- 4. Target gold table status ----
print("\n--- Target Gold Table ---")
target_exists = False
if target_table:
    try:
        tdf = spark.table(target_table)
        target_exists = True
        target_cols = [(c.name, str(c.dataType)) for c in tdf.schema.fields]
        target_count = tdf.count()
        print(f"⚠ {target_table} ALREADY EXISTS ({target_count:,} rows, {len(target_cols)} cols)")
    except Exception:
        print(f"✓ {target_table} does NOT exist yet (will be created)")

# ---- 5. Existing gold tables ----
existing_gold = spark.sql(f"""
    SELECT table_name FROM {CATALOG}.information_schema.tables
    WHERE table_schema = 'gold' ORDER BY table_name
""").collect()
print(f"\nExisting gold tables in {CATALOG}.gold: {len(existing_gold)}")
for r in existing_gold:
    print(f"  - {CATALOG}.gold.{r['table_name']}")

# ---- 6. Consolidated dependency summary ----
print(f"\n{'=' * 60}")
print("DEPENDENCY SUMMARY")
print(f"{'=' * 60}")
print(f"  Git repos: {len(git_repos)}  |  Codebase in Git: {codebase_in_git}")
print(f"  UC lineage writer notebooks: {writer_ids}")
print(f"  Overlapping codebase notebooks: {len(overlapping_notebooks)}")
print(f"  Git repo notebook hits: {sum(len(v) for v in repo_scan_results.values())}")
print(f"  Existing pipelines: {len(existing_pipelines)}")
print(f"  Existing jobs: {len(existing_jobs)}")
print(f"  Target table exists: {target_exists}")
print(f"  Existing gold tables: {len(existing_gold)}")

# COMMAND ----------

# DBTITLE 1,LLM Analysis — Recommendation + Dependency Report
# ---------------------------------------------------------------------------
# Send all gathered context to the LLM for a structured recommendation
# ---------------------------------------------------------------------------

def _profile_summary() -> str:
    parts = []
    for fqn, prof in table_profiles.items():
        if "error" in prof:
            parts.append(f"{fqn}: ERROR - {prof['error']}")
        else:
            col_list = ", ".join(f"{n} ({t})" for n, t in prof["columns"])
            parts.append(f"{fqn}: {prof['row_count']:,} rows | Columns: {col_list}")
    return "\n".join(parts)

def _overlap_summary() -> str:
    if not overlapping_notebooks:
        return "No existing notebooks reference the same tables."
    parts = []
    for nb in overlapping_notebooks:
        parts.append(f"{nb['path']} references {nb['matched_tables']}")
        parts.append(f"  Preview: {nb['source_preview'][:300]}")
    return "\n".join(parts)

def _lineage_summary() -> str:
    parts = []
    for fqn, lg in lineage_graph.items():
        parts.append(f"{fqn}:")
        for wr in lg["upstream_writers"]:
            parts.append(f"  Written by [{wr['type']}] id={wr.get('id')} {wr.get('name','')}")
        for sr in lg["upstream_sources"]:
            parts.append(f"  Sourced from [{sr['type']}] {sr['path']}")
        for cn in lg["downstream_consumers"]:
            parts.append(f"  Read by [{cn['type']}] id={cn.get('id')} {cn.get('name','')}")
    return "\n".join(parts)

def _git_summary() -> str:
    parts = [f"Git folders in workspace: {len(git_repos)}"]
    for r in git_repos:
        parts.append(f"  {r['url']} branch={r['branch']} path={r['path']}")
    parts.append(f"Codebase in Git: {codebase_in_git}")
    if repo_scan_results:
        parts.append("Git repo hits (notebooks referencing our tables):")
        for url, hits in repo_scan_results.items():
            for h in hits:
                parts.append(f"  {h['path']} -> {h['matched']}")
    else:
        parts.append("No Git repo notebooks reference the required tables.")
    return "\n".join(parts)

def _pipeline_job_summary() -> str:
    parts = [f"Existing Lakeflow Pipelines: {len(existing_pipelines)}"]
    for p in existing_pipelines:
        parts.append(f"  {p['name']} [{p['state']}]")
    parts.append(f"Existing Jobs: {len(existing_jobs)}")
    for j in existing_jobs[:10]:
        parts.append(f"  {j['name']}")
    return "\n".join(parts)

analysis_prompt = f"""
You are a senior data engineer reviewing a requirement spec and the current state of
the codebase and data catalog. Produce a structured analysis report.

== REQUIREMENT SPEC ==
{yaml.dump(yaml_spec, default_flow_style=False)}

== BRONZE TABLE PROFILES ==
{_profile_summary()}

== UNITY CATALOG LINEAGE (real API data) ==
{_lineage_summary()}

== GIT REPO ANALYSIS ==
{_git_summary()}

== EXISTING PIPELINES & JOBS ==
{_pipeline_job_summary()}

== OVERLAPPING NOTEBOOKS (workspace codebase) ==
{_overlap_summary()}

== EXISTING GOLD TABLES ==
{chr(10).join(CATALOG + '.gold.' + r['table_name'] for r in existing_gold)}

== TARGET TABLE EXISTS ==
{target_exists}

Produce a YAML report with this exact shape:

request_id: {REQUEST_ID}
recommendation: <"new_pipeline" or "modify_existing">
recommendation_reason: <1-3 sentence explanation citing the lineage and Git evidence>
git_analysis:
  repos_found: <count>
  codebase_in_git: <true/false>
  relevant_repo_hits: [<list of repo paths with table references>]
  recommendation: <"move_to_git" | "already_tracked" | "create_new_repo">
lineage_summary:
  upstream_writer: <notebook or pipeline that writes the bronze tables>
  source_format: <e.g. NDJSON from UC Volume>
  downstream_consumers: [<list of notebooks/pipelines already reading these tables>]
overlapping_assets:
  - path: <notebook path>
    overlap_type: <"same_sources" | "same_target" | "similar_logic">
    notes: <what overlaps>
dependency_graph:
  bronze_to_gold:
    - source: <bronze table>
      target: {target_table}
      join_keys: [<columns likely used as join keys based on the schema>]
      notes: <any data quality observations from lineage>
schema_recommendation:
  - column: <column name>
    source_table: <where it comes from>
    transform: <any transformation needed>
risks:
  - <anything that could go wrong, including Git/lineage gaps>
open_questions:
  - <questions from the spec + anything new found in lineage/Git analysis>

Output YAML only. No markdown fences. No commentary.
"""

def _generate_analysis(max_retries: int = 3) -> tuple[str, dict]:
    for i in range(max_retries):
        resp = client.chat.completions.create(
            model=FM_ENDPOINT,
            messages=[
                {"role": "system", "content": "You are a senior data engineer. Produce only strict YAML."},
                {"role": "user", "content": analysis_prompt}
            ],
            max_tokens=2000,
            temperature=0.0 if i > 0 else 0.2,
        )
        raw = resp.choices[0].message.content.strip()
        # Clean
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:ya?ml)?\s*\n", "", raw)
            raw = re.sub(r"\n```\s*$", "", raw)
        idx = raw.find("request_id:")
        if idx > 0:
            raw = raw[idx:]
        try:
            parsed = yaml.safe_load(raw)
            if isinstance(parsed, dict):
                return raw, parsed
        except yaml.YAMLError as e:
            print(f"  ⚠ Attempt {i+1}: invalid YAML: {str(e)[:100]}")
    raise ValueError("Failed to generate valid analysis YAML")

analysis_yaml, analysis_parsed = _generate_analysis()
print(analysis_yaml)

# COMMAND ----------

# DBTITLE 1,Persist Analysis Report
# ---------------------------------------------------------------------------
# Persist the analysis report to agentops.control.analysis_reports
# ---------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType

report_schema = StructType([
    StructField("request_id", StringType(), False),
    StructField("recommendation", StringType()),
    StructField("recommendation_reason", StringType()),
    StructField("target_table", StringType()),
    StructField("target_exists", BooleanType()),
    StructField("git_repos_found", StringType()),
    StructField("codebase_in_git", BooleanType()),
    StructField("uc_lineage_writers", StringType()),
    StructField("overlapping_notebook_count", StringType()),
    StructField("pipeline_count", StringType()),
    StructField("analysis_yaml", StringType()),
    StructField("analysed_at", TimestampType()),
])

report_row = [(
    REQUEST_ID,
    analysis_parsed.get("recommendation", ""),
    analysis_parsed.get("recommendation_reason", ""),
    target_table,
    target_exists,
    str(len(git_repos)),
    codebase_in_git,
    ",".join(str(wid) for wid in writer_ids),
    str(len(analysis_parsed.get("overlapping_assets", []))),
    str(len(existing_pipelines)),
    analysis_yaml,
    datetime.now(timezone.utc),
)]

report_df = spark.createDataFrame(report_row, schema=report_schema)
report_df.write.format("delta").mode("append").option(
    "mergeSchema", "true"
).saveAsTable(f"{CATALOG}.control.analysis_reports")

print(f"\u2713 Analysis report persisted to {CATALOG}.control.analysis_reports")
print(f"\nRecommendation: {analysis_parsed.get('recommendation')}")
print(f"Reason: {analysis_parsed.get('recommendation_reason')}")
print(f"Risks: {analysis_parsed.get('risks', [])}")
display(spark.table(f"{CATALOG}.control.analysis_reports").where(f"request_id = '{REQUEST_ID}'"))