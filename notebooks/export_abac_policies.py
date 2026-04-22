# Databricks notebook source
# MAGIC %md
# MAGIC # Export ABAC Policies & Functions
# MAGIC
# MAGIC Exports ABAC policies, legacy row filters, column masks, and their UDFs from a source catalog.
# MAGIC Generates a portable JSON manifest + SQL script for import into another metastore.
# MAGIC
# MAGIC **Export Modes:**
# MAGIC | Mode | What it exports | How |
# MAGIC |------|----------------|-----|
# MAGIC | `all` | ABAC policies + legacy row filters/column masks + UDFs | Combines both approaches below |
# MAGIC | `policies_only` | Only ABAC policies (new framework) at catalog, schema, and table levels | Unity Catalog REST API (`/api/2.1/unity-catalog/effective-policies`) |
# MAGIC | `rls_cls_functions` | Only legacy row filters and column masks bound directly to tables | SDK table metadata (`table_info.row_filter`, `column_info.mask`) + `ALTER TABLE` SQL |
# MAGIC
# MAGIC **When to use each mode:**
# MAGIC - `all` — Default. Use when you're not sure which mechanism the source uses, or when it uses both.
# MAGIC - `policies_only` — Use when ABAC policies were created via the Policies UI or REST API (visible in the **Policies** tab of Catalog Explorer).
# MAGIC - `rls_cls_functions` — Use when row filters/column masks were applied via `ALTER TABLE ... SET ROW FILTER` / `SET MASK` (visible in the **Details** tab of Catalog Explorer).
# MAGIC
# MAGIC **Parameters:**
# MAGIC | Widget | Description | Example |
# MAGIC |--------|-------------|---------|
# MAGIC | `source_catalog` | Catalog to export from | `my_catalog` |
# MAGIC | `target_catalog` | Catalog for import SQL generation | `target_catalog` |
# MAGIC | `schemas` | Comma-separated schemas (empty = all) | `schema1,schema2` |
# MAGIC | `output_volume` | UC Volume for output files | `my_catalog.my_schema.exports` |
# MAGIC | `export_mode` | What to export: `all`, `policies_only`, `rls_cls_functions` | `all` |
# MAGIC | `apply_to_target` | Auto-apply exported policies to target catalog | `false` |
# MAGIC | `tables_filter` | Comma-separated tables (empty = all in schema) | `table1,table2` |
# MAGIC | `table_name_map` | Remap table names on import (`source:target` pairs) | `tbl_origin:tbl_target` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("source_catalog", "", "Source Catalog")
dbutils.widgets.text("target_catalog", "", "Target Catalog (for SQL generation)")
dbutils.widgets.text("schemas", "", "Schemas (comma-separated, empty = all)")
dbutils.widgets.text("output_volume", "", "Output Volume (catalog.schema.volume)")
dbutils.widgets.dropdown("export_mode", "all", ["all", "policies_only", "rls_cls_functions"], "Export Mode")
dbutils.widgets.dropdown("apply_to_target", "false", ["true", "false"], "Apply to Target")
dbutils.widgets.text("tables_filter", "", "Tables Filter (comma-separated, empty = all)")
dbutils.widgets.text("table_name_map", "", "Table Name Map (source:target, e.g. tbl_a:tbl_b)")

source_catalog = dbutils.widgets.get("source_catalog")
target_catalog = dbutils.widgets.get("target_catalog")
schemas_filter = [s.strip() for s in dbutils.widgets.get("schemas").split(",") if s.strip()]
output_volume = dbutils.widgets.get("output_volume")
export_mode = dbutils.widgets.get("export_mode")
apply_to_target = dbutils.widgets.get("apply_to_target") == "true"
tables_filter = [t.strip() for t in dbutils.widgets.get("tables_filter").split(",") if t.strip()]

# Parse table name map (source_table:target_table pairs)
table_name_map = {}
raw_map = dbutils.widgets.get("table_name_map")
if raw_map.strip():
    for pair in raw_map.split(","):
        parts = pair.strip().split(":")
        if len(parts) == 2:
            table_name_map[parts[0].strip()] = parts[1].strip()

assert source_catalog, "source_catalog is required"
assert target_catalog, "target_catalog is required"
assert output_volume, "output_volume is required"

print(f"Source:          {source_catalog}")
print(f"Target:          {target_catalog}")
print(f"Schemas:         {schemas_filter or 'ALL'}")
print(f"Tables:          {tables_filter or 'ALL'}")
print(f"Table name map:  {table_name_map or 'NONE (same names)'}")
print(f"Export mode:     {export_mode}")
print(f"Apply to target: {apply_to_target}")
print(f"Output:          /Volumes/{output_volume.replace('.', '/')}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

import json
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def quote_fn_name(full_name: str) -> str:
    """Quote a fully-qualified name as `catalog`.`schema`.`object`."""
    parts = full_name.split(".")
    return ".".join(f"`{p}`" for p in parts)

def remap_catalog(name: str) -> str:
    """Replace source catalog with target catalog in a fully-qualified name."""
    return name.replace(f"{source_catalog}.", f"{target_catalog}.", 1)

def remap_table(full_name: str) -> str:
    """Remap catalog and optionally table name using table_name_map."""
    remapped = remap_catalog(full_name)
    for src_tbl, tgt_tbl in table_name_map.items():
        remapped = remapped.replace(f".{src_tbl}", f".{tgt_tbl}")
    return remapped

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover Schemas & Tables

# COMMAND ----------

all_schemas = [
    s.name for s in w.schemas.list(catalog_name=source_catalog)
    if s.name not in ("information_schema", "default")
]
schemas_to_export = [s for s in all_schemas if s in schemas_filter] if schemas_filter else all_schemas
print(f"Schemas to export: {schemas_to_export}")

# Build table list per schema
schema_tables = {}
for schema_name in schemas_to_export:
    try:
        all_tables = list(w.tables.list(catalog_name=source_catalog, schema_name=schema_name))
        if tables_filter:
            schema_tables[schema_name] = [t for t in all_tables if t.name in tables_filter]
        else:
            schema_tables[schema_name] = all_tables
        print(f"  {schema_name}: {len(schema_tables[schema_name])} tables")
    except Exception as e:
        print(f"  WARN: Could not list tables in {schema_name}: {e}")
        schema_tables[schema_name] = []

total_tables = sum(len(v) for v in schema_tables.values())
print(f"\nTotal tables to scan: {total_tables}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Export ABAC Policies (New Framework)

# COMMAND ----------

exported_policies = []
seen_policy_ids = set()

def _fetch_policies(securable_type: str, securable_full_name: str, label: str):
    """Fetch ABAC policies for a given securable (CATALOG, SCHEMA, or TABLE)."""
    fetched = []
    try:
        r = w.api_client.do(
            "GET", "/api/2.1/unity-catalog/effective-policies",
            query={"securable_type": securable_type, "securable_full_name": securable_full_name}
        )
        fetched = r.get("policies", [])
    except Exception:
        try:
            r = w.api_client.do(
                "GET", "/api/2.1/unity-catalog/policies",
                query={"securable_type": securable_type, "securable_full_name": securable_full_name}
            )
            fetched = r.get("policies", [])
        except Exception:
            pass
    # Deduplicate by policy id
    for p in fetched:
        pid = p.get("id", "")
        if pid and pid in seen_policy_ids:
            continue
        if pid:
            seen_policy_ids.add(pid)
        p["_source_securable"] = securable_full_name
        p["_source_securable_type"] = securable_type
        p["_target_securable"] = remap_table(securable_full_name)
        exported_policies.append(p)
        print(f"  [{securable_type}] {securable_full_name}: {p.get('name','?')} ({p.get('policy_type','?')})")

if export_mode in ("all", "policies_only"):
    print("Exporting ABAC policies via REST API...")

    # Level 1: Catalog-level policies
    print(f"\n-- Catalog: {source_catalog}")
    _fetch_policies("CATALOG", source_catalog, f"catalog:{source_catalog}")

    # Level 2: Schema-level policies
    for schema_name in schemas_to_export:
        full_schema = f"{source_catalog}.{schema_name}"
        print(f"\n-- Schema: {full_schema}")
        _fetch_policies("SCHEMA", full_schema, f"schema:{full_schema}")

    # Level 3: Table-level policies
    for schema_name in schemas_to_export:
        for tbl in schema_tables[schema_name]:
            full_name = f"{source_catalog}.{schema_name}.{tbl.name}"
            _fetch_policies("TABLE", full_name, f"table:{full_name}")

    print(f"\nExported {len(exported_policies)} ABAC policies (catalog + schema + table levels)")
else:
    print("Skipping ABAC policies (export_mode = rls_cls_functions)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Export Legacy Row Filters & Column Masks

# COMMAND ----------

exported_row_filters = []
exported_column_masks = []
referenced_functions = set()
row_filter_sqls = []
column_mask_sqls = []

if export_mode in ("all", "rls_cls_functions"):
    print("Exporting legacy row filters & column masks...")
    for schema_name in schemas_to_export:
        for tbl in schema_tables[schema_name]:
            full_name = f"{source_catalog}.{schema_name}.{tbl.name}"
            try:
                table_info = w.tables.get(full_name=full_name)
            except Exception as e:
                print(f"  WARN: Could not read {full_name}: {e}")
                continue

            # Row filter
            rf = table_info.row_filter
            if rf and rf.function_name:
                referenced_functions.add(rf.function_name)
                target_fn = remap_catalog(rf.function_name)
                input_cols = list(rf.input_column_names) if rf.input_column_names else []
                target_full = remap_table(full_name)
                target_table_quoted = quote_fn_name(target_full)
                sql = f"ALTER TABLE {target_table_quoted} SET ROW FILTER {quote_fn_name(target_fn)} ON ({', '.join(input_cols)});"
                exported_row_filters.append({
                    "source_table": full_name,
                    "target_table": target_full,
                    "function_name": rf.function_name,
                    "target_function_name": target_fn,
                    "input_column_names": input_cols,
                    "sql": sql,
                })
                row_filter_sqls.append(sql)
                print(f"  ROW FILTER: {full_name} -> {target_full}")

            # Column masks
            if table_info.columns:
                for col in table_info.columns:
                    if not col.mask or not col.mask.function_name:
                        continue
                    referenced_functions.add(col.mask.function_name)
                    target_fn = remap_catalog(col.mask.function_name)
                    using_cols = list(col.mask.using_column_names) if col.mask.using_column_names else []
                    target_full = remap_table(full_name)
                    target_table_quoted = quote_fn_name(target_full)
                    using_clause = f" USING COLUMNS ({', '.join(using_cols)})" if using_cols else ""
                    sql = f"ALTER TABLE {target_table_quoted} ALTER COLUMN `{col.name}` SET MASK {quote_fn_name(target_fn)}{using_clause};"
                    exported_column_masks.append({
                        "source_table": full_name,
                        "target_table": target_full,
                        "column_name": col.name,
                        "function_name": col.mask.function_name,
                        "target_function_name": target_fn,
                        "using_column_names": using_cols,
                        "sql": sql,
                    })
                    column_mask_sqls.append(sql)
                    print(f"  COL MASK:   {full_name}.{col.name} -> {target_full}")

    print(f"\nLegacy row filters: {len(exported_row_filters)}")
    print(f"Legacy column masks: {len(exported_column_masks)}")
    print(f"Referenced functions: {len(referenced_functions)}")
else:
    print("Skipping legacy RLS/CLS (export_mode = policies_only)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Export Functions (UDFs)

# COMMAND ----------

exported_functions = []
function_ddls = []

# Also collect functions referenced by ABAC policies
for p in exported_policies:
    rf = p.get("row_filter", {})
    if rf and rf.get("function_name"):
        referenced_functions.add(rf["function_name"])
    cm = p.get("column_mask", {})
    if cm and cm.get("function_name"):
        referenced_functions.add(cm["function_name"])

if referenced_functions:
    print(f"Exporting {len(referenced_functions)} function DDLs...")
    for fn_full_name in sorted(referenced_functions):
        try:
            quoted_fn = quote_fn_name(fn_full_name)
            desc_rows = spark.sql(f"DESCRIBE FUNCTION EXTENDED {quoted_fn}").collect()

            fn_meta = {}
            for row in desc_rows:
                line = row[0] if row else ""
                for key in ["Function:", "Input:", "Returns:", "Body:"]:
                    if line.startswith(key):
                        fn_meta[key.rstrip(":")] = line[len(key):].strip()

            body = fn_meta.get("Body", "")
            input_params = fn_meta.get("Input", "")
            returns = fn_meta.get("Returns", "STRING")

            if not body:
                raise ValueError(f"No body found in DESCRIBE for {fn_full_name}")

            ddl = f"CREATE FUNCTION {quoted_fn}({input_params})\nRETURNS {returns}\nRETURN\n  {body}"
            target_ddl = ddl.replace(f"`{source_catalog}`", f"`{target_catalog}`")
            target_ddl = target_ddl.replace(f"{source_catalog}.", f"{target_catalog}.")
            target_ddl = target_ddl.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)

            parts = fn_full_name.split(".")
            exported_functions.append({
                "source_full_name": fn_full_name,
                "catalog": parts[0] if len(parts) == 3 else source_catalog,
                "schema": parts[1] if len(parts) == 3 else "",
                "name": parts[-1],
                "source_ddl": ddl,
                "target_ddl": target_ddl,
            })
            function_ddls.append(target_ddl)
            print(f"  OK: {fn_full_name}")
        except Exception as e:
            print(f"  ERROR: {fn_full_name}: {e}")
            exported_functions.append({
                "source_full_name": fn_full_name,
                "name": fn_full_name.split(".")[-1],
                "error": str(e),
            })

    print(f"\nExported {len(function_ddls)} function DDLs")
else:
    print("No functions to export")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("ABAC Export Summary")
print("=" * 60)
print(f"Source catalog:    {source_catalog}")
print(f"Target catalog:    {target_catalog}")
print(f"Schemas:           {len(schemas_to_export)}")
print(f"Tables scanned:    {total_tables}")
print(f"Export mode:       {export_mode}")
print(f"ABAC Policies:     {len(exported_policies)}")
print(f"Legacy Row Filters:{len(exported_row_filters)}")
print(f"Legacy Col Masks:  {len(exported_column_masks)}")
print(f"Functions (UDFs):  {len(function_ddls)}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. (Optional) Apply to Target

# COMMAND ----------

applied_results = []

if apply_to_target:
    print("Applying ABAC to target...")

    # Step 1: Create functions
    for ddl in function_ddls:
        try:
            spark.sql(ddl)
            print(f"  FUNCTION OK")
            applied_results.append({"type": "function", "status": "success"})
        except Exception as e:
            print(f"  FUNCTION ERROR: {e}")
            applied_results.append({"type": "function", "status": "failed", "error": str(e)})

    # Step 2: Apply legacy row filters
    for sql in row_filter_sqls:
        try:
            spark.sql(sql.rstrip(";"))
            print(f"  ROW FILTER OK: {sql[:80]}")
            applied_results.append({"type": "row_filter", "status": "success"})
        except Exception as e:
            print(f"  ROW FILTER ERROR: {e}")
            applied_results.append({"type": "row_filter", "status": "failed", "error": str(e)})

    # Step 3: Apply legacy column masks
    for sql in column_mask_sqls:
        try:
            spark.sql(sql.rstrip(";"))
            print(f"  COL MASK OK: {sql[:80]}")
            applied_results.append({"type": "column_mask", "status": "success"})
        except Exception as e:
            print(f"  COL MASK ERROR: {e}")
            applied_results.append({"type": "column_mask", "status": "failed", "error": str(e)})

    # Step 4: Recreate ABAC policies on target
    for p in exported_policies:
        try:
            policy_body = {k: v for k, v in p.items() if not k.startswith("_")}
            # Remap securable to target (catalog + table name)
            if "on_securable_fullname" in policy_body:
                policy_body["on_securable_fullname"] = remap_table(policy_body["on_securable_fullname"])
            # Remap function names
            for section in ("row_filter", "column_mask"):
                if section in policy_body and "function_name" in policy_body[section]:
                    policy_body[section]["function_name"] = remap_catalog(policy_body[section]["function_name"])
            # Remove read-only fields
            for field in ("id", "created_at", "updated_at", "created_by", "updated_by"):
                policy_body.pop(field, None)

            w.api_client.do("POST", "/api/2.1/unity-catalog/policies", body=policy_body)
            print(f"  POLICY OK: {p.get('name', '?')}")
            applied_results.append({"type": "policy", "status": "success", "name": p.get("name")})
        except Exception as e:
            print(f"  POLICY ERROR: {p.get('name', '?')}: {e}")
            applied_results.append({"type": "policy", "status": "failed", "name": p.get("name"), "error": str(e)})

    ok = sum(1 for r in applied_results if r["status"] == "success")
    fail = sum(1 for r in applied_results if r["status"] == "failed")
    print(f"\nApplied: {ok} success, {fail} failed")
else:
    print("apply_to_target = false, skipping. Set to 'true' to auto-apply.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate Output Files

# COMMAND ----------

timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
volume_path = f"/Volumes/{output_volume.replace('.', '/')}"
output_dir = f"{volume_path}/abac_export_{timestamp}"

dbutils.fs.mkdirs(output_dir)

# ── JSON manifest ──
manifest = {
    "export_timestamp": datetime.now(timezone.utc).isoformat(),
    "source_catalog": source_catalog,
    "target_catalog": target_catalog,
    "schemas": schemas_to_export,
    "export_mode": export_mode,
    "tables_filter": tables_filter or "ALL",
    "functions": exported_functions,
    "abac_policies": exported_policies,
    "legacy_row_filters": exported_row_filters,
    "legacy_column_masks": exported_column_masks,
    "applied_results": applied_results if apply_to_target else [],
    "stats": {
        "functions_count": len(function_ddls),
        "abac_policies_count": len(exported_policies),
        "legacy_row_filters_count": len(exported_row_filters),
        "legacy_column_masks_count": len(exported_column_masks),
    },
}

manifest_path = f"{output_dir}/abac_manifest.json"
dbutils.fs.put(manifest_path, json.dumps(manifest, indent=2, ensure_ascii=False))
print(f"Manifest: {manifest_path}")

# ── SQL import script ──
sql_lines = []
sql_lines.append(f"-- ABAC Policy Import Script")
sql_lines.append(f"-- Source: {source_catalog} -> Target: {target_catalog}")
sql_lines.append(f"-- Generated: {datetime.now(timezone.utc).isoformat()}")
sql_lines.append(f"-- Mode: {export_mode}")
sql_lines.append(f"-- Total: {len(function_ddls)} functions, {len(exported_row_filters)} row filters, {len(exported_column_masks)} column masks")
sql_lines.append("")

if function_ddls:
    sql_lines.append("-- " + "=" * 58)
    sql_lines.append("-- STEP 1: Create/Replace Functions (UDFs)")
    sql_lines.append("-- " + "=" * 58)
    sql_lines.append("")
    for ddl in function_ddls:
        sql_lines.append(ddl + ";")
        sql_lines.append("")

if row_filter_sqls:
    sql_lines.append("-- " + "=" * 58)
    sql_lines.append("-- STEP 2: Apply Row Filters")
    sql_lines.append("-- " + "=" * 58)
    sql_lines.append("")
    for sql in row_filter_sqls:
        sql_lines.append(sql)
        sql_lines.append("")

if column_mask_sqls:
    sql_lines.append("-- " + "=" * 58)
    sql_lines.append("-- STEP 3: Apply Column Masks")
    sql_lines.append("-- " + "=" * 58)
    sql_lines.append("")
    for sql in column_mask_sqls:
        sql_lines.append(sql)
        sql_lines.append("")

if exported_policies:
    sql_lines.append("-- " + "=" * 58)
    sql_lines.append("-- NOTE: ABAC Policies (new framework) are in the JSON manifest")
    sql_lines.append("-- They must be applied via the REST API, not SQL.")
    sql_lines.append("-- Use apply_to_target=true or the API directly.")
    sql_lines.append("-- " + "=" * 58)
    sql_lines.append("")

sql_path = f"{output_dir}/import_abac_policies.sql"
dbutils.fs.put(sql_path, "\n".join(sql_lines))
print(f"SQL script: {sql_path}")

# ── Functions-only SQL ──
if function_ddls:
    fn_sql_path = f"{output_dir}/import_functions_only.sql"
    fn_lines = [f"-- Functions from {source_catalog} -> {target_catalog}", ""]
    for ddl in function_ddls:
        fn_lines.append(ddl + ";")
        fn_lines.append("")
    dbutils.fs.put(fn_sql_path, "\n".join(fn_lines))
    print(f"Functions SQL: {fn_sql_path}")

# ── ABAC Policies JSON (for API import) ──
if exported_policies:
    policies_path = f"{output_dir}/abac_policies_api.json"
    dbutils.fs.put(policies_path, json.dumps(exported_policies, indent=2, ensure_ascii=False))
    print(f"ABAC Policies JSON: {policies_path}")

print(f"\nAll files written to: {output_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Files
# MAGIC
# MAGIC | File | Description |
# MAGIC |------|-------------|
# MAGIC | `abac_manifest.json` | Full manifest with all metadata |
# MAGIC | `import_abac_policies.sql` | SQL script for legacy row filters + column masks |
# MAGIC | `import_functions_only.sql` | Only the UDF DDLs |
# MAGIC | `abac_policies_api.json` | ABAC policies for REST API import |
# MAGIC
# MAGIC ### To import in the target metastore:
# MAGIC
# MAGIC **Option A — Auto-apply:** Re-run this notebook with `apply_to_target = true`
# MAGIC
# MAGIC **Option B — Manual:**
# MAGIC 1. Copy output files to the target workspace
# MAGIC 2. Run `import_abac_policies.sql` for legacy RLS/CLS
# MAGIC 3. Use `abac_policies_api.json` with the REST API for ABAC policies
# MAGIC 4. Verify: `DESCRIBE EXTENDED <target_table>`
