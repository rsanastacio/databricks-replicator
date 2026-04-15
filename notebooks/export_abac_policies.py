# Databricks notebook source
# MAGIC %md
# MAGIC # Export ABAC Policies & Functions
# MAGIC
# MAGIC Exports row filters, column masks, and their corresponding UDFs from a source catalog.
# MAGIC Generates a portable JSON manifest + SQL script for import into another metastore.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("source_catalog", "", "Source Catalog")
dbutils.widgets.text("target_catalog", "", "Target Catalog (for SQL generation)")
dbutils.widgets.text("schemas", "", "Schemas (comma-separated, empty = all)")
dbutils.widgets.text("output_volume", "", "Output Volume (catalog.schema.volume)")

source_catalog = dbutils.widgets.get("source_catalog")
target_catalog = dbutils.widgets.get("target_catalog")
schemas_filter = [s.strip() for s in dbutils.widgets.get("schemas").split(",") if s.strip()]
output_volume = dbutils.widgets.get("output_volume")

assert source_catalog, "source_catalog is required"
assert target_catalog, "target_catalog is required"
assert output_volume, "output_volume is required"

print(f"Source: {source_catalog}")
print(f"Target: {target_catalog}")
print(f"Schemas filter: {schemas_filter or 'ALL'}")
print(f"Output: /Volumes/{output_volume.replace('.', '/')}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discovery

# COMMAND ----------

import json
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# ── Discover schemas ──
all_schemas = [s.name for s in w.schemas.list(catalog_name=source_catalog) if s.name not in ("information_schema", "default")]
schemas_to_export = [s for s in all_schemas if s in schemas_filter] if schemas_filter else all_schemas
print(f"Schemas to export: {schemas_to_export}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Export Functions (UDFs)

# COMMAND ----------

exported_functions = []
function_ddls = []
referenced_functions = set()  # track which functions are needed by ABAC policies

# First pass: discover which functions are referenced by row filters / column masks
print("Discovering ABAC-referenced functions...")
for schema_name in schemas_to_export:
    tables = list(w.tables.list(catalog_name=source_catalog, schema_name=schema_name))
    for tbl in tables:
        try:
            table_info = w.tables.get(full_name=f"{source_catalog}.{schema_name}.{tbl.name}")
        except Exception as e:
            print(f"  WARN: Could not read {source_catalog}.{schema_name}.{tbl.name}: {e}")
            continue

        # Row filter
        if table_info.row_filter and table_info.row_filter.function_name:
            referenced_functions.add(table_info.row_filter.function_name)

        # Column masks
        if table_info.columns:
            for col in table_info.columns:
                if col.mask and col.mask.function_name:
                    referenced_functions.add(col.mask.function_name)

print(f"Found {len(referenced_functions)} referenced functions:")
for fn in sorted(referenced_functions):
    print(f"  - {fn}")

# COMMAND ----------

# Second pass: export DDL only for referenced functions
print("\nExporting function DDLs...")
for fn_full_name in sorted(referenced_functions):
    try:
        ddl_rows = spark.sql(f"SHOW CREATE FUNCTION `{fn_full_name}`").collect()
        ddl = "\n".join(row[0] for row in ddl_rows)

        # Generate target DDL
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Export Row Filters

# COMMAND ----------

exported_row_filters = []
row_filter_sqls = []

print("Exporting row filters...")
for schema_name in schemas_to_export:
    tables = list(w.tables.list(catalog_name=source_catalog, schema_name=schema_name))
    for tbl in tables:
        try:
            table_info = w.tables.get(full_name=f"{source_catalog}.{schema_name}.{tbl.name}")
        except Exception:
            continue

        rf = table_info.row_filter
        if not rf or not rf.function_name:
            continue

        source_fn = rf.function_name
        target_fn = source_fn.replace(f"{source_catalog}.", f"{target_catalog}.", 1)
        input_cols = list(rf.input_column_names) if rf.input_column_names else []
        input_cols_str = ", ".join(input_cols)

        target_table = f"`{target_catalog}`.`{schema_name}`.`{tbl.name}`"
        sql = f"ALTER TABLE {target_table} SET ROW FILTER `{target_fn}` ON ({input_cols_str});"

        exported_row_filters.append({
            "source_table": f"{source_catalog}.{schema_name}.{tbl.name}",
            "target_table": f"{target_catalog}.{schema_name}.{tbl.name}",
            "function_name": source_fn,
            "target_function_name": target_fn,
            "input_column_names": input_cols,
            "sql": sql,
        })
        row_filter_sqls.append(sql)
        print(f"  {source_catalog}.{schema_name}.{tbl.name} -> fn: {source_fn}")

print(f"\nExported {len(exported_row_filters)} row filters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Export Column Masks

# COMMAND ----------

exported_column_masks = []
column_mask_sqls = []

print("Exporting column masks...")
for schema_name in schemas_to_export:
    tables = list(w.tables.list(catalog_name=source_catalog, schema_name=schema_name))
    for tbl in tables:
        try:
            table_info = w.tables.get(full_name=f"{source_catalog}.{schema_name}.{tbl.name}")
        except Exception:
            continue

        if not table_info.columns:
            continue

        for col in table_info.columns:
            if not col.mask or not col.mask.function_name:
                continue

            source_fn = col.mask.function_name
            target_fn = source_fn.replace(f"{source_catalog}.", f"{target_catalog}.", 1)
            using_cols = list(col.mask.using_column_names) if col.mask.using_column_names else []

            target_table = f"`{target_catalog}`.`{schema_name}`.`{tbl.name}`"
            using_clause = f" USING COLUMNS ({', '.join(using_cols)})" if using_cols else ""
            sql = f"ALTER TABLE {target_table} ALTER COLUMN `{col.name}` SET MASK `{target_fn}`{using_clause};"

            exported_column_masks.append({
                "source_table": f"{source_catalog}.{schema_name}.{tbl.name}",
                "target_table": f"{target_catalog}.{schema_name}.{tbl.name}",
                "column_name": col.name,
                "function_name": source_fn,
                "target_function_name": target_fn,
                "using_column_names": using_cols,
                "sql": sql,
            })
            column_mask_sqls.append(sql)
            print(f"  {source_catalog}.{schema_name}.{tbl.name}.{col.name} -> fn: {source_fn}")

print(f"\nExported {len(exported_column_masks)} column masks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("ABAC Export Summary")
print("=" * 60)
print(f"Source catalog:  {source_catalog}")
print(f"Target catalog:  {target_catalog}")
print(f"Schemas:         {len(schemas_to_export)}")
print(f"Functions:       {len(function_ddls)}")
print(f"Row Filters:     {len(exported_row_filters)}")
print(f"Column Masks:    {len(exported_column_masks)}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate Output Files

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
    "functions": exported_functions,
    "row_filters": exported_row_filters,
    "column_masks": exported_column_masks,
    "stats": {
        "functions_count": len(function_ddls),
        "row_filters_count": len(exported_row_filters),
        "column_masks_count": len(exported_column_masks),
    },
}

manifest_path = f"{output_dir}/abac_manifest.json"
dbutils.fs.put(manifest_path, json.dumps(manifest, indent=2, ensure_ascii=False))
print(f"Manifest: {manifest_path}")

# ── SQL import script (ordered: functions first, then row filters, then column masks) ──
sql_lines = []
sql_lines.append(f"-- ABAC Policy Import Script")
sql_lines.append(f"-- Source: {source_catalog} -> Target: {target_catalog}")
sql_lines.append(f"-- Generated: {datetime.now(timezone.utc).isoformat()}")
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

sql_path = f"{output_dir}/import_abac_policies.sql"
dbutils.fs.put(sql_path, "\n".join(sql_lines))
print(f"SQL script: {sql_path}")

# ── Functions-only SQL (for standalone UDF export) ──
if function_ddls:
    fn_sql_path = f"{output_dir}/import_functions_only.sql"
    fn_lines = [f"-- Functions from {source_catalog} -> {target_catalog}", ""]
    for ddl in function_ddls:
        fn_lines.append(ddl + ";")
        fn_lines.append("")
    dbutils.fs.put(fn_sql_path, "\n".join(fn_lines))
    print(f"Functions SQL: {fn_sql_path}")

print(f"\nAll files written to: {output_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output Files
# MAGIC
# MAGIC | File | Description |
# MAGIC |------|-------------|
# MAGIC | `abac_manifest.json` | Full manifest with metadata, DDLs, and bindings |
# MAGIC | `import_abac_policies.sql` | Complete SQL script (functions + row filters + column masks) |
# MAGIC | `import_functions_only.sql` | Only the UDF DDLs |
# MAGIC
# MAGIC ### To import in the target metastore:
# MAGIC 1. Copy the output files to the target workspace
# MAGIC 2. Run `import_abac_policies.sql` in a SQL editor or notebook
# MAGIC 3. Verify with: `SELECT * FROM information_schema.routines WHERE routine_catalog = '<target_catalog>'`
