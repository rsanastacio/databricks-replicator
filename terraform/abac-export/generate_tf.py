"""
ABAC Policy Terraform Generator

Reads ABAC policies (row filters, column masks) and their referenced UDFs
from a source Databricks catalog and generates Terraform HCL files for
importing them into a target catalog.

Usage:
    python generate_tf.py \
        --source-host https://source.cloud.databricks.com \
        --source-catalog my_catalog \
        --target-catalog target_catalog \
        --schemas schema1,schema2 \
        --warehouse-id abc123 \
        --output-dir ./generated

Generates:
    - functions.tf     — UDF creation via databricks_sql_query
    - row_filters.tf   — Row filter policies via ALTER TABLE
    - column_masks.tf  — Column mask policies via ALTER TABLE
    - terraform.tfvars — Pre-filled variables
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone

try:
    from databricks.sdk import WorkspaceClient
except ImportError:
    print("ERROR: databricks-sdk not installed. Run: pip install databricks-sdk")
    sys.exit(1)


def sanitize_tf_name(name: str) -> str:
    """Convert a dotted name to a valid Terraform resource name."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


def escape_hcl(text: str) -> str:
    """Escape a string for HCL heredoc content."""
    return text.replace("${", "$${").replace("%{", "%%{")


def discover_abac(w: WorkspaceClient, catalog: str, schemas: list[str]) -> dict:
    """Discover all ABAC policies and referenced functions in a catalog."""
    all_schemas = [
        s.name for s in w.schemas.list(catalog_name=catalog)
        if s.name not in ("information_schema", "default")
    ]
    target_schemas = [s for s in all_schemas if s in schemas] if schemas else all_schemas

    referenced_functions = set()
    row_filters = []
    column_masks = []

    for schema_name in target_schemas:
        print(f"  Scanning {catalog}.{schema_name}...")
        try:
            tables = list(w.tables.list(catalog_name=catalog, schema_name=schema_name))
        except Exception as e:
            print(f"    WARN: Could not list tables: {e}")
            continue

        for tbl in tables:
            try:
                table_info = w.tables.get(full_name=f"{catalog}.{schema_name}.{tbl.name}")
            except Exception:
                continue

            # Row filter
            rf = table_info.row_filter
            if rf and rf.function_name:
                referenced_functions.add(rf.function_name)
                row_filters.append({
                    "table": f"{catalog}.{schema_name}.{tbl.name}",
                    "schema": schema_name,
                    "table_name": tbl.name,
                    "function_name": rf.function_name,
                    "input_column_names": list(rf.input_column_names) if rf.input_column_names else [],
                })

            # Column masks
            if table_info.columns:
                for col in table_info.columns:
                    if col.mask and col.mask.function_name:
                        referenced_functions.add(col.mask.function_name)
                        column_masks.append({
                            "table": f"{catalog}.{schema_name}.{tbl.name}",
                            "schema": schema_name,
                            "table_name": tbl.name,
                            "column_name": col.name,
                            "function_name": col.mask.function_name,
                            "using_column_names": list(col.mask.using_column_names) if col.mask.using_column_names else [],
                        })

    # Get function DDLs via DESCRIBE FUNCTION EXTENDED
    # (SHOW CREATE FUNCTION is not supported on SQL warehouses)
    functions = []
    for fn_name in sorted(referenced_functions):
        try:
            parts = fn_name.split(".")
            quoted_fn = ".".join(f"`{p}`" for p in parts)
            desc_result = w.statement_execution.execute_statement(
                warehouse_id=args.warehouse_id,
                statement=f"DESCRIBE FUNCTION EXTENDED {quoted_fn}",
                wait_timeout="30s",
            )
            rows = desc_result.result.data_array if desc_result.result else []

            # Parse DESCRIBE output to reconstruct DDL
            fn_meta = {}
            for row in rows:
                line = row[0] if row else ""
                for key in ["Function:", "Input:", "Returns:", "Body:"]:
                    if line.startswith(key):
                        fn_meta[key.rstrip(":")] = line[len(key):].strip()

            body = fn_meta.get("Body", "")
            input_params = fn_meta.get("Input", "")
            returns = fn_meta.get("Returns", "STRING")

            if body:
                ddl = f"CREATE OR REPLACE FUNCTION {quoted_fn}({input_params})\nRETURNS {returns}\nRETURN\n  {body}"
                functions.append({"name": fn_name, "ddl": ddl})
                print(f"    Function OK: {fn_name}")
            else:
                print(f"    Function WARN: {fn_name} (no body found)")
                functions.append({"name": fn_name, "ddl": None, "error": "no body in DESCRIBE"})

        except Exception as e:
            print(f"    Function ERROR: {fn_name}: {e}")
            functions.append({"name": fn_name, "ddl": None, "error": str(e)})

    return {
        "schemas": target_schemas,
        "functions": functions,
        "row_filters": row_filters,
        "column_masks": column_masks,
    }


def generate_functions_tf(abac: dict, source_catalog: str, target_catalog: str) -> str:
    """Generate Terraform HCL for UDF creation."""
    lines = [
        "# Auto-generated — ABAC Functions (UDFs)",
        f"# Source: {source_catalog} -> Target: {target_catalog}",
        f"# Generated: {datetime.now(timezone.utc).isoformat()}",
        "#",
        "# NOTE: There is no databricks_function resource yet.",
        "# Using databricks_sql_query to create functions via DDL.",
        "",
    ]

    for func in abac["functions"]:
        if not func.get("ddl"):
            lines.append(f"# SKIPPED: {func['name']} (error: {func.get('error', 'no DDL')})")
            lines.append("")
            continue

        tf_name = sanitize_tf_name(func["name"])
        target_ddl = func["ddl"].replace(
            f"`{source_catalog}`", f"`{target_catalog}`"
        ).replace(
            f"{source_catalog}.", f"{target_catalog}."
        )

        lines.append(f'resource "databricks_sql_query" "fn_{tf_name}" {{')
        lines.append(f'  provider     = databricks.target')
        lines.append(f'  warehouse_id = var.warehouse_id')
        lines.append(f'  sql          = <<-EOQ')
        lines.append(f'    {escape_hcl(target_ddl)}')
        lines.append(f'  EOQ')
        lines.append(f'}}')
        lines.append("")

    return "\n".join(lines)


def generate_row_filters_tf(abac: dict, source_catalog: str, target_catalog: str) -> str:
    """Generate Terraform HCL for row filter application."""
    lines = [
        "# Auto-generated — ABAC Row Filters",
        f"# Source: {source_catalog} -> Target: {target_catalog}",
        f"# Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
    ]

    for rf in abac["row_filters"]:
        tf_name = sanitize_tf_name(f"{rf['schema']}_{rf['table_name']}")
        target_fn = rf["function_name"].replace(f"{source_catalog}.", f"{target_catalog}.", 1)
        fn_parts = target_fn.split(".")
        quoted_fn = ".".join(f"`{p}`" for p in fn_parts)
        input_cols = ", ".join(rf["input_column_names"])

        # Dependency on the function resource
        fn_tf_name = sanitize_tf_name(rf["function_name"])

        lines.append(f'resource "databricks_sql_query" "rf_{tf_name}" {{')
        lines.append(f'  provider     = databricks.target')
        lines.append(f'  warehouse_id = var.warehouse_id')
        lines.append(f'  sql          = "ALTER TABLE `{target_catalog}`.`{rf["schema"]}`.`{rf["table_name"]}` SET ROW FILTER {quoted_fn} ON ({input_cols})"')
        lines.append(f'')
        lines.append(f'  depends_on = [databricks_sql_query.fn_{fn_tf_name}]')
        lines.append(f'}}')
        lines.append("")

    return "\n".join(lines)


def generate_column_masks_tf(abac: dict, source_catalog: str, target_catalog: str) -> str:
    """Generate Terraform HCL for column mask application."""
    lines = [
        "# Auto-generated — ABAC Column Masks",
        f"# Source: {source_catalog} -> Target: {target_catalog}",
        f"# Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
    ]

    for cm in abac["column_masks"]:
        tf_name = sanitize_tf_name(f"{cm['schema']}_{cm['table_name']}_{cm['column_name']}")
        target_fn = cm["function_name"].replace(f"{source_catalog}.", f"{target_catalog}.", 1)
        fn_parts = target_fn.split(".")
        quoted_fn = ".".join(f"`{p}`" for p in fn_parts)

        using_cols = cm.get("using_column_names", [])
        using_clause = f" USING COLUMNS ({', '.join(using_cols)})" if using_cols else ""

        fn_tf_name = sanitize_tf_name(cm["function_name"])

        lines.append(f'resource "databricks_sql_query" "cm_{tf_name}" {{')
        lines.append(f'  provider     = databricks.target')
        lines.append(f'  warehouse_id = var.warehouse_id')
        lines.append(f'  sql          = "ALTER TABLE `{target_catalog}`.`{cm["schema"]}`.`{cm["table_name"]}` ALTER COLUMN `{cm["column_name"]}` SET MASK {quoted_fn}{using_clause}"')
        lines.append(f'')
        lines.append(f'  depends_on = [databricks_sql_query.fn_{fn_tf_name}]')
        lines.append(f'}}')
        lines.append("")

    return "\n".join(lines)


def generate_tfvars(source_catalog: str, target_catalog: str, args) -> str:
    """Generate terraform.tfvars."""
    lines = [
        f'source_host    = "{args.source_host}"',
        f'target_host    = "{args.target_host or args.source_host}"',
        f'source_catalog = "{source_catalog}"',
        f'target_catalog = "{target_catalog}"',
        f'warehouse_id   = "{args.warehouse_id}"',
    ]
    if args.schemas:
        schema_list = ", ".join(f'"{s}"' for s in args.schemas.split(","))
        lines.append(f'schemas        = [{schema_list}]')
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Terraform HCL for ABAC policy replication")
    parser.add_argument("--source-host", required=True, help="Source Databricks workspace URL")
    parser.add_argument("--target-host", default=None, help="Target Databricks workspace URL (default: same as source)")
    parser.add_argument("--source-catalog", required=True, help="Source catalog name")
    parser.add_argument("--target-catalog", required=True, help="Target catalog name")
    parser.add_argument("--schemas", default="", help="Comma-separated list of schemas (empty=all)")
    parser.add_argument("--warehouse-id", required=True, help="SQL warehouse ID for DDL execution")
    parser.add_argument("--output-dir", default="./generated", help="Output directory for .tf files")
    parser.add_argument("--profile", default=None, help="Databricks CLI profile name")
    args = parser.parse_args()

    schemas = [s.strip() for s in args.schemas.split(",") if s.strip()]

    print(f"Connecting to {args.source_host}...")
    w_kwargs = {"host": args.source_host}
    if args.profile:
        w_kwargs["profile"] = args.profile
    w = WorkspaceClient(**w_kwargs)

    print(f"Discovering ABAC in {args.source_catalog}...")
    abac = discover_abac(w, args.source_catalog, schemas)

    print(f"\nFound:")
    print(f"  Functions:    {len(abac['functions'])}")
    print(f"  Row Filters:  {len(abac['row_filters'])}")
    print(f"  Column Masks: {len(abac['column_masks'])}")

    if not any([abac["functions"], abac["row_filters"], abac["column_masks"]]):
        print("\nNo ABAC policies found. Nothing to generate.")
        sys.exit(0)

    os.makedirs(args.output_dir, exist_ok=True)

    # Generate .tf files
    fn_tf = generate_functions_tf(abac, args.source_catalog, args.target_catalog)
    rf_tf = generate_row_filters_tf(abac, args.source_catalog, args.target_catalog)
    cm_tf = generate_column_masks_tf(abac, args.source_catalog, args.target_catalog)
    tfvars = generate_tfvars(args.source_catalog, args.target_catalog, args)

    for filename, content in [
        ("functions.tf", fn_tf),
        ("row_filters.tf", rf_tf),
        ("column_masks.tf", cm_tf),
        ("terraform.tfvars", tfvars),
    ]:
        path = os.path.join(args.output_dir, filename)
        with open(path, "w") as f:
            f.write(content)
        print(f"  Written: {path}")

    # Save manifest
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_catalog": args.source_catalog,
        "target_catalog": args.target_catalog,
        "source_host": args.source_host,
        "target_host": args.target_host or args.source_host,
        "stats": {
            "functions": len(abac["functions"]),
            "row_filters": len(abac["row_filters"]),
            "column_masks": len(abac["column_masks"]),
        },
        "functions": [f["name"] for f in abac["functions"]],
        "row_filters": [{"table": rf["table"], "function": rf["function_name"]} for rf in abac["row_filters"]],
        "column_masks": [{"table": cm["table"], "column": cm["column_name"], "function": cm["function_name"]} for cm in abac["column_masks"]],
    }
    manifest_path = os.path.join(args.output_dir, "abac_manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    print(f"  Written: {manifest_path}")

    print(f"\nDone. Run 'cd {args.output_dir} && terraform init && terraform plan' to preview.")
