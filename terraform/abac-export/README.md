# ABAC Policy Terraform Export

Generates Terraform HCL files from existing ABAC policies (row filters, column masks, and their UDFs) in a Databricks Unity Catalog.

## Quick Start

```bash
# 1. Generate .tf files from source catalog
python generate_tf.py \
  --source-host https://source.cloud.databricks.com \
  --source-catalog my_catalog \
  --target-catalog target_catalog \
  --schemas schema1,schema2 \
  --warehouse-id <warehouse-id> \
  --output-dir ./generated \
  --profile my-profile

# 2. Review generated files
ls generated/
#   functions.tf       — UDF creation via SQL
#   row_filters.tf     — Row filter bindings
#   column_masks.tf    — Column mask bindings
#   terraform.tfvars   — Pre-filled variables
#   abac_manifest.json — Export manifest

# 3. Copy providers.tf and variables.tf to generated/
cp providers.tf variables.tf generated/

# 4. Apply
cd generated
terraform init
terraform plan
terraform apply
```

## What It Does

1. **Discovers** ABAC policies on tables in the source catalog via the Databricks SDK
2. **Exports** DDL for referenced UDFs via `SHOW CREATE FUNCTION`
3. **Generates** Terraform HCL with proper dependency ordering:
   - `functions.tf` — Creates UDFs first (via `databricks_sql_query`)
   - `row_filters.tf` — Applies row filters (depends on functions)
   - `column_masks.tf` — Applies column masks (depends on functions)

## Limitations

- No `databricks_function` resource exists yet — UDFs are created via `databricks_sql_query`
- `databricks_sql_table` does not support row_filter/column_mask attributes — using ALTER TABLE via SQL
- Functions that reference other functions may need manual dependency ordering
