terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.57.0"
    }
  }
}

# Source workspace provider (reads ABAC policies)
provider "databricks" {
  alias = "source"
  host  = var.source_host
}

# Target workspace provider (applies ABAC policies)
provider "databricks" {
  alias = "target"
  host  = var.target_host
}
