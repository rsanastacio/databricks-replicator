variable "source_host" {
  description = "Source Databricks workspace URL"
  type        = string
}

variable "target_host" {
  description = "Target Databricks workspace URL"
  type        = string
}

variable "source_catalog" {
  description = "Source catalog name"
  type        = string
}

variable "target_catalog" {
  description = "Target catalog name"
  type        = string
}

variable "schemas" {
  description = "List of schemas to replicate ABAC policies from. Empty = all schemas."
  type        = list(string)
  default     = []
}

variable "warehouse_id" {
  description = "SQL warehouse ID on the target workspace for executing DDL"
  type        = string
}
