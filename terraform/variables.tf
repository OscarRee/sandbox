variable "databricks_host" {
    description = "Databricks workspace URL"
    type = string
}

variable "catalog_names" {
    description = "List of catalog names to create"
    type        = list(string)
    default     = ["ingest", "bronze", "silver", "gold"]
}

variable "default_schema_name" {
    description = "The default schema name"
    type        = string
    default     = "default"
}