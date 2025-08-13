terraform {
    required_providers {
        databricks = {
            source = "databricks/databricks"
            version = "~> 1.0"
        }
    }
}

provider "databricks" {
    host = var.databricks_host
}

resource "databricks_schema" "ingest_statbank" {
    catalog_name = databricks_catalog.ingest.name
    name         = "statbank"
}