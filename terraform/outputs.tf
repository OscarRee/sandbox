output "catalog_names" {
    description = "List of catalog names"
    value = [for catalog in databricks_catalog.catalogs : catalog.name]
}

output "schema_names" {
    description = "List of schema names"
    value = [for schema in databricks_schema.default_schemas : "${schema.catalog_name}.${schema.name}"]
}