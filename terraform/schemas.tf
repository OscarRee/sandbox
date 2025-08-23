resource "databricks_schema" "default_schemas" {
    for_each = toset(var.catalog_names)
    catalog_name = databricks_catalog.catalogs[each.value].name
    name = var.default_schema_name
}