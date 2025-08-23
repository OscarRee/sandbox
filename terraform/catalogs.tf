#Create all catalogs using for_each
resource "databricks_catalog" "catalogs" {
    for_each = toset(var.catalog_names)
    name = each.value
}

