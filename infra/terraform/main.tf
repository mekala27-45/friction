# The catalog objects of friction, at workspace level. Free Edition has no account console and the
# API refuses to create a catalog without a storage location (results/workspace_probe.json), so
# each environment is a set of schemas with a prefix under the workspace catalog.
#
# State lives in Neon through the pg backend (schema terraform); the connection string comes from
# the PG_CONN_STR environment variable in GitHub Actions and nowhere else. Plan runs on every push;
# apply only from a committed request (requests/*.json, kind terraform_apply).

terraform {
  required_version = ">= 1.6"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.90"
    }
  }
  backend "pg" {
    schema_name = "terraform"
  }
}

provider "databricks" {}

variable "environment" {
  type        = string
  description = "dev or prod"
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment is dev or prod"
  }
}

variable "catalog" {
  type    = string
  default = "workspace"
}

variable "reader_group" {
  type        = string
  default     = "account users"
  description = "The account level group that reads gold through masks and the row filter; Unity Catalog grants take account principals only, so the workspace group users is refused"
}

locals {
  prefix = "friction_${var.environment}"
  layers = {
    raw    = "Volumes only: the outbox upstream writes to, the landing zone, and exports"
    bronze = "Every drop as it landed, text, with its file; restricted columns masked"
    silver = "Typed, conformed, deduplicated; the quarantine beside it"
    cdc    = "Card and label history as SCD type 2, labels on two clocks"
    gold   = "Point in time features, training sets, scores, decisions and metrics"
    ml     = "Models in Unity Catalog with champion and challenger aliases"
    ops    = "Run ledger, SLOs, drift, alerts, the compute ledger and entitlements"
  }
  volumes = {
    outbox  = "What upstream delivers, before it lands"
    landing = "The landing zone Auto Loader reads"
    exports = "Scored sets and results the repository grades"
  }
}

resource "databricks_schema" "layer" {
  for_each     = local.layers
  catalog_name = var.catalog
  name         = "${local.prefix}_${each.key}"
  comment      = each.value
  properties = {
    project     = "friction"
    environment = var.environment
    layer       = each.key
  }
}

resource "databricks_volume" "raw" {
  for_each     = local.volumes
  catalog_name = var.catalog
  schema_name  = databricks_schema.layer["raw"].name
  name         = each.key
  volume_type  = "MANAGED"
  comment      = each.value
}

# The reader group may use gold and read through the masks; it never sees bronze.
resource "databricks_grant" "gold_use" {
  schema     = "${var.catalog}.${databricks_schema.layer["gold"].name}"
  principal  = var.reader_group
  privileges = ["USE_SCHEMA", "SELECT"]
}

resource "databricks_grant" "silver_use" {
  schema     = "${var.catalog}.${databricks_schema.layer["silver"].name}"
  principal  = var.reader_group
  privileges = ["USE_SCHEMA", "SELECT"]
}

output "schemas" {
  value = { for k, s in databricks_schema.layer : k => s.name }
}

output "volumes" {
  value = { for k, v in databricks_volume.raw : k => "/Volumes/${var.catalog}/${v.schema_name}/${v.name}" }
}
