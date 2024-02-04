locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "us-west1"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  type = list(object({
    id          = string
    description = string
  }))
}

# variable "TABLE_NAME" {
#   description = "BigQuery Table"
#   type = string
#   default = "ny_trips"
# }