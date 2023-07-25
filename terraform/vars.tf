variable "ingestion_lambda_name" {
    type = string
    default = "ingest-sql-totes"
}

variable "transformation_lambda_name" {
    type = string
    default = "trans-to-star-schema"
}

variable "eventbridge_name" {
    type = string
    default = "ingestion-eventbridge"
}

variable "pythonversion" {
    type = string
    default = "python3.10"
}
