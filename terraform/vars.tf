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

variable "ingested_bucket_name" {
    type = string
    default = "ingested-data-vox-indicium"
    description = "S3 bucket name for storage of ingested data directly from database"
}

variable "processed_bucket_name" {
    type = string
    default = "processed-data-vox-indicium"
    description = "S3 bucket name for storage of processed data following data transformation"
}

