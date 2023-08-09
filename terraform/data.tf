data "aws_caller_identity" "current" { }


data "aws_region" "current" {
  name = "eu-west-2"
}

data "archive_file" "ingestion_lambda" {
  type        = "zip"
  source_dir = "${path.module}/../deployment/ingestion_function" 
  output_path = "${path.module}/../ingestion_function.zip"
}


data "archive_file" "transform_lambda" {
  type        = "zip"
  source_dir = "${path.module}/../deployment/transformation_function/unzipped/fp_v3.10"
  output_path = "${path.module}/../transformation_function.zip"
}


data "archive_file" "warehouse_lambda" {
  type        = "zip"
  source_dir = "${path.module}/../deployment/loading_function/src/fp_v3.10"
  output_path = "${path.module}/../loading_function.zip"
}

