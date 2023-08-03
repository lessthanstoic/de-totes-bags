data "aws_caller_identity" "current" { }


data "aws_region" "current" {
  name = "eu-west-2"
}

data "archive_file" "ingestion_lambda" {
  type        = "zip"
  source_dir = "${path.module}/../python/ingestion_function/src" 
  output_path = "${path.module}/../ingestion_function.zip"
}


data "archive_file" "transform_lambda" {
  type        = "zip"
  source_dir = "${path.module}/../python/transformation_function/src"
  output_path = "${path.module}/../transformation_function.zip"
}


data "archive_file" "warehouse_lambda" {
  type        = "zip"
  source_dir = "${path.module}/../python/loading_function/src"
  output_path = "${path.module}/../loading_function.zip"
}

