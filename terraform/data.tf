data "aws_caller_identity" "current" { }


data "aws_region" "current" {
  name = "eu-west-2"
}

# file paths need updating for the lambdas when in sub-folders
data "archive_file" "ingestion_lambda" {
  type        = "zip"
  source_dir = "${path.module}/../python"
  output_path = "${path.module}/../function.zip"
}


data "archive_file" "transform_lambda" {
  type        = "zip"
  source_dir = "${path.module}/../python"
  output_path = "${path.module}/../function.zip"
}


data "archive_file" "warehouse_lambda" {
  type        = "zip"
  source_dir = "${path.module}/../python"
  output_path = "${path.module}/../function.zip"
}

