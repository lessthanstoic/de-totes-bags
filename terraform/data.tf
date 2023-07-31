data "aws_caller_identity" "current" { }


data "aws_region" "current" {
  name = "eu-west-2"
}

data "archive_file" "lambda" {
  type        = "zip"
  source_dir = "${path.module}/../python"
  output_path = "${path.module}/../function.zip"
}