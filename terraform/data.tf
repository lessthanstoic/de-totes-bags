data "aws_caller_identity" "current" { }


data "aws_region" "current" {
  name = "eu-west-2"
}

data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/file_reader/reader.py"
  output_path = "${path.module}/../function.zip"
}