data "aws_caller_identity" "current" { }


data "aws_region" "current" {
  name = "eu-west-2"
}

# Will need filling in
# data "archive_file" "lambda" {
#   type        = "zip"
#   source_file = "${path.module}/..//python/src/ingestion/reader.py"
#   output_path = "${path.module}/../function.zip"
# }