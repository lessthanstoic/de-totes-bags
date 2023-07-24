resource "aws_lambda_function" "s3_file_reader" {
  function_name = var.ingestion_lambda_name
  filename = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  role = aws_iam_role.lambda_role.arn
  handler = "reader.lambda_handler" # pythonfilename.functionname
  runtime = "python3.9"
}