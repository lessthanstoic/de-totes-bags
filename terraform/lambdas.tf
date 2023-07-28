resource "aws_lambda_function" "s3_file_reader" {
  function_name = var.ingestion_lambda_name
  filename = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  role = aws_iam_role.iam_for_lambda.arn
  handler = "postgres_data_capture.postgres_data_capture" # pythonfilename.functionname
  runtime = var.pythonversion
  depends_on = [ aws_cloudwatch_log_group.ingestion_lambda_log ]
}

# creates the log group for the lambda

resource "aws_cloudwatch_log_group" "ingestion_lambda_log" {
  name = "${var.ingestion_lambda_name}"
  retention_in_days = 30

}

resource "aws_cloudwatch_log_stream" "ingestion_log_stream" {
  name = "ingest-sql-totes"
  log_group_name = aws_cloudwatch_log_group.ingestion_lambda_log.name
}