#
# eventbridges
#

##############################################################################
# Cloudwatch on timer to trigger the lambda 1: Ingestion
#
resource "aws_cloudwatch_event_rule" "every_20_minutes" {
  name        = var.eventbridge_name
  description = "Rule to trigger Lambda function every 20 minutes"

  schedule_expression = "rate(10 minute)"
  
  tags = {
    Environment = "Extract"
    Project     = "Totesys"
    Owner       = "Project_team_1"
  }
}


resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.every_20_minutes.name
  target_id = "SendToLambda"
  arn       = "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.ingestion_lambda_name}"
}


resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = var.ingestion_lambda_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_20_minutes.arn
}


##############################################################################
# Cloudwatch on s3 data upload to trigger the lambda 2: Transformation
# Trigger is on upload of .txt file
#

resource "aws_s3_bucket_notification" "aws-lambda-trigger" {
  bucket = var.ingested_bucket_name
  depends_on = [ aws_s3_bucket.ingested_data_bucket ]
  
  lambda_function {
      lambda_function_arn = aws_lambda_function.data_transform.arn
      events              = ["s3:ObjectCreated:*"]
      filter_suffix       = ".txt"
  }
}


resource "aws_lambda_permission" "allow_trans_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridgeTrans"
  action        = "lambda:InvokeFunction"
  function_name = var.transformation_lambda_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.ingested_data_bucket.arn
}

##############################################################################
# Cloudwatch on s3 data upload to trigger the lambda 3: Warehousing
# Trigger is on upload of parquet file
#

resource "aws_lambda_permission" "allow_warehouse_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridgeLoad"
  action        = "lambda:InvokeFunction"
  function_name = var.warehousing_lambda_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.processed_data_bucket.arn
}

resource "aws_s3_bucket_notification" "aws-load-trigger" {
  bucket = var.processed_bucket_name
  lambda_function {
      lambda_function_arn = aws_lambda_function.data_warehouse.arn
      events              = ["s3:ObjectCreated:*"]
      filter_suffix       = ".txt"
  }
}
