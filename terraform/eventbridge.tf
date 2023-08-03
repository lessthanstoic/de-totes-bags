#
# eventbridges
#

##############################################################################
# Cloudwatch on timer to trigger the lambda 1: Ingestion
#
resource "aws_cloudwatch_event_rule" "every_20_minutes" {
  name        = var.eventbridge_name
  description = "Rule to trigger Lambda function every 20 minutes"
  schedule_expression = "rate(20 minutes)"
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
# Pretty sure this is correct - if its not then its probably around the suffix/key part of the event_pattern
#
resource "aws_cloudwatch_event_rule" "txtfile_to_s3_lambda" {
  name        = var.cloudwatch_upload
  description = "Trigger transformation lambda after upload of txt file to s3"
  event_pattern = <<EOF
  {
    "source": ["aws.s3"],
    "detail-type": ["AWS API Call via CloudTrail"],
    "detail": {
      "eventSource": ["s3.amazonaws.com"],
      "eventName": ["PutObject"],
      "requestParameters": { 
        "bucketName": ["ingested-data-vox-indicium"],
      "key": [{ "suffix": ".txt" }]
      }
    }
  }
  EOF
}


resource "aws_cloudwatch_event_target" "transform_lambda_target" {
  rule      = aws_cloudwatch_event_rule.txtfile_to_s3_lambda.name
  target_id = "CallTransLambda"
  arn       = "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.transformation_lambda_name}"
}


resource "aws_lambda_permission" "allow_trans_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridgeTrans"
  action        = "lambda:InvokeFunction"
  function_name = var.transformation_lambda_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.txtfile_to_s3_lambda.arn
}

##############################################################################
# Cloudwatch on s3 data upload to trigger the lambda 3: Warehousing
# Trigger is on upload of parquet file
#
resource "aws_cloudwatch_event_rule" "parquet_to_s3_lambda" {
  name        = var.cloudwatch_upload
  description = "Trigger transformation lambda after upload of parquet file to s3"
  event_pattern = <<EOF
  {
    "source": ["aws.s3"],
    "detail-type": ["AWS API Call via CloudTrail"],
    "detail": {
      "eventSource": ["s3.amazonaws.com"],
      "eventName": ["PutObject"],
      "requestParameters": { 
        "bucketName": ["processed-data-vox-indicium"],
      "key": [{ "suffix": ".parquet" }]
      }
    }
  }
  EOF
}


resource "aws_cloudwatch_event_target" "warehouse_lambda_target" {
  rule      = aws_cloudwatch_event_rule.parquet_to_s3_lambda.name
  target_id = "CallTransLambda"
  arn       = "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.warehousing_lambda_name}"
}


resource "aws_lambda_permission" "allow_warehouse_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridgeLoad"
  action        = "lambda:InvokeFunction"
  function_name = var.warehousing_lambda_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.parquet_to_s3_lambda.arn
}