# eventbridge
resource "aws_cloudwatch_event_rule" "every_2_minutes" {
  name        = var.eventbridge_name
  description = "Rule to trigger Lambda function every 2 minutes"
  schedule_expression = "rate(2 minutes)"
}


resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.every_2_minutes.name
  target_id = "SendToLambda"
  arn       = "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.ingestion_lambda_name}"
}


resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = var.ingestion_lambda_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_2_minutes.arn
}

# eventbridge - the module way
# module "eventbridge" {
#   source = "terraform-aws-modules/eventbridge/aws"

#   create_bus = false

#   rules = {
#     crons = {
#       description         = "Trigger for Ingestion Lambda"
#       schedule_expression = "rate(10 minutes)"
#     }
#   }

#   targets = {
#     crons = [
#       {
#         name  = "${var.eventbridge_name}"
#         arn = "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.ingestion_lambda_name}"
#       }
#     ]
#   }
# }