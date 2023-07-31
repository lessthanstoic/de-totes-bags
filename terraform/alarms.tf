# Alarm for ingestion errors
resource "aws_cloudwatch_log_metric_filter" "alert_for_ingestion_error" {
  name           = "ingestion_error"
  # will need to change the pattern
  pattern        = "ERROR"
  log_group_name = "/aws/lambda/ingestion-alert"

  metric_transformation {
    name      = "IngestionError"
    namespace = "ErrorLogMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "alert_ingestion_errors" {
  alarm_name                = "ingestion-error-alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "IngestionError"
  namespace                 = "ErrorLogMetrics"
  period                    = 600
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "Ingestion error"
  alarm_actions             = [aws_sns_topic.email_topic.arn]
}


# Alarm for transformation errors
resource "aws_cloudwatch_log_metric_filter" "alert_for_transformation_error" {
  name           = "transformation_error"
  # will need to change the pattern
  pattern        = "ERROR"
  log_group_name = "/aws/lambda/transformation-alert"

  metric_transformation {
    name      = "TransformationError"
    namespace = "ErrorLogMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "alert_transformation_errors" {
  alarm_name                = "transformation-error-alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "TransformationError"
  namespace                 = "ErrorLogMetrics"
  period                    = 600
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "Transformation error"
  alarm_actions             = [aws_sns_topic.email_topic.arn]
}

# Alarm for warehousing errors
resource "aws_cloudwatch_log_metric_filter" "alert_for_warehousing_error" {
  name           = "warehousing_error"
  # will need to change the pattern
  pattern        = "ERROR"
  log_group_name = "/aws/lambda/warehousing-alert"

  metric_transformation {
    name      = "WarehousingError"
    namespace = "ErrorLogMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "alert_warehousing_errors" {
  alarm_name                = "warehousing-error-alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "WarehousingError"
  namespace                 = "ErrorLogMetrics"
  period                    = 600
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "Warehousing error"
  alarm_actions             = [aws_sns_topic.email_topic.arn]
}


resource "aws_sns_topic" "email_topic" {
  name           = "email_alert_topic"    
}

resource "aws_sns_topic_subscription" "email_target" {
  topic_arn = aws_sns_topic.email_topic.arn
  protocol = "email"
  endpoint = var.my_email_alerts  
}
