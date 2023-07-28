##############################################################################
#
# lambdas
#

# Create the policy for the lambda ingestion function to use temp security credentials
data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# provides an IAM role for the lambda and attaches the above policy, so it can use the credentials
resource "aws_iam_role" "iam_for_lambda" {
  name               = "role-${var.ingestion_lambda_name}"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}



####################################################################################
#
# Cloud watch
#

# cloudwatch policy (defines the permissions to be attributed to a role) 
# which allows the creation and "put" to the logs
data "aws_iam_policy_document" "cw_document" {

  statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.ingestion_lambda_name}:*"
    ]
  }
}

# attach the cloudwatch policy created above to the lambda IAM role
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
    role = aws_iam_role.iam_for_lambda.name
    policy_arn = aws_iam_policy.cw_policy.arn
}

resource "aws_iam_policy" "cw_policy" {
  name        = "cloudwatch-log-policy"
  description = "A policy to give ingestion lambda permissions to log to cloudwatch"
  policy      = data.aws_iam_policy_document.cw_document.json
}