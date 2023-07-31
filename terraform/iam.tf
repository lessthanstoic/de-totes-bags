##############################################################################
#
# lambdas
#

# Lambda 1: Ingestion Function
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


# Lambda 2: Transformation Function
#
# We attached the same role as above
resource "aws_iam_role" "iam_for_transformation_lambda" {
  name               = "role-${var.transformation_lambda_name}"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}


# Lambda 3: Warehousing Function
#
# We attached the same role as above
resource "aws_iam_role" "iam_for_warehousing_lambda" {
  name               = "role-${var.warehousing_lambda_name}"
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
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.ingestion_lambda_name}:*",
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.transformation_lambda_name}:*",
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.warehousing_lambda_name}:*",
    ]
  }
}

# attach the cloudwatch policy created above to the lambda IAM role
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
    role = aws_iam_role.iam_for_lambda.name
    policy_arn = data.aws_iam_policy_document.cw_document.arn
}

# attach the cloudwatch policy created above to the lambda IAM role
resource "aws_iam_role_policy_attachment" "transformation_lambda_cw_policy_attachment" {
    role = aws_iam_role.iam_for_transformation_lambda.name
    policy_arn = aws_iam_policy_document.cw_document.arn
}

# attach the cloudwatch policy created above to the lambda IAM role
resource "aws_iam_role_policy_attachment" "warehousing_lambda_cw_policy_attachment" {
    role = aws_iam_role.iam_for_warehousing_lambda.name
    policy_arn = aws_iam_policy_document.cw_document.arn
}



