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

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      # I presume this works for all lambdas now?
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
}

# attach the cloudwatch policy created above to the lambda IAM role
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
    role = aws_iam_role.iam_for_lambda.name
    policy_arn = aws_iam_policy.cw_policy.arn
}

# Unfortunately we need another policy, even if identical
# https://github.com/hashicorp/terraform/issues/11873
data "aws_iam_policy_document" "another_cw_document" {
  statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {

    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      # I presume this works for all lambdas now?
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
}

# attach the cloudwatch policy created above to the lambda IAM role
resource "aws_iam_role_policy_attachment" "transformation_lambda_cw_policy_attachment" {
    role = aws_iam_role.iam_for_transformation_lambda.name
    policy_arn = aws_iam_policy_document.cw_document.arn
}

# attach the cloudwatch policy created above to the lambda IAM role
resource "aws_iam_role_policy_attachment" "warehousing_lambda_cw_policy_attachment" {
    role = aws_iam_role.iam_for_warehousing_lambda.name
    policy_arn = aws_iam_policy_document.another_cw_document.arn
}

resource "aws_iam_policy" "cw_policy" {
  name        = "cloudwatch-log-policy"
  description = "A policy to give ingestion lambda permissions to log to cloudwatch"
  policy      = data.aws_iam_policy_document.cw_document.json
}


####################################################################################
#
# Secrets Manager
#

resource "aws_iam_role_policy_attachment" "lambda_secretsmanager_policy_attachment" {
    role = aws_iam_role.iam_for_lambda.name
    policy_arn = "arn:aws:iam::170940005209:policy/get_tote_db_credentials"
}


####################################################################################
#
# S3 Write Permission
#

resource "aws_iam_policy" "s3_write_policy" {
  name = "s3-write-to-ingestion-bucket-policy"
  description = "A policy to give ingestion lambda permissions to write to S3 bucket"


  policy = <<EOF
{
"Version": "2012-10-17",
"Statement": [
    {
        "Effect": "Allow",
        "Action": [
            "s3:*"
        ],
        "Resource": "arn:aws:s3:::${var.ingested_bucket_name}/*"
    }
]

}
    EOF
    }



resource "aws_iam_role_policy_attachment" "lambda_S3_write_policy_attachment" {
    role = aws_iam_role.iam_for_lambda.name
    policy_arn = aws_iam_policy.s3_write_policy.arn
}


resource "aws_iam_role_policy_attachment" "lambda_S3_write_policy_attachment_transformation" {
    role = aws_iam_role.iam_for_transformation_lambda.name
    policy_arn = aws_iam_policy.s3_write_policy.arn
}
