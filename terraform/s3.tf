# Bucket 1: Ingested Data
# Create s3
# Attach policy
# Add bucket trigger
resource "aws_s3_bucket" "ingested_data_bucket" {
    bucket="ingested-data-vox-indicium"
    force_destroy = true
}

resource "aws_s3_object" "timestamp_text" {
  bucket = aws_s3_bucket.ingested_data_bucket.bucket
  key = "postgres-datetime.txt"
  source = "../deployment/ingestion_function/src/postgres-datetime.txt"
}

resource "aws_s3_bucket_policy" "ingested_data_policy" {
  bucket=aws_s3_bucket.ingested_data_bucket.id
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Principal": {
        "AWS": "arn:aws:iam::170940005209:root"
      },
      "Action": [
        "s3:*"
      ],
      "Effect": "Allow",
      "Resource": "${aws_s3_bucket.ingested_data_bucket.arn}/*"
    }
  ]
}
EOF
}

# resource "aws_s3_bucket_notification" "transform_lambda_trigger" {
#   bucket = aws_s3_bucket.ingested_data_bucket.id
#   lambda_function {
#     lambda_function_arn = aws_lambda_function.data_transform.arn
#     events = ["s3:ObjectCreated:*"]
#   }
# }

# Bucket 2: Transformed Data
# Create s3
# Attach policy
# Add bucket trigger
resource "aws_s3_bucket" "processed_data_bucket" {
    bucket="processed-data-vox-indicium"
    force_destroy = true
}

resource "aws_s3_bucket_policy" "processed_data_policy" {
  bucket=aws_s3_bucket.processed_data_bucket.id
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Principal": {
        "AWS": "arn:aws:iam::170940005209:root"
      },
      "Action": [
        "s3:*"
      ],
      "Effect": "Allow",
      "Resource": "${aws_s3_bucket.processed_data_bucket.arn}/*"
    }
  ]
}
EOF
}
