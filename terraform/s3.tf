# Bucket 1: Ingested Data
# Create s3
# Attach policy
# Add bucket trigger
resource "aws_s3_bucket" "ingested_data_bucket" {
    #bucket="ingested-data-vox-indicium"
    bucket=var.ingested_bucket_name

      tags = {
      Environment = "Extract"
      Project     = "Totesys"
      Owner       = "Project_team_1"
    }
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

# Bucket 2: Transformed Data
# Create s3
# Attach policy
# Add bucket trigger
resource "aws_s3_bucket" "processed_data_bucket" {
  #bucket="processed-data-vox-indicium"
  bucket=var.processed_bucket_name

  tags = {
    Environment = "Transform"
    Project     = "Totesys"
    Owner       = "Project_team_1"
  }

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
      "Resource": ["${aws_s3_bucket.processed_data_bucket.arn}/*", "${aws_s3_bucket.processed_data_bucket.arn}"]
    }
  ]
}
EOF
}
