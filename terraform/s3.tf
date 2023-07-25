resource "aws_s3_bucket" "ingested_data_bucket" {
    bucket="ingested-data-vox-indicium"
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

resource "aws_s3_bucket" "processed_data_bucket" {
    bucket="processed-data-vox-indicium"
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
