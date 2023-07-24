resource "aws_s3_bucket" "ingested_data_bucket" {
    bucket="ingested_data_vox_indicium"
}

resource "aws_iam_policy" "ingested_data_policy" {
  name="ingested_data_policy"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
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


resource "aws_iam_user" "ingested_data_user" {
    name="ingested_data_user"
}