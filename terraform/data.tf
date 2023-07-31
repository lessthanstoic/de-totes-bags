data "aws_caller_identity" "current" { }


data "aws_region" "current" {
  name = "eu-west-2"
}

# file paths need updating for the 2x lambdas
data "archive_file" "ingestion_lambda" {
  type        = "zip"
  output_path = "${path.module}/files/dotfiles.zip"
  excludes    = ["${path.module}/unwanted.zip"]

  source {
    content  = data.template_file.vimrc.rendered
    filename = ".vimrc"
  }

  source {
    content  = data.template_file.ssh_config.rendered
    filename = ".ssh/config"
  }
}

data "archive_file" "transform_lambda" {
  type        = "zip"
  output_path = "${path.module}/files/dotfiles.zip"
  excludes    = ["${path.module}/unwanted.zip"]

  source {
    content  = data.template_file.vimrc.rendered
    filename = ".vimrc"
  }

  source {
    content  = data.template_file.ssh_config.rendered
    filename = ".ssh/config"
  }
}

data "archive_file" "warehouse_lambda" {
  type        = "zip"
  output_path = "${path.module}/files/dotfiles.zip"
  excludes    = ["${path.module}/unwanted.zip"]

  source {
    content  = data.template_file.vimrc.rendered
    filename = ".vimrc"
  }

  source {
    content  = data.template_file.ssh_config.rendered
    filename = ".ssh/config"
  }
}