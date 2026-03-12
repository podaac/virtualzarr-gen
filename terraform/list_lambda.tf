data "archive_file" "list_lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/list_lambda.py"
  output_path = "${path.module}/list_lambda.zip"
}

resource "aws_lambda_function" "s3_list" {
  function_name = "virtualizarr-${var.stage}-s3-bucket-list"
  role          = aws_iam_role.app_task_exec.arn

  handler = "list_lambda.handler"
  runtime = "python3.12"
  layers  = ["arn:aws:lambda:us-west-2:336392948345:layer:AWSSDKPandas-Python312:22"]

  timeout     = 300
  memory_size = 128

  filename         = data.archive_file.list_lambda_zip.output_path
  source_code_hash = data.archive_file.list_lambda_zip.output_base64sha256
}
