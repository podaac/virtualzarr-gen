data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/sync_lambda.py"
  output_path = "${path.module}/sync_lambda.zip"
}

resource "aws_lambda_function" "s3_sync" {
  function_name = "virtualizarr-${var.stage}-s3-bucket-sync"
  role          = aws_iam_role.app_task_exec.arn

  handler = "sync_lambda.handler"
  runtime = "python3.12"

  timeout     = 900
  memory_size = 128

  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

}
