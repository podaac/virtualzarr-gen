/* ---------------------------------------------------------
   CNM-R Transform Lambda: SNS → transform → SQS FIFO
   --------------------------------------------------------- */

# --- Lambda package ---

data "archive_file" "cnm_transform_zip" {
  type        = "zip"
  source_file = "${path.module}/cnm_transform_lambda.py"
  output_path = "${path.module}/cnm_transform_lambda.zip"
}

# --- Lambda Function ---

resource "aws_lambda_function" "cnm_transform" {
  function_name = "${local.resource_prefix}-cnm-transform"
  role          = aws_iam_role.cnm_transform_role.arn

  handler = "cnm_transform_lambda.handler"
  runtime = "python3.12"

  timeout     = 60
  memory_size = 128

  filename         = data.archive_file.cnm_transform_zip.output_path
  source_code_hash = data.archive_file.cnm_transform_zip.output_base64sha256

  environment {
    variables = {
      APPEND_QUEUE_URL     = aws_sqs_queue.append_granule.url
      COLLECTION_ALLOWLIST = var.cnm_collection_allowlist
    }
  }

  tags = {
    Name = "${local.resource_prefix}-cnm-transform"
  }
}

# --- SNS Subscription ---

resource "aws_sns_topic_subscription" "cnm_to_transform" {
  count     = var.cnm_sns_topic_arn != "" ? 1 : 0
  topic_arn = var.cnm_sns_topic_arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.cnm_transform.arn
}

resource "aws_lambda_permission" "sns_invoke_cnm_transform" {
  count         = var.cnm_sns_topic_arn != "" ? 1 : 0
  statement_id  = "AllowSNSInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.cnm_transform.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = var.cnm_sns_topic_arn
}

# --- IAM Role ---

resource "aws_iam_role" "cnm_transform_role" {
  name                 = "${local.resource_prefix}-cnm-transform-role"
  permissions_boundary = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/NGAPShRoleBoundary"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "cnm_transform_basic" {
  role       = aws_iam_role.cnm_transform_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "cnm_transform_sqs" {
  name = "sqs-send"
  role = aws_iam_role.cnm_transform_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sqs:SendMessage"]
      Resource = aws_sqs_queue.append_granule.arn
    }]
  })
}
