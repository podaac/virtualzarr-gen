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

# --- Owned CNM-R SNS Topic ---
# We own this topic (rather than subscribing to Cumulus's shared response
# topic). Cumulus is configured to publish CNM-R messages here; the access
# policy below grants the configured publisher principals sns:Publish.

resource "aws_sns_topic" "cnm_response" {
  name = "${local.resource_prefix}-cnm-response"

  tags = {
    Name = "${local.resource_prefix}-cnm-response"
  }
}

resource "aws_sns_topic_policy" "cnm_response" {
  arn = aws_sns_topic.cnm_response.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Account owner retains full control of the topic. Topic resource
        # policies only accept resource-scoped actions (not SNS:* which would
        # include account-level actions and is rejected as "out of service
        # scope"); this is the standard owner action set.
        Sid       = "Owner"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action = [
          "SNS:GetTopicAttributes",
          "SNS:SetTopicAttributes",
          "SNS:AddPermission",
          "SNS:RemovePermission",
          "SNS:DeleteTopic",
          "SNS:Subscribe",
          "SNS:ListSubscriptionsByTopic",
          "SNS:Publish",
        ]
        Resource = aws_sns_topic.cnm_response.arn
      },
      {
        # Publishers (e.g. the Cumulus publish role). Defaults to this account
        # when var.cnm_publisher_principal_arns is empty.
        Sid    = "AllowPublish"
        Effect = "Allow"
        Principal = {
          AWS = length(var.cnm_publisher_principal_arns) > 0 ? var.cnm_publisher_principal_arns : ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
        }
        Action   = "SNS:Publish"
        Resource = aws_sns_topic.cnm_response.arn
      },
    ]
  })
}

# --- SNS Subscription (transform Lambda to the owned topic) ---

resource "aws_sns_topic_subscription" "cnm_to_transform" {
  topic_arn = aws_sns_topic.cnm_response.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.cnm_transform.arn
}

resource "aws_lambda_permission" "sns_invoke_cnm_transform" {
  statement_id  = "AllowSNSInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.cnm_transform.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.cnm_response.arn
}

# --- Output: hand this ARN to Cumulus so it publishes CNM-R here ---

output "cnm_response_topic_arn" {
  value       = aws_sns_topic.cnm_response.arn
  description = "SNS topic ARN that Cumulus should publish CNM-R messages to."
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
