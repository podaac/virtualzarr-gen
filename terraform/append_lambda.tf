/* -------------------------------------------------------
   SQS FIFO Queue + Container Lambda for granule appends
   ------------------------------------------------------- */

# --- SQS FIFO Queue ---

resource "aws_sqs_queue" "append_granule" {
  name                        = "${local.resource_prefix}-append-granule.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  visibility_timeout_seconds  = 900  # match Lambda timeout
  message_retention_seconds   = 1209600  # 14 days
  receive_wait_time_seconds   = 20

  tags = {
    Name = "${local.resource_prefix}-append-granule"
  }
}

resource "aws_sqs_queue" "append_granule_dlq" {
  name                        = "${local.resource_prefix}-append-granule-dlq.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  message_retention_seconds   = 1209600  # 14 days

  tags = {
    Name = "${local.resource_prefix}-append-granule-dlq"
  }
}

resource "aws_sqs_queue_redrive_policy" "append_granule" {
  queue_url = aws_sqs_queue.append_granule.id

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.append_granule_dlq.arn
    maxReceiveCount     = 3
  })
}

# --- ECR Repository for the append Lambda image ---

resource "aws_ecr_repository" "append_lambda" {
  name                 = "${local.resource_prefix}-append-lambda"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

# --- Build and push Lambda container image to ECR ---

resource "null_resource" "append_lambda_image" {
  depends_on = [aws_ecr_repository.append_lambda]

  triggers = {
    dockerfile_hash = filesha256("${path.module}/../Dockerfile")
    handler_hash    = filesha256("${path.module}/../append_lambda_handler.py")
    repo_url        = aws_ecr_repository.append_lambda.repository_url
    tag             = var.app_version
  }

  provisioner "local-exec" {
    working_dir = "${path.module}/.."
    command     = <<-EOT
      aws ecr get-login-password --region ${var.region} | \
        docker login --username AWS --password-stdin ${aws_ecr_repository.append_lambda.repository_url}

      docker build --target lambda -t ${aws_ecr_repository.append_lambda.repository_url}:${var.app_version} .

      docker push ${aws_ecr_repository.append_lambda.repository_url}:${var.app_version}
    EOT
  }
}

# --- Lambda Function (container image) ---

resource "aws_lambda_function" "append_granule" {
  depends_on    = [null_resource.append_lambda_image]
  function_name = "${local.resource_prefix}-append-granule"
  role          = aws_iam_role.append_lambda_role.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.append_lambda.repository_url}:${var.app_version}"

  timeout     = 900  # 15 minutes
  memory_size = 3008 # 3 GB

  environment {
    variables = {
      STORE_BUCKET = var.output_bucket[0]
    }
  }

  vpc_config {
    subnet_ids         = [for subnet in data.aws_subnet.private_application_subnet : subnet.id]
    security_group_ids = [data.aws_security_groups.vpc_default_sg.ids[0]]
  }

  tags = {
    Name = "${local.resource_prefix}-append-granule"
  }
}

# --- SQS -> Lambda Event Source Mapping ---

resource "aws_lambda_event_source_mapping" "append_granule_sqs" {
  event_source_arn = aws_sqs_queue.append_granule.arn
  function_name    = aws_lambda_function.append_granule.arn
  enabled          = true

  batch_size = 10

  scaling_config {
    maximum_concurrency = var.append_lambda_max_concurrency
  }

  function_response_types = ["ReportBatchItemFailures"]
}

# --- IAM Role for Append Lambda ---

resource "aws_iam_role" "append_lambda_role" {
  name                 = "${local.resource_prefix}-append-lambda-role"
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

# VPC access for Lambda
resource "aws_iam_role_policy_attachment" "append_lambda_vpc" {
  role       = aws_iam_role.append_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# SQS permissions
resource "aws_iam_role_policy" "append_lambda_sqs" {
  name = "sqs-access"
  role = aws_iam_role.append_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes",
      ]
      Resource = aws_sqs_queue.append_granule.arn
    }]
  })
}

# S3 permissions (read source buckets + read/write store bucket)
resource "aws_iam_role_policy" "append_lambda_s3" {
  name = "s3-access"
  role = aws_iam_role.append_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [for bucket in var.output_bucket : "arn:aws:s3:::${bucket}"]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:GetObjectVersion",
        ]
        Resource = [for bucket in var.output_bucket : "arn:aws:s3:::${bucket}/*"]
      },
    ]
  })
}

# CloudWatch Logs
resource "aws_iam_role_policy" "append_lambda_logs" {
  name = "cloudwatch-logs"
  role = aws_iam_role.append_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
      ]
      Resource = "arn:aws:logs:*:*:*"
    }]
  })
}

# SSM access for EDL credentials
resource "aws_iam_role_policy" "append_lambda_ssm" {
  name = "ssm-access"
  role = aws_iam_role.append_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "ssm:GetParameter",
        "ssm:GetParameters",
      ]
      Resource = "arn:aws:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter/*"
    }]
  })
}

# --- Outputs ---

output "append_sqs_queue_url" {
  value       = aws_sqs_queue.append_granule.url
  description = "URL of the SQS FIFO queue for granule append messages"
}

output "append_sqs_queue_arn" {
  value       = aws_sqs_queue.append_granule.arn
  description = "ARN of the SQS FIFO queue for granule append messages"
}

output "append_lambda_ecr_url" {
  value       = aws_ecr_repository.append_lambda.repository_url
  description = "ECR repository URL for the append Lambda image"
}
