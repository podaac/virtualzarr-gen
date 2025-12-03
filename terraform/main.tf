terraform {
  backend "s3" {}
}

resource "aws_ecs_cluster" "default" {
  name = "${local.resource_prefix}-cluster"

  configuration {
    execute_command_configuration {
      kms_key_id = aws_kms_key.app_log.id
      logging = "OVERRIDE"

      log_configuration {
        cloud_watch_encryption_enabled = true
        cloud_watch_log_group_name = aws_cloudwatch_log_group.app.name
      }
    }
  }
}

/* -- Cloudwatch -- */
resource "aws_cloudwatch_log_group" "app" {
  name = "/ecs/${local.name}"
  retention_in_days = 30
}

resource "aws_kms_key" "app_log" {
  description = "${local.resource_prefix}-app-log"
  deletion_window_in_days = 7
}

resource "aws_ecs_task_definition" "app" {
  family = "${local.resource_prefix}-app-task"
  requires_compatibilities = ["EC2"]

  network_mode = "awsvpc"

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([
        {
            "name": "cloud-optimization-generation",
            "image": var.image_name,
            "cpu": 0,
            "memory": 209715, 
            "memoryReservation": 199229,
            "portMappings": [
                {
                    "name": "workers",
                    "containerPort": 8787,
                    "hostPort": 8787,
                    "protocol": "tcp"
                }
            ],
            "essential": true,
            "environment": [
                {
                    "name": "LOADABLE_VARS",
                    "value": ""
                },
                {
                    "name": "COLLECTION",
                    "value": ""
                },
                {
                    "name": "END_DATE",
                    "value": ""
                },
                {
                    "name": "OUTPUT_BUCKET",
                    "value": ""
                },
                {
                    "name": "START_DATE",
                    "value": ""
                },
                {
                    "name": "SSM_EDL_USERNAME",
                    "value": ""
                },
                {
                    "name": "SSM_EDL_PASSWORD",
                    "value": ""
                }
            ],
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    awslogs-region = var.region
                    awslogs-group = aws_cloudwatch_log_group.app.name
                    awslogs-stream-prefix = local.resource_prefix
                    "awslogs-create-group": "true",
                },
            },
        }
    ])

  task_role_arn = aws_iam_role.app_task.arn
  execution_role_arn = aws_iam_role.app_task_exec.arn
}

/* -- IAM -- */
resource "aws_iam_role" "app_task" {
  name = "${local.resource_prefix}-ecs-task-role"

  permissions_boundary = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/NGAPShRoleBoundary"
  managed_policy_arns = [
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/NGAPProtAppInstanceMinimalPolicy"
  ]

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Sid = ""
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })  
}

/*
TODO should update these policies for specific buckets/accounts.
*/
resource "aws_iam_role" "app_task_exec" {
  name = "${local.resource_prefix}-ecs-task-exec-role"

  permissions_boundary = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/NGAPShRoleBoundary"
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
    "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role",
    "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
    "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
    "arn:aws:iam::aws:policy/AmazonSSMReadOnlyAccess",
    "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
  ]

  assume_role_policy = jsonencode({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {
                    "Service": "ecs-tasks.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            },
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {
                    "Service": "ec2.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    })

  inline_policy {
    name = "allow-s3-writes"
    policy = jsonencode({
        "Version": "2012-10-17",
        "Statement": [
            {
            "Sid":"ReadWriteS3",
            "Action": [
              "s3:ListBucket"
              ],
            "Effect": "Allow",
            "Resource": ["arn:aws:s3:::${var.output_bucket}"]
            },
            {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:GetObjectTagging",
                "s3:DeleteObject",              
                "s3:DeleteObjectVersion",
                "s3:GetObjectVersion",
                "s3:GetObjectVersionTagging",
                "s3:GetObjectACL",
                "s3:PutObjectACL"
            ],
            "Resource": ["arn:aws:s3:::${var.output_bucket}/*"]
            },
            {
              Effect = "Allow",
              Action = [
                "ecs:RegisterContainerInstance"
              ],
              Resource = "*"
            }
        ]
    })
  }

  inline_policy {
    name = "allow-logging"
    policy = jsonencode({
      Version = "2012-10-17",
      Statement = [
        {
          Sid = ""
          Effect = "Allow",
          Action = [
            "logs:CreateLogStream",
            "logs:PutLogEvents"
          ]
          Resource = "*"
        }
      ]
    })
  }
}

/*
--------------------
 EC2 configurations
--------------------
*/
/* EC2 Launch Template */
resource "aws_launch_template" "app-lt" {
  name = "${local.resource_prefix}-lt"
  iam_instance_profile {
    name = aws_iam_instance_profile.ec2profile.name
  }
  image_id = data.aws_ssm_parameter.ecs_amis.value
  vpc_security_group_ids = [data.aws_security_groups.vpc_default_sg.ids[0]]
  instance_type = "r5.8xlarge"
  user_data = base64encode(templatefile("scripts/ecs.sh", { ecs_cluster = aws_ecs_cluster.default.name}))
  update_default_version = true
}

/* Instance Profile */
resource "aws_iam_instance_profile" "ec2profile" {
  name = "${local.resource_prefix}-instanceProfile"
  role = aws_iam_role.app_task_exec.name
}

/* Autoscaling group with instance-types */
resource "aws_autoscaling_group" "app-ag" {
  name = "${local.resource_prefix}-app-asg"
  vpc_zone_identifier = [for subnet in data.aws_subnet.private_application_subnet: subnet.id]
  
  desired_capacity   = 0
  max_size           = 2
  min_size           = 0
  protect_from_scale_in = false

  launch_template {
    id      = aws_launch_template.app-lt.id
    version = "$Latest"
  }

  # ... other configuration, including potentially other tags ...

  tag {
    key                 = "AmazonECSManaged"
    value               = true
    propagate_at_launch = true
  }
}

resource "aws_ecs_capacity_provider" "app_cap_provider" {
  name = "${local.resource_prefix}-ecs-capacity-provider"
  auto_scaling_group_provider {
    auto_scaling_group_arn         = aws_autoscaling_group.app-ag.arn
    managed_termination_protection = "DISABLED"

    managed_scaling {
      maximum_scaling_step_size = 1
      minimum_scaling_step_size = 1
      status                    = "ENABLED"
      instance_warmup_period    = 300
      target_capacity           = 100
    }
  }
}

resource "aws_ecs_cluster_capacity_providers" "my_cluster_capacity_providers" {
  cluster_name = aws_ecs_cluster.default.name

  capacity_providers = [
    aws_ecs_capacity_provider.app_cap_provider.name,
  ]

  default_capacity_provider_strategy {
    base = 1
    weight = 100
    capacity_provider = aws_ecs_capacity_provider.app_cap_provider.name
  }

}