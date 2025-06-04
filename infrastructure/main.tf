/**
 * Terraform configuration for Contact Information Extraction Pipeline
 * This file defines the AWS infrastructure for the pipeline.
 */

provider "aws" {
  region = var.aws_region
}

# S3 bucket for storing data
resource "aws_s3_bucket" "data_bucket" {
  bucket = var.s3_bucket_name
  
  tags = {
    Name        = "Contact Extraction Data Bucket"
    Environment = var.environment
    Project     = "Contact Extraction Pipeline"
  }
}

# S3 bucket for storing code and artifacts
resource "aws_s3_bucket" "code_bucket" {
  bucket = "${var.s3_bucket_name}-code"
  
  tags = {
    Name        = "Contact Extraction Code Bucket"
    Environment = var.environment
    Project     = "Contact Extraction Pipeline"
  }
}

# IAM role for EMR Serverless
resource "aws_iam_role" "emr_serverless_role" {
  name = "emr-serverless-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "emr-serverless.amazonaws.com"
        }
      }
    ]
  })
}

# IAM policy for EMR Serverless
resource "aws_iam_policy" "emr_serverless_policy" {
  name        = "emr-serverless-policy"
  description = "Policy for EMR Serverless to access S3 and other services"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Effect   = "Allow"
        Resource = [
          "${aws_s3_bucket.data_bucket.arn}",
          "${aws_s3_bucket.data_bucket.arn}/*",
          "${aws_s3_bucket.code_bucket.arn}",
          "${aws_s3_bucket.code_bucket.arn}/*",
          "arn:aws:s3:::commoncrawl",
          "arn:aws:s3:::commoncrawl/*"
        ]
      },
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "emr_serverless_policy_attachment" {
  role       = aws_iam_role.emr_serverless_role.name
  policy_arn = aws_iam_policy.emr_serverless_policy.arn
}

# EMR Serverless application
resource "aws_emrserverless_application" "contact_extraction" {
  name          = "contact-extraction"
  release_label = "emr-6.9.0"
  type          = "SPARK"
  
  initial_capacity {
    initial_capacity_type = "Driver"
    
    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "4 vCPU"
        memory = "16 GB"
      }
    }
  }
  
  initial_capacity {
    initial_capacity_type = "Executor"
    
    initial_capacity_config {
      worker_count = 10
      worker_configuration {
        cpu    = "4 vCPU"
        memory = "16 GB"
      }
    }
  }
  
  maximum_capacity {
    cpu    = "400 vCPU"
    memory = "1600 GB"
  }
  
  auto_start_configuration {
    enabled = true
  }
  
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }
}

# Step Functions state machine for orchestration
resource "aws_sfn_state_machine" "contact_extraction_pipeline" {
  name     = "contact-extraction-pipeline"
  role_arn = aws_iam_role.step_functions_role.arn
  
  definition = <<EOF
{
  "Comment": "Contact Information Extraction Pipeline",
  "StartAt": "StartEMRServerlessJob",
  "States": {
    "StartEMRServerlessJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::emr-serverless:startJobRun",
      "Parameters": {
        "ApplicationId": "${aws_emrserverless_application.contact_extraction.id}",
        "ExecutionRoleArn": "${aws_iam_role.emr_serverless_role.arn}",
        "JobDriver": {
          "SparkSubmit": {
            "EntryPoint": "s3://${aws_s3_bucket.code_bucket.bucket}/src/extraction/main.py",
            "EntryPointArguments": [
              "--crawl-id", "$.crawlId",
              "--output-path", "s3://${aws_s3_bucket.data_bucket.bucket}/output",
              "--clickhouse-host", "${var.clickhouse_host}",
              "--clickhouse-port", "${var.clickhouse_port}",
              "--clickhouse-db", "${var.clickhouse_db}",
              "--clickhouse-table", "${var.clickhouse_table}",
              "--clickhouse-user", "${var.clickhouse_user}",
              "--clickhouse-password", "${var.clickhouse_password}",
              "--limit-files", "$.limitFiles",
              "--partition-count", "$.partitionCount"
            ],
            "SparkSubmitParameters": "--conf spark.executor.cores=4 --conf spark.executor.memory=16g --conf spark.driver.cores=4 --conf spark.driver.memory=16g --conf spark.executor.instances=10"
          }
        },
        "ConfigurationOverrides": {
          "MonitoringConfiguration": {
            "S3MonitoringConfiguration": {
              "LogUri": "s3://${aws_s3_bucket.data_bucket.bucket}/logs/"
            }
          }
        }
      },
      "Next": "CheckJobStatus",
      "Retry": [
        {
          "ErrorEquals": ["States.ALL"],
          "IntervalSeconds": 60,
          "MaxAttempts": 3,
          "BackoffRate": 2.0
        }
      ]
    },
    "CheckJobStatus": {
      "Type": "Task",
      "Resource": "arn:aws:states:::emr-serverless:getJobRun",
      "Parameters": {
        "ApplicationId": "${aws_emrserverless_application.contact_extraction.id}",
        "JobRunId.$": "$.JobRunId"
      },
      "Next": "JobComplete?",
      "Retry": [
        {
          "ErrorEquals": ["States.ALL"],
          "IntervalSeconds": 30,
          "MaxAttempts": 10,
          "BackoffRate": 1.5
        }
      ]
    },
    "JobComplete?": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.JobRun.State",
          "StringEquals": "SUCCESS",
          "Next": "JobSucceeded"
        },
        {
          "Variable": "$.JobRun.State",
          "StringEquals": "FAILED",
          "Next": "JobFailed"
        }
      ],
      "Default": "WaitForJobCompletion"
    },
    "WaitForJobCompletion": {
      "Type": "Wait",
      "Seconds": 60,
      "Next": "CheckJobStatus"
    },
    "JobSucceeded": {
      "Type": "Succeed"
    },
    "JobFailed": {
      "Type": "Fail",
      "Error": "EMRServerlessJobFailed",
      "Cause": "EMR Serverless job failed. Check the logs for details."
    }
  }
}
EOF
}

# IAM role for Step Functions
resource "aws_iam_role" "step_functions_role" {
  name = "step-functions-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })
}

# IAM policy for Step Functions
resource "aws_iam_policy" "step_functions_policy" {
  name        = "step-functions-policy"
  description = "Policy for Step Functions to start EMR Serverless jobs"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "emr-serverless:StartJobRun",
          "emr-serverless:GetJobRun",
          "emr-serverless:CancelJobRun"
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Action = [
          "iam:PassRole"
        ]
        Effect   = "Allow"
        Resource = aws_iam_role.emr_serverless_role.arn
      }
    ]
  })
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "step_functions_policy_attachment" {
  role       = aws_iam_role.step_functions_role.name
  policy_arn = aws_iam_policy.step_functions_policy.arn
}

# CloudWatch Events rule to trigger pipeline on schedule
resource "aws_cloudwatch_event_rule" "pipeline_schedule" {
  name                = "contact-extraction-pipeline-schedule"
  description         = "Schedule for Contact Extraction Pipeline"
  schedule_expression = var.pipeline_schedule
}

# CloudWatch Events target for Step Functions
resource "aws_cloudwatch_event_target" "pipeline_target" {
  rule      = aws_cloudwatch_event_rule.pipeline_schedule.name
  arn       = aws_sfn_state_machine.contact_extraction_pipeline.arn
  role_arn  = aws_iam_role.events_role.arn
  
  input = jsonencode({
    crawlId        = var.default_crawl_id
    limitFiles     = var.default_limit_files
    partitionCount = var.default_partition_count
  })
}

# IAM role for CloudWatch Events
resource "aws_iam_role" "events_role" {
  name = "events-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

# IAM policy for CloudWatch Events
resource "aws_iam_policy" "events_policy" {
  name        = "events-policy"
  description = "Policy for CloudWatch Events to start Step Functions"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "states:StartExecution"
        ]
        Effect   = "Allow"
        Resource = aws_sfn_state_machine.contact_extraction_pipeline.arn
      }
    ]
  })
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "events_policy_attachment" {
  role       = aws_iam_role.events_role.name
  policy_arn = aws_iam_policy.events_policy.arn
}