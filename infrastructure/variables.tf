/**
 * Variables for Contact Information Extraction Pipeline
 */

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment (e.g., dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for storing data"
  type        = string
  default     = "contact-extraction-data"
}

variable "pipeline_schedule" {
  description = "Schedule expression for the pipeline (cron or rate)"
  type        = string
  default     = "rate(1 day)"
}

variable "default_crawl_id" {
  description = "Default Common Crawl ID to process"
  type        = string
  default     = "CC-MAIN-2023-06"
}

variable "default_limit_files" {
  description = "Default limit for number of WARC files to process"
  type        = number
  default     = 100
}

variable "default_partition_count" {
  description = "Default number of partitions for output data"
  type        = number
  default     = 100
}

variable "clickhouse_host" {
  description = "ClickHouse host"
  type        = string
}

variable "clickhouse_port" {
  description = "ClickHouse port"
  type        = number
  default     = 9000
}

variable "clickhouse_db" {
  description = "ClickHouse database"
  type        = string
  default     = "contact_data"
}

variable "clickhouse_table" {
  description = "ClickHouse table"
  type        = string
  default     = "contacts"
}

variable "clickhouse_user" {
  description = "ClickHouse user"
  type        = string
  sensitive   = true
}

variable "clickhouse_password" {
  description = "ClickHouse password"
  type        = string
  sensitive   = true
}