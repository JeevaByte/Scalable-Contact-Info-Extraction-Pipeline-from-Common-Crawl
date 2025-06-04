# Common Crawl Contact Information Extraction Pipeline

This project implements a scalable data pipeline that extracts contact information (emails, phone numbers) from Common Crawl datasets using AWS EMR Serverless and loads the results into ClickHouse for analytics and sales.

## Architecture

- **Data Source**: Common Crawl WARC files stored in S3
- **Processing**: AWS EMR Serverless running PySpark jobs
- **Orchestration**: AWS Step Functions
- **Storage**: ClickHouse database
- **Infrastructure**: Managed with Terraform
- **CI/CD**: GitHub Actions

## Components

- `src/`: PySpark extraction code and utilities
- `infrastructure/`: Terraform code for AWS resources
- `orchestration/`: Step Functions workflow definitions
- `schema/`: ClickHouse table schemas and migrations
- `monitoring/`: CloudWatch dashboards and alerts
- `tests/`: Unit and integration tests

## Getting Started

1. Set up infrastructure: `cd infrastructure && terraform init && terraform apply`
2. Configure environment variables in `.env`
3. Deploy the pipeline: `make deploy`
4. Monitor jobs: `make monitor`

## Cost Optimization

The pipeline includes cost estimation and optimization features to balance performance and expenses.# Scalable-Contact-Info-Extraction-Pipeline-from-Common-Crawl
