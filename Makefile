.PHONY: setup deploy test clean monitor cost-estimate

# Variables
REGION ?= us-east-1
ENV ?= dev
CRAWL_ID ?= CC-MAIN-2023-06
LIMIT_FILES ?= 100
S3_BUCKET ?= contact-extraction-data-$(ENV)
CODE_BUCKET ?= contact-extraction-code-$(ENV)
CLICKHOUSE_HOST ?= localhost
CLICKHOUSE_PORT ?= 9000
CLICKHOUSE_DB ?= contact_data
CLICKHOUSE_TABLE ?= contacts

# Setup
setup:
	@echo "Setting up environment..."
	pip install -r requirements.txt
	@echo "Creating .env file..."
	@echo "AWS_REGION=$(REGION)" > .env
	@echo "S3_BUCKET=$(S3_BUCKET)" >> .env
	@echo "CODE_BUCKET=$(CODE_BUCKET)" >> .env
	@echo "CLICKHOUSE_HOST=$(CLICKHOUSE_HOST)" >> .env
	@echo "CLICKHOUSE_PORT=$(CLICKHOUSE_PORT)" >> .env
	@echo "CLICKHOUSE_DB=$(CLICKHOUSE_DB)" >> .env
	@echo "CLICKHOUSE_TABLE=$(CLICKHOUSE_TABLE)" >> .env
	@echo "Environment setup complete."

# Infrastructure
infrastructure:
	@echo "Deploying infrastructure..."
	cd infrastructure && \
	terraform init && \
	terraform apply \
		-var="aws_region=$(REGION)" \
		-var="environment=$(ENV)" \
		-var="s3_bucket_name=$(S3_BUCKET)" \
		-var="clickhouse_host=$(CLICKHOUSE_HOST)" \
		-var="clickhouse_port=$(CLICKHOUSE_PORT)" \
		-var="clickhouse_db=$(CLICKHOUSE_DB)" \
		-var="clickhouse_table=$(CLICKHOUSE_TABLE)"

# Deploy code to S3
deploy-code:
	@echo "Deploying code to S3..."
	aws s3 sync src/ s3://$(CODE_BUCKET)/src/ --exclude "__pycache__/*" --region $(REGION)
	@echo "Code deployed to S3://$(CODE_BUCKET)/src/"

# Deploy
deploy: infrastructure deploy-code
	@echo "Deployment complete."

# Run tests
test:
	@echo "Running tests..."
	pytest tests/

# Clean
clean:
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name "*.egg" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".coverage" -exec rm -rf {} +
	find . -type d -name "htmlcov" -exec rm -rf {} +
	find . -type d -name "dist" -exec rm -rf {} +
	find . -type d -name "build" -exec rm -rf {} +
	@echo "Cleanup complete."

# Monitor
monitor:
	@echo "Monitoring EMR Serverless jobs..."
	aws emr-serverless list-job-runs \
		--application-id $$(aws emr-serverless list-applications --query "applications[?name=='contact-extraction'].id" --output text) \
		--region $(REGION)

# Cost estimate
cost-estimate:
	@echo "Estimating costs..."
	python monitoring/cost_estimator.py \
		--data-size 100 \
		--target-duration 2 \
		--region $(REGION) \
		--output cost_estimate.json
	@echo "Cost estimate saved to cost_estimate.json"

# Run local test with small dataset
local-test:
	@echo "Running local test with small dataset..."
	spark-submit \
		--master local[*] \
		src/extraction/main.py \
		--crawl-id $(CRAWL_ID) \
		--output-path ./output \
		--clickhouse-host $(CLICKHOUSE_HOST) \
		--clickhouse-port $(CLICKHOUSE_PORT) \
		--clickhouse-db $(CLICKHOUSE_DB) \
		--clickhouse-table $(CLICKHOUSE_TABLE) \
		--clickhouse-user default \
		--clickhouse-password "" \
		--limit-files 1

# Help
help:
	@echo "Available commands:"
	@echo "  setup          - Set up the development environment"
	@echo "  infrastructure - Deploy AWS infrastructure with Terraform"
	@echo "  deploy-code    - Deploy code to S3"
	@echo "  deploy         - Deploy infrastructure and code"
	@echo "  test           - Run tests"
	@echo "  clean          - Clean up temporary files"
	@echo "  monitor        - Monitor EMR Serverless jobs"
	@echo "  cost-estimate  - Estimate costs for processing data"
	@echo "  local-test     - Run a local test with a small dataset"
	@echo "  help           - Show this help message"