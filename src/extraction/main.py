"""
Main entry point for the contact extraction pipeline.
This script runs on AWS EMR Serverless to extract contact information from Common Crawl.
"""
import os
import sys
import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession

from contact_extractor import ContactExtractor, create_warc_schema
from warc_reader import WARCReader
from clickhouse_writer import ClickHouseWriter

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Extract contact information from Common Crawl')
    
    parser.add_argument('--crawl-id', type=str, required=True,
                        help='Common Crawl ID (e.g., CC-MAIN-2023-06)')
    parser.add_argument('--output-path', type=str, required=True,
                        help='S3 path for output data')
    parser.add_argument('--clickhouse-host', type=str, required=True,
                        help='ClickHouse host')
    parser.add_argument('--clickhouse-port', type=int, default=9000,
                        help='ClickHouse port (default: 9000)')
    parser.add_argument('--clickhouse-db', type=str, required=True,
                        help='ClickHouse database')
    parser.add_argument('--clickhouse-table', type=str, required=True,
                        help='ClickHouse table')
    parser.add_argument('--clickhouse-user', type=str, required=True,
                        help='ClickHouse user')
    parser.add_argument('--clickhouse-password', type=str, required=True,
                        help='ClickHouse password')
    parser.add_argument('--limit-files', type=int, default=None,
                        help='Limit number of WARC files to process (default: None)')
    parser.add_argument('--partition-count', type=int, default=100,
                        help='Number of partitions for output data (default: 100)')
    
    return parser.parse_args()

def create_spark_session():
    """Create and configure a Spark session."""
    spark = SparkSession.builder \
        .appName("ContactInfoExtraction") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def run_extraction_pipeline(args):
    """Run the contact extraction pipeline."""
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Initialize components
        warc_reader = WARCReader(spark)
        contact_extractor = ContactExtractor(spark)
        clickhouse_writer = ClickHouseWriter(
            host=args.clickhouse_host,
            port=args.clickhouse_port,
            database=args.clickhouse_db,
            table=args.clickhouse_table,
            user=args.clickhouse_user,
            password=args.clickhouse_password
        )
        
        # List WARC files
        logger.info(f"Listing WARC files for crawl ID: {args.crawl_id}")
        warc_files = warc_reader.list_warc_files(args.crawl_id, args.limit_files)
        logger.info(f"Found {len(warc_files)} WARC files")
        
        if not warc_files:
            logger.error(f"No WARC files found for crawl ID: {args.crawl_id}")
            return
        
        # Create schema
        schema = create_warc_schema()
        
        # Create DataFrame from WARC files
        warc_df = warc_reader.create_dataframe_from_warcs(warc_files, schema)
        
        # Extract contact information
        contacts_df = contact_extractor.process_warc_records(warc_df)
        
        # Validate and enrich contact information
        enriched_df = contact_extractor.validate_and_enrich(contacts_df)
        
        # Save to S3
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"{args.output_path}/contacts_{timestamp}"
        
        logger.info(f"Saving contact information to: {output_path}")
        enriched_df.repartition(args.partition_count).write.parquet(output_path)
        
        # Write to ClickHouse
        logger.info(f"Writing contact information to ClickHouse: {args.clickhouse_db}.{args.clickhouse_table}")
        clickhouse_writer.write_dataframe(enriched_df)
        
        logger.info("Contact extraction pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Error in contact extraction pipeline: {str(e)}")
        raise
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    # Parse arguments
    args = parse_arguments()
    
    # Run pipeline
    run_extraction_pipeline(args)