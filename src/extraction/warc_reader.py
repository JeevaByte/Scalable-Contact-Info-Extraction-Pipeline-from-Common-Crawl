"""
WARC file reader for Common Crawl data.
This module handles reading and parsing WARC files from S3.
"""
import io
import gzip
import logging
import boto3
from typing import List, Dict, Iterator, Any, Tuple
from warcio.archiveiterator import ArchiveIterator
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WARCReader:
    """Reader for Common Crawl WARC files stored in S3."""
    
    def __init__(self, spark: SparkSession, s3_bucket: str = "commoncrawl"):
        """Initialize the WARC reader.
        
        Args:
            spark: Active SparkSession
            s3_bucket: S3 bucket containing Common Crawl data (default: "commoncrawl")
        """
        self.spark = spark
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3')
        logger.info(f"WARCReader initialized with bucket: {s3_bucket}")
    
    def list_warc_files(self, crawl_id: str, limit: int = None) -> List[str]:
        """List WARC files for a specific Common Crawl ID.
        
        Args:
            crawl_id: Common Crawl ID (e.g., 'CC-MAIN-2023-06')
            limit: Maximum number of files to return (default: None)
            
        Returns:
            List of S3 paths to WARC files
        """
        prefix = f"crawl-data/{crawl_id}/segments/"
        
        # List segments
        segments = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix, Delimiter='/'):
            if 'CommonPrefixes' in page:
                segments.extend([p['Prefix'] for p in page['CommonPrefixes']])
        
        # List WARC files in each segment
        warc_files = []
        for segment in segments:
            warc_prefix = segment + "warc/"
            for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=warc_prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.warc.gz'):
                            warc_files.append(f"s3://{self.s3_bucket}/{obj['Key']}")
                            if limit and len(warc_files) >= limit:
                                return warc_files
        
        return warc_files
    
    @staticmethod
    def parse_warc_record(record: Any) -> Dict[str, Any]:
        """Parse a WARC record into a dictionary.
        
        Args:
            record: WARC record from warcio
            
        Returns:
            Dictionary with parsed record data
        """
        if record.rec_type != 'response':
            return None
        
        try:
            url = record.rec_headers.get_header('WARC-Target-URI')
            record_id = record.rec_headers.get_header('WARC-Record-ID')
            warc_filename = record.rec_headers.get_header('WARC-Filename', '')
            timestamp = record.rec_headers.get_header('WARC-Date', '')
            
            # Get content
            content = record.content_stream().read().decode('utf-8', errors='ignore')
            
            return {
                "url": url,
                "warc_filename": warc_filename,
                "warc_record_id": record_id,
                "content": content,
                "timestamp": timestamp
            }
        except Exception as e:
            logger.warning(f"Error parsing WARC record: {str(e)}")
            return None
    
    @staticmethod
    def process_warc_file(warc_path: str) -> Iterator[Dict[str, Any]]:
        """Process a single WARC file and yield records.
        
        Args:
            warc_path: S3 path to WARC file
            
        Yields:
            Dictionaries containing parsed WARC records
        """
        s3 = boto3.resource('s3')
        
        # Parse S3 path
        bucket = warc_path.split('/')[2]
        key = '/'.join(warc_path.split('/')[3:])
        
        try:
            # Get object from S3
            obj = s3.Object(bucket, key)
            data = obj.get()['Body'].read()
            
            # Parse WARC file
            with gzip.open(io.BytesIO(data), 'rb') as stream:
                for record in ArchiveIterator(stream):
                    parsed = WARCReader.parse_warc_record(record)
                    if parsed:
                        yield parsed
        except Exception as e:
            logger.error(f"Error processing WARC file {warc_path}: {str(e)}")
    
    def create_dataframe_from_warcs(self, warc_paths: List[str], schema: StructType) -> DataFrame:
        """Create a DataFrame from WARC files.
        
        Args:
            warc_paths: List of S3 paths to WARC files
            schema: Schema for the DataFrame
            
        Returns:
            DataFrame containing WARC records
        """
        logger.info(f"Creating DataFrame from {len(warc_paths)} WARC files")
        
        # Create RDD from WARC files
        rdd = self.spark.sparkContext.parallelize(warc_paths, len(warc_paths)) \
            .flatMap(WARCReader.process_warc_file)
        
        # Create DataFrame
        df = self.spark.createDataFrame(rdd, schema)
        
        logger.info(f"Created DataFrame with {df.count()} records")
        return df