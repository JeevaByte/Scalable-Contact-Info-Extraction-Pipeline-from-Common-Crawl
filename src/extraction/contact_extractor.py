"""
Contact information extractor for Common Crawl WARC files.
This module contains PySpark code to extract emails and phone numbers from WARC files.
"""
import re
import logging
from typing import List, Dict, Any, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, explode, array, lit
from pyspark.sql.types import ArrayType, StringType, StructType, StructField

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Regular expressions for contact information extraction
# Email regex pattern - handles common email formats while avoiding false positives
EMAIL_PATTERN = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'

# Phone regex patterns - handles international formats with various separators
PHONE_PATTERNS = [
    r'\+\d{1,3}\s?[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}',  # International format: +1 (123) 456-7890
    r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',                           # US format: (123) 456-7890
    r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',                                 # Simple format: 123-456-7890
    r'\d{5}[-.\s]?\d{5,7}'                                            # Other common formats
]

class ContactExtractor:
    """Extract contact information from Common Crawl WARC files using PySpark."""
    
    def __init__(self, spark: SparkSession):
        """Initialize the extractor with a SparkSession.
        
        Args:
            spark: Active SparkSession
        """
        self.spark = spark
        logger.info("ContactExtractor initialized")
    
    @staticmethod
    def extract_emails(text: str) -> List[str]:
        """Extract email addresses from text.
        
        Args:
            text: Input text to search for emails
            
        Returns:
            List of extracted email addresses
        """
        if not text or not isinstance(text, str):
            return []
        
        # Find all matches and filter out common false positives
        emails = re.findall(EMAIL_PATTERN, text)
        
        # Filter out common invalid patterns and normalize
        valid_emails = []
        for email in emails:
            # Skip emails with invalid TLDs or common false positives
            if any(email.lower().endswith(invalid) for invalid in ['.png', '.jpg', '.gif', '.example', '.test']):
                continue
                
            # Skip emails with suspicious patterns
            if '..' in email or '@.' in email or '.@' in email:
                continue
                
            valid_emails.append(email.lower())
            
        return list(set(valid_emails))  # Remove duplicates
    
    @staticmethod
    def extract_phones(text: str) -> List[str]:
        """Extract phone numbers from text.
        
        Args:
            text: Input text to search for phone numbers
            
        Returns:
            List of extracted phone numbers
        """
        if not text or not isinstance(text, str):
            return []
        
        # Find all matches for each pattern
        all_phones = []
        for pattern in PHONE_PATTERNS:
            phones = re.findall(pattern, text)
            all_phones.extend(phones)
        
        # Normalize phone numbers (remove spaces, dashes, etc.)
        normalized = []
        for phone in all_phones:
            # Keep only digits and essential characters
            clean = re.sub(r'[^\d+]', '', phone)
            
            # Filter out numbers that are too short or too long
            if 7 <= len(clean) <= 15:
                normalized.append(clean)
        
        return list(set(normalized))  # Remove duplicates
    
    def process_warc_records(self, warc_df: DataFrame) -> DataFrame:
        """Process WARC records to extract contact information.
        
        Args:
            warc_df: DataFrame containing WARC records with 'url', 'content' columns
            
        Returns:
            DataFrame with extracted contact information
        """
        logger.info("Processing WARC records to extract contact information")
        
        # Define UDFs for extraction
        extract_emails_udf = udf(self.extract_emails, ArrayType(StringType()))
        extract_phones_udf = udf(self.extract_phones, ArrayType(StringType()))
        
        # Extract contact information
        result_df = warc_df.withColumn("emails", extract_emails_udf(col("content"))) \
                          .withColumn("phone_numbers", extract_phones_udf(col("content"))) \
                          .select(
                              col("url"),
                              col("warc_filename"),
                              col("warc_record_id"),
                              col("timestamp"),
                              col("emails"),
                              col("phone_numbers")
                          )
        
        # Filter out records with no contact information
        filtered_df = result_df.filter(
            (col("emails").isNotNull() & (col("emails").cast("string") != "[]")) | 
            (col("phone_numbers").isNotNull() & (col("phone_numbers").cast("string") != "[]"))
        )
        
        # Explode the arrays to create separate rows for each contact
        emails_df = filtered_df.select(
            col("url"),
            col("warc_filename"),
            col("warc_record_id"),
            col("timestamp"),
            explode(col("emails")).alias("email"),
            lit(None).alias("phone_number"),
            lit("email").alias("contact_type")
        )
        
        phones_df = filtered_df.select(
            col("url"),
            col("warc_filename"),
            col("warc_record_id"),
            col("timestamp"),
            lit(None).alias("email"),
            explode(col("phone_numbers")).alias("phone_number"),
            lit("phone").alias("contact_type")
        )
        
        # Union the two dataframes
        contacts_df = emails_df.union(phones_df)
        
        logger.info(f"Extracted {contacts_df.count()} contact records")
        return contacts_df
    
    def validate_and_enrich(self, contacts_df: DataFrame) -> DataFrame:
        """Validate and enrich contact information.
        
        Args:
            contacts_df: DataFrame with extracted contact information
            
        Returns:
            DataFrame with validated and enriched contact information
        """
        logger.info("Validating and enriching contact information")
        
        # Add domain information for emails
        def extract_domain(email):
            if email and '@' in email:
                return email.split('@')[1]
            return None
        
        extract_domain_udf = udf(extract_domain, StringType())
        
        # Add extraction timestamp and source information
        enriched_df = contacts_df.withColumn(
            "domain", 
            extract_domain_udf(col("email"))
        ).withColumn(
            "extraction_date", 
            lit(self.spark.sparkContext.startTime)
        )
        
        return enriched_df


def create_warc_schema() -> StructType:
    """Create schema for WARC DataFrame.
    
    Returns:
        StructType schema for WARC data
    """
    return StructType([
        StructField("url", StringType(), True),
        StructField("warc_filename", StringType(), True),
        StructField("warc_record_id", StringType(), True),
        StructField("content", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])