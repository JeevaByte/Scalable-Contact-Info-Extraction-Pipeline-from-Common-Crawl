"""
Unit tests for the contact extractor module.
"""
import unittest
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extraction.contact_extractor import ContactExtractor, create_warc_schema

class TestContactExtractor(unittest.TestCase):
    """Test cases for ContactExtractor."""
    
    @classmethod
    def setUpClass(cls):
        """Set up SparkSession for tests."""
        cls.spark = SparkSession.builder \
            .appName("TestContactExtractor") \
            .master("local[2]") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Stop SparkSession after tests."""
        cls.spark.stop()
    
    def setUp(self):
        """Set up test fixtures."""
        self.extractor = ContactExtractor(self.spark)
    
    def test_extract_emails(self):
        """Test email extraction."""
        # Test with valid emails
        text = "Contact us at info@example.com or support@test.org for more information."
        emails = self.extractor.extract_emails(text)
        self.assertEqual(len(emails), 2)
        self.assertIn("info@example.com", emails)
        self.assertIn("support@test.org", emails)
        
        # Test with invalid emails
        text = "This is not an email: example.com, neither is this: @test.org"
        emails = self.extractor.extract_emails(text)
        self.assertEqual(len(emails), 0)
        
        # Test with mixed content
        text = "Valid: user@domain.com, Invalid: user@.com, Image: image.jpg@example.com"
        emails = self.extractor.extract_emails(text)
        self.assertEqual(len(emails), 1)
        self.assertIn("user@domain.com", emails)
    
    def test_extract_phones(self):
        """Test phone number extraction."""
        # Test with valid phone numbers
        text = "Call us at +1 (123) 456-7890 or 555-123-4567"
        phones = self.extractor.extract_phones(text)
        self.assertEqual(len(phones), 2)
        self.assertIn("+11234567890", phones)
        self.assertIn("5551234567", phones)
        
        # Test with invalid phone numbers
        text = "This is not a phone: 123-45, neither is this: 12345"
        phones = self.extractor.extract_phones(text)
        self.assertEqual(len(phones), 0)
        
        # Test with international formats
        text = "International: +44 20 1234 5678, Another: +33 1 23 45 67 89"
        phones = self.extractor.extract_phones(text)
        self.assertEqual(len(phones), 2)
    
    def test_process_warc_records(self):
        """Test processing WARC records."""
        # Create test data
        data = [
            {
                "url": "https://example.com",
                "warc_filename": "test.warc.gz",
                "warc_record_id": "record1",
                "content": "Contact us at info@example.com or call +1 (123) 456-7890",
                "timestamp": "2023-01-01T00:00:00Z"
            },
            {
                "url": "https://test.org",
                "warc_filename": "test.warc.gz",
                "warc_record_id": "record2",
                "content": "No contact information here",
                "timestamp": "2023-01-01T00:00:00Z"
            }
        ]
        
        # Create DataFrame
        schema = create_warc_schema()
        df = self.spark.createDataFrame(data, schema)
        
        # Process records
        result_df = self.extractor.process_warc_records(df)
        
        # Check results
        self.assertEqual(result_df.count(), 2)  # One email and one phone
        
        # Check that records with no contact info are filtered out
        emails = result_df.filter(result_df.contact_type == "email").count()
        phones = result_df.filter(result_df.contact_type == "phone").count()
        self.assertEqual(emails, 1)
        self.assertEqual(phones, 1)
    
    def test_validate_and_enrich(self):
        """Test validating and enriching contact information."""
        # Create test data
        data = [
            {
                "url": "https://example.com",
                "warc_filename": "test.warc.gz",
                "warc_record_id": "record1",
                "timestamp": "2023-01-01T00:00:00Z",
                "email": "info@example.com",
                "phone_number": None,
                "contact_type": "email"
            },
            {
                "url": "https://example.com",
                "warc_filename": "test.warc.gz",
                "warc_record_id": "record1",
                "timestamp": "2023-01-01T00:00:00Z",
                "email": None,
                "phone_number": "+11234567890",
                "contact_type": "phone"
            }
        ]
        
        # Create DataFrame
        df = self.spark.createDataFrame(data)
        
        # Validate and enrich
        enriched_df = self.extractor.validate_and_enrich(df)
        
        # Check results
        self.assertEqual(enriched_df.count(), 2)
        
        # Check domain extraction
        domains = [row.domain for row in enriched_df.filter(enriched_df.email.isNotNull()).collect()]
        self.assertEqual(domains, ["example.com"])
        
        # Check that extraction_date is added
        self.assertTrue("extraction_date" in enriched_df.columns)


if __name__ == "__main__":
    unittest.main()