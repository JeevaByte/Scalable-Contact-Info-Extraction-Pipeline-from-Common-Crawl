"""
ClickHouse writer for contact information.
This module handles writing data to ClickHouse.
"""
import logging
from typing import Dict, Any, List
import clickhouse_driver
from pyspark.sql import DataFrame

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ClickHouseWriter:
    """Writer for contact information to ClickHouse."""
    
    def __init__(self, host: str, port: int, database: str, table: str, user: str, password: str):
        """Initialize the ClickHouse writer.
        
        Args:
            host: ClickHouse host
            port: ClickHouse port
            database: ClickHouse database
            table: ClickHouse table
            user: ClickHouse user
            password: ClickHouse password
        """
        self.host = host
        self.port = port
        self.database = database
        self.table = table
        self.user = user
        self.password = password
        logger.info(f"ClickHouseWriter initialized for {database}.{table}")
    
    def get_connection(self):
        """Get a connection to ClickHouse.
        
        Returns:
            ClickHouse connection
        """
        return clickhouse_driver.Client(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
    
    def create_table_if_not_exists(self):
        """Create the contacts table if it doesn't exist."""
        client = self.get_connection()
        
        # Create database if not exists
        client.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        
        # Create table if not exists
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.{self.table} (
            url String,
            warc_filename String,
            warc_record_id String,
            timestamp String,
            email String,
            phone_number String,
            contact_type String,
            domain String,
            extraction_date DateTime,
            source_crawl String
        ) ENGINE = MergeTree()
        ORDER BY (domain, email, phone_number)
        PARTITION BY toYYYYMM(extraction_date)
        SETTINGS index_granularity = 8192
        """
        
        client.execute(create_table_query)
        logger.info(f"Created table {self.database}.{self.table} if it didn't exist")
    
    def write_dataframe(self, df: DataFrame, batch_size: int = 100000):
        """Write a DataFrame to ClickHouse.
        
        Args:
            df: DataFrame to write
            batch_size: Number of rows to write in each batch
        """
        # Ensure table exists
        self.create_table_if_not_exists()
        
        # Convert DataFrame to list of dictionaries
        rows = df.collect()
        data = [row.asDict() for row in rows]
        
        # Write data in batches
        client = self.get_connection()
        total_rows = len(data)
        
        for i in range(0, total_rows, batch_size):
            batch = data[i:i+batch_size]
            client.execute(
                f"INSERT INTO {self.database}.{self.table} VALUES",
                batch
            )
            logger.info(f"Wrote batch {i//batch_size + 1} ({len(batch)} rows) to ClickHouse")
        
        logger.info(f"Successfully wrote {total_rows} rows to {self.database}.{self.table}")
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a query on ClickHouse.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Query results
        """
        client = self.get_connection()
        result = client.execute(query, with_column_types=True)
        
        # Convert result to list of dictionaries
        rows, columns = result
        column_names = [col[0] for col in columns]
        
        return [dict(zip(column_names, row)) for row in rows]