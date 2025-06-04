-- ClickHouse schema for contact information

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS contact_data;

-- Create contacts table
CREATE TABLE IF NOT EXISTS contact_data.contacts (
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
SETTINGS index_granularity = 8192;

-- Create materialized view for domain statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS contact_data.domain_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(extraction_date)
ORDER BY (domain, extraction_date)
AS SELECT
    domain,
    extraction_date,
    count() AS contact_count,
    countIf(contact_type = 'email') AS email_count,
    countIf(contact_type = 'phone') AS phone_count,
    uniqExact(url) AS unique_urls
FROM contact_data.contacts
GROUP BY domain, extraction_date;

-- Create dictionary for domain categories
CREATE DICTIONARY IF NOT EXISTS contact_data.domain_categories (
    domain String,
    category String,
    subcategory String,
    is_business UInt8,
    country_code String
) PRIMARY KEY domain
SOURCE(CLICKHOUSE(
    HOST 'localhost'
    PORT 9000
    USER 'default'
    TABLE 'domain_categories_source'
    DB 'contact_data'
))
LIFETIME(MIN 300 MAX 3600)
LAYOUT(HASHED());

-- Create table for domain categories source
CREATE TABLE IF NOT EXISTS contact_data.domain_categories_source (
    domain String,
    category String,
    subcategory String,
    is_business UInt8,
    country_code String
) ENGINE = MergeTree()
ORDER BY domain;