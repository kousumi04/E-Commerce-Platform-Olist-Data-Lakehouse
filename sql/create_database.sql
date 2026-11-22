-- ============================================================================
-- RetailPulse: Create Analytical Data Warehouse Database in AWS Glue Catalog
-- ============================================================================
CREATE DATABASE IF NOT EXISTS retailpulse_dw
COMMENT 'RetailPulse Olist E-Commerce Star Schema Data Warehouse'
LOCATION 's3://retailpulse-data-lake-kousumi-2026/curated/';