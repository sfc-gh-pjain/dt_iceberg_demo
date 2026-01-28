
/*=============================================================================
  Dynamic Iceberg Tables Demo - Setup Script
  
  This script creates the required infrastructure for the Dynamic Iceberg 
  Tables demo including database, schema, warehouse, and external volume.
  
  PREREQUISITES:
  - ACCOUNTADMIN or role with CREATE DATABASE, CREATE WAREHOUSE privileges
  - Cloud storage configured (AWS S3 or Azure Blob Storage)
  - Storage integration created for your cloud provider
=============================================================================*/

-- Set context
USE ROLE SYSADMIN;

-- Create dedicated database for demo
CREATE DATABASE IF NOT EXISTS DT_ICE_DEMO;

-- Create schema
CREATE SCHEMA IF NOT EXISTS DT_ICE_DEMO.DEMO;

-- Create warehouse for demo
CREATE WAREHOUSE IF NOT EXISTS DT_ICE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Set working context
USE DATABASE DT_ICE_DEMO;
USE SCHEMA DEMO;
USE WAREHOUSE DT_ICE_WH;

/*-----------------------------------------------------------------------------
  EXTERNAL VOLUME CONFIGURATION
  
  NOTE: Update the values below to match your cloud storage configuration.
  
  For AWS S3:
  - Replace <your-bucket> with your S3 bucket name
  - Replace <your-storage-integration> with your storage integration name
  - Replace <your-iam-role-arn> with your IAM role ARN
  
  For Azure Blob Storage:
  - Uncomment the Azure section and update accordingly
-----------------------------------------------------------------------------*/

-- Option 1: AWS S3 External Volume
CREATE OR REPLACE EXTERNAL VOLUME dt_ice_ext_volume
    STORAGE_LOCATIONS = (
        (
            NAME = 'dt_ice_s3_location'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://sfpjain-us-west-2/dt_iceberg_dw/tables/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::484577546576:role/sfpjain-demo-role'
            STORAGE_AWS_EXTERNAL_ID = 'sfpjaindefault'
        )
    )
    ALLOW_WRITES = TRUE;

-- Describe external volume to verify configuration
DESC EXTERNAL VOLUME dt_ice_ext_volume;

/*
-- Option 2: Azure Blob Storage External Volume (uncomment and configure if using Azure)
CREATE OR REPLACE EXTERNAL VOLUME dt_ice_ext_volume
    STORAGE_LOCATIONS = (
        (
            NAME = 'dt_ice_azure_location'
            STORAGE_PROVIDER = 'AZURE'
            STORAGE_BASE_URL = 'azure://<storage-account>.blob.core.windows.net/<container>/dt_ice_demo/'
            AZURE_TENANT_ID = '<your-tenant-id>'
        )
    );
*/

-- Grant necessary privileges for external volume
GRANT USAGE ON EXTERNAL VOLUME dt_ice_ext_volume TO ROLE ACCOUNTADMIN;

-- Verify setup
SHOW DATABASES LIKE 'DT_ICE_DEMO';
SHOW SCHEMAS IN DATABASE DT_ICE_DEMO;
SHOW WAREHOUSES LIKE 'DT_ICE_WH';
SHOW EXTERNAL VOLUMES LIKE 'dt_ice_ext_volume';

/*
  Setup complete! 
  
  Next steps:
  1. Update the external volume configuration with your cloud storage details
  2. Run 02_source_tables.sql to create source data
*/
