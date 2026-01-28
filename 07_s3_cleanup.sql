/*=============================================================================
  Dynamic Iceberg Tables Demo - S3 Data Cleanup
  
  This script removes the data files from S3 storage that were created by
  the Iceberg tables during the demo.
  
  NOTE: Run this AFTER 06_cleanup.sql to clean up the cloud storage.
=============================================================================*/

USE ROLE ACCOUNTADMIN;

/*-----------------------------------------------------------------------------
  STEP 1: Create Temporary Stage for Cleanup
  
  Uses the storage integration to access the S3 bucket.
-----------------------------------------------------------------------------*/

CREATE OR REPLACE STAGE dt_ice_cleanup_stage
    URL = '<your-s3>'
    STORAGE_INTEGRATION = 'your storage int';

/*-----------------------------------------------------------------------------
  STEP 2: List Files (Optional - verify what will be deleted)
  
  NOTE: Snowflake-managed Iceberg tables append a unique suffix to BASE_LOCATION
  (e.g., products_staging.kTkUsUpz/).
-----------------------------------------------------------------------------*/

LIST @dt_ice_cleanup_stage/;

/*-----------------------------------------------------------------------------
  STEP 3: Remove Data Files from S3
  
  WARNING: This permanently deletes data from your S3 bucket!
-----------------------------------------------------------------------------*/

REMOVE @dt_ice_cleanup_stage/;

/*-----------------------------------------------------------------------------
  STEP 4: Verify Cleanup
-----------------------------------------------------------------------------*/

LIST @dt_ice_cleanup_stage/;

/*-----------------------------------------------------------------------------
  STEP 5: Drop the Cleanup Stage
-----------------------------------------------------------------------------*/

DROP STAGE IF EXISTS dt_ice_cleanup_stage;

/*
  S3 Cleanup Complete!
  
  All Iceberg data files have been removed from:
  your s3 location
*/
