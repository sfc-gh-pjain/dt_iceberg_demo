/*=============================================================================
  Dynamic Iceberg Tables Demo - Cleanup Script
  
  This script removes all objects created during the demo.
  
  WARNING: This will permanently delete all demo data and objects!
=============================================================================*/

USE ROLE SYSADMIN;

/*-----------------------------------------------------------------------------
  STEP 1: Drop Dynamic Iceberg Tables
  
  Drop dynamic tables first as they depend on source tables
-----------------------------------------------------------------------------*/

DROP DYNAMIC TABLE IF EXISTS DT_ICE_DEMO.DEMO.order_details_dit;
DROP DYNAMIC TABLE IF EXISTS DT_ICE_DEMO.DEMO.product_sales_summary_dit;
DROP DYNAMIC TABLE IF EXISTS DT_ICE_DEMO.DEMO.regional_sales_dit;
DROP DYNAMIC TABLE IF EXISTS DT_ICE_DEMO.DEMO.order_analytics_immutable_dit;
DROP DYNAMIC TABLE IF EXISTS DT_ICE_DEMO.DEMO.sales_fixed_cutoff_dit;

/*-----------------------------------------------------------------------------
  STEP 2: Drop Source Iceberg Tables
-----------------------------------------------------------------------------*/

DROP ICEBERG TABLE IF EXISTS DT_ICE_DEMO.DEMO.orders_staging;
DROP ICEBERG TABLE IF EXISTS DT_ICE_DEMO.DEMO.products_staging;

/*-----------------------------------------------------------------------------
  STEP 3: Drop Schema and Database
-----------------------------------------------------------------------------*/

DROP SCHEMA IF EXISTS DT_ICE_DEMO.DEMO;
DROP DATABASE IF EXISTS DT_ICE_DEMO;

/*-----------------------------------------------------------------------------
  STEP 4: Drop Warehouse
-----------------------------------------------------------------------------*/

DROP WAREHOUSE IF EXISTS DT_ICE_WH;

/*-----------------------------------------------------------------------------
  STEP 5: Drop External Volume
  
  NOTE: The external volume references cloud storage. Dropping the volume
  does NOT delete data from your cloud storage. You may want to manually
  clean up the S3/Azure paths if needed.
-----------------------------------------------------------------------------*/

DROP EXTERNAL VOLUME IF EXISTS dt_ice_ext_volume;

/*-----------------------------------------------------------------------------
  VERIFICATION
-----------------------------------------------------------------------------*/

-- Verify all objects are removed
SHOW DATABASES LIKE 'DT_ICE_DEMO';
SHOW WAREHOUSES LIKE 'DT_ICE_WH';
SHOW EXTERNAL VOLUMES LIKE 'dt_ice_ext_volume';

/*
  Cleanup Complete!
  
  All demo objects have been removed from Snowflake.
  
  NOTE: If you created data in cloud storage (S3/Azure), you may want to
  manually delete the following paths:
  - s3://sfpjain-us-west-2/dt_iceberg_dw/tables/products_staging/
  - s3://sfpjain-us-west-2/dt_iceberg_dw/tables/orders_staging/
  - s3://sfpjain-us-west-2/dt_iceberg_dw/tables/order_details_dit/
  - s3://sfpjain-us-west-2/dt_iceberg_dw/tables/product_sales_summary_dit/
  - s3://sfpjain-us-west-2/dt_iceberg_dw/tables/regional_sales_dit/
  - s3://sfpjain-us-west-2/dt_iceberg_dw/tables/order_analytics_immutable_dit/
  - s3://sfpjain-us-west-2/dt_iceberg_dw/tables/sales_fixed_cutoff_dit/
*/
