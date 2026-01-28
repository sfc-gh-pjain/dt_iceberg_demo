/*=============================================================================
  Dynamic Iceberg Tables Demo - Dynamic Iceberg Tables
  
  This script creates Dynamic Iceberg Tables that automatically transform
  and aggregate data from the source staging tables.
  
  Key Features Demonstrated:
  - Declarative SQL transformations
  - TARGET_LAG for freshness control
  - Joins between Iceberg tables
  - Aggregations and analytics
=============================================================================*/

USE ROLE SYSADMIN;
USE DATABASE DT_ICE_DEMO;
USE SCHEMA DEMO;
USE WAREHOUSE DT_ICE_WH;

/*-----------------------------------------------------------------------------
  DYNAMIC ICEBERG TABLE 1: Order Details (Enriched)
  
  Joins orders with products to create an enriched order view.
  TARGET_LAG = 10 minutes means data will be refreshed within 10 minutes
  of changes in source tables.
-----------------------------------------------------------------------------*/

CREATE OR REPLACE DYNAMIC ICEBERG TABLE order_details_dit
(
    order_id NUMBER(10,0),
    customer_id NUMBER(10,0),
    product_id NUMBER(10,0),
    product_name STRING,
    category STRING,
    quantity NUMBER(5,0),
    unit_price NUMBER(10,2),
    total_amount NUMBER(12,2),
    order_date DATE,
    order_status STRING,
    region STRING
)
    TARGET_LAG = '10 minutes'
    WAREHOUSE = DT_ICE_WH
    EXTERNAL_VOLUME = 'dt_ice_ext_volume'
    CATALOG = 'SNOWFLAKE'
    BASE_LOCATION = 'order_details_dit/'
AS
SELECT 
    o.order_id,
    o.customer_id,
    o.product_id,
    p.product_name,
    p.category,
    o.quantity,
    p.unit_price,
    (o.quantity * p.unit_price) AS total_amount,
    o.order_date,
    o.order_status,
    o.region
FROM orders_staging o
JOIN products_staging p ON o.product_id = p.product_id;

/*-----------------------------------------------------------------------------
  DYNAMIC ICEBERG TABLE 2: Product Sales Summary
  
  Aggregates sales data by product with various metrics.
  TARGET_LAG = 20 minutes for less time-sensitive analytics.
-----------------------------------------------------------------------------*/

CREATE OR REPLACE DYNAMIC ICEBERG TABLE product_sales_summary_dit
(
    product_id NUMBER(10,0),
    product_name STRING,
    category STRING,
    total_orders NUMBER(10,0),
    total_quantity_sold NUMBER(10,0),
    total_revenue NUMBER(14,2),
    avg_order_quantity NUMBER(10,2),
    first_order_date DATE,
    last_order_date DATE
)
    TARGET_LAG = '20 minutes'
    WAREHOUSE = DT_ICE_WH
    EXTERNAL_VOLUME = 'dt_ice_ext_volume'
    CATALOG = 'SNOWFLAKE'
    BASE_LOCATION = 'product_sales_summary_dit/'
AS
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.quantity) AS total_quantity_sold,
    SUM(o.quantity * p.unit_price) AS total_revenue,
    AVG(o.quantity) AS avg_order_quantity,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date
FROM products_staging p
LEFT JOIN orders_staging o ON p.product_id = o.product_id
GROUP BY p.product_id, p.product_name, p.category;

/*-----------------------------------------------------------------------------
  DYNAMIC ICEBERG TABLE 3: Regional Sales Dashboard
  
  Regional performance metrics for executive dashboards.
  TARGET_LAG = 30 minutes for batch reporting use cases.
-----------------------------------------------------------------------------*/

CREATE OR REPLACE DYNAMIC ICEBERG TABLE regional_sales_dit
(
    region STRING,
    order_month DATE,
    total_orders NUMBER(10,0),
    total_revenue NUMBER(14,2),
    unique_customers NUMBER(10,0),
    top_category STRING,
    avg_order_value NUMBER(12,2)
)
    TARGET_LAG = '30 minutes'
    WAREHOUSE = DT_ICE_WH
    EXTERNAL_VOLUME = 'dt_ice_ext_volume'
    CATALOG = 'SNOWFLAKE'
    BASE_LOCATION = 'regional_sales_dit/'
AS
WITH regional_metrics AS (
    SELECT 
        o.region,
        DATE_TRUNC('MONTH', o.order_date) AS order_month,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.quantity * p.unit_price) AS total_revenue,
        COUNT(DISTINCT o.customer_id) AS unique_customers,
        AVG(o.quantity * p.unit_price) AS avg_order_value
    FROM orders_staging o
    JOIN products_staging p ON o.product_id = p.product_id
    GROUP BY o.region, DATE_TRUNC('MONTH', o.order_date)
),
category_revenue AS (
    SELECT 
        o.region,
        DATE_TRUNC('MONTH', o.order_date) AS order_month,
        p.category,
        SUM(o.quantity * p.unit_price) AS category_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY o.region, DATE_TRUNC('MONTH', o.order_date) 
            ORDER BY SUM(o.quantity * p.unit_price) DESC
        ) AS rn
    FROM orders_staging o
    JOIN products_staging p ON o.product_id = p.product_id
    GROUP BY o.region, DATE_TRUNC('MONTH', o.order_date), p.category
)
SELECT 
    rm.region,
    rm.order_month,
    rm.total_orders,
    rm.total_revenue,
    rm.unique_customers,
    cr.category AS top_category,
    rm.avg_order_value
FROM regional_metrics rm
LEFT JOIN category_revenue cr 
    ON rm.region = cr.region 
    AND rm.order_month = cr.order_month 
    AND cr.rn = 1;

/*-----------------------------------------------------------------------------
  VERIFY DYNAMIC ICEBERG TABLES
-----------------------------------------------------------------------------*/

-- Show all dynamic tables in schema
SHOW DYNAMIC TABLES IN SCHEMA DT_ICE_DEMO.DEMO;

-- Check Iceberg tables metadata
SHOW ICEBERG TABLES IN SCHEMA DT_ICE_DEMO.DEMO;

/*
  Dynamic Iceberg Tables created!
  
  Summary:
  - order_details_dit: Enriched order data (10 min lag)
  - product_sales_summary_dit: Product-level metrics (20 min lag)
  - regional_sales_dit: Regional dashboard data (30 min lag)
  
  The tables will automatically refresh based on their TARGET_LAG settings.
  
  Next: Run 04_demo_operations.sql to explore operations and queries
*/
