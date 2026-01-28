/*=============================================================================
  Dynamic Iceberg Tables Demo - Operations & Monitoring
  
  This script demonstrates operational tasks for Dynamic Iceberg Tables:
  - Monitoring refresh status
  - Manual refresh operations
  - Querying transformed data
  - Viewing Iceberg metadata
  - Inserting new data to trigger refreshes
=============================================================================*/

USE ROLE SYSADMIN;
USE DATABASE DT_ICE_DEMO;
USE SCHEMA DEMO;
USE WAREHOUSE DT_ICE_WH;

/*=============================================================================
  SECTION 1: MONITORING DYNAMIC ICEBERG TABLES
=============================================================================*/

-- View all dynamic tables and their current status
SHOW DYNAMIC TABLES IN SCHEMA DT_ICE_DEMO.DEMO;

-- Detailed refresh history for a specific dynamic table
SELECT *
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME => 'DT_ICE_DEMO.DEMO.ORDER_DETAILS_DIT'
))
ORDER BY REFRESH_START_TIME DESC
LIMIT 10;

-- Check data freshness and lag across all dynamic tables
SELECT 
    name,
    target_lag,
    refresh_mode,
    refresh_mode_reason,
    scheduling_state,
    data_timestamp
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY())
WHERE SCHEMA_NAME = 'DEMO'
ORDER BY name;

/*=============================================================================
  SECTION 2: QUERYING DYNAMIC ICEBERG TABLES
=============================================================================*/

-- Query enriched order details
SELECT * FROM order_details_dit
ORDER BY order_date DESC, order_id;

-- Query product sales summary
SELECT 
    product_name,
    category,
    total_orders,
    total_quantity_sold,
    total_revenue,
    ROUND(total_revenue / NULLIF(total_orders, 0), 2) AS revenue_per_order
FROM product_sales_summary_dit
ORDER BY total_revenue DESC;

-- Query regional sales dashboard
SELECT 
    region,
    TO_CHAR(order_month, 'YYYY-MM') AS month,
    total_orders,
    total_revenue,
    unique_customers,
    top_category,
    ROUND(avg_order_value, 2) AS avg_order_value
FROM regional_sales_dit
ORDER BY order_month DESC, total_revenue DESC;

-- Analytics: Top products by revenue
SELECT 
    product_name,
    category,
    total_revenue,
    ROUND(100.0 * total_revenue / SUM(total_revenue) OVER (), 2) AS revenue_pct
FROM product_sales_summary_dit
WHERE total_revenue > 0
ORDER BY total_revenue DESC;

-- Analytics: Regional performance comparison
SELECT 
    region,
    SUM(total_orders) AS total_orders,
    SUM(total_revenue) AS total_revenue,
    SUM(unique_customers) AS total_customers,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM regional_sales_dit
GROUP BY region
ORDER BY total_revenue DESC;

/*=============================================================================
  SECTION 3: MANUAL REFRESH OPERATIONS
=============================================================================*/

-- Manually refresh a specific dynamic iceberg table
ALTER DYNAMIC TABLE order_details_dit REFRESH;

-- Suspend automatic refresh (for maintenance)
ALTER DYNAMIC TABLE order_details_dit SUSPEND;

-- Resume automatic refresh
ALTER DYNAMIC TABLE order_details_dit RESUME;

-- Check current state after operations
SELECT 
    name,
    scheduling_state
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY())
WHERE SCHEMA_NAME = 'DEMO' AND NAME = 'ORDER_DETAILS_DIT';

/*=============================================================================
  SECTION 4: SIMULATING DATA CHANGES
=============================================================================*/

-- Insert new orders to trigger downstream refreshes
INSERT INTO orders_staging (order_id, customer_id, product_id, quantity, order_date, order_status, region)
VALUES 
    (1016, 110, 1, 2, CURRENT_DATE(), 'PENDING', 'NORTH'),
    (1017, 111, 6, 1, CURRENT_DATE(), 'PENDING', 'EAST'),
    (1018, 112, 3, 3, CURRENT_DATE(), 'PENDING', 'WEST');

-- Add a new product
INSERT INTO products_staging (product_id, product_name, category, unit_price)
VALUES (11, 'Wireless Earbuds', 'Electronics', 199.99);

-- Verify new data in source
SELECT * FROM orders_staging WHERE order_date = CURRENT_DATE();
SELECT * FROM products_staging WHERE product_id = 11;

-- Manually trigger refresh to see changes immediately
ALTER DYNAMIC TABLE order_details_dit REFRESH;
ALTER DYNAMIC TABLE product_sales_summary_dit REFRESH;
ALTER DYNAMIC TABLE regional_sales_dit REFRESH;

-- Verify updated data in dynamic tables
SELECT * FROM order_details_dit WHERE order_date = CURRENT_DATE();

/*=============================================================================
  SECTION 5: ICEBERG TABLE METADATA
=============================================================================*/

-- View Iceberg table properties
SHOW ICEBERG TABLES LIKE '%_dit' IN SCHEMA DT_ICE_DEMO.DEMO;

-- Check table storage location
SELECT 
    TABLE_NAME,
    BASE_LOCATION,
    CATALOG_NAME,
    EXTERNAL_VOLUME_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'DEMO' 
  AND TABLE_TYPE = 'ICEBERG TABLE';

-- View snapshot history for an Iceberg table (if available)
-- Note: Iceberg metadata functions may vary by Snowflake version
SELECT 
    SYSTEM$GET_ICEBERG_TABLE_INFORMATION('DT_ICE_DEMO.DEMO.ORDER_DETAILS_DIT') AS iceberg_info;

/*=============================================================================
  SECTION 6: PERFORMANCE ANALYSIS
=============================================================================*/

-- Analyze query performance on dynamic iceberg tables
EXPLAIN USING JSON
SELECT * FROM order_details_dit WHERE region = 'NORTH';

-- View warehouse usage for dynamic table refreshes
SELECT 
    QUERY_TYPE,
    COUNT(*) AS query_count,
    SUM(TOTAL_ELAPSED_TIME)/1000 AS total_seconds,
    AVG(TOTAL_ELAPSED_TIME)/1000 AS avg_seconds
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    CURRENT_TIMESTAMP()
))
WHERE WAREHOUSE_NAME = 'DT_ICE_WH'
  AND QUERY_TYPE IN ('CREATE_TABLE_AS_SELECT', 'INSERT', 'SELECT')
GROUP BY QUERY_TYPE
ORDER BY total_seconds DESC;

/*
  Operations Demo Complete!
  
  Key Operations Covered:
  - Monitoring refresh status and history
  - Querying transformed data with analytics
  - Manual refresh control (suspend/resume/refresh)
  - Simulating data changes and observing propagation
  - Viewing Iceberg metadata
  
  Next: Run 05_cleanup.sql when ready to remove demo objects
*/
