/*=============================================================================
  Dynamic Iceberg Tables Demo - Immutability
  
  This script demonstrates the IMMUTABLE WHERE feature for Dynamic Iceberg Tables:
  - Mark historical data as unchangeable for optimized incremental refreshes
  - Skip re-processing of completed/finalized records
  
  NOTE: BACKFILL FROM is NOT supported for Dynamic Iceberg Tables.
  
  Use Case: Completed orders won't change, allowing Snowflake to skip 
  re-processing them during incremental refreshes.
=============================================================================*/

USE ROLE SYSADMIN;
USE DATABASE DT_ICE_DEMO;
USE SCHEMA DEMO;
USE WAREHOUSE DT_ICE_WH;

/*-----------------------------------------------------------------------------
  SECTION 1: UNDERSTANDING IMMUTABLE WHERE
  
  The IMMUTABLE WHERE clause tells Snowflake that rows matching the predicate
  will NOT change. This enables:
  - Skipping re-processing of historical data during refreshes
  - More efficient incremental refresh operations
  - Better performance for append-heavy workloads
  
  IMPORTANT RULES:
  - Predicates must only GROW the immutable region over time, never shrink
  - ❌ order_date < CURRENT_DATE() - 7  (shrinks as time passes - NOT ALLOWED)
  - ✅ order_status = 'COMPLETED'        (only grows as orders complete)
  - ✅ order_date < '2024-01-01'         (fixed cutoff, never changes)
-----------------------------------------------------------------------------*/

/*-----------------------------------------------------------------------------
  SECTION 2: DYNAMIC ICEBERG TABLE WITH STATUS-BASED IMMUTABILITY
  
  Orders with status 'COMPLETED' are considered final and won't change.
  This is ideal for transactional data with lifecycle states.
-----------------------------------------------------------------------------*/

CREATE OR REPLACE DYNAMIC ICEBERG TABLE order_analytics_immutable_dit
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
    region STRING,
    is_immutable BOOLEAN
)
    TARGET_LAG = '5 minutes'
    WAREHOUSE = DT_ICE_WH
    EXTERNAL_VOLUME = 'dt_ice_ext_volume'
    CATALOG = 'SNOWFLAKE'
    BASE_LOCATION = 'order_analytics_immutable_dit/'
    IMMUTABLE WHERE (order_status = 'COMPLETED')
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
    o.region,
    CASE WHEN o.order_status = 'COMPLETED' THEN TRUE ELSE FALSE END AS is_immutable
FROM orders_staging o
JOIN products_staging p ON o.product_id = p.product_id;

SELECT 'Dynamic Iceberg table with IMMUTABLE WHERE created' AS status;

/*-----------------------------------------------------------------------------
  SECTION 3: VERIFY IMMUTABILITY CONFIGURATION
-----------------------------------------------------------------------------*/

SHOW DYNAMIC TABLES LIKE 'ORDER_ANALYTICS_IMMUTABLE_DIT';

SELECT 'Current data breakdown by status:' AS info;
SELECT 
    order_status,
    is_immutable,
    COUNT(*) AS order_count
FROM order_analytics_immutable_dit
GROUP BY order_status, is_immutable
ORDER BY order_status;

/*-----------------------------------------------------------------------------
  SECTION 4: TEST INCREMENTAL REFRESH BEHAVIOR
  
  Insert new orders and observe efficient incremental processing.
  Completed orders are skipped; only new/pending orders are processed.
-----------------------------------------------------------------------------*/

SELECT 'Inserting new PENDING orders...' AS status;

INSERT INTO orders_staging (order_id, customer_id, product_id, quantity, order_date, order_status, region, created_at)
VALUES 
    (1020, 120, 1, 1, CURRENT_DATE(), 'PENDING', 'NORTH', CURRENT_TIMESTAMP()),
    (1021, 121, 5, 2, CURRENT_DATE(), 'PENDING', 'EAST', CURRENT_TIMESTAMP());

ALTER DYNAMIC TABLE order_analytics_immutable_dit REFRESH;

SELECT 
    name, 
    state, 
    refresh_action,
    refresh_trigger,
    refresh_start_time
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME => 'DT_ICE_DEMO.DEMO.ORDER_ANALYTICS_IMMUTABLE_DIT'
))
ORDER BY refresh_start_time DESC
LIMIT 3;

SELECT 'New pending orders in dynamic table:' AS status;
SELECT * FROM order_analytics_immutable_dit 
WHERE order_status = 'PENDING'
ORDER BY order_id DESC;

/*-----------------------------------------------------------------------------
  SECTION 5: SIMULATE ORDER COMPLETION
  
  When orders become COMPLETED, they become immutable.
  Future refreshes will skip these rows.
-----------------------------------------------------------------------------*/

SELECT 'Completing one of the pending orders...' AS status;

UPDATE orders_staging 
SET order_status = 'COMPLETED'
WHERE order_id = 1020;

ALTER DYNAMIC TABLE order_analytics_immutable_dit REFRESH;

SELECT 'Updated order is now immutable:' AS status;
SELECT order_id, order_status, is_immutable 
FROM order_analytics_immutable_dit 
WHERE order_id IN (1020, 1021);

/*-----------------------------------------------------------------------------
  SECTION 6: FIXED DATE IMMUTABILITY EXAMPLE
  
  Alternative pattern: Use a fixed historical cutoff date.
  All data before this date is considered immutable.
-----------------------------------------------------------------------------*/

CREATE OR REPLACE DYNAMIC ICEBERG TABLE sales_fixed_cutoff_dit
(
    region STRING,
    order_month DATE,
    total_orders NUMBER(10,0),
    total_revenue NUMBER(14,2)
)
    TARGET_LAG = '10 minutes'
    WAREHOUSE = DT_ICE_WH
    EXTERNAL_VOLUME = 'dt_ice_ext_volume'
    CATALOG = 'SNOWFLAKE'
    BASE_LOCATION = 'sales_fixed_cutoff_dit/'
    IMMUTABLE WHERE (order_month < '2024-01-01')
AS
SELECT 
    o.region,
    DATE_TRUNC('MONTH', o.order_date)::DATE AS order_month,
    COUNT(*) AS total_orders,
    SUM(o.quantity * p.unit_price) AS total_revenue
FROM orders_staging o
JOIN products_staging p ON o.product_id = p.product_id
GROUP BY o.region, DATE_TRUNC('MONTH', o.order_date);

SELECT 'Fixed date cutoff dynamic table created' AS status;
SELECT * FROM sales_fixed_cutoff_dit ORDER BY order_month;

/*
  Immutability Demo Complete!
  
  Key Takeaways:
  1. IMMUTABLE WHERE optimizes refreshes by skipping unchanged historical data
  2. Use status-based predicates (order_status = 'COMPLETED') for lifecycle data
  3. Use fixed date predicates (order_date < '2024-01-01') for historical cutoffs
  4. Predicates must only GROW the immutable region, never shrink
  5. BACKFILL FROM and ALTER IMMUTABLE are NOT supported for Dynamic Iceberg Tables
  
  Next: Run 06_cleanup.sql when ready to remove all demo objects
*/
