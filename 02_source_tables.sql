/*=============================================================================
  Dynamic Iceberg Tables Demo - Source Tables
  
  This script creates source/staging Iceberg tables with sample data that will
  be used as input for the Dynamic Iceberg Tables.
=============================================================================*/

USE ROLE SYSADMIN;
USE DATABASE DT_ICE_DEMO;
USE SCHEMA DEMO;
USE WAREHOUSE DT_ICE_WH;

/*-----------------------------------------------------------------------------
  PRODUCTS STAGING TABLE
  
  Reference data for products - stored as Iceberg table
-----------------------------------------------------------------------------*/

CREATE OR REPLACE ICEBERG TABLE products_staging (
    product_id NUMBER(10,0),
    product_name STRING,
    category STRING,
    unit_price NUMBER(10,2),
    created_at TIMESTAMP_NTZ
)
    EXTERNAL_VOLUME = 'dt_ice_ext_volume'
    CATALOG = 'SNOWFLAKE'
    BASE_LOCATION = 'products_staging/';

-- Insert sample product data
INSERT INTO products_staging (product_id, product_name, category, unit_price, created_at)
VALUES 
    (1, 'Laptop Pro 15', 'Electronics', 1299.99, CURRENT_TIMESTAMP()),
    (2, 'Wireless Mouse', 'Electronics', 49.99, CURRENT_TIMESTAMP()),
    (3, 'USB-C Hub', 'Electronics', 79.99, CURRENT_TIMESTAMP()),
    (4, 'Standing Desk', 'Furniture', 599.99, CURRENT_TIMESTAMP()),
    (5, 'Ergonomic Chair', 'Furniture', 449.99, CURRENT_TIMESTAMP()),
    (6, 'Monitor 27 inch', 'Electronics', 399.99, CURRENT_TIMESTAMP()),
    (7, 'Mechanical Keyboard', 'Electronics', 149.99, CURRENT_TIMESTAMP()),
    (8, 'Desk Lamp', 'Furniture', 89.99, CURRENT_TIMESTAMP()),
    (9, 'Webcam HD', 'Electronics', 129.99, CURRENT_TIMESTAMP()),
    (10, 'Cable Management Kit', 'Accessories', 29.99, CURRENT_TIMESTAMP());

-- Verify products data
SELECT * FROM products_staging ORDER BY product_id;

/*-----------------------------------------------------------------------------
  ORDERS STAGING TABLE
  
  Transactional order data - stored as Iceberg table
-----------------------------------------------------------------------------*/

CREATE OR REPLACE ICEBERG TABLE orders_staging (
    order_id NUMBER(10,0),
    customer_id NUMBER(10,0),
    product_id NUMBER(10,0),
    quantity NUMBER(5,0),
    order_date DATE,
    order_status STRING,
    region STRING,
    created_at TIMESTAMP_NTZ
)
    EXTERNAL_VOLUME = 'dt_ice_ext_volume'
    CATALOG = 'SNOWFLAKE'
    BASE_LOCATION = 'orders_staging/';

-- Insert sample order data
INSERT INTO orders_staging (order_id, customer_id, product_id, quantity, order_date, order_status, region, created_at)
VALUES 
    -- January 2024 orders
    (1001, 101, 1, 1, '2024-01-15', 'COMPLETED', 'NORTH', CURRENT_TIMESTAMP()),
    (1002, 102, 2, 3, '2024-01-16', 'COMPLETED', 'SOUTH', CURRENT_TIMESTAMP()),
    (1003, 103, 4, 1, '2024-01-17', 'COMPLETED', 'EAST', CURRENT_TIMESTAMP()),
    (1004, 101, 3, 2, '2024-01-18', 'COMPLETED', 'NORTH', CURRENT_TIMESTAMP()),
    (1005, 104, 5, 1, '2024-01-19', 'COMPLETED', 'WEST', CURRENT_TIMESTAMP()),
    
    -- February 2024 orders
    (1006, 105, 1, 2, '2024-02-10', 'COMPLETED', 'SOUTH', CURRENT_TIMESTAMP()),
    (1007, 102, 6, 1, '2024-02-12', 'COMPLETED', 'SOUTH', CURRENT_TIMESTAMP()),
    (1008, 106, 7, 1, '2024-02-14', 'COMPLETED', 'EAST', CURRENT_TIMESTAMP()),
    (1009, 103, 2, 5, '2024-02-15', 'COMPLETED', 'EAST', CURRENT_TIMESTAMP()),
    (1010, 107, 8, 2, '2024-02-18', 'SHIPPED', 'NORTH', CURRENT_TIMESTAMP()),
    
    -- March 2024 orders
    (1011, 101, 9, 1, '2024-03-01', 'COMPLETED', 'NORTH', CURRENT_TIMESTAMP()),
    (1012, 108, 10, 4, '2024-03-05', 'COMPLETED', 'WEST', CURRENT_TIMESTAMP()),
    (1013, 104, 1, 1, '2024-03-10', 'SHIPPED', 'WEST', CURRENT_TIMESTAMP()),
    (1014, 109, 4, 1, '2024-03-12', 'PENDING', 'SOUTH', CURRENT_TIMESTAMP()),
    (1015, 102, 5, 2, '2024-03-15', 'PENDING', 'SOUTH', CURRENT_TIMESTAMP());

-- Verify orders data
SELECT * FROM orders_staging ORDER BY order_id;

-- Show record counts
SELECT 'products_staging' AS table_name, COUNT(*) AS row_count FROM products_staging
UNION ALL
SELECT 'orders_staging' AS table_name, COUNT(*) AS row_count FROM orders_staging;

/*
  Source tables created!
  
  Summary:
  - products_staging: 10 products across 3 categories
  - orders_staging: 15 orders from various customers and regions
  
  Next: Run 03_dynamic_iceberg.sql to create Dynamic Iceberg Tables
*/
