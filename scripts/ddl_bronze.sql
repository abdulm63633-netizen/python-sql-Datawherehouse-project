/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Purpose: 
    This script defines the raw ingestion layer (Bronze). 
    It drops existing tables before recreating them to ensure a clean setup.
===============================================================================
*/


-- ============================================================================
-- CRM Tables
-- ============================================================================

IF OBJECT_ID('bronze.crm_customer_info', 'U') IS NOT NULL DROP TABLE bronze.crm_customer_info;
CREATE TABLE bronze.crm_customer_info (
    cust_id            INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_create_date    DATE
);

IF OBJECT_ID('bronze.crm_product_info', 'U') IS NOT NULL DROP TABLE bronze.crm_product_info;
CREATE TABLE bronze.crm_product_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE
);

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

-- ============================================================================
-- ERP Tables
-- ============================================================================

IF OBJECT_ID('bronze.erp_customer_az12', 'U') IS NOT NULL DROP TABLE bronze.erp_customer_az12;
CREATE TABLE bronze.erp_customer_az12 (
    cid   NVARCHAR(50),
    bdate DATE,
    gen   NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_location_a101', 'U') IS NOT NULL DROP TABLE bronze.erp_location_a101;
CREATE TABLE bronze.erp_location_a101 (
    cid   NVARCHAR(50),
    cntry NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_product_category', 'U') IS NOT NULL DROP TABLE bronze.erp_product_category;
CREATE TABLE bronze.erp_product_category (
    id          NVARCHAR(50),
    cat         NVARCHAR(50),
    subcat      NVARCHAR(50),
    maintenance NVARCHAR(50)
);
