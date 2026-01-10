/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Purpose: 
    This script defines the cleansed layer (Silver). 
    Each table includes a 'load_timestamp' to track when data was processed.
===============================================================================
*/


-- ============================================================================
-- CRM Tables
-- ============================================================================

IF OBJECT_ID('silver.crm_customer_info', 'U') IS NOT NULL DROP TABLE silver.crm_customer_info;
CREATE TABLE silver.crm_customer_info (
    cust_id            INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_create_date    DATE,
    load_timestamp      DATETIME2 DEFAULT GETDATE() -- Automatically adds timestamp
);

IF OBJECT_ID('silver.crm_product_info', 'U') IS NOT NULL DROP TABLE silver.crm_product_info;
CREATE TABLE silver.crm_product_info (
    prd_id        INT,
    prd_key       NVARCHAR(50),
    category_id nvarchar(50),
    prd_nm        NVARCHAR(50),
    prd_cost      INT,
    prd_line      NVARCHAR(50),
    prd_start_dt  DATE,
    prd_end_dt    DATE,
    load_timestamp DATETIME DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num   NVARCHAR(50),
    sls_prd_key   NVARCHAR(50),
    sls_cust_id   INT,
    sls_order_dt  DATE,
    sls_ship_dt   DATE,
    sls_due_dt    DATE,
    sls_sales     INT,
    sls_quantity  INT,
    sls_price     INT,
    load_timestamp DATETIME DEFAULT GETDATE()
);

-- ============================================================================
-- ERP Tables
-- ============================================================================

IF OBJECT_ID('silver.erp_customer_az12', 'U') IS NOT NULL DROP TABLE silver.erp_customer_az12;
CREATE TABLE silver.erp_customer_az12 (
    cid           NVARCHAR(50),
    bdate         DATE,
    gen           NVARCHAR(50),
    load_timestamp DATETIME DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_location_a101', 'U') IS NOT NULL DROP TABLE silver.erp_location_a101;
CREATE TABLE silver.erp_location_a101 (
    cid           NVARCHAR(50),
    cntry         NVARCHAR(50),
    load_timestamp DATETIME DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_product_category', 'U') IS NOT NULL DROP TABLE silver.erp_product_category;
CREATE TABLE silver.erp_product_category (
    id            NVARCHAR(50),
    cat           NVARCHAR(50),
    subcat        NVARCHAR(50),
    maintenance   NVARCHAR(50),
    load_timestamp DATETIME DEFAULT GETDATE()
);
