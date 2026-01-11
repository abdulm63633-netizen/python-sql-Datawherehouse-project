/*
===============================================================================
Stored Procedure: Load Gold Layer (Star Schema)
===============================================================================
Purpose:
    This procedure populates the Gold Layer by joining cleaned Silver tables.
    It creates:
    - dim_customer: Unified view of customers from CRM and ERP.
    - dim_product: Unified view of products and categories.
    - fact_sales: Central transactional table linking to dimensions.

Parameters:
    None

Usage:
    EXEC gold.load_gold_layer ;
===============================================================================
*/

CREATE OR ALTER PROCEDURE gold.load_gold_layer AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;
    
    BEGIN TRY
        SET @start_time = GETDATE();
        PRINT '>> Starting Gold Layer Load at ' + CAST(@start_time AS VARCHAR);

        -- ====================================================================
        -- 1) Load Dimension: gold.dim_customer
        -- ====================================================================
        PRINT '--> Loading gold.dim_customer';
        TRUNCATE TABLE gold.dim_customer;
        INSERT INTO gold.dim_customer (
            customer_id, customer_number, first_name, last_name, 
            birth_date, gender, marital_status, country, create_date
        )
        SELECT 
            cc.cust_id AS customer_id,
            cc.cst_key AS customer_number,
            cc.cst_firstname AS first_name,
            cc.cst_lastname AS last_name,
            ec.bdate AS birth_date,
            CASE 
                WHEN cc.cst_gndr != 'Unknown' THEN cc.cst_gndr
                ELSE COALESCE(ec.gen, 'Unknown') 
            END AS gender,
            cc.cst_marital_status AS marital_status,
            el.cntry AS country,
            cc.cst_create_date AS create_date
        FROM silver.crm_customer_info AS cc  
        LEFT JOIN silver.erp_customer_az12 AS ec ON cc.cst_key = ec.cid
        LEFT JOIN silver.erp_location_a101 AS el ON cc.cst_key = el.cid;

        -- ====================================================================
        -- 2) Load Dimension: gold.dim_product
        -- ====================================================================
        PRINT '--> Loading gold.dim_product';
        TRUNCATE TABLE gold.dim_product;
        INSERT INTO gold.dim_product (
            product_id, product_number, category_id, product_name, 
            category, sub_category, maintainance, product_line, 
            product_cost, start_date, end_date
        )
        SELECT 
            cp.prd_id AS product_id,
            cp.prd_key AS product_number,
            cp.category_id AS category_id,
            cp.prd_nm AS product_name,
            ep.cat AS category,
            ep.subcat AS sub_category,
            ep.maintenance AS maintainance,
            cp.prd_line AS product_line,
            cp.prd_cost AS product_cost,
            cp.prd_start_dt AS start_date,
            cp.prd_end_dt AS end_date
        FROM silver.crm_product_info AS cp 
        LEFT JOIN silver.erp_product_category AS ep ON cp.category_id = ep.id;

        -- ====================================================================
        -- 3) Load Fact: gold.fact_sales
        -- ====================================================================
        PRINT '--> Loading gold.fact_sales';
        TRUNCATE TABLE gold.fact_sales;
        INSERT INTO gold.fact_sales (
            order_number, product_key, customer_key, order_date, 
            ship_date, due_date, sales_amount, quantitiy, price
        )
        SELECT 
            sd.sls_ord_num AS order_number,
            dp.product_key AS product_key,
            dc.customer_key AS customer_key,
            sd.sls_order_dt AS order_date,
            sd.sls_ship_dt AS ship_date,
            sd.sls_due_dt AS due_date,
            sd.sls_sales AS sales_amount,
            sd.sls_quantity AS quantitiy,
            sd.sls_price AS price
        FROM silver.crm_sales_details AS sd
        LEFT JOIN gold.dim_product AS dp ON sd.sls_prd_key = dp.product_number
        LEFT JOIN gold.dim_customer AS dc ON sd.sls_cust_id = dc.customer_id;

        SET @end_time = GETDATE();
        PRINT '>> Gold Layer Load Completed Successfully in ' + 
              CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds.';

    END TRY
    BEGIN CATCH
        PRINT '!!! ERROR OCCURRED DURING GOLD LAYER LOAD !!!';
        PRINT ERROR_MESSAGE();
    END CATCH
END;

EXEC gold.load_gold_layer 
