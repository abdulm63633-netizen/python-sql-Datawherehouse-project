/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Purpose:
    This procedure truncates and reloads the Bronze tables from source CSV files.
    It Perfrom the following action :
        Truncate the bronze table before inserting data
        use 'Bulk insert' command to load data from csv file to bronze tables
Usage example :

    EXEC bronze.load_bronze
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;
    SET @start_time = GETDATE();
    
    PRINT '================================================';
    PRINT 'Loading Bronze Layer';
    PRINT '================================================';

    print '-- -------------------------------------------------------------------------'
    print '-- INSERTING TO  CRM Tables'
    print '-- -------------------------------------------------------------------------'
    PRINT '>> Truncating and Loading: bronze.crm_customer_info';
    TRUNCATE TABLE bronze.crm_customer_info;
    BULK INSERT bronze.crm_customer_info
    FROM 'C:\Users\khali\OneDrive\Desktop\data engineering project\sql + python data wherehouse project\datasets\source_crm\cust_info.csv'
    WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

    PRINT '>> Truncating and Loading: bronze.crm_product_info';
    TRUNCATE TABLE bronze.crm_product_info;
    BULK INSERT bronze.crm_product_info
    FROM 'C:\Users\khali\OneDrive\Desktop\data engineering project\sql + python data wherehouse project\datasets\source_crm\prd_info.csv'
    WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

    PRINT '>> Truncating and Loading: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\khali\OneDrive\Desktop\data engineering project\sql + python data wherehouse project\datasets\source_crm\sales_details.csv'
    WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

    print '-- -------------------------------------------------------------------------'
    print '-- INSERTING TO  ERP Tables'
    print '-- -------------------------------------------------------------------------'
    PRINT '>> Truncating and Loading: bronze.erp_customer_az12';
    TRUNCATE TABLE bronze.erp_customer_az12;
    BULK INSERT bronze.erp_customer_az12
    FROM 'C:\Users\khali\OneDrive\Desktop\data engineering project\sql + python data wherehouse project\datasets\source_erp\cust_az12.csv'
    WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

    PRINT '>> Truncating and Loading: bronze.erp_location_a101';
    TRUNCATE TABLE bronze.erp_location_a101;
    BULK INSERT bronze.erp_location_a101
    FROM 'C:\Users\khali\OneDrive\Desktop\data engineering project\sql + python data wherehouse project\datasets\source_erp\loc_a101.csv'
    WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

    PRINT '>> Truncating and Loading: bronze.erp_product_category';
    TRUNCATE TABLE bronze.erp_product_category;
    BULK INSERT bronze.erp_product_category
    FROM 'C:\Users\khali\OneDrive\Desktop\data engineering project\sql + python data wherehouse project\datasets\source_erp\px_cat_g1v2.csv'
    WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

    SET @end_time = GETDATE();
    PRINT '================================================';
    PRINT 'Bronze Load Completed Successfully';
    PRINT 'Total Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
    PRINT '================================================';
END;
GO



exec bronze.load_bronze
