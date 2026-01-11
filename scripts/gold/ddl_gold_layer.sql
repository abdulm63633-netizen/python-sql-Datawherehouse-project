
-- =============================================================================
-- 1) Create Dimension: gold.dim_customer
-- =============================================================================
IF OBJECT_ID('gold.dim_customer', 'U') IS NOT NULL DROP TABLE gold.dim_customer;
CREATE TABLE gold.dim_customer (
    customer_key     INT IDENTITY PRIMARY KEY,   -- surrogate key
    customer_id      INT,
    customer_number  NVARCHAR(50),  
    first_name       NVARCHAR(100),
    last_name        NVARCHAR(100),
    birth_date       DATE,
    gender           VARCHAR(20),
    marital_status   VARCHAR(30),
    create_date      DATE,
    country          VARCHAR(50)
);

-- =============================================================================
-- 2) Create Dimension: gold.dim_product
-- =============================================================================
IF OBJECT_ID('gold.dim_product', 'U') IS NOT NULL DROP TABLE gold.dim_product;
CREATE TABLE gold.dim_product (
    product_key      INT IDENTITY PRIMARY KEY,   -- surrogate key
    product_id       INT,
    product_number   NVARCHAR(50),  
    category_id      NVARCHAR(50),
    product_name     NVARCHAR(100),
    category         NVARCHAR(50),
    sub_category     NVARCHAR(50),
    maintainance     VARCHAR(20),
    product_line     VARCHAR(30),
    product_cost     INT,
    start_date       DATE,
    end_date         DATE
);

-- =============================================================================
-- 3) Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'U') IS NOT NULL DROP TABLE gold.fact_sales;
CREATE TABLE gold.fact_sales (
   order_number     NVARCHAR(50),
   product_key      INT, -- links to dim_product.product_key
   customer_key     INT, -- links to dim_customer.customer_key
   order_date       DATE,
   ship_date        DATE,
   due_date         DATE,
   sales_amount     INT,
   quantitiy        INT,
   price            INT
);
