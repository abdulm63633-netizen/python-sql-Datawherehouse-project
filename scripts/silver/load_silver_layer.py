
"""
====================================================================================================
🥈 SILVER LAYER: DATA TRANSFORMATION & CLEANSED AREA
====================================================================================================

📌 PURPOSE:
    This module defines the cleansing and transformation logic for the Data Warehouse. 
    It processes raw data from the 'Bronze' schema and loads it into the 'Silver' 
    schema using a Truncate-and-Load strategy.

🛠️ WHAT WAS DONE (START TO END):
    1.  DATA EXTRACTION: Raw data read from SQL Server Bronze tables without source mutation.
    2.  CLEANING & STANDARDIZATION: Used Python and Pandas to fix invalid dates, remove 
        duplicates, handle missing values, and normalize text fields.
    3.  BUSINESS RULES: Applied rules to ensure consistency, such as standardizing gender, 
        marital status, product categories, and country names.
    4.  DATA QUALITY CHECKS: Enforced checks to ensure only valid business keys and trusted 
        records are maintained in the Silver layer.
    5.  METADATA ENRICHMENT: Added columns like 'load_timestamp' to support data lineage 
        and auditing.
    6.  BATCH PROCESSING: Existing Silver tables were truncated and reloaded with clean, 
        trusted data using a controlled batch process.
    7.  ORCHESTRATION: A central Python orchestrator executed all loads in the correct 
        dependency order for repeatable data processing.

🏗️ ARCHITECTURE OVERVIEW:
    * STRATEGY: Truncate-and-Load for 100% data consistency.
    * TOOLS: Python (Pandas) for logic; SQL Server for storage.
    * ORCHESTRATION: Modular functions executed in specific dependency order.

====================================================================================================
"""


import numpy as np
import pandas as pd
from sqlalchemy import create_engine, engine
from sqlalchemy import text


# ========================================================== EXTRACTING DATA FROM BRONZE LAYER ==============================================================================

# 1. Connection Setup
driver = 'ODBC Driver 18 for SQL Server'
server = 'DELL\\SQLEXPRESS'
database = 'sql_python_data_wherehouse'

connection_string = (
    f"Driver={{{driver}}};"
    f"Server={server};"
    f"Database={database};"
    f"Trusted_Connection=yes;"
    f"TrustServerCertificate=yes;"
)

# 2. Create the Engine (to avoid Pandas warnings)
connection_url = engine.URL.create(
    "mssql+pyodbc", 
    query={"odbc_connect": connection_string}
)
db_engine = create_engine(connection_url)

try:
    # 3. Load tables into separate DataFrames for individual CLEANING
    print("🔄 Connecting to SQL Server and fetching tables...")
    
    customer_info    = pd.read_sql("SELECT * FROM bronze.crm_customer_info", db_engine)
    product_info     = pd.read_sql("SELECT * FROM bronze.crm_product_info", db_engine)
    sales_details    = pd.read_sql("SELECT * FROM bronze.crm_sales_details", db_engine)
    customer_az12    = pd.read_sql("SELECT * FROM bronze.erp_customer_az12", db_engine)
    location_a101    = pd.read_sql("SELECT * FROM bronze.erp_location_a101", db_engine)
    product_category = pd.read_sql("SELECT * FROM bronze.erp_product_category", db_engine)

    print("✅ All tables loaded successfully into separate DataFrames.")

   
    
except Exception as e:
    print(f"❌ Connection failed: {e}")




# ========================================================== CLEANING  DATA ==============================================================================

    # '========================================================== CLEANING crm_customer_info =================================='

def load_crm_customer_info(engine):
    try:
        customer_info    = pd.read_sql("SELECT * FROM bronze.crm_customer_info", db_engine)
        # 1. Drop invalid business keys first
        customer_info = customer_info.dropna(subset=['cust_id'])
        
        # 2. Convert date
        customer_info['cst_create_date'] = pd.to_datetime(
            customer_info['cst_create_date'],
            errors='coerce'
        ).dt.date
        
        # 3. Sort and deduplicate (keep latest)
        customer_info = (
            customer_info
            .sort_values(by=['cust_id', 'cst_create_date'], ascending=[True, False])
            .drop_duplicates(subset='cust_id', keep='first')
        )
        
        # 4. Clean names
        customer_info['cst_firstname'] = (
            customer_info['cst_firstname']
            .str.strip()
            .replace('', 'Unknown')
        )
        
        customer_info['cst_lastname'] = (
            customer_info['cst_lastname']
            .str.strip()
            .replace('', 'Unknown')
        )
        
        # 5. Clean marital status
        customer_info['cst_marital_status'] = (
            customer_info['cst_marital_status']
            .str.upper()
            .str.strip()
            .replace({
                'M': 'Married',
                'MARRIED': 'Married',
                'S': 'Single',
                'SINGLE': 'Single'
            })
            .fillna('Unknown')
        )
        
        # 6. Clean gender
        customer_info['cst_gndr'] = (
            customer_info['cst_gndr']
            .str.upper()
            .str.strip()
            .replace({
                'M': 'Male',
                'MALE': 'Male',
                'F': 'Female',
                'FEMALE': 'Female'
            })
            .fillna('Unknown')
        )
        
        # 7. Reset index at the end
        customer_info = customer_info.reset_index(drop=True)
        customer_info['load_timestamp'] = pd.Timestamp.now()
    # '========================================================== complete cleaning crm_customer_info =================================='
        
            #=========================== truncating table crm_customer_info =========================================#
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE silver.crm_customer_info"))
         #=========================== loading to silver layer =========================================#
        customer_info.to_sql(
            'crm_customer_info',
            engine,
            schema='silver',
            if_exists='append',
            index=False
        )
        print("✅ Loaded silver.crm_customer_info")
    except Exception as e :
       print(f"❌ Failed loading {e}")




 # '========================================================== Completed ✅ CLEANING crm_customer_info =================================='



# '==========================================================  CLEANING crm_product_info =================================='
def load_crm_product_info(engine):
    try:
        product_info  = pd.read_sql("SELECT * FROM bronze.crm_product_info", db_engine)
        # 1. Drop invalid business keys
        product_info = product_info.dropna(subset=['prd_key'])
        
        # 2. Convert dates
        product_info['prd_start_dt'] = pd.to_datetime(product_info['prd_start_dt'], errors='coerce').dt.date
        product_info['prd_end_dt']   = pd.to_datetime(product_info['prd_end_dt'], errors='coerce').dt.date
        
        # 7. Sort for SCD logic
        product_info = product_info.sort_values(['prd_key', 'prd_start_dt'])
        
        # 8. Derive end date
        product_info['prd_end_dt'] = (
            product_info
            .groupby('prd_key')['prd_start_dt']
            .shift(-1) - pd.Timedelta(days=1)
        )
        
        
        # 3. Extract category_id
        product_info['category_id'] = (
            product_info['prd_key']
            .str[:5]
            .str.replace('-', '_', regex=False)
        )
        ## shifting category_id column from last to 2nd position 
        col_series = product_info.pop('category_id')
        product_info.insert(2, 'category_id', col_series)
        
        # 4. Extract actual product key
        product_info['prd_key'] = product_info['prd_key'].str[6:]
        
        # 5. Handle missing cost
        product_info['prd_cost'] = product_info['prd_cost'].fillna(0)
        
        # 6. Standardize product line
        product_info['prd_line'] = (
            product_info['prd_line']
            .str.upper()
            .str.strip()
            .replace({
                'M': 'Mountain',
                'MOUNTAIN': 'Mountain',
                'R': 'Road',
                'ROAD': 'Road',
                'S': 'Other Sales',
                'OTHER SALES': 'Other Sales',
                'T': 'Touring',
                'TOURING': 'Touring'
            })
            .fillna('Unknown')
        )


# 9. Reset index
# product_info = product_info.reset_index(drop=True)

        product_info['load_timestamp'] = pd.Timestamp.now()
        #=========================== truncating table crm_product_info =========================================#
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE silver.crm_product_info"))
        #=========================== loading to silver layer =========================================#
        product_info.to_sql(
            'crm_product_info',
            engine,
            schema='silver',
            if_exists='append',
            index=False
        )
        print("✅ Loaded silver.crm_product_info")
    except Exception as e :
        print(f"❌ Failed loading {e}")


# '==========================================================  Completed ✅ CLEANING crm_product_info =================================='




# '========================================================== CLEANING crm_sales_details =================================='
def load_crm_sales_details(engine):
    try:
        sales_details    = pd.read_sql("SELECT * FROM bronze.crm_sales_details", db_engine)
        def clean_yyyymmdd(col):
            col = col.astype(str).str.split('.').str[0] # Handle cases like '20230101.0'
            col = np.where(col.str.len() != 8, None, col)
            return pd.to_datetime(col, format='%Y%m%d', errors='coerce')
        
            # Apply the CLEANING
        sales_details['sls_order_dt'] = clean_yyyymmdd(sales_details['sls_order_dt'])
        sales_details['sls_ship_dt']  = clean_yyyymmdd(sales_details['sls_ship_dt'])
        sales_details['sls_due_dt']   = clean_yyyymmdd(sales_details['sls_due_dt'])
        
        
        
        # ===========================================================================
        # ===========================================================================
        
        # We pre-fill quantity and price with 0 during the calculation to prevent new NaNs
        calc_sales = sales_details['sls_quantity'].fillna(0) * sales_details['sls_price'].fillna(0).abs()
        
        sales_condition = (
            sales_details['sls_sales'].isna() | 
            (sales_details['sls_sales'] <= 0) | 
            (sales_details['sls_sales'] != calc_sales)
        )
        
        sales_details['sls_sales'] = np.where(sales_condition, calc_sales, sales_details['sls_sales'])
        
        # 2. SLS_PRICE CLEANSING
        # We use replace(0, np.nan) to mimic SQL's NULLIF and prevent division by zero
        price_condition = (sales_details['sls_price'].isna()) | (sales_details['sls_price'] <= 0)
        
        calc_price = sales_details['sls_sales'] / sales_details['sls_quantity'].replace(0, np.nan)
        
        sales_details['sls_price'] = np.where(price_condition, calc_price, sales_details['sls_price'])
        
        # 3. FINAL SAFETY STEP (Removes the remaining 7 nulls)
        # Fill any remaining NaNs (caused by 0/0 or initial nulls not caught by conditions)
        sales_details[['sls_sales', 'sls_price']] = sales_details[['sls_sales', 'sls_price']].fillna(0)
    
        sales_details['load_timestamp'] = pd.Timestamp.now()
           #=========================== truncating table crm_sales_details =========================================#
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE silver.crm_sales_details"))
         #=========================== loading to silver layer =========================================#
        sales_details.to_sql(
            'crm_sales_details',
            engine,
            schema='silver',
            if_exists='append',
            index=False
        )
        print("✅ Loaded silver.crm_sales_details")
    except Exception as e :
        print(f"❌ Failed loading {e}")

# '========================================================== Completed ✅ CLEANING crm_sales_details =================================='



   # '========================================================== CLEANING erp_customer_az12 =================================='

def load_erp_customer_az12(engine):
    try:
        customer_az12    = pd.read_sql("SELECT * FROM bronze.erp_customer_az12", db_engine)
                
                # ---------- basic inspection ----------
                # print(customer_az12.info())
                # print(customer_az12['gen'].unique())
                
                # ---------- drop invalid business keys ----------
        customer_az12 = customer_az12.dropna(subset=['cid'])
                
                # ---------- remove duplicates ----------
        customer_az12 = customer_az12.drop_duplicates(subset=['cid'])
                
                # ---------- clean cid ----------
        customer_az12['cid'] = np.where(
                    customer_az12['cid'].str.startswith('NAS'),
                    customer_az12['cid'].str[3:],
                    customer_az12['cid']
                )
                
                # ---------- convert birth date ----------
        customer_az12['bdate'] = pd.to_datetime(
                    customer_az12['bdate'],
                    errors='coerce'
                ).dt.date
                
                # ---------- remove future dates ----------
        today = pd.Timestamp.now().date()
        
        customer_az12.loc[customer_az12['bdate'] > today, 'bdate'] = pd.NaT
                
                # ---------- clean gender ----------
        customer_az12['gen'] = (
                    customer_az12['gen']
                    .str.strip()
                    .str.upper()
                    .replace({
                        'M': 'Male',
                        'MALE': 'Male',
                        'F': 'Female',
                        'FEMALE': 'Female',
                        'UNKNOWN': 'Unknown',
                        '': 'Unknown'
                    })
                    .fillna('Unknown')
                )
                
                # ---------- reset index ----------
        customer_az12 = customer_az12.reset_index(drop=True)
        customer_az12['load_timestamp'] = pd.Timestamp.now()
       #=========================== truncating table erp_customer_az12 =========================================#
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE silver.erp_customer_az12"))
           #=========================== loading to silver layer =========================================#
        customer_az12.to_sql(
            'erp_customer_az12',
            engine,
            schema='silver',
            if_exists='replace',
            index=False
        )
        print("✅ Loaded silver.erp_customer_az12")
    except Exception as e :
        print(f"❌ Failed loading {e}") 
 


 


   # '========================================================== Completed ✅ CLEANING erp_customer_az12 =================================='



        # '========================================================== CLEANING erp_location_a101 =================================='
def load_erp_location_a101(engine):
    try:
        
        location_a101    = pd.read_sql("SELECT * FROM bronze.erp_location_a101", db_engine)
        # ---------- basic inspection ----------
        # print(location_a101.info())
        
        # ---------- drop invalid business keys ----------
        location_a101 = location_a101.dropna(subset=['cid'])
        
        # ---------- remove duplicates ----------
        location_a101 = location_a101.drop_duplicates(subset=['cid'])
        
                # ---------- clean cid ----------
        location_a101['cid'] = location_a101['cid'].str.replace('-','',regex=False)
        
        # ---------- clean country ----------
        location_a101['cntry'] = (
            location_a101['cntry']
            .str.strip()
            .str.upper()
            .replace({
                'US': 'United States',
                'USA': 'United States',
                'DE': 'Germany',
                '': 'Unknown',
                'FEMALE': 'Unknown'   # invalid value, treated as unknown
            }).str.title()
            .fillna('Unknown')
        )
        
        # ---------- reset index ----------
        location_a101 = location_a101.reset_index(drop=True)
        location_a101['load_timestamp'] = pd.Timestamp.now()
           #=========================== truncating table erp_location_a101 =========================================#
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE silver.erp_location_a101"))
          #=========================== loading to silver layer =========================================#
        location_a101.to_sql(
            'erp_location_a101',
            engine,
            schema='silver',
            if_exists='append',
            index=False
        )
        print("✅ Loaded silver.erp_location_a101")
    except Exception as e :
        print(f"❌ Failed loading {e}")

        # '========================================================== Completed ✅ CLEANING erp_location_a101 =================================='


        # '========================================================== CLEANING erp_product_category =================================='
def load_erp_product_category(engine):
    try:
        
        product_category = pd.read_sql("SELECT * FROM bronze.erp_product_category", db_engine)
        # ---------- basic inspection ----------
        # print(product_category.info())
        # ---------- checking duplicate key ----------
        product_category['id'].duplicated().sum()
        # ---------- checking null ----------
        product_category.isnull().sum()
        product_category['load_timestamp'] = pd.Timestamp.now()
          #=========================== truncating table erp_product_category =========================================#
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE silver.erp_product_category"))
          #=========================== loading to silver layer =========================================#
        product_category.to_sql(
            'erp_product_category',
            engine,
            schema='silver',
            if_exists='append',
            index=False
        )
        print("✅ Loaded silver.erp_product_category")
    except Exception as e :
        print(f"❌ Failed loading {e}")
              

   # '========================================================== Completed ✅ CLEANING erp_product_category =================================='



   # ========================================================== LOADING DATA TO SILVER LAYER =======================================================#

def main(engine):
    try:
        print("🚀 Starting Silver layer load")

        # ---------- CRM ----------
        print("➡ Loading CRM customer info")
        load_crm_customer_info(engine)

        print("➡ Loading CRM product info")
        load_crm_product_info(engine)
        
        print("➡ Loading CRM sales details")
        load_crm_sales_details(engine)

        # ---------- ERP ----------
        print("➡ Loading ERP customer AZ12")
        load_erp_customer_az12(engine)

        print("➡ Loading ERP location A101")
        load_erp_location_a101(engine)

        print("➡ Loading ERP product category")
        load_erp_product_category(engine)

      

        print("✅ Silver layer load completed successfully")

    except Exception as e:
        print("❌ Silver layer load FAILED")
        raise
if __name__ == "__main__":
    main(db_engine)
