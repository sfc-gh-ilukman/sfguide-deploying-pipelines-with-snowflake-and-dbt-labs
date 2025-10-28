--!jinja

/*-----------------------------------------------------------------------------
Hands-On Lab: Deploying pipelines with Snowflake and dbt labs
Script:       setup_dbt_infrastructure.sql
Author:       Generated for dbt profile setup
Last Updated: {{ now() }}
Description:  Creates databases, warehouses, roles, and users for dbt dev and prod environments
-----------------------------------------------------------------------------*/

-- ============================================================================
-- USER SETUP
-- ============================================================================

-- Create dbt user (if not exists)
CREATE OR REPLACE USER dbt_hol_user
    PASSWORD = 'Snowflake@BW123'
    DEFAULT_ROLE = 'dbt_hol_role_dev'
    DEFAULT_WAREHOUSE = 'vwh_dbt_hol_dev'
    COMMENT = 'dbt user for HOL 2025';

-- ============================================================================
-- DEVELOPMENT ENVIRONMENT SETUP
-- ============================================================================

-- Create development database
-- CREATE OR REPLACE DATABASE dbt_hol_2025_dev;

--create service vwh
CREATE OR REPLACE WAREHOUSE vwh_dbt_hol
    WAREHOUSE_SIZE = 'XSMALL'
    INITIALLY_SUSPENDED = true
    AUTO_RESUME = true
    AUTO_SUSPEND = 60
    COMMENT = 'Development warehouse for dbt HOL 2025';

-- Create development warehouse
CREATE OR REPLACE WAREHOUSE vwh_dbt_hol_dev
    WAREHOUSE_SIZE = 'XSMALL'
    INITIALLY_SUSPENDED = true
    AUTO_RESUME = true
    AUTO_SUSPEND = 60
    COMMENT = 'Development warehouse for dbt HOL 2025';

-- Create development role
CREATE OR REPLACE ROLE dbt_hol_role_dev;

-- Grant permissions to development role
GRANT ALL PRIVILEGES ON DATABASE dbt_hol_2025_dev TO ROLE dbt_hol_role_dev;
GRANT ALL PRIVILEGES ON SCHEMA dbt_hol_2025_dev.public TO ROLE dbt_hol_role_dev;
GRANT ALL PRIVILEGES ON WAREHOUSE vwh_dbt_hol_dev TO ROLE dbt_hol_role_dev;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE dbt_hol_2025_dev TO ROLE dbt_hol_role_dev;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN DATABASE dbt_hol_2025_dev TO ROLE dbt_hol_role_dev;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE dbt_hol_2025_dev TO ROLE dbt_hol_role_dev;

-- Grant imported privileges for marketplace data (if available)
-- GRANT IMPORTED PRIVILEGES ON DATABASE STOCK_TRACKING_US_STOCK_PRICES_BY_DAY TO ROLE dbt_hol_role_dev;
-- GRANT IMPORTED PRIVILEGES ON DATABASE FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY TO ROLE dbt_hol_role_dev;

-- ============================================================================
-- PRODUCTION ENVIRONMENT SETUP
-- ============================================================================

-- Create production database
-- CREATE OR REPLACE DATABASE dbt_hol_2025_prod;

-- Create production warehouse
CREATE OR REPLACE WAREHOUSE vwh_dbt_hol_prod
    WAREHOUSE_SIZE = 'XSMALL'
    INITIALLY_SUSPENDED = true
    AUTO_RESUME = true
    AUTO_SUSPEND = 60
    COMMENT = 'Production warehouse for dbt HOL 2025';

-- Create production role
CREATE OR REPLACE ROLE dbt_hol_role_prod;

-- Grant permissions to production role
GRANT ALL PRIVILEGES ON DATABASE dbt_hol_2025_prod TO ROLE dbt_hol_role_prod;
GRANT ALL PRIVILEGES ON SCHEMA dbt_hol_2025_prod.public TO ROLE dbt_hol_role_prod;
GRANT ALL PRIVILEGES ON WAREHOUSE vwh_dbt_hol_prod TO ROLE dbt_hol_role_prod;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE dbt_hol_2025_prod TO ROLE dbt_hol_role_prod;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN DATABASE dbt_hol_2025_prod TO ROLE dbt_hol_role_prod;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE dbt_hol_2025_prod TO ROLE dbt_hol_role_prod;

-- Grant imported privileges for marketplace data (if available)
-- GRANT IMPORTED PRIVILEGES ON DATABASE STOCK_TRACKING_US_STOCK_PRICES_BY_DAY TO ROLE dbt_hol_role_prod;
-- GRANT IMPORTED PRIVILEGES ON DATABASE FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY TO ROLE dbt_hol_role_prod;


-- ============================================================================
-- ACCOUNTADMIN PRIVILEGES
-- ============================================================================

-- Grant roles to ACCOUNTADMIN (as requested)
GRANT ROLE dbt_hol_role_dev TO ROLE ACCOUNTADMIN;
GRANT ROLE dbt_hol_role_prod TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Verify databases were created
SHOW DATABASES LIKE 'dbt_hol_2025_%';

-- Verify warehouses were created
SHOW WAREHOUSES LIKE 'vwh_dbt_hol_%';

-- Verify roles were created
SHOW ROLES LIKE 'dbt_hol_role_%';

-- Verify user was created
SHOW USERS LIKE 'dbt_hol_user';

-- Grant roles to user
GRANT ROLE dbt_hol_role_dev TO USER dbt_hol_user;
GRANT ROLE dbt_hol_role_prod TO USER dbt_hol_user;

-- Verify role grants
SHOW GRANTS TO ROLE dbt_hol_role_dev;
SHOW GRANTS TO ROLE dbt_hol_role_prod;
SHOW GRANTS TO USER dbt_hol_user;


-- ================================================================================
-- Grant permissions on STOCK database
-- ================================================================================

GRANT USAGE ON DATABASE STOCK_TRACKING_US_STOCK_PRICES_BY_DAY TO ROLE dbt_hol_role_dev;
GRANT USAGE ON SCHEMA STOCK_TRACKING_US_STOCK_PRICES_BY_DAY.STOCK TO ROLE dbt_hol_role_dev;
GRANT SELECT ON ALL TABLES IN SCHEMA STOCK_TRACKING_US_STOCK_PRICES_BY_DAY.STOCK TO ROLE dbt_hol_role_dev;
GRANT SELECT ON FUTURE TABLES IN SCHEMA STOCK_TRACKING_US_STOCK_PRICES_BY_DAY.STOCK TO ROLE dbt_hol_role_dev;

-- ================================================================================
-- Grant permissions on FOREX database
-- ================================================================================

GRANT USAGE ON DATABASE FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY TO ROLE dbt_hol_role_dev;
GRANT USAGE ON SCHEMA FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY.STOCK TO ROLE dbt_hol_role_dev;
GRANT SELECT ON ALL TABLES IN SCHEMA FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY.STOCK TO ROLE dbt_hol_role_dev;
GRANT SELECT ON FUTURE TABLES IN SCHEMA FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY.STOCK TO ROLE dbt_hol_role_dev;

-- ================================================================================
-- Also grant to PROD role (if you're using it)
-- ================================================================================

GRANT USAGE ON DATABASE STOCK_TRACKING_US_STOCK_PRICES_BY_DAY TO ROLE dbt_hol_role_prod;
GRANT USAGE ON SCHEMA STOCK_TRACKING_US_STOCK_PRICES_BY_DAY.STOCK TO ROLE dbt_hol_role_prod;
GRANT SELECT ON ALL TABLES IN SCHEMA STOCK_TRACKING_US_STOCK_PRICES_BY_DAY.STOCK TO ROLE dbt_hol_role_prod;
GRANT SELECT ON FUTURE TABLES IN SCHEMA STOCK_TRACKING_US_STOCK_PRICES_BY_DAY.STOCK TO ROLE dbt_hol_role_prod;

GRANT USAGE ON DATABASE FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY TO ROLE dbt_hol_role_prod;
GRANT USAGE ON SCHEMA FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY.STOCK TO ROLE dbt_hol_role_prod;
GRANT SELECT ON ALL TABLES IN SCHEMA FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY.STOCK TO ROLE dbt_hol_role_prod;
GRANT SELECT ON FUTURE TABLES IN SCHEMA FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY.STOCK TO ROLE dbt_hol_role_prod;


--- Troubleshoot
USE ROLE ACCOUNTADMIN;


-- Grant dynamic table privileges to your dbt role
GRANT CREATE DYNAMIC TABLE ON SCHEMA dbt_hol_2025_dev.public_02_intermediate TO ROLE dbt_hol_role_dev;
GRANT CREATE DYNAMIC TABLE ON SCHEMA dbt_hol_2025_dev.public_03_marts TO ROLE dbt_hol_role_dev;

-- Grant usage on the warehouse for dynamic table refreshes
GRANT USAGE ON WAREHOUSE VWH_DBT_HOL TO ROLE dbt_hol_role_dev;
GRANT OPERATE ON WAREHOUSE VWH_DBT_HOL TO ROLE dbt_hol_role_dev;

-- Grant dynamic table privileges to your dbt role
GRANT CREATE DYNAMIC TABLE ON SCHEMA dbt_hol_2025_dev.public_02_intermediate TO ROLE dbt_hol_role_prod;
GRANT CREATE DYNAMIC TABLE ON SCHEMA dbt_hol_2025_dev.public_03_marts TO ROLE dbt_hol_role_prod;

-- Grant usage on the warehouse for dynamic table refreshes
GRANT USAGE ON WAREHOUSE VWH_DBT_HOL TO ROLE dbt_hol_role_prod;
GRANT OPERATE ON WAREHOUSE VWH_DBT_HOL TO ROLE dbt_hol_role_prod;

-- ================================================================================
-- Verify permissions
-- ================================================================================

SHOW GRANTS TO ROLE dbt_hol_role_dev;

SELECT '✅ Permissions granted successfully!' AS status;
