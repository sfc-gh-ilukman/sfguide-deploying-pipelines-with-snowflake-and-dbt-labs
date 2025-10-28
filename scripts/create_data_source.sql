-- ================================================================================
-- DBT Project - Source Data Generation Script
-- ================================================================================
-- This script creates the necessary source tables and populates them with dummy data
-- based on the DBT models and transformations discovered in the project.
--
-- Source Tables Created:
--   1. US_STOCK_METRICS - Stock market data for US equities
--   2. FOREX_METRICS - Foreign exchange rates data
--   3. trading_books - Trading activity records
--   4. weights_table - Portfolio target allocations
-- ================================================================================

CREATE DATABASE IF NOT EXISTS sandbox ;

-- ================================================================================
-- SECTION 1: Create Database and Schemas
-- ================================================================================

-- Create databases for stock and forex data (simulating external data sources)
CREATE DATABASE IF NOT EXISTS STOCK_TRACKING_US_STOCK_PRICES_BY_DAY;
CREATE DATABASE IF NOT EXISTS FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY;

-- Create database for your internal trading data (adjust name based on environment)
-- CREATE DATABASE IF NOT EXISTS dbt_hol_2025_dev;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS STOCK_TRACKING_US_STOCK_PRICES_BY_DAY.STOCK;
CREATE SCHEMA IF NOT EXISTS FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY.STOCK;
-- CREATE SCHEMA IF NOT EXISTS dbt_hol_2025_dev.PUBLIC;

-- ================================================================================
-- SECTION 2: Create US_STOCK_METRICS Table
-- ================================================================================

USE SCHEMA STOCK_TRACKING_US_STOCK_PRICES_BY_DAY.STOCK;

CREATE OR REPLACE TABLE US_STOCK_METRICS (
    RUN_DATE DATE NOT NULL,
    TICKER VARCHAR(10) NOT NULL,
    OPEN DECIMAL(18,2),
    HIGH DECIMAL(18,2),
    LOW DECIMAL(18,2),
    CLOSE DECIMAL(18,2),
    VOLUME BIGINT,
    PRIMARY KEY (RUN_DATE, TICKER)
);

-- Insert stock price data for AAPL
-- Base prices and generate realistic daily variations
INSERT INTO US_STOCK_METRICS (RUN_DATE, TICKER, OPEN, HIGH, LOW, CLOSE, VOLUME)
WITH date_series AS (
    SELECT DATEADD(day, SEQ4(), '2024-01-01'::DATE) AS run_date
    FROM TABLE(GENERATOR(ROWCOUNT => 365))
    WHERE DAYOFWEEK(DATEADD(day, SEQ4(), '2024-01-01'::DATE)) NOT IN (0, 6) -- Exclude weekends
),
aapl_prices AS (
    SELECT 
        run_date,
        'AAPL' AS ticker,
        -- Generate realistic AAPL prices (base around $175-185)
        175 + (UNIFORM(0, 1000, RANDOM()) / 100.0) AS base_price,
        UNIFORM(0, 100, RANDOM()) / 100.0 AS volatility
    FROM date_series
)
SELECT 
    run_date,
    ticker,
    ROUND(base_price, 2) AS open,
    ROUND(base_price + volatility * 2, 2) AS high,
    ROUND(base_price - volatility * 1.5, 2) AS low,
    ROUND(base_price + (volatility - 0.5), 2) AS close,
    50000000 + UNIFORM(0, 100000000, RANDOM()) AS volume
FROM aapl_prices;

-- Insert stock price data for MSFT
INSERT INTO US_STOCK_METRICS (RUN_DATE, TICKER, OPEN, HIGH, LOW, CLOSE, VOLUME)
WITH date_series AS (
    SELECT DATEADD(day, SEQ4(), '2024-01-01'::DATE) AS run_date
    FROM TABLE(GENERATOR(ROWCOUNT => 365))
    WHERE DAYOFWEEK(DATEADD(day, SEQ4(), '2024-01-01'::DATE)) NOT IN (0, 6)
),
msft_prices AS (
    SELECT 
        run_date,
        'MSFT' AS ticker,
        -- Generate realistic MSFT prices (base around $405-415)
        405 + (UNIFORM(0, 1000, RANDOM()) / 100.0) AS base_price,
        UNIFORM(0, 100, RANDOM()) / 100.0 AS volatility
    FROM date_series
)
SELECT 
    run_date,
    ticker,
    ROUND(base_price, 2) AS open,
    ROUND(base_price + volatility * 2, 2) AS high,
    ROUND(base_price - volatility * 1.5, 2) AS low,
    ROUND(base_price + (volatility - 0.5), 2) AS close,
    40000000 + UNIFORM(0, 80000000, RANDOM()) AS volume
FROM msft_prices;

-- Insert stock price data for GOOGL
INSERT INTO US_STOCK_METRICS (RUN_DATE, TICKER, OPEN, HIGH, LOW, CLOSE, VOLUME)
WITH date_series AS (
    SELECT DATEADD(day, SEQ4(), '2024-01-01'::DATE) AS run_date
    FROM TABLE(GENERATOR(ROWCOUNT => 365))
    WHERE DAYOFWEEK(DATEADD(day, SEQ4(), '2024-01-01'::DATE)) NOT IN (0, 6)
),
googl_prices AS (
    SELECT 
        run_date,
        'GOOGL' AS ticker,
        -- Generate realistic GOOGL prices (base around $141-146)
        141 + (UNIFORM(0, 500, RANDOM()) / 100.0) AS base_price,
        UNIFORM(0, 100, RANDOM()) / 100.0 AS volatility
    FROM date_series
)
SELECT 
    run_date,
    ticker,
    ROUND(base_price, 2) AS open,
    ROUND(base_price + volatility * 2, 2) AS high,
    ROUND(base_price - volatility * 1.5, 2) AS low,
    ROUND(base_price + (volatility - 0.5), 2) AS close,
    30000000 + UNIFORM(0, 60000000, RANDOM()) AS volume
FROM googl_prices;

-- ================================================================================
-- SECTION 3: Create FOREX_METRICS Table
-- ================================================================================

USE SCHEMA FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY.STOCK;

CREATE OR REPLACE TABLE FOREX_METRICS (
    RUN_DATE DATE NOT NULL,
    CURRENCY_PAIR_NAME VARCHAR(20) NOT NULL,
    OPEN DECIMAL(18,6),
    HIGH DECIMAL(18,6),
    LOW DECIMAL(18,6),
    CLOSE DECIMAL(18,6),
    PRIMARY KEY (RUN_DATE, CURRENCY_PAIR_NAME)
);

-- Insert EUR/USD forex data
INSERT INTO FOREX_METRICS (RUN_DATE, CURRENCY_PAIR_NAME, OPEN, HIGH, LOW, CLOSE)
WITH date_series AS (
    SELECT DATEADD(day, SEQ4(), '2024-01-01'::DATE) AS run_date
    FROM TABLE(GENERATOR(ROWCOUNT => 365))
    WHERE DAYOFWEEK(DATEADD(day, SEQ4(), '2024-01-01'::DATE)) NOT IN (0, 6)
),
eurusd_rates AS (
    SELECT 
        run_date,
        'EUR/USD' AS currency_pair_name,
        -- Generate realistic EUR/USD rates (base around 1.08-1.10)
        1.08 + (UNIFORM(0, 200, RANDOM()) / 10000.0) AS base_rate,
        UNIFORM(0, 100, RANDOM()) / 100000.0 AS volatility
    FROM date_series
)
SELECT 
    run_date,
    currency_pair_name,
    ROUND(base_rate, 6) AS open,
    ROUND(base_rate + volatility * 2, 6) AS high,
    ROUND(base_rate - volatility * 1.5, 6) AS low,
    ROUND(base_rate + (volatility - 0.0005), 6) AS close
FROM eurusd_rates;

-- Insert GBP/USD forex data
INSERT INTO FOREX_METRICS (RUN_DATE, CURRENCY_PAIR_NAME, OPEN, HIGH, LOW, CLOSE)
WITH date_series AS (
    SELECT DATEADD(day, SEQ4(), '2024-01-01'::DATE) AS run_date
    FROM TABLE(GENERATOR(ROWCOUNT => 365))
    WHERE DAYOFWEEK(DATEADD(day, SEQ4(), '2024-01-01'::DATE)) NOT IN (0, 6)
),
gbpusd_rates AS (
    SELECT 
        run_date,
        'GBP/USD' AS currency_pair_name,
        -- Generate realistic GBP/USD rates (base around 1.26-1.28)
        1.26 + (UNIFORM(0, 200, RANDOM()) / 10000.0) AS base_rate,
        UNIFORM(0, 100, RANDOM()) / 100000.0 AS volatility
    FROM date_series
)
SELECT 
    run_date,
    currency_pair_name,
    ROUND(base_rate, 6) AS open,
    ROUND(base_rate + volatility * 2, 6) AS high,
    ROUND(base_rate - volatility * 1.5, 6) AS low,
    ROUND(base_rate + (volatility - 0.0005), 6) AS close
FROM gbpusd_rates;

-- ================================================================================
-- SECTION 4: Create trading_books Table
-- ================================================================================

-- USE SCHEMA dbt_hol_2025_dev.PUBLIC;

-- CREATE OR REPLACE TABLE trading_books (
--     TRADE_ID INTEGER PRIMARY KEY,
--     TRADE_DATE DATE NOT NULL,
--     TRADER_NAME VARCHAR(100),
--     DESK VARCHAR(50),
--     TICKER VARCHAR(20),
--     QUANTITY DECIMAL(18,2),
--     PRICE DECIMAL(18,6),
--     TRADE_TYPE VARCHAR(10),
--     NOTES VARCHAR(500)
-- );

-- -- Insert sample trading data
-- -- Note: These trades must align with the dates and prices in the stock/forex metrics
-- INSERT INTO trading_books (TRADE_ID, TRADE_DATE, TRADER_NAME, DESK, TICKER, QUANTITY, PRICE, TRADE_TYPE, NOTES) VALUES
-- -- March 1, 2024 - Equity Trades
-- (1001, '2024-03-01', 'John Smith', 'Equity Desk', 'AAPL', 100, 175.50, 'BUY', 'Initiated position based on strong earnings report'),
-- (1002, '2024-03-01', 'John Smith', 'Equity Desk', 'AAPL', 100, 178.25, 'SELL', 'Taking profits after 1.5% gain'),
-- (1005, '2024-03-01', 'Michael Chen', 'Equity Desk', 'MSFT', 50, 405.75, 'BUY', 'Adding to position on AI momentum'),
-- (1006, '2024-03-01', 'Michael Chen', 'Equity Desk', 'MSFT', 50, 408.50, 'SELL', 'Quick profit on AI news'),
-- (1009, '2024-03-01', 'David Kim', 'Equity Desk', 'GOOGL', 75, 141.25, 'BUY', 'Strong search revenue growth'),
-- (1010, '2024-03-01', 'David Kim', 'Equity Desk', 'GOOGL', 75, 142.50, 'SELL', 'Profit taking'),

-- -- March 1, 2024 - FX Trades
-- (1003, '2024-03-01', 'Sarah Johnson', 'FX Desk', 'EUR/USD', 100000, 1.0850, 'BUY', 'Positioning for ECB rate decision'),
-- (1004, '2024-03-01', 'Sarah Johnson', 'FX Desk', 'EUR/USD', 100000, 1.0920, 'SELL', 'Closing position after ECB announcement'),
-- (1007, '2024-03-01', 'Lisa Wong', 'FX Desk', 'GBP/USD', 75000, 1.2650, 'BUY', 'Technical breakout signal'),
-- (1008, '2024-03-01', 'Lisa Wong', 'FX Desk', 'GBP/USD', 75000, 1.2720, 'SELL', 'Target reached'),

-- -- March 4, 2024 - Equity Trades
-- (1011, '2024-03-04', 'John Smith', 'Equity Desk', 'AAPL', 150, 176.25, 'BUY', 'Re-entering on pullback'),
-- (1012, '2024-03-04', 'John Smith', 'Equity Desk', 'AAPL', 150, 179.50, 'SELL', 'Technical resistance hit'),
-- (1015, '2024-03-04', 'Michael Chen', 'Equity Desk', 'MSFT', 75, 407.25, 'BUY', 'Cloud growth acceleration'),
-- (1016, '2024-03-04', 'Michael Chen', 'Equity Desk', 'MSFT', 75, 410.75, 'SELL', 'Target reached'),
-- (1019, '2024-03-04', 'David Kim', 'Equity Desk', 'GOOGL', 100, 142.25, 'BUY', 'Ad revenue growth'),
-- (1020, '2024-03-04', 'David Kim', 'Equity Desk', 'GOOGL', 100, 144.50, 'SELL', 'Technical target hit'),

-- -- March 4, 2024 - FX Trades
-- (1013, '2024-03-04', 'Sarah Johnson', 'FX Desk', 'EUR/USD', 125000, 1.0880, 'BUY', 'Strong economic data'),
-- (1014, '2024-03-04', 'Sarah Johnson', 'FX Desk', 'EUR/USD', 125000, 1.0950, 'SELL', 'Profit taking on data'),
-- (1017, '2024-03-04', 'Lisa Wong', 'FX Desk', 'GBP/USD', 100000, 1.2680, 'BUY', 'BOE rate decision'),
-- (1018, '2024-03-04', 'Lisa Wong', 'FX Desk', 'GBP/USD', 100000, 1.2750, 'SELL', 'Position closed after decision'),

-- -- March 5, 2024 - Equity Trades
-- (1021, '2024-03-05', 'John Smith', 'Equity Desk', 'AAPL', 200, 177.50, 'BUY', 'Strong iPhone sales'),
-- (1022, '2024-03-05', 'John Smith', 'Equity Desk', 'AAPL', 200, 180.25, 'SELL', 'Profit taking'),
-- (1025, '2024-03-05', 'Michael Chen', 'Equity Desk', 'MSFT', 100, 409.50, 'BUY', 'AI partnership news'),
-- (1026, '2024-03-05', 'Michael Chen', 'Equity Desk', 'MSFT', 100, 413.25, 'SELL', 'Target reached'),
-- (1029, '2024-03-05', 'David Kim', 'Equity Desk', 'GOOGL', 125, 143.75, 'BUY', 'Search market share'),
-- (1030, '2024-03-05', 'David Kim', 'Equity Desk', 'GOOGL', 125, 146.25, 'SELL', 'Technical target hit'),

-- -- March 5, 2024 - FX Trades
-- (1023, '2024-03-05', 'Sarah Johnson', 'FX Desk', 'EUR/USD', 150000, 1.0900, 'BUY', 'ECB minutes release'),
-- (1024, '2024-03-05', 'Sarah Johnson', 'FX Desk', 'EUR/USD', 150000, 1.0980, 'SELL', 'Position closed'),
-- (1027, '2024-03-05', 'Lisa Wong', 'FX Desk', 'GBP/USD', 125000, 1.2700, 'BUY', 'UK GDP data'),
-- (1028, '2024-03-05', 'Lisa Wong', 'FX Desk', 'GBP/USD', 125000, 1.2780, 'SELL', 'Position closed'),

-- -- Additional trades for more variety (March 6-8)
-- (1031, '2024-03-06', 'John Smith', 'Equity Desk', 'AAPL', 175, 178.75, 'BUY', 'Support level bounce'),
-- (1032, '2024-03-06', 'John Smith', 'Equity Desk', 'AAPL', 175, 181.00, 'SELL', 'Resistance level reached'),
-- (1033, '2024-03-06', 'Sarah Johnson', 'FX Desk', 'EUR/USD', 110000, 1.0910, 'BUY', 'Technical indicator signal'),
-- (1034, '2024-03-06', 'Sarah Johnson', 'FX Desk', 'EUR/USD', 110000, 1.0965, 'SELL', 'Profit target achieved'),

-- (1035, '2024-03-07', 'Michael Chen', 'Equity Desk', 'MSFT', 80, 410.00, 'BUY', 'Market dip opportunity'),
-- (1036, '2024-03-07', 'Michael Chen', 'Equity Desk', 'MSFT', 80, 414.00, 'SELL', 'Quick gain realized'),
-- (1037, '2024-03-07', 'Lisa Wong', 'FX Desk', 'GBP/USD', 95000, 1.2710, 'BUY', 'Brexit news positive'),
-- (1038, '2024-03-07', 'Lisa Wong', 'FX Desk', 'GBP/USD', 95000, 1.2765, 'SELL', 'Position closed'),

-- (1039, '2024-03-08', 'David Kim', 'Equity Desk', 'GOOGL', 110, 144.50, 'BUY', 'AI product launch'),
-- (1040, '2024-03-08', 'David Kim', 'Equity Desk', 'GOOGL', 110, 147.25, 'SELL', 'Strong momentum exit'),
-- (1041, '2024-03-08', 'Sarah Johnson', 'FX Desk', 'EUR/USD', 140000, 1.0925, 'BUY', 'Fed commentary impact'),
-- (1042, '2024-03-08', 'Sarah Johnson', 'FX Desk', 'EUR/USD', 140000, 1.0990, 'SELL', 'Target reached');

-- ================================================================================
-- SECTION 5: Create weights_table Table
-- ================================================================================

-- CREATE OR REPLACE TABLE weights_table (
--     REGION VARCHAR(100) NOT NULL,
--     DESK VARCHAR(50) NOT NULL,
--     TARGET_ALLOCATION DECIMAL(5,4) NOT NULL,
--     PRIMARY KEY (REGION, DESK)
-- );

-- -- Insert target allocation data
-- INSERT INTO weights_table (REGION, DESK, TARGET_ALLOCATION) VALUES
-- ('North America', 'Equity Desk', 0.7500),
-- ('Europe', 'FX Desk', 0.2500),
-- ('Asia', 'Equity Desk', 0.5000),
-- ('North America', 'FX Desk', 0.3000),
-- ('Europe', 'Equity Desk', 0.6000),
-- ('Asia', 'FX Desk', 0.4000);

-- -- ================================================================================
-- -- SECTION 6: Data Validation Queries
-- -- ================================================================================

-- -- Verify data was created successfully
-- SELECT '=== US_STOCK_METRICS ===' AS info;
-- SELECT 
--     TICKER,
--     MIN(RUN_DATE) AS first_date,
--     MAX(RUN_DATE) AS last_date,
--     COUNT(*) AS row_count,
--     AVG(CLOSE) AS avg_close_price
-- FROM STOCK_TRACKING_US_STOCK_PRICES_BY_DAY.STOCK.US_STOCK_METRICS
-- GROUP BY TICKER
-- ORDER BY TICKER;

-- SELECT '=== FOREX_METRICS ===' AS info;
-- SELECT 
--     CURRENCY_PAIR_NAME,
--     MIN(RUN_DATE) AS first_date,
--     MAX(RUN_DATE) AS last_date,
--     COUNT(*) AS row_count,
--     AVG(CLOSE) AS avg_close_rate
-- FROM FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY.STOCK.FOREX_METRICS
-- GROUP BY CURRENCY_PAIR_NAME
-- ORDER BY CURRENCY_PAIR_NAME;

-- SELECT '=== trading_books ===' AS info;
-- SELECT 
--     DESK,
--     TRADE_TYPE,
--     COUNT(*) AS trade_count,
--     SUM(QUANTITY) AS total_quantity,
--     AVG(PRICE) AS avg_price
-- FROM dbt_hol_2025_dev.PUBLIC.trading_books
-- GROUP BY DESK, TRADE_TYPE
-- ORDER BY DESK, TRADE_TYPE;

-- SELECT '=== weights_table ===' AS info;
-- SELECT * FROM dbt_hol_2025_dev.PUBLIC.weights_table
-- ORDER BY REGION, DESK;

-- ================================================================================
-- SECTION 7: Notes and Next Steps
-- ================================================================================

/*
NOTES:
------
1. The script creates 4 source tables:
   - US_STOCK_METRICS: 750+ rows (3 tickers × 250+ trading days)
   - FOREX_METRICS: 500+ rows (2 currency pairs × 250+ trading days)
   - trading_books: 42 rows (sample trading activity)
   - weights_table: 6 rows (target allocations)

2. Data characteristics:
   - Stock data: Realistic daily OHLC prices with volume
   - Forex data: Realistic exchange rates with intraday volatility
   - Trades: Matched BUY/SELL pairs for P&L calculation
   - Dates: Excludes weekends, covers 2024-01-01 onwards

3. To use this data with your DBT project:
   - Run this script in Snowflake
   - Update your profiles.yml to point to dbt_hol_2025_dev database
   - Run: dbt deps (if needed)
   - Run: dbt seed (to load the CSV seed files)
   - Run: dbt run (to build all models)
   - Run: dbt test (to validate data quality)

4. Environment considerations:
   - This script creates the 'dev' environment database (dbt_hol_2025_dev)
   - For 'prod' environment, create dbt_hol_2025_prod and copy the tables
   - Or modify the database names in sources.yml to use a single database

5. Additional data generation:
   - To generate more trading days, increase ROWCOUNT in GENERATOR()
   - To add more traders/tickers, insert additional rows in trading_books
   - To add more currency pairs, insert additional rows in FOREX_METRICS

TROUBLESHOOTING:
---------------
- If you get "object does not exist" errors, ensure you have CREATE privileges
- If dates don't match, check the TRADE_DATE values align with market data dates
- If joins fail in DBT models, verify ticker/currency_pair_name values match exactly
- For production use, consider partitioning large tables by date

*/

-- End of script
SELECT '✓ Source data generation completed successfully!' AS status;


