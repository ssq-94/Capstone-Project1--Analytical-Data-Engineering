-- 1. Business Requirements
-- sum_qty_wk: sum(catalog_sales.cs_quantity) group by date_dim.week_num and item
    -- --> Grain: Week Number and Item
-- sum_amt_wk: sum(catalog_sales.cs_sales_price * catalog_sales.cs_quantity) group by date_dim.week_num, item
-- sum_profit_wk: sum(catalog_sales.cs_net_profit) group by date_dim.week_num, item
-- avg_qty_dy: = sum_qty_wk/7
-- inv_on_hand_qty_wk: inventory.inv_quantity_on_hand at date_dim.week_num, warehouse
-- wks_sply: = inv_on_hand_qty_wk/sum_qty_wk
-- low_stock_flg_wk: ((avg_qty_dy > 0 && ((avg_qty_dy) > (inventory_on_hand_qty_wk))
-- --> In order to understand inventory better, we should have warehouse as a grain as well
-- Integrate Customer Dimension: Customer(SCD Type 2) + Customer_Address + Customer_Demographics + Household_Demographics + Income_Band
 -- --> Grain: Week Number, Item and warehouse

-- 2. Data Model
-- Weekly Sales Inventory - Fact Table
-- Dimensions: week_num, item, warehouse
-- Facts: sum_qty_wk, sum_amt_wk, sum_profit_wk, avg_qty_dy, inv_on_hand_qty_wk, wks_sply,low_stock_flg_wk

-- Customer Dimension
-- Customer
SALUTATION, PREFERRED_CUST_FLAG, FIRST_SALES_DATE_SK, CUSTOMER_SK, LOGIN, CURRENT_CDEMO_SK, FIRST_NAME, CURRENT_HDEMO_SK, CURRENT_ADDR_SK, LAST_NAME, CUSTOMER_ID, LAST_REVIEW_DATE_SK, BIRTH_MONTH, BIRTH_COUNTRY, BIRTH_YEAR, BIRTH_DAY, EMAIL_ADDRESS, FIRST_SHIPTO_DATE_SK

-- Customer Address
STREET_NAME, SUITE_NUMBER, STATE, LOCATION_TYPE, ADDRESS_SK, COUNTRY, ADDRESS_ID, COUNTY, STREET_NUMBER, ZIP, CITY, STREET_TYPE, GMT_OFFSET

-- Customer Demographics
DEP_EMPLOYED_COUNT, DEMO_SK, DEP_COUNT, CREDIT_RATING, EDUCATION_STATUS, PURCHASE_ESTIMATE, MARITAL_STATUS, DEP_COLLEGE_COUNT, GENDER

-- Household Demographics
BUY_POTENTIAL, INCOME_BAND_SK, DEMO_SK, DEP_COUNT, VEHICLE_COUNT

-- Income Band
LOWER_BOUND, INCOME_BAND_SK, UPPER_BOUND

-- SCD Type 2
Valid From, Valid To