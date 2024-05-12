SELECT * FROM TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT;

-- merge in t1 using t2 => if they
--1.If All Parts NOT MATCHED:
--   1.1- Insert data from t2 to t1 at the first load.
--   1.2- something updated/changed in the record ==> inserts also the new raw. (then update the end date for old ones)
-- -----------------
MERGE INTO TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT t1
USING TPCDS.RAW.CUSTOMER t2
ON  t1.SALUTATION=t2.C_SALUTATION
    AND t1.PREFERRED_CUST_FLAG=t2.C_PREFERRED_CUST_FLAG 
    AND coalesce(t1.FIRST_SALES_DATE_SK, 0) = coalesce(t2.C_FIRST_SALES_DATE_SK,0) 
    AND t1.CUSTOMER_SK=t2.C_CUSTOMER_SK
    AND t1.LOGIN=t2.C_LOGIN
    AND coalesce(t1.CURRENT_CDEMO_SK,0) = coalesce(t2.C_CURRENT_CDEMO_SK,0)
    AND t1.FIRST_NAME=t2.C_FIRST_NAME
    AND coalesce(t1.CURRENT_HDEMO_SK,0) = coalesce(t2.C_CURRENT_HDEMO_SK,0)
    AND t1.CURRENT_ADDR_SK=t2.C_CURRENT_ADDR_SK
    AND t1.LAST_NAME=t2.C_LAST_NAME
    AND t1.CUSTOMER_ID=t2.C_CUSTOMER_ID
    AND coalesce(t1.LAST_REVIEW_DATE_SK,0) = coalesce(t2.C_LAST_REVIEW_DATE_SK,0)
    AND coalesce(t1.BIRTH_MONTH,0) = coalesce(t2.C_BIRTH_MONTH,0)
    AND t1.BIRTH_COUNTRY = t2.C_BIRTH_COUNTRY
    AND coalesce(t1.BIRTH_YEAR,0) = coalesce(t2.C_BIRTH_YEAR,0)
    AND coalesce(t1.BIRTH_DAY,0) = coalesce(t2.C_BIRTH_DAY,0)
    AND t1.EMAIL_ADDRESS = t2.C_EMAIL_ADDRESS
    AND coalesce(t1.FIRST_SHIPTO_DATE_SK,0) = coalesce(t2.C_FIRST_SHIPTO_DATE_SK,0)
WHEN NOT MATCHED 
THEN INSERT (
    SALUTATION, 
    PREFERRED_CUST_FLAG, 
    FIRST_SALES_DATE_SK, 
    CUSTOMER_SK, 
    LOGIN, 
    CURRENT_CDEMO_SK, 
    FIRST_NAME, 
    CURRENT_HDEMO_SK, 
    CURRENT_ADDR_SK, 
    LAST_NAME, 
    CUSTOMER_ID, 
    LAST_REVIEW_DATE_SK, 
    BIRTH_MONTH, 
    BIRTH_COUNTRY, 
    BIRTH_YEAR, 
    BIRTH_DAY, 
    EMAIL_ADDRESS, 
    FIRST_SHIPTO_DATE_SK,
    START_DATE,
    END_DATE)
VALUES (
    t2.C_SALUTATION, 
    t2.C_PREFERRED_CUST_FLAG, 
    t2.C_FIRST_SALES_DATE_SK, 
    t2.C_CUSTOMER_SK, 
    t2.C_LOGIN, 
    t2.C_CURRENT_CDEMO_SK, 
    t2.C_FIRST_NAME, 
    t2.C_CURRENT_HDEMO_SK, 
    t2.C_CURRENT_ADDR_SK, 
    t2.C_LAST_NAME, 
    t2.C_CUSTOMER_ID, 
    t2.C_LAST_REVIEW_DATE_SK, 
    t2.C_BIRTH_MONTH, 
    t2.C_BIRTH_COUNTRY, 
    t2.C_BIRTH_YEAR, 
    t2.C_BIRTH_DAY, 
    t2.C_EMAIL_ADDRESS, 
    t2.C_FIRST_SHIPTO_DATE_SK,
    CURRENT_DATE(),
    NULL
);

SELECT * FROM TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT;


--2. If Soarugate Key Matched: 
--   Incremental load happen.
--   when loading new data => WE EXPIRED THE END DATE OF OLD DATA.
--   (sk the same, but another record in the raw doesn't matched ==> expired the raw By update the end date)
-- --------------------
MERGE INTO TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT t1
USING TPCDS.RAW.CUSTOMER t2
ON  t1.CUSTOMER_SK=t2.C_CUSTOMER_SK
WHEN MATCHED
    AND (
    t1.SALUTATION!=t2.C_SALUTATION
    OR t1.PREFERRED_CUST_FLAG!=t2.C_PREFERRED_CUST_FLAG 
    OR coalesce(t1.FIRST_SALES_DATE_SK, 0) != coalesce(t2.C_FIRST_SALES_DATE_SK,0) 
    OR t1.LOGIN!=t2.C_LOGIN
    OR coalesce(t1.CURRENT_CDEMO_SK,0) != coalesce(t2.C_CURRENT_CDEMO_SK,0)
    OR t1.FIRST_NAME!=t2.C_FIRST_NAME
    OR coalesce(t1.CURRENT_HDEMO_SK,0) != coalesce(t2.C_CURRENT_HDEMO_SK,0)
    OR t1.CURRENT_ADDR_SK!=t2.C_CURRENT_ADDR_SK
    OR t1.LAST_NAME!=t2.C_LAST_NAME
    OR t1.CUSTOMER_ID!=t2.C_CUSTOMER_ID
    OR coalesce(t1.LAST_REVIEW_DATE_SK,0) != coalesce(t2.C_LAST_REVIEW_DATE_SK,0)
    OR coalesce(t1.BIRTH_MONTH,0) != coalesce(t2.C_BIRTH_MONTH,0)
    OR t1.BIRTH_COUNTRY != t2.C_BIRTH_COUNTRY
    OR coalesce(t1.BIRTH_YEAR,0) != coalesce(t2.C_BIRTH_YEAR,0)
    OR coalesce(t1.BIRTH_DAY,0) != coalesce(t2.C_BIRTH_DAY,0)
    OR t1.EMAIL_ADDRESS != t2.C_EMAIL_ADDRESS
    OR coalesce(t1.FIRST_SHIPTO_DATE_SK,0) != coalesce(t2.C_FIRST_SHIPTO_DATE_SK,0)
    ) 
THEN UPDATE SET
    end_date = current_date();


-- Join data from multiple tables into customer dimension.
-- Note: 
-- We want current raws W/O past history raws ==> use where condition.
create or replace table TPCDS.ANALYTICS.CUSTOMER_DIM as
        (select 
        SALUTATION,
        PREFERRED_CUST_FLAG,
        FIRST_SALES_DATE_SK,
        CUSTOMER_SK,
        LOGIN,
        CURRENT_CDEMO_SK,
        FIRST_NAME,
        CURRENT_HDEMO_SK,
        CURRENT_ADDR_SK,
        LAST_NAME,
        CUSTOMER_ID,
        LAST_REVIEW_DATE_SK,
        BIRTH_MONTH,
        BIRTH_COUNTRY,
        BIRTH_YEAR,
        BIRTH_DAY,
        EMAIL_ADDRESS,
        FIRST_SHIPTO_DATE_SK,
        CA_STREET_NAME AS STREET_NAME,
        CA_SUITE_NUMBER AS SUITE_NUMBER,
        CA_STATE AS STATE,
        CA_LOCATION_TYPE AS LOCATION_TYPE,
        CA_COUNTRY AS COUNTRY,
        CA_ADDRESS_ID AS ADDRESS_ID,
		CA_COUNTY AS COUNTY,
		CA_STREET_NUMBER AS STREET_NUMBER,
		CA_ZIP AS ZIP,
		CA_CITY AS CITY,
		CA_GMT_OFFSET AS GMT_OFFSET,
		CD_DEP_EMPLOYED_COUNT AS DEP_EMPLOYED_COUNT,
		CD_DEP_COUNT AS DEP_COUNT,
		CD_CREDIT_RATING AS CREDIT_RATING,
		CD_EDUCATION_STATUS AS EDUCATION_STATUS,
		CD_PURCHASE_ESTIMATE AS PURCHASE_ESTIMATE,
		CD_MARITAL_STATUS AS MARITAL_STATUS,
		CD_DEP_COLLEGE_COUNT AS DEP_COLLEGE_COUNT,
		CD_GENDER AS GENDER,
		HD_BUY_POTENTIAL AS BUY_POTENTIAL,
		HD_DEP_COUNT AS HD_DEP_COUNT,
		HD_VEHICLE_COUNT AS VEHICLE_COUNT,
		HD_INCOME_BAND_SK AS INCOME_BAND_SK,
		IB_LOWER_BOUND AS LOWER_BOUND,
		IB_UPPER_BOUND AS UPPER_BOUND,
        START_DATE,
        END_DATE 
from TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT
LEFT JOIN tpcds.RAW.customer_address ON current_addr_sk = ca_address_sk
LEFT join tpcds.RAW.customer_demographics ON current_cdemo_sk = cd_demo_sk
LEFT join tpcds.RAW.household_demographics ON current_hdemo_sk = hd_demo_sk
LEFT join tpcds.RAW.income_band ON HD_INCOME_BAND_SK = IB_INCOME_BAND_SK
where end_date is null
-- end date is null ==> because we need the current date not the past history.
        );   


-- Showing cases where C_FIRST_SALES_DATE_SK is null
select count(*) from TPCDS.RAW.CUSTOMER where C_FIRST_SALES_DATE_SK is null; --3518 records

-- -----------------------
-- Example for comparison happened between columns in t1 & t2: 
-- The result of comparison is null (not True or False)==> A Big Problem 
-- That is the resoan for changing Null ==> 0. So use coalesce(null,0) function to replace all null value ---> by 0.
select null='Mr';
select null=null;
select null=1;

-- If TRUE ==> The values matched
select 0=0;

-- If False ==> The values Not matched
-- select 'Mr'= 0;


-- -----------------------
-- To check if you have null value in you tables & you need to use coalesce
-- implement this query 
select * from TPCDS.RAW.CUSTOMER;
-- then check each clmn if it is filled 100% or not.