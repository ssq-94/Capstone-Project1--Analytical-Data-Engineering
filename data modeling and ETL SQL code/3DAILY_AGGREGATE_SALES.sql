USE TPCDS.INTERMEDIATE;

-- Getting Last Date (max date)
-- we will use surragete key to get the date
SET LAST_SOLD_DATE_SK = (SELECT MAX(SOLD_DATE_SK) FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES);

-- in initial load, we didn't have something in our table
SELECT SOLD_DATE_Sk FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES;


-- Removing partial records from the last date
-- (remove the record for max date)
DELETE FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES WHERE sold_date_sk=$LAST_SOLD_DATE_SK;

-- ----------------------------

-- In daily aggregate sales we will make 3 Req: sum_qty_dy , sum_amt_dy , sum_profit_dy. 

CREATE OR REPLACE TEMPORARY TABLE TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES_TMP AS (
-- compiling all incremental sales records in New Table
with incremental_sales as (
-- grabbing records from catalog_sales & web_sales 
    SELECT 
            CS_WAREHOUSE_SK as warehouse_sk,
            CS_ITEM_SK as item_sk,
            CS_SOLD_DATE_SK as sold_date_sk,
            CS_QUANTITY as quantity,
            cs_sales_price * cs_quantity as sales_amt, -- the total price getting from the specific item sold many times
            CS_NET_PROFIT as net_profit
    from tpcds.RAW.catalog_sales
    WHERE sold_date_sk >= NVL($LAST_SOLD_DATE_SK,0)  --?? why ?? 16/04/2024 >= 01/04/2024 maybe because calculate for specific week?
        and quantity is not null
        and sales_amt is not null
    
    union all

    SELECT 
            WS_WAREHOUSE_SK as warehouse_sk,
            WS_ITEM_SK as item_sk,
            WS_SOLD_DATE_SK as sold_date_sk,
            WS_QUANTITY as quantity,
            ws_sales_price * ws_quantity as sales_amt,
            WS_NET_PROFIT as net_profit
    from tpcds.RAW.web_sales
    WHERE sold_date_sk >= NVL($LAST_SOLD_DATE_SK,0)  
        and quantity is not null
        and sales_amt is not null
),

-- Make the grain on a daily level:
aggregating_records_to_daily_sales as
(
select 
    warehouse_sk,   -- w:1 | I:902 | D: 02/10/2023 | qty: 30 | amt: 2500 | net_prft: 228
    item_sk,
    sold_date_sk,  -- for each day, calculate the qty & sales_amt & net_profit
    sum(quantity) as daily_qty,
    sum(sales_amt) as daily_sales_amt,
    sum(net_profit) as daily_net_profit 
from incremental_sales
group by 1, 2, 3

),

-- Grabbing the week number & year (based on the date)==> to help in getting the weekly sales
adding_week_number_and_yr_number as
(
select 
    *,
    date.wk_num as sold_wk_num, -- from the date that we have==> we'll know the number of week that exist in.
    date.yr_num as sold_yr_num
from aggregating_records_to_daily_sales 
LEFT JOIN tpcds.RAW.date_dim date 
    ON sold_date_sk = d_date_sk

)

-- aggregate all things that we need:
SELECT 
	warehouse_sk,
    item_sk,
    sold_date_sk,
    max(sold_wk_num) as sold_wk_num, -- ???
    max(sold_yr_num) as sold_yr_num, -- ???
    sum(daily_qty) as daily_qty,
    sum(daily_sales_amt) as daily_sales_amt,
    sum(daily_net_profit) as daily_net_profit 
FROM adding_week_number_and_yr_number
GROUP BY 1,2,3
ORDER BY 1,2,3
)
;




-- Inserting new records
INSERT INTO TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES
(	
    WAREHOUSE_SK, 
    ITEM_SK, 
    SOLD_DATE_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    DAILY_QTY, 
    DAILY_SALES_AMT, 
    DAILY_NET_PROFIT
)
SELECT 
    DISTINCT
	warehouse_sk,
    item_sk,
    sold_date_sk,
    sold_wk_num,
    sold_yr_num,
    daily_qty,
    daily_sales_amt,
    daily_net_profit 
FROM TPCDS.INTERMEDIATE.DAILY_AGGREGATED_SALES_TMP;
