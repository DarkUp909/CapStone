-- populating the data into customer dim

CREATE OR REPLACE PROCEDURE TPCDS.ANALYTICS.inserting_customer()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  BEGIN
 MERGE INTO TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT k1
USING TPCDS.RAW.CUSTOMER k2
ON  k1.C_SALUTATION=k2.C_SALUTATION
    AND k1.C_PREFERRED_CUST_FLAG=k2.C_PREFERRED_CUST_FLAG 
    AND coalesce(k1.C_FIRST_SALES_DATE_SK, 0) = coalesce(k2.C_FIRST_SALES_DATE_SK,0) 
    AND k1.C_CUSTOMER_SK=k2.C_CUSTOMER_SK
    AND k1.C_LOGIN=k2.C_LOGIN
    AND coalesce(k1.C_CURRENT_CDEMO_SK,0) = coalesce(k2.C_CURRENT_CDEMO_SK,0)
    AND k1.C_FIRST_NAME=k2.C_FIRST_NAME
    AND coalesce(k1.C_CURRENT_HDEMO_SK,0) = coalesce(k2.C_CURRENT_HDEMO_SK,0)
    AND k1.C_CURRENT_ADDR_SK=k2.C_CURRENT_ADDR_SK
    AND k1.C_LAST_NAME=k2.C_LAST_NAME
    AND k1.C_CUSTOMER_ID=k2.C_CUSTOMER_ID
    AND coalesce(k1.C_LAST_REVIEW_DATE_SK,0) = coalesce(k2.C_LAST_REVIEW_DATE_SK,0)
    AND coalesce(k1.C_BIRTH_MONTH,0) = coalesce(k2.C_BIRTH_MONTH,0)
    AND k1.C_BIRTH_COUNTRY = k2.C_BIRTH_COUNTRY
    AND coalesce(k1.C_BIRTH_YEAR,0) = coalesce(k2.C_BIRTH_YEAR,0)
    AND coalesce(k1.C_BIRTH_DAY,0) = coalesce(k2.C_BIRTH_DAY,0)
    AND k1.C_EMAIL_ADDRESS = k2.C_EMAIL_ADDRESS
    AND coalesce(k1.C_FIRST_SHIPTO_DATE_SK,0) = coalesce(k2.C_FIRST_SHIPTO_DATE_SK,0)
WHEN NOT MATCHED 
THEN INSERT (
    C_SALUTATION, 
    C_PREFERRED_CUST_FLAG, 
    C_FIRST_SALES_DATE_SK, 
    C_CUSTOMER_SK, C_LOGIN, 
    C_CURRENT_CDEMO_SK, 
    C_FIRST_NAME, 
    C_CURRENT_HDEMO_SK, 
    C_CURRENT_ADDR_SK, 
    C_LAST_NAME, 
    C_CUSTOMER_ID, 
    C_LAST_REVIEW_DATE_SK, 
    C_BIRTH_MONTH, 
    C_BIRTH_COUNTRY, 
    C_BIRTH_YEAR, 
    C_BIRTH_DAY, 
    C_EMAIL_ADDRESS, 
    C_FIRST_SHIPTO_DATE_SK,
    START_DATE,
    END_DATE)
VALUES (
    k2.C_SALUTATION, 
    k2.C_PREFERRED_CUST_FLAG, 
    k2.C_FIRST_SALES_DATE_SK, 
    k2.C_CUSTOMER_SK, 
    k2.C_LOGIN, 
    k2.C_CURRENT_CDEMO_SK, 
    k2.C_FIRST_NAME, 
    k2.C_CURRENT_HDEMO_SK, 
    k2.C_CURRENT_ADDR_SK, 
    k2.C_LAST_NAME, 
    k2.C_CUSTOMER_ID, 
    k2.C_LAST_REVIEW_DATE_SK, 
    k2.C_BIRTH_MONTH, 
    k2.C_BIRTH_COUNTRY, 
    k2.C_BIRTH_YEAR, 
    k2.C_BIRTH_DAY, 
    k2.C_EMAIL_ADDRESS, 
    k2.C_FIRST_SHIPTO_DATE_SK,
    CURRENT_DATE(),
    NULL
);



MERGE INTO TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT k1
USING TPCDS.RAW.CUSTOMER k2
ON  k1.C_CUSTOMER_SK=k2.C_CUSTOMER_SK
WHEN MATCHED
    AND (
    k1.C_SALUTATION!=k2.C_SALUTATION
    OR k1.C_PREFERRED_CUST_FLAG!=k2.C_PREFERRED_CUST_FLAG 
    OR coalesce(k1.C_FIRST_SALES_DATE_SK, 0) != coalesce(k2.C_FIRST_SALES_DATE_SK,0) 
    OR k1.C_LOGIN!=k2.C_LOGIN
    OR coalesce(k1.C_CURRENT_CDEMO_SK,0) != coalesce(k2.C_CURRENT_CDEMO_SK,0)
    OR k1.C_FIRST_NAME!=k2.C_FIRST_NAME
    OR coalesce(k1.C_CURRENT_HDEMO_SK,0) != coalesce(k2.C_CURRENT_HDEMO_SK,0)
    OR k1.C_CURRENT_ADDR_SK!=k2.C_CURRENT_ADDR_SK
    OR k1.C_LAST_NAME!=k2.C_LAST_NAME
    OR k1.C_CUSTOMER_ID!=k2.C_CUSTOMER_ID
    OR coalesce(k1.C_LAST_REVIEW_DATE_SK,0) != coalesce(k2.C_LAST_REVIEW_DATE_SK,0)
    OR coalesce(k1.C_BIRTH_MONTH,0) != coalesce(k2.C_BIRTH_MONTH,0)
    OR k1.C_BIRTH_COUNTRY != k2.C_BIRTH_COUNTRY
    OR coalesce(k1.C_BIRTH_YEAR,0) != coalesce(k2.C_BIRTH_YEAR,0)
    OR coalesce(k1.C_BIRTH_DAY,0) != coalesce(k2.C_BIRTH_DAY,0)
    OR k1.C_EMAIL_ADDRESS != k2.C_EMAIL_ADDRESS
    OR coalesce(k1.C_FIRST_SHIPTO_DATE_SK,0) != coalesce(k2.C_FIRST_SHIPTO_DATE_SK,0)
    ) 
THEN UPDATE SET
    end_date = current_date();


create or replace table TPCDS.ANALYTICS.CUSTOMER_DIM as
        (select 
       C_SALUTATION as Salutation ,
        C_PREFERRED_CUST_FLAG as Perferred_Customer_Flag,
        C_FIRST_SALES_DATE_SK as First_Sales_Date_SK,
        C_CUSTOMER_SK as CUSTOMER_SK,
        C_LOGIN as login,
        C_CURRENT_CDEMO_SK as CURRENT_Customer_Demographices_SK ,
        C_FIRST_NAME as FIRST_NAME,
        C_CURRENT_HDEMO_SK as CURRENT_household_Demographices_SK ,
        C_CURRENT_ADDR_SK as CURRENT_adderss_SK,
        C_LAST_NAME as LAST_NAME,
        C_CUSTOMER_ID as CUSTOMER_ID,
        C_LAST_REVIEW_DATE_SK as LAST_REVIEW_DATE_SK,
        C_BIRTH_MONTH as BIRTH_MONTH,
        C_BIRTH_COUNTRY as BIRTH_COUNTRY,
        C_BIRTH_YEAR as BIRTH_YEAR,
        C_BIRTH_DAY as BIRTH_DAY ,
        C_EMAIL_ADDRESS as EMAIL_ADDRESS,
        C_FIRST_SHIPTO_DATE_SK as FIRST_SHIPTO_DATE_SK,
        CA_STREET_NAME as STREET_NAME ,
        CA_SUITE_NUMBER as SUITE_NUMBER,
        CA_STATE as STATE,
        CA_LOCATION_TYPE as LOCATION_TYPE,
        CA_COUNTRY as COUNTRY,
        CA_ADDRESS_ID as ADDRESS_ID,
        CA_COUNTY as COUNTY,
        CA_STREET_NUMBER as STREET_NUMBER,
        CA_ZIP as ZIP,
        CA_CITY as CITY,
        CA_GMT_OFFSET,
        CD_DEP_EMPLOYED_COUNT as DEP_EMPLOYED_COUNT,
        CD_DEP_COUNT as DEP_COUNTS,
        CD_CREDIT_RATING as CREDIT_RATING,
        CD_EDUCATION_STATUS as EDUCATION_STATUS,
        CD_PURCHASE_ESTIMATE as PURCHASE_ESTIMATE,
        CD_MARITAL_STATUS as MARITAL_STATUS,
        CD_DEP_COLLEGE_COUNT as DEP_COLLEGE_COUNT,
        CD_GENDER as GENDER,
        HD_BUY_POTENTIAL as BUY_POTENTIAL,
        HD_DEP_COUNT as DEP_COUNT,
        HD_VEHICLE_COUNT as VEHICLE_COUNT,
        HD_INCOME_BAND_SK as INCOME_BAND_SK,
        IB_LOWER_BOUND as LOWER_BOUND,
        IB_UPPER_BOUND as UPPER_BOUND,
        START_DATE,
        END_DATE
from TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT
LEFT JOIN tpcds.raw.customer_address ON c_current_addr_sk = ca_address_sk
LEFT join tpcds.raw.customer_demographics ON c_current_cdemo_sk = cd_demo_sk
LEFT join tpcds.raw.household_demographics ON c_current_hdemo_sk = hd_demo_sk
LEFT join tpcds.raw.income_band ON HD_INCOME_BAND_SK = IB_INCOME_BAND_SK
        );
  END
  $$;


-- populating the data into daily_aggregated_sales
CREATE OR REPLACE PROCEDURE tpcds.intermediate.inserting_daily_aggregated_sales_incrementally()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
      DECLARE 
        LAST_SOLD_DATE_SK number;
    BEGIN
      SELECT MAX(SOLD_DATE_SK) INTO :LAST_SOLD_DATE_SK FROM tpcds.INTERMEDIATE.DAS; 
   delete from TPCDS.INTERMEDIATE.DAS where sold_date_sk = :last_sold_date_sk;

create or replace TEMPORARY table TPCDS.INTERMEDIATE.DAS_temp as (
-- compiling all incremental sales records
with incremental_sales as (
SELECT 
            CS_WAREHOUSE_SK as warehouse_sk,
            CS_ITEM_SK as item_sk,
            CS_SOLD_DATE_SK as sold_date_sk,
            CS_QUANTITY as quantity,
            cs_sales_price * cs_quantity as sales_amt,
            CS_NET_PROFIT as net_profit
    from tpcds.raw.catalog_sales
    WHERE sold_date_sk >= NVL(:LAST_SOLD_DATE_SK,0) 
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
    from tpcds.raw.web_sales
    WHERE sold_date_sk >= NVL(:LAST_SOLD_DATE_SK,0) 
        and quantity is not null
        and sales_amt is not null
),

aggregating_records_to_daily_sales as
(
select 
    warehouse_sk,
    item_sk,
    sold_date_sk, 
    sum(quantity) as daily_qty,
    sum(sales_amt) as daily_sales_amt,
    sum(net_profit) as daily_net_profit 
from incremental_sales
group by 1, 2, 3

),

adding_week_number_and_yr_number as
(
select 
    *,
    date.wk_num as sold_wk_num,
    date.yr_num as sold_yr_num
from aggregating_records_to_daily_sales 
LEFT JOIN tpcds.raw.date_dim date 
    ON sold_date_sk = d_date_sk

)

SELECT 
	warehouse_sk,
    item_sk,
    sold_date_sk,
    max(sold_wk_num) as sold_wk_num,
    max(sold_yr_num) as sold_yr_num,
    sum(daily_qty) as daily_qty,
    sum(daily_sales_amt) as daily_sales_amt,
    sum(daily_net_profit) as daily_net_profit 
FROM adding_week_number_and_yr_number
GROUP BY 1,2,3
ORDER BY 1,2,3
);

insert into tpcds.intermediate.das(
warehouse_sk,
item_sk,
sold_date_sk,
sold_wk_num,
sold_yr_num,
daily_qty,
daily_sales_amt,
daily_net_profit 
)
select 
  distinct
    warehouse_sk,
    item_sk,
    sold_date_sk,
    sold_wk_num,
    sold_yr_num,
    daily_qty,
    daily_sales_amt,
    daily_net_profit
from tpcds.intermediate.das_temp;
  END
  $$;


-- populating the data into weekly_aggregated_sales
CREATE OR REPLACE PROCEDURE tpcds.ANALYTICS.inserting_weekly_aggregated_sales_incrementally()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
      DECLARE 
        LAST_SOLD_wk_SK number;
    BEGIN
     DELETE FROM  TPCDS.ANALYTICS.WEEKLY_SALES WHERE sold_wk_sk=:LAST_SOLD_WK_SK;

-- compiling all incremental sales records
CREATE OR REPLACE TEMPORARY TABLE  TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY_TMP AS (
with aggregating_daily_sales_to_week as (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK, 
    MIN(SOLD_DATE_SK) AS SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM(DAILY_QTY) AS SUM_QTY_WK, 
    SUM(DAILY_SALES_AMT) AS SUM_AMT_WK, 
    SUM(DAILY_NET_PROFIT) AS SUM_PROFIT_WK
FROM
     TPCDS.INTERMEDIATE.DAS
GROUP BY
    1,2,4,5
HAVING 
    sold_wk_sk >= NVL(:LAST_SOLD_WK_SK,0)
),

-- We need to have the same sold_wk_sk for all the items. Currently, any items that didn't have any sales on Sunday (first day of the week) would not have Sunday date as sold_wk_sk so this CTE will correct that.
finding_first_date_of_the_week as (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK, 
    date.d_date_sk AS SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_QTY_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK
FROM
    aggregating_daily_sales_to_week daily_sales
INNER JOIN TPCDS.RAW.DATE_DIM as date
on daily_sales.SOLD_WK_NUM=date.wk_num
and daily_sales.sold_yr_num=date.yr_num
and date.day_of_wk_num=0
),

-- This will help sales and inventory tables to join together using wk_num and yr_num
date_columns_in_inventory_table as (
SELECT 
    inventory.*,
    date.wk_num as inv_wk_num,
    date.yr_num as inv_yr_num
FROM
    tpcds.raw.inventory inventory
INNER JOIN TPCDS.RAW.DATE_DIM as date
on inventory.inv_date_sk = date.d_date_sk
)

select 
       warehouse_sk, 
       item_sk, 
       min(SOLD_WK_SK) as sold_wk_sk,
       sold_wk_num as sold_wk_num,
       sold_yr_num as sold_yr_num,
       sum(sum_qty_wk) as sum_qty_wk,
       sum(sum_amt_wk) as sum_amt_wk,
       sum(sum_profit_wk) as sum_profit_wk,
       sum(sum_qty_wk)/7 as avg_qty_dy,
       sum(coalesce(inv.inv_quantity_on_hand, 0)) as inv_qty_wk, 
       sum(coalesce(inv.inv_quantity_on_hand, 0)) / sum(sum_qty_wk) as wks_sply,
       iff(avg_qty_dy>0 and avg_qty_dy>inv_qty_wk, true , false) as low_stock_flg_wk
from finding_first_date_of_the_week
left join date_columns_in_inventory_table inv 
    on inv_wk_num = sold_wk_num and inv_yr_num = sold_yr_num and item_sk = inv_item_sk and inv_warehouse_sk = warehouse_sk
group by 1, 2, 4, 5
-- extra precaution because we don't want negative or zero quantities in our final model
having sum(sum_qty_wk) > 0
);

-- Inserting new records
INSERT INTO  TPCDS.ANALYTICS.WEEKLY_SALES
(	
	WAREHOUSE_SK, 
    ITEM_SK, 
    SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_QTY_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK, 
    AVG_QTY_DY, 
    INV_QTY_WK, 
    WKS_SPLY, 
    LOW_STOCK_FLG_WK
    
)
SELECT 
    DISTINCT
	WAREHOUSE_SK, 
    ITEM_SK, 
    SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_QTY_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK, 
    AVG_QTY_DY, 
    INV_QTY_WK, 
    WKS_SPLY, 
    LOW_STOCK_FLG_WK
FROM  TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY_TMP;

  END
  $$;