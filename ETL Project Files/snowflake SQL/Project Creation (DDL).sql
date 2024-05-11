-- creating the Intermediante layer/Schema
create SCHEMA INTERMEDIATE;

-- creating the Analytics layer/Schema
create schema ANALYTICS;

-- based on the Business Requirements we clone Item, Werehouse and Date Tables from Raw layer/Schema to Intermediante layer/Schema
create or replace TABLE TPCDS.ANALYTICS.Item clone TPCDS.RAW.ITEM;
select * from TPCDS.ANALYTICS.Item limit 3;

create or replace TABLE TPCDS.ANALYTICS.Warehouse clone TPCDS.RAW.Warehouse;
select * from TPCDS.ANALYTICS.Warehouse limit 3;

create or replace TABLE TPCDS.ANALYTICS.Date_dim clone TPCDS.RAW.Date_dim;
select * from TPCDS.ANALYTICS.Date_dim limit 3;


-- creating the Weekly Sales Table based on the Business Requirements and define the it's Grain in the Analytics layer/Schema
-- and breaking down the Date into 3 Columns 'sold_wk_sk', 'sold_wk_num' and 'sold_yr_num'
create or replace table TPCDS.ANALYTICS.WEEKLY_SALES(
WAREHOUSE_SK NUMBER(38,0),
ITEM_SK NUMBER(38,0),
SOLD_WK_SK NUMBER(38,0),
SOLD_WK_NUM NUMBER(38,0),
SOLD_YR_NUM NUMBER(38,0),
sum_qty_wk number(38,0),
sum_amt_wk float,
sum_profit_wk float,
avg_qty_dy float,
INV_QTY_WK number(38,0),
wks_sply number(38,0),
low_stock_flg_wk boolean
);

-- creating one big customer table the Analytics layer/Schema to display the combined data form all the related tables to the costumer form the Raw schema 
create or replace TABLE TPCDS.ANALYTICS.CUSTOMER_DIM (
	C_SALUTATION VARCHAR(16777216),
	C_PREFERRED_CUST_FLAG VARCHAR(16777216),
	C_FIRST_SALES_DATE_SK NUMBER(38,0),
	C_CUSTOMER_SK NUMBER(38,0),
	C_LOGIN VARCHAR(16777216),
	C_CURRENT_CDEMO_SK NUMBER(38,0),
	C_FIRST_NAME VARCHAR(16777216),
	C_CURRENT_HDEMO_SK NUMBER(38,0),
	C_CURRENT_ADDR_SK NUMBER(38,0),
	C_LAST_NAME VARCHAR(16777216),
	C_CUSTOMER_ID VARCHAR(16777216),
	C_LAST_REVIEW_DATE_SK NUMBER(38,0),
	C_BIRTH_MONTH NUMBER(38,0),
	C_BIRTH_COUNTRY VARCHAR(16777216),
	C_BIRTH_YEAR NUMBER(38,0),
	C_BIRTH_DAY NUMBER(38,0),
	C_EMAIL_ADDRESS VARCHAR(16777216),
	C_FIRST_SHIPTO_DATE_SK NUMBER(38,0),
	CA_STREET_NAME VARCHAR(16777216),
	CA_SUITE_NUMBER VARCHAR(16777216),
	CA_STATE VARCHAR(16777216),
	CA_LOCATION_TYPE VARCHAR(16777216),
	CA_COUNTRY VARCHAR(16777216),
	CA_ADDRESS_ID VARCHAR(16777216),
	CA_COUNTY VARCHAR(16777216),
	CA_STREET_NUMBER VARCHAR(16777216),
	CA_ZIP VARCHAR(16777216),
	CA_CITY VARCHAR(16777216),
	CA_GMT_OFFSET FLOAT,
	CD_DEP_EMPLOYED_COUNT NUMBER(38,0),
	CD_DEP_COUNT NUMBER(38,0),
	CD_CREDIT_RATING VARCHAR(16777216),
	CD_EDUCATION_STATUS VARCHAR(16777216),
	CD_PURCHASE_ESTIMATE NUMBER(38,0),
	CD_MARITAL_STATUS VARCHAR(16777216),
	CD_DEP_COLLEGE_COUNT NUMBER(38,0),
	CD_GENDER VARCHAR(16777216),
	HD_BUY_POTENTIAL VARCHAR(16777216),
	HD_DEP_COUNT NUMBER(38,0),
	HD_VEHICLE_COUNT NUMBER(38,0),
	HD_INCOME_BAND_SK NUMBER(38,0),
	IB_LOWER_BOUND NUMBER(38,0),
	IB_UPPER_BOUND NUMBER(38,0),
	START_DATE TIMESTAMP_NTZ(9),
	END_DATE TIMESTAMP_NTZ(9)
);

-- creating the Daily Aggregated Sales (DAS) table in Intermediante layer/Schema to break down the sales to a daily level 
create or replace TABLE TPCDS.INTERMEDIATE.DAS(
warehouse_sk number(38,0),
item_sk number(38,0),
sold_date_sk number(38,0),
sold_wk_num number(38,0),
sold_yr_num number(38,0),
daily_qty number(38,0),
daily_sales_amt float,
daily_net_profit float
);

-- creating the coustomer snapshot table to facilitate combining the data form all the related tables to the costumer from RAW layer/Schema
CREATE OR REPLACE TABLE TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT (
	C_SALUTATION VARCHAR(16777216),
	C_PREFERRED_CUST_FLAG VARCHAR(16777216),
	C_FIRST_SALES_DATE_SK NUMBER(38,0),
	C_CUSTOMER_SK NUMBER(38,0),
	C_LOGIN VARCHAR(16777216),
	C_CURRENT_CDEMO_SK NUMBER(38,0),
	C_FIRST_NAME VARCHAR(16777216),
	C_CURRENT_HDEMO_SK NUMBER(38,0),
	C_CURRENT_ADDR_SK NUMBER(38,0),
	C_LAST_NAME VARCHAR(16777216),
	C_CUSTOMER_ID VARCHAR(16777216),
	C_LAST_REVIEW_DATE_SK NUMBER(38,0),
	C_BIRTH_MONTH NUMBER(38,0),
	C_BIRTH_COUNTRY VARCHAR(16777216),
	C_BIRTH_YEAR NUMBER(38,0),
	C_BIRTH_DAY NUMBER(38,0),
	C_EMAIL_ADDRESS VARCHAR(16777216),
	C_FIRST_SHIPTO_DATE_SK NUMBER(38,0),
	START_DATE TIMESTAMP_NTZ(9),
	END_DATE TIMESTAMP_NTZ(9)
);