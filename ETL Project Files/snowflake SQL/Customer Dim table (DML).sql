-- Inserting the data coming form the customer table in RAW layer/Schema into customer snapshot table
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


-- Making sure the date is up to date by checking the Date
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

-- Inserting and combining all data in the customer related tables to the customer table in Analytics layer/Schema
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