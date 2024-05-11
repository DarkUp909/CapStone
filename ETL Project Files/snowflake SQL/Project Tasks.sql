CREATE OR REPLACE TASK tpcds.intermediate.creating_daily_aggregated_sales_incrementally
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON * 8 * * * UTC'
    AS
CALL inserting_daily_aggregated_sales_incrementally();

ALTER TASK tpcds.intermediate.creating_daily_aggregated_sales_incrementally RESUME;
EXECUTE TASK TPCDS.INTERMEDIATE.CREATING_DAILY_AGGREGATED_SALES_INCREMENTALLY;
-- drop TASK TPCDS.INTERMEDIATE.CREATING_DAILY_AGGREGATED_SALES_INCREMENTALLY;

-- truncate table tpcds.intermediate.das;



-- automate task to create and populate the costomer_dim (DAS) everyday at 8 am
CREATE OR REPLACE TASK tpcds.ANALYTICS.inserting_customer
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON * 8 * * * UTC'
    AS
CALL ANALYTICS.inserting_customer();

ALTER TASK tpcds.ANALYTICS.inserting_customer RESUME;
EXECUTE TASK tpcds.ANALYTICS.inserting_customer;
-- drop task tpcds.ANALYTICS.inserting_customer();

-- truncate table tpcds.intermediate.customer_snapshot;
-- truncate table tpcds.ANALYTICS.customer_dim;


-- automate task to create and populate the weekly sales at the beginning of every week at 8 am
CREATE OR REPLACE TASK tpcds.ANALYTICS.crafting_weekly_aggregated_sales_incrementally
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON * 8 * * 0 UTC'
    AS
CALL ANALYTICS.inserting_weekly_aggregated_sales_incrementally();

ALTER TASK tpcds.ANALYTICS.crafting_weekly_aggregated_sales_incrementally RESUME;
EXECUTE TASK tpcds.ANALYTICS.crafting_weekly_aggregated_sales_incrementally;
-- drop task tpcds.ANALYTICS.crafting_weekly_aggregated_sales_incrementally();

-- truncate table tpcds.analytics.weekly_sales;