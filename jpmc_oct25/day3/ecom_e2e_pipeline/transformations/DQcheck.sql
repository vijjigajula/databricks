CREATE STREAMING TABLE dev.silver.sales_cleaned
(CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW)
select distinct * from stream dev.bronze.sales