/*
    Author Name: Jay Peddi
    Create Date: 24/03/2025
    Description: This script executes the ETL data pipeline tasks, validates data, and manages task states.
*/

USE pacificretail_db;

-- creates a GOLD schema for analytics
CREATE SCHEMA IF NOT EXISTS gold;

-- sets working context to BRONZE layer for raw data operations
USE pacificretail_db.bronze;

-- checks the initial state of customer data
SELECT * FROM raw_customer;

-- executes BRONZE layer loading tasks
EXECUTE TASK load_customer_data_task;
EXECUTE TASK load_product_data_task;
EXECUTE TASK load_order_data_task;

-- suspends BRONZE layer tasks after manual execution
ALTER TASK load_customer_data_task SUSPEND;
ALTER TASK load_product_data_task SUSPEND;
ALTER TASK load_order_data_task SUSPEND;

-- validates data was loaded into BRONZE tables
SELECT * FROM raw_customer;
SELECT * FROM raw_product;
SELECT * FROM raw_order;

-- checks stream status
SHOW STREAMS;
SELECT * FROM customer_changes_stream;

-- sets working context to SILVER layer for transformation operations
USE pacificretail_db.silver;

-- shows SILVER layer tasks
SHOW TASKS;

-- executes SILVER layer transformation tasks
EXECUTE TASK merge_customer_data_task;
EXECUTE TASK merge_product_data_task;
EXECUTE TASK merge_order_data_task;

-- suspends SILVER tasks after manual execution
ALTER TASK merge_customer_data_task SUSPEND;
ALTER TASK merge_product_data_task SUSPEND;
ALTER TASK merge_order_data_task SUSPEND;

-- validates data was loaded into SILVER tables
SELECT * FROM customer;
SELECT * FROM product;
SELECT * FROM orders;