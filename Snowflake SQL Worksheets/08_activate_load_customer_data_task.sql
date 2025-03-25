/*
    Author Name: Jay Peddi
    Create Date: 24/03/2025
    Description: This Snowflake SQL command changes the task's state to active.
*/

USE pacificretail_db.bronze;

-- changes the task's state from SUSPENDED (INACTIVE) to ACTIVE
ALTER TASK load_customer_data_task RESUME;