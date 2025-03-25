/*
    Author Name: Jay Peddi
    Create Date: 20/03/2025
    Description: This Snowflake SQL command creates a table named raw_customer.
*/

USE pacificretail_db.bronze;

CREATE TABLE IF NOT EXISTS raw_customer (
    customer_id INT,
    name STRING,
    email STRING,
    country STRING,
    customer_type STRING,
    registration_date DATE,
    age INT,
    gender STRING,
    total_purchases INT,
    source_file_name STRING,
    source_file_row_number INT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

/*
    POINTS TO REMEMBER:
    * ingestion_timestamp column clearly indicates that it will store the timestamp of when a row was ingested or loaded.
    * TIMESTAMP_NTZ specifies the datatype of the column. It stores a date and time value without any time zone information.
    * CURRENT_TIMESTAMP() is a function that returns the current date and time at the moment the row is inserted.
    * DEFAULT ensures that if a value is not explicitly provided during an INSERT operation, the current timestamp will be automatically inserted.
*/