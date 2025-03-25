/*
    Author Name: Jay Peddi
    Create Date: 24/03/2025
    Description: This script sets up an automated data pipeline in Snowflake to load order data from Parquet files.
*/

USE pacificretail_db.bronze;

-- creates a Parquet file format definition
CREATE OR REPLACE FILE FORMAT parquet_file_format
    TYPE = PARQUET
    COMPRESSION = AUTO
    -- indicates that binary data in the Parquet files should be treated as binary rather than being converted to text
    BINARY_AS_TEXT = FALSE
    -- prevents from automatically removing leading and trailing whitespace from string values
    TRIM_SPACE = FALSE;

-- TEST
SELECT
    *
FROM
    @adls_stage/Order/
    (FILE_FORMAT => parquet_file_format)
LIMIT 10;

-- creates a destination table called raw_order to store order transaction data
CREATE OR REPLACE TABLE raw_order (
    customer_id INT,
    payment_method STRING,
    product_id INT,
    quantity INT,
    store_type STRING,
    total_amount DOUBLE,
    transaction_date DATE,
    transaction_id STRING,
    source_file_name STRING,
    source_file_row_number INT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- sets up a scheduled task that runs daily at 4 AM Eastern Time
CREATE OR REPLACE TASK load_order_data_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 4 * * * America/New_York'
AS
    COPY INTO raw_order (
        customer_id,
        payment_method,
        product_id,
        quantity,
        store_type,
        total_amount,
        transaction_date,
        transaction_id,
        source_file_name,
        source_file_row_number
    )
    FROM (
        SELECT
            $1:customer_id::INT,
            $1:payment_method::STRING,
            $1:product_id::INT,
            $1:quantity::INT,
            $1:store_type::STRING,
            $1:total_amount::DOUBLE,
            $1:transaction_date::DATE,
            $1:transaction_id::STRING,
            metadata$filename,
            metadata$file_row_number
        FROM
            @adls_stage/Order/
    )
    FILE_FORMAT = (FORMAT_NAME = 'parquet_file_format')
    ON_ERROR = 'CONTINUE'
    PATTERN = '.*[.]parquet';

-- activates the task
ALTER TASK load_order_data_task RESUME;