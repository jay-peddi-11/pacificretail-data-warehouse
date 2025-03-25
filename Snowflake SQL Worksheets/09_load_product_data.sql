/*
    Author Name: Jay Peddi
    Create Date: 24/03/2025
    Description: This script sets up an automated data pipeline in Snowflake to load product data from JSON files.
*/

USE pacificretail_db.bronze;

-- creates a JSON file format definition that handles arrays and UTF-8 encoding issues
CREATE OR REPLACE FILE FORMAT json_file_format
    TYPE = JSON
    -- removes the outer array brackets from JSON files that contain arrays of objects
    STRIP_OUTER_ARRAY = TRUE
    -- skips over invalid UTF-8 character sequences instead of failing
    IGNORE_UTF8_ERRORS = TRUE;

-- TEST
SELECT
    $1
FROM
    @adls_stage/Product/
    (FILE_FORMAT => json_file_format)
LIMIT 10;

-- creates a destination table called raw_product with columns for product details and metadata
CREATE TABLE IF NOT EXISTS raw_product (
    product_id INT,
    name STRING,
    category STRING,
    brand STRING,
    price FLOAT,
    stock_quantity INT,
    rating FLOAT,
    is_active BOOLEAN,
    source_file_name STRING,
    source_file_row_number INT,
    ingestion_time_stamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- sets up a scheduled task that runs daily at 3 AM Eastern Time
CREATE OR REPLACE TASK load_product_data_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 3 * * * America/New_York'
AS
    COPY INTO raw_product (
        product_id,
        name,
        category,
        brand,
        price,
        stock_quantity,
        rating,
        is_active,
        source_file_name,
        source_file_row_number
    )
    FROM (
        SELECT
            $1:product_id::INT,
            $1:name::STRING,
            $1:category::STRING,
            $1:brand::STRING,
            $1:price::FLOAT,
            $1:stock_quantity::INT,
            $1:rating::FLOAT,
            $1:is_active::BOOLEAN,
            metadata$filename,
            metadata$file_row_number
        FROM
            @adls_stage/Product/
    )
    FILE_FORMAT = (FORMAT_NAME = 'json_file_format')
    ON_ERROR = 'CONTINUE'
    PATTERN = '.*[.]json';

-- activates the task
ALTER TASK load_product_data_task RESUME;
