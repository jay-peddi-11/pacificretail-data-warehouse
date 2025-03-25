/*
    Author Name: Jay Peddi
    Create Date: 24/03/2025
    Description: This SQL code creates a Snowflake scheduled task that loads customer data from files into a table.
*/

USE pacificretail_db.bronze;

CREATE OR REPLACE TASK load_customer_data_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 2 * * * America/New_York' -- runs daily at 2 AM Eastern Time
AS
    COPY INTO raw_customer ( -- inserts data into the raw_customer table
        customer_id,
        name,
        email,
        country,
        customer_type,
        registration_date,
        age,
        gender,
        total_purchases,
        source_file_name,
        source_file_row_number
    )
    FROM (
        SELECT
            $1,
            $2,
            $3,
            $4,
            $5,
            $6::DATE, -- converts to DATE format
            $7,
            $8,
            $9,
            metadata$filename,
            metadata$file_row_number
        FROM
            @adls_stage/Customer/
    )
    FILE_FORMAT = (FORMAT_NAME = 'csv_file_format')
    ON_ERROR = 'CONTINUE' -- skips the rows with errors
    PATTERN = '.*[.]csv'; -- only processes CSV files