/*
    Author Name: Jay Peddi
    Create Date: 20/03/2025
    Description: This Snowflake SQL command creates or replaces a file format definition named csv_file_format.
*/

USE pacificretail_db.bronze

-- creates or replaces the specified file format depending on its existence
CREATE OR REPLACE FILE FORMAT csv_file_format
    -- indicates the type of file
    TYPE = CSV
    -- indicates the character used to separate fields within each row
    FIELD_DELIMITER = ','
    -- skips the first row of the file
    SKIP_HEADER = 1
    -- specifies that the following strings should be interpreted as NULL values
    NULL_IF = ('NULL', 'null', '')
    -- handles cases where a field between delimiters is completely empty
    EMPTY_FIELD_AS_NULL = TRUE
    -- automatically detects and handles the compression of the file
    COMPRESSION = AUTO;

-- TEST
SELECT
    $1,
    $2,
    $3,
    $4,
    $5,
    $6
FROM
    @adls_stage/Customer
    (FILE_FORMAT => csv_file_format) -- instructs the query to use the specified file format to interpret the data in the file
LIMIT 10;