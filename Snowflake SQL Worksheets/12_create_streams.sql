/*
    Author Name: Jay Peddi
    Create Date: 24/03/2025
    Description: This script creates Snowflake streams to track changes in the raw data tables.
*/

-- sets the current working context to the BRONZE schema in the PACIFICRETAIL_DB
USE pacificretail_db.bronze;

-- creates three streams, each monitoring a different raw table
CREATE OR REPLACE STREAM customer_changes_stream ON TABLE raw_customer
    APPEND_ONLY = TRUE; -- only captures new rows added to the table, not updates or deletions

CREATE OR REPLACE STREAM product_changes_stream ON TABLE raw_product
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM order_changes_stream ON TABLE raw_order
    APPEND_ONLY = TRUE;

-- displays a list of all streams in the PACIFICRETAIL_DB.BRONZE schema
SHOW STREAMS IN pacificretail_db.bronze;

/*
    POINTS TO REMEMBER:
    * Streams are used to efficiently identify records that are new since the last processing run, 
      enabling incremental data loading without having to process all data each time.
    * This approach supports data pipeline efficiency and reduces processing costs.
*/