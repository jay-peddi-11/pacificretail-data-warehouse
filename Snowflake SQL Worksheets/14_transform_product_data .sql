/*
    Author Name: Jay Peddi
    Create Date: 24/03/2025
    Description: This script creates a stored procedure and scheduled task to process product data 
                 from the bronze to silver layer with data quality transformations.
*/

-- sets the current working context to the SILVER schema
USE pacificretail_db.silver;

-- creates a stored procedure to process product changes
CREATE OR REPLACE PROCEDURE process_product_changes()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  rows_inserted INT;
  rows_updated INT;
BEGIN
  -- Merge changes into the silver layer
  MERGE INTO silver.product AS target
  USING (
    SELECT
      product_id,
      name,
      category,
      
      -- Price validation and normalisation
      CASE
        WHEN price < 0 THEN 0
        ELSE price
      END AS price,
      
      brand,
      
      -- Stock quantity validation
      CASE
        WHEN stock_quantity > 0 THEN stock_quantity
        ELSE 0
      END AS stock_quantity,
      
      -- Rating validation
      CASE
        WHEN rating BETWEEN 1 AND 5 THEN rating
        ELSE 0
      END AS rating,
      
      is_active,
      CURRENT_TIMESTAMP() AS last_updated_timestamp
    FROM
        bronze.product_changes_stream
  ) AS source
  ON target.product_id = source.product_id
  WHEN MATCHED THEN
    UPDATE SET
      name = source.name,
      category = source.category,
      price = source.price,
      brand = source.brand,
      stock_quantity = source.stock_quantity,
      rating = source.rating,
      is_active = source.is_active,
      last_updated_timestamp = source.last_updated_timestamp
  WHEN NOT MATCHED THEN
    INSERT (
        product_id,
        name,
        category,
        price,
        brand,
        stock_quantity,
        rating,
        is_active,
        last_updated_timestamp
    )
    VALUES (
        source.product_id,
        source.name,
        source.category,
        source.price,
        source.brand,
        source.stock_quantity,
        source.rating,
        source.is_active,
        source.last_updated_timestamp
    );
  
  -- Return summary of operations
  RETURN 'Product records processed successfully.';
END;
$$;

-- creates a scheduled task to run the procedure every 4 hours (15 minutes past the hour)
CREATE OR REPLACE TASK merge_product_data_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 15 */4 * * * America/New_York'
AS
  CALL process_product_changes();

-- activates the task to begin running on schedule
ALTER TASK merge_product_data_task RESUME;