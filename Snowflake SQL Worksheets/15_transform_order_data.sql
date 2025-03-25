/*
    Author Name: Jay Peddi
    Create Date: 24/03/2025
    Description: This script creates a stored procedure and scheduled task to process order data 
                 from the bronze to silver layer with data quality filtering.
*/

-- sets the current working context to the SILVER schema
USE pacificretail_db.silver;

-- creates a stored procedure to process order changes
CREATE OR REPLACE PROCEDURE process_order_changes()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  rows_inserted INT;
  rows_updated INT;
BEGIN
  -- Merge changes into the silver layer
  MERGE INTO silver.orders AS target
  USING (
    SELECT
      transaction_id,
      customer_id,
      product_id,
      quantity,
      store_type,
      total_amount,
      transaction_date,
      payment_method,
      CURRENT_TIMESTAMP() AS last_updated_timestamp
    FROM
        bronze.order_changes_stream
    WHERE
        transaction_id IS NOT NULL
        AND total_amount > 0  -- Basic data quality rule
  ) AS source
  ON target.transaction_id = source.transaction_id
  WHEN MATCHED THEN
    UPDATE SET
      customer_id = source.customer_id,
      product_id = source.product_id,
      quantity = source.quantity,
      store_type = source.store_type,
      total_amount = source.total_amount,
      transaction_date = source.transaction_date,
      payment_method = source.payment_method,
      last_updated_timestamp = source.last_updated_timestamp
  WHEN NOT MATCHED THEN
    INSERT (
        transaction_id,
        customer_id,
        product_id,
        quantity,
        store_type,
        total_amount,
        transaction_date,
        payment_method,
        last_updated_timestamp
    )
    VALUES (
        source.transaction_id,
        source.customer_id,
        source.product_id,
        source.quantity,
        source.store_type,
        source.total_amount,
        source.transaction_date,
        source.payment_method,
        source.last_updated_timestamp
    );
  
  -- Return summary of operations
  RETURN 'Order records processed successfully.';
END;
$$;

-- creates a scheduled task to run the procedure every 2 hours (30 minutes past the hour)
CREATE OR REPLACE TASK merge_order_data_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 30 */2 * * * America/New_York'
AS
  CALL process_order_changes();

-- activates the task to begin running on schedule
ALTER TASK merge_order_data_task RESUME;