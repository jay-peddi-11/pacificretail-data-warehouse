/*
    Author Name: Jay Peddi
    Create Date: 24/03/2025
    Description: This script creates a stored procedure and scheduled task to process customer data 
                 from the bronze to silver layer with data quality transformations.
*/

-- sets the current working context to the SILVER schema
USE pacificretail_db.silver;

-- creates a stored procedure to process customer changes
CREATE OR REPLACE PROCEDURE process_customer_changes()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  rows_inserted INT;
  rows_updated INT;
BEGIN
  -- Merge changes into the silver layer
  MERGE INTO silver.customer AS target
  USING (
    SELECT
      customer_id,
      name,
      email,
      country,
      
      -- Customer type standardisation
      CASE
        WHEN TRIM(UPPER(customer_type)) IN ('REGULAR', 'REG', 'R') THEN 'Regular'
        WHEN TRIM(UPPER(customer_type)) IN ('PREMIUM', 'PREM', 'P') THEN 'Premium'
        ELSE 'Unknown'
      END AS customer_type,
      
      registration_date,
      
      -- Age validation
      CASE
        WHEN age BETWEEN 18 AND 120 THEN age
        ELSE NULL
      END AS age,
      
      -- Gender standardisation
      CASE
        WHEN TRIM(UPPER(gender)) IN ('M', 'MALE') THEN 'Male'
        WHEN TRIM(UPPER(gender)) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'Other'
      END AS gender,
      
      -- Total purchases validation
      CASE
        WHEN total_purchases > 0 THEN total_purchases
        ELSE 0
      END AS total_purchases,
      
      current_timestamp() AS last_updated_timestamp
    FROM
        bronze.customer_changes_stream
    WHERE
        customer_id IS NOT NULL
        AND email IS NOT NULL -- Basic data quality rule
  ) AS source
  ON target.customer_id = source.customer_id
  WHEN MATCHED THEN
    UPDATE SET
      name = source.name,
      email = source.email,
      country = source.country,
      customer_type = source.customer_type,
      registration_date = source.registration_date,
      age = source.age,
      gender = source.gender,
      total_purchases = source.total_purchases,
      last_updated_timestamp = source.last_updated_timestamp
  WHEN NOT MATCHED THEN
    INSERT (
        customer_id,
        name,
        email,
        country,
        customer_type,
        registration_date,
        age,
        gender,
        total_purchases,
        last_updated_timestamp
    )
    VALUES (
        source.customer_id,
        source.name,
        source.email,
        source.country,
        source.customer_type,
        source.registration_date,
        source.age,
        source.gender,
        source.total_purchases,
        source.last_updated_timestamp
    );
  
  -- Return summary of operations
  RETURN 'Customer records processed successfully.';
END;
$$;

-- creates a scheduled task to run the procedure every 4 hours
CREATE OR REPLACE TASK merge_customer_data_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 */4 * * * America/New_York'
AS
  CALL process_customer_changes();

-- activates the task to begin running on schedule
ALTER TASK merge_customer_data_task RESUME;