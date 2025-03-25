/*
    Author Name: Jay Peddi
    Create Date: 24/03/2025
    Description: This script creates a SILVER schema and three tables within it for PacificRetail's data warehouse design.
*/

USE pacificretail_db

-- creates a new schema called SILVER within the database
CREATE SCHEMA silver;

-- sets the current working context
USE pacificretail_db.silver;

-- creates three tables with cleaned, structured data
CREATE TABLE IF NOT EXISTS customer (
    customer_id INT,
    name STRING,
    email STRING,
    country STRING,
    customer_type STRING,
    registration_date DATE,
    age INT,
    gender STRING,
    total_purchases INT,
    last_updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS product (
    product_id INT,
    name STRING,
    category STRING,
    brand STRING,
    price FLOAT,
    stock_quantity INT,
    rating FLOAT,
    is_active BOOLEAN,
    last_updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS orders (
    transaction_id STRING,
    customer_id INT,
    product_id INT,
    quantity INT,
    store_type STRING,
    total_amount DOUBLE,
    transaction_date DATE,
    payment_method STRING,
    last_updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
