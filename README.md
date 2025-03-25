# PacificRetail Data Warehouse Project

## Overview

This project implements a modern cloud-based data warehouse solution for PacificRetail, a rapidly growing e-commerce company operating in 15 countries across North America and Europe. With over 5 million active customers and a catalog exceeding 100,000 products, this solution addresses PacificRetail's data management challenges.

## Business Problem

PacificRetail faced several critical challenges with their existing data infrastructure:

• Data Silos: Customer data, product information, and transaction records were stored in separate systems, preventing a unified view of the business.

• Slow Reporting: Their batch processing methods caused 24-hour delays for sales reports, hindering timely decision-making.

• Limited Scalability: The on-premises data warehouse struggled to handle increasing data volumes, especially during peak sales periods.

• Poor Data Quality: Inconsistent data formats and lack of standardisation across different countries led to unreliable reporting.

• Limited Analytics Capabilities: The existing setup couldn't support advanced analytics or machine learning initiatives needed for personalised marketing and demand forecasting.

## Solution Architecture

![PacificRetail Data Warehouse Architecture](https://github.com/jay-peddi-11/pacificretail-data-warehouse/blob/main/Architecture%20Diagram/PacificRetail%20Data%20Warehouse%20Architecture.png)

### Data Sources

• Customer Data: CSV files from the CRM system (daily exports)

• Product Catalog: JSON files from the inventory management system (hourly updates)

• Transaction Logs: Parquet files from the e-commerce platform (real-time generation)

### Data Flow

• Data is landed in Azure Data Lake Storage (ADLS) from source systems

• Automated Snowflake tasks load data from ADLS into the data warehouse

• Data moves through three processing layers: Bronze Layer, Silver Layer, and Gold Layer

• Business intelligence tools connect to the Gold Layer for reporting and analysis

### Key Technical Features

• External stages connecting Snowflake to Azure Data Lake Storage

• Automated data pipelines using Snowflake tasks and streams

• Data quality validation and transformation logic

• Business intelligence views for reporting and analysis

## Business Benefits and Results

The implementation delivered significant improvements:

• Unified Data View: Created a single source of truth by consolidating data from various sources.

• Faster Insights: Reduced data processing time from 24 hours to just 1 hour, enabling near real-time analytics.

• Enhanced Scalability: Built to handle 5x current data volumes without performance degradation.

• Improved Data Quality: Implemented robust data quality checks and standardisation processes.

• Advanced Analytics Foundation: Created the groundwork for future machine learning and advanced analytics initiatives.

• Self-Service Analytics: Enabled business users across different departments to access trusted data for decision-making.

• Improved Reporting Accuracy: Achieved 99% accuracy in cross-channel sales reporting.

## Future Expansion

This architecture provides a foundation that can be extended to support:

• Advanced machine learning models for customer behaviour prediction

• Real-time dashboards for operational monitoring

• Expanded data sources and additional business domains
