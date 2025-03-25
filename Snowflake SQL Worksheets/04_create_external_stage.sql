/*
    Author Name: Jay Peddi
    Create Date: 20/03/2025
    Description: This Snowflake command creates an external stage in Snowflake that connects to Azure Data Lake Storage.
*/

-- sets the current working context to BRONZE schema within the PACIFICRETAIL_DB database
USE pacificretail_db.bronze

-- initiates the creation of a stage named adls_stage or replaces it if it already exists
CREATE OR REPLACE STAGE adls_stage
    -- specifies that the stage will use the previously created integration to authenticate and connect to Azure
    STORAGE_INTEGRATION = azure_pacificretail_integration
    -- specifies the Azure Blob Storage Account that this stage will point to
    URL = 'azure://pacificretail99.blob.core.windows.net/data-source'

-- lists the files available in the external stage named adls_stage
ls @adls_stage

/*
    POINTS TO REMEMBER:
    * A stage in Snowflake is a location where data files can be stored for loading into and unloading from Snowflake tables.
    * Think of INTEGRATION as having permission to enter a building (the cloud storage account), 
      while STAGE is like knowing the specific room number where you need to go.
    * For example, you might create one storage integration for Azure, 
      but then create multiple stages that point to different containers or folders with in that Azure account, 
      all using the same integration for authentication.
*/