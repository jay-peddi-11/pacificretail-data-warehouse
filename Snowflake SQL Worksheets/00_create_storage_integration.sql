/*
    Author Name: Jay Peddi
    Create Date: 20/03/2025
    Description: This Snowflake command creates a storage integration that connects Snowflake to Microsoft Azure Blog Storage.
*/

-- initiates the creation of storage integration named azure_pacificretail_integration
CREATE OR REPLACE STORAGE INTEGRATION azure_pacificretail_integration
    -- specifies integration with external stages, which are locations outside of Snowflake where data can be stored
    TYPE = EXTERNAL_STAGE
    -- specifies the cloud storage provider being integrated
    STORAGE_PROVIDER = AZURE
    -- activates the integration immediately upon creation
    ENABLED = TRUE
    -- provides the Azure Tenant ID, which is a unique identifier for your Azure Active Directory instance
    AZURE_TENANT_ID = '926a1ef0-45ac-49df-9456-5b2d67f521b7'
    -- defines which storage container this integration can access
    STORAGE_ALLOWED_LOCATIONS = ('azure://pacificretail99.blob.core.windows.net/data-source/');