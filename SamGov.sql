-- Step 2.2: Create the Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION AZURE_BLOB_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'AZURE'
    ENABLED = TRUE
    AZURE_TENANT_ID = '00000000-0000-0000-0000-000000000000'
    STORAGE_ALLOWED_LOCATIONS = ('azure://highergovdata.blob.core.windows.net/landing/');

-- Step 2.3: Allow Your Role to Use the Integration
GRANT USAGE ON INTEGRATION AZURE_BLOB_INTEGRATION TO ROLE AccountAdmin;

-- Step 2.4: Get the Consent URL and Service Principal
DESC STORAGE INTEGRATION AZURE_BLOB_INTEGRATION;

-- Step 3.1: Create the Stage
CREATE OR REPLACE STAGE HIGHERGOV_DB.STAGING.AZURE_BLOB_STAGE
    URL = 'azure://highergovdata.blob.core.windows.net/landing/Bronze/'
    STORAGE_INTEGRATION = AZURE_BLOB_INTEGRATION
    FILE_FORMAT = (TYPE = 'PARQUET');

-- Step 3.2: Allow Your Role to Use the Stage
GRANT USAGE ON STAGE HIGHERGOV_DB.STAGING.AZURE_BLOB_STAGE TO ROLE AccountAdmin;

-- Step 3.3: Check the Stage
LIST @HIGHERGOV_DB.STAGING.AZURE_BLOB_STAGE;

-- Step 4.1: Create the Staging Table
CREATE OR REPLACE TABLE HIGHERGOV_DB.STAGING.GRANT_HISTORY (
    "Award ID" VARCHAR,
    "Recipient Name" VARCHAR,
    "Recipient UEI" VARCHAR,
    "Recipient CAGE" VARCHAR,
    "Recipient Path" VARCHAR,
    "Parent Name" VARCHAR,
    "Parent UEI" VARCHAR,
    "Latest Transaction Key" VARCHAR,
    "Last Modified Date" DATE,
    "Latest Action Date" DATE,
    "Fiscal Year" NUMBER(4,0),
    "Start Date" DATE,
    "End Date" DATE,
    "Total Obligated Amount" DECIMAL(18,2),
    "Federal Obligation" DECIMAL(18,2),
    "Non-Federal Amount" DECIMAL(18,2),
    "Solicitation ID" VARCHAR,
    "ZIP" VARCHAR,
    "County" VARCHAR,
    "City" VARCHAR,
    "State Code" VARCHAR,
    "State Name" VARCHAR,
    "Country" VARCHAR,
    "Description" VARCHAR,
    "CFDA Number" VARCHAR,
    "Program Title" VARCHAR,
    "Popular Program Title" VARCHAR,
    "Awarding Agency" VARCHAR,
    "Awarding Agency Abbr." VARCHAR,
    "Funding Agency" VARCHAR,
    "Funding Agency Abbr." VARCHAR,
    "Capture Time" TIMESTAMP_NTZ(9),
    "Sector" VARCHAR,
    "Filename" VARCHAR,
    "Load_Timestamp" TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    "Processed" BOOLEAN DEFAULT FALSE
);


-- Step 4.2: Allow Your Role to Use the Table
GRANT SELECT, INSERT, UPDATE ON TABLE HIGHERGOV_DB.STAGING.GRANT_HISTORY TO ROLE AccountAdmin;

-- Step 5.1: Create a Notification Integration
CREATE OR REPLACE NOTIFICATION INTEGRATION GRANT_NOTIFICATION
    ENABLED = TRUE
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
    AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://highergovdata.queue.core.windows.net/snowpipequeue'
    AZURE_TENANT_ID = '00000000-0000-0000-0000-000000000000';

-- Step 5.3: Create the Snowpipe
CREATE OR REPLACE PIPE HIGHERGOV_DB.STAGING.GRANT_PIPE
    AUTO_INGEST = TRUE
    INTEGRATION = 'AZURE_NOTIFICATION_INT'
AS
COPY INTO HIGHERGOV_DB.STAGING.GRANT_HISTORY (
    "Award ID", "Recipient Name", "Recipient UEI", "Recipient CAGE", "Recipient Path",
    "Parent Name", "Parent UEI", "Latest Transaction Key",
    "Last Modified Date", "Latest Action Date", "Fiscal Year", "Start Date", "End Date",
    "Total Obligated Amount", "Federal Obligation", "Non-Federal Amount", "Solicitation ID",
    "ZIP", "County", "City", "State Code", "State Name", "Country", "Description",
    "CFDA Number", "Program Title", "Popular Program Title", "Awarding Agency",
    "Awarding Agency Abbr.", "Funding Agency", "Funding Agency Abbr.", "Capture Time",
    "Sector", "Filename", "Load_Timestamp", "Processed"
)
FROM (
    SELECT 
        $1:"Award ID"::VARCHAR,
        $1:"Recipient Name"::VARCHAR,
        $1:"Recipient UEI"::VARCHAR,
        $1:"Recipient CAGE"::VARCHAR,
        $1:"Recipient Path"::VARCHAR,
        $1:"Parent Name"::VARCHAR,
        $1:"Parent UEI"::VARCHAR,
        $1:"Latest Transaction Key"::VARCHAR,
        $1:"Last Modified Date"::DATE,
        $1:"Latest Action Date"::DATE,
        $1:"Fiscal Year"::NUMBER(4,0),
        $1:"Start Date"::DATE,
        $1:"End Date"::DATE,
        $1:"Total Obligated Amount"::DECIMAL(18,2),
        $1:"Federal Obligation"::DECIMAL(18,2),
        $1:"Non-Federal Amount"::DECIMAL(18,2),
        $1:"Solicitation ID"::VARCHAR,
        $1:"ZIP"::VARCHAR,
        $1:"County"::VARCHAR,
        $1:"City"::VARCHAR,
        $1:"State Code"::VARCHAR,
        $1:"State Name"::VARCHAR,
        $1:"Country"::VARCHAR,
        $1:"Description"::VARCHAR,
        $1:"CFDA Number"::VARCHAR,
        $1:"Program Title"::VARCHAR,
        $1:"Popular Program Title"::VARCHAR,
        $1:"Awarding Agency"::VARCHAR,
        $1:"Awarding Agency Abbr."::VARCHAR,
        $1:"Funding Agency"::VARCHAR,
        $1:"Funding Agency Abbr."::VARCHAR,
        $1:"Capture Time"::TIMESTAMP_NTZ(9),
        HIGHERGOV_DB.STAGING.EXTRACT_SECTOR(METADATA$FILENAME) AS "Sector",
        METADATA$FILENAME,
        CURRENT_TIMESTAMP(),
        FALSE
    FROM @HIGHERGOV_DB.STAGING.AZURE_BLOB_STAGE
)
FILE_FORMAT = (TYPE = 'PARQUET')
PATTERN = '.*[.]parquet'
ON_ERROR = 'CONTINUE';


-- Step 5.4: Allow Your Role to Use the Pipe
GRANT OWNERSHIP ON PIPE HIGHERGOV_DB.STAGING.GRANT_PIPE TO ROLE AccountAdmin;


CREATE OR REPLACE FUNCTION HIGHERGOV_DB.STAGING.EXTRACT_SECTOR(filename VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    try {
        // Split the filename by '/' and get the folder name (second-to-last element)
        var parts = FILENAME.split('/');
        if (parts.length >= 2) {
            // The folder name is the part before the file name
            var folder = parts[parts.length - 2];
            // Ensure the folder name is lowercase and contains only valid characters
            return folder.toLowerCase().replace(/[^a-z0-9_]/g, '_');
        }
        return 'unknown'; // Fallback if the path doesn't contain a folder
    } catch (err) {
        return 'unknown'; // Fallback in case of errors
    }
$$;


-- Step 7.1: Create a Stored Procedure for Distribution
CREATE OR REPLACE PROCEDURE HIGHERGOV_DB.STAGING.DISTRIBUTE_TO_SECTOR_TABLES()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
try {
    // Check for unprocessed records with a valid Sector and .parquet extension
    var checkRecords = snowflake.execute({
        sqlText: `
            SELECT COUNT(*) 
            FROM HIGHERGOV_DB.STAGING.GRANT_HISTORY 
            WHERE "Processed" = FALSE 
              AND "Sector" IS NOT NULL 
              AND "Sector" != ''
              AND "Filename" LIKE '%.parquet'
        `
    });
    checkRecords.next();
    var recordCount = checkRecords.getColumnValue(1);
    
    if (recordCount === 0) {
        return "No unprocessed records to distribute.";
    }

    // Get distinct sectors
    var sectorsQuery = snowflake.execute({
        sqlText: `
            SELECT DISTINCT "Sector" 
            FROM HIGHERGOV_DB.STAGING.GRANT_HISTORY 
            WHERE "Processed" = FALSE 
              AND "Sector" IS NOT NULL 
              AND "Sector" != ''
              AND "Filename" LIKE '%.parquet'
        `
    });

    var sectorsProcessed = 0;
    while (sectorsQuery.next()) {
        var sector = sectorsQuery.getColumnValue(1);
        var sectorTableName = sector.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
        var fullTableName = 'HIGHERGOV_DB.PRODUCTION.' + sectorTableName;

        // Create the sector table if it doesn't exist
        snowflake.execute({
            sqlText: `
                CREATE TABLE IF NOT EXISTS ${fullTableName} (
                    "Award ID" VARCHAR,
                    "Recipient Name" VARCHAR,
                    "Recipient UEI" VARCHAR,
                    "Recipient CAGE" VARCHAR,
                    "Recipient Path" VARCHAR,
                    "Parent Name" VARCHAR,
                    "Parent UEI" VARCHAR,
                    "Latest Transaction Key" VARCHAR,
                    "Last Modified Date" DATE,
                    "Latest Action Date" DATE,
                    "Fiscal Year" NUMBER(4,0),
                    "Start Date" DATE,
                    "End Date" DATE,
                    "Total Obligated Amount" DECIMAL(18,2),
                    "Federal Obligation" DECIMAL(18,2),
                    "Non-Federal Amount" DECIMAL(18,2),
                    "Solicitation ID" VARCHAR,
                    "ZIP" VARCHAR,
                    "County" VARCHAR,
                    "City" VARCHAR,
                    "State Code" VARCHAR,
                    "State Name" VARCHAR,
                    "Country" VARCHAR,
                    "Description" VARCHAR,
                    "CFDA Number" VARCHAR,
                    "Program Title" VARCHAR,
                    "Popular Program Title" VARCHAR,
                    "Awarding Agency" VARCHAR,
                    "Awarding Agency Abbr." VARCHAR,
                    "Funding Agency" VARCHAR,
                    "Funding Agency Abbr." VARCHAR,
                    "Capture Time" TIMESTAMP_NTZ(9),
                    "Sector" VARCHAR,
                    "Filename" VARCHAR,
                    "Load_Timestamp" TIMESTAMP_NTZ(9),
                    "Processed" BOOLEAN DEFAULT FALSE
                )
            `
        });

        // Insert records into the sector table
        snowflake.execute({
            sqlText: `
            INSERT INTO ${fullTableName} (
                "Award ID", "Recipient Name", "Recipient UEI", "Recipient CAGE", "Recipient Path", 
                "Parent Name", "Parent UEI", "Latest Transaction Key", 
                "Last Modified Date", "Latest Action Date", "Fiscal Year", "Start Date", "End Date", 
                "Total Obligated Amount", "Federal Obligation", "Non-Federal Amount", "Solicitation ID", 
                "ZIP", "County", "City", "State Code", "State Name", "Country", "Description", 
                "CFDA Number", "Program Title", "Popular Program Title", "Awarding Agency", 
                "Awarding Agency Abbr.", "Funding Agency", "Funding Agency Abbr.", "Capture Time", 
                "Sector", "Filename", "Load_Timestamp", "Processed"
            )
            SELECT 
                "Award ID", "Recipient Name", "Recipient UEI", "Recipient CAGE", "Recipient Path", 
                "Parent Name", "Parent UEI", "Latest Transaction Key", 
                "Last Modified Date", "Latest Action Date", "Fiscal Year", "Start Date", "End Date", 
                "Total Obligated Amount", "Federal Obligation", "Non-Federal Amount", "Solicitation ID", 
                "ZIP", "County", "City", "State Code", "State Name", "Country", "Description", 
                "CFDA Number", "Program Title", "Popular Program Title", "Awarding Agency", 
                "Awarding Agency Abbr.", "Funding Agency", "Funding Agency Abbr.", "Capture Time", 
                "Sector", "Filename", "Load_Timestamp", "Processed"
            FROM HIGHERGOV_DB.STAGING.GRANT_HISTORY
            WHERE "Processed" = FALSE 
              AND "Sector" = ?
              AND "Filename" LIKE '%.parquet'
            `,
            binds: [sector]
        });

        sectorsProcessed++;
    }

    // Mark records as processed
    snowflake.execute({
        sqlText: `
            UPDATE HIGHERGOV_DB.STAGING.GRANT_HISTORY 
            SET "Processed" = TRUE 
            WHERE "Processed" = FALSE 
              AND "Sector" IS NOT NULL 
              AND "Sector" != ''
              AND "Filename" LIKE '%.parquet'
        `
    });

    return "Successfully distributed records for " + sectorsProcessed + " sectors (" + recordCount + " total records).";
}
catch (err) {
    return "Error: " + err.message;
}
$$;

-- Step 7.2: Create a Task to Run the Distribution Daily
CREATE OR REPLACE TASK HIGHERGOV_DB.STAGING.DISTRIBUTE_SECTOR_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 0 * * * UTC'
    AS
    CALL HIGHERGOV_DB.STAGING.DISTRIBUTE_TO_SECTOR_TABLES();

-- Step 7.3: Start the Task
ALTER TASK HIGHERGOV_DB.STAGING.DISTRIBUTE_SECTOR_TASK RESUME;

-- Step 8.3: Verify Snowpipe Loaded the Data
SELECT SYSTEM$PIPE_STATUS('HIGHERGOV_DB.STAGING.GRANT_PIPE') AS pipe_status;

-- Step 8.3: Check the Staging Table
SELECT * FROM HIGHERGOV_DB.STAGING.GRANT_HISTORY WHERE Sector = 'Education';

-- Step 8.4: Run the Distribution Manually
EXECUTE TASK HIGHERGOV_DB.STAGING.DISTRIBUTE_SECTOR_TASK;

-- Step 8.4: Check the Production Table
SELECT * FROM HIGHERGOV_DB.PRODUCTION.education;