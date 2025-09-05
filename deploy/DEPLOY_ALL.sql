-- =====================================================
-- TR2000 ETL System - Master Deployment Script
-- Date: 2025-01-05
-- Purpose: Deploy all database objects in correct order
-- =====================================================

SET ECHO ON
SET SERVEROUTPUT ON
WHENEVER SQLERROR EXIT SQL.SQLCODE

PROMPT ========================================
PROMPT TR2000 ETL System Deployment
PROMPT ========================================
PROMPT
PROMPT This will deploy:
PROMPT - All tables (control, audit, staging, reference, PCS detail)
PROMPT - All sequences
PROMPT - All indexes  
PROMPT - All packages (with safe data conversion)
PROMPT - All procedures
PROMPT - All views
PROMPT - Control data
PROMPT
PROMPT Press Ctrl+C to abort, Enter to continue...
PAUSE

-- =====================================================
-- STEP 1: Prerequisites (if API_SERVICE doesn't exist)
-- =====================================================
-- PROMPT Creating API_SERVICE user and proxy...
-- @@00_prerequisites/01_create_api_service_user.sql
-- @@00_prerequisites/02_grant_proxy_access.sql

-- =====================================================
-- STEP 2: Deploy Tables
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Deploying Tables...
PROMPT ========================================

-- Note: Tables should be deployed via existing snapshot for now
-- Future: Split into logical groups
-- @@01_tables/01_control_tables.sql
-- @@01_tables/02_audit_tables.sql
-- @@01_tables/03_staging_tables.sql
-- @@01_tables/04_reference_tables.sql
-- @@01_tables/05_pcs_detail_tables.sql

PROMPT Tables already exist - skipping table deployment
PROMPT To recreate tables, run: @Database/Snapshots/snapshot_20250905_working_etl/01_tables.sql

-- =====================================================
-- STEP 3: Deploy Sequences
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Deploying Sequences...
PROMPT ========================================

-- Note: Sequences already exist
-- @@02_sequences/all_sequences.sql

PROMPT Sequences already exist - skipping sequence deployment
PROMPT To recreate sequences, run: @Database/Snapshots/snapshot_20250905_working_etl/02_sequences.sql

-- =====================================================
-- STEP 4: Deploy Indexes
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Deploying Indexes...
PROMPT ========================================

-- Note: Indexes already exist
-- @@03_indexes/all_indexes.sql

PROMPT Indexes already exist - skipping index deployment
PROMPT To recreate indexes, run: @Database/Snapshots/snapshot_20250905_working_etl/03_indexes.sql

-- =====================================================
-- STEP 5: Deploy Packages (MAIN DEPLOYMENT)
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Deploying Packages...
PROMPT ========================================

PROMPT Deploying PKG_ETL_VALIDATION (NEW - Safe conversions)...
@@04_packages/PKG_ETL_VALIDATION.sql

PROMPT Deploying PKG_API_CLIENT...
@@04_packages/PKG_API_CLIENT.sql

-- Future package deployments:
-- PROMPT Deploying PKG_DATE_UTILS...
-- @@04_packages/PKG_DATE_UTILS.sql

-- PROMPT Deploying PKG_ETL_LOGGING...
-- @@04_packages/PKG_ETL_LOGGING.sql

-- PROMPT Deploying PKG_ETL_PROCESSOR...
-- @@04_packages/PKG_ETL_PROCESSOR.sql

-- PROMPT Deploying PKG_PCS_DETAIL_PROCESSOR...
-- @@04_packages/PKG_PCS_DETAIL_PROCESSOR.sql

-- PROMPT Deploying PKG_MAIN_ETL_CONTROL...
-- @@04_packages/PKG_MAIN_ETL_CONTROL.sql

-- PROMPT Deploying PKG_ETL_TEST_UTILS...
-- @@04_packages/PKG_ETL_TEST_UTILS.sql

-- PROMPT Deploying PKG_INDEPENDENT_ETL_CONTROL...
-- @@04_packages/PKG_INDEPENDENT_ETL_CONTROL.sql

-- =====================================================
-- STEP 6: Deploy Procedures
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Deploying Procedures...
PROMPT ========================================

-- Note: Procedures already exist
-- @@05_procedures/FIX_EMBEDDED_NOTES_PARSER.sql
-- @@05_procedures/FIX_PCS_LIST_PARSER.sql
-- @@05_procedures/FIX_VDS_CATALOG_PARSER.sql
-- @@05_procedures/TEMP_FIX_VDS_PARSE.sql

PROMPT Procedures already exist - skipping procedure deployment
PROMPT To recreate procedures, run: @Database/Snapshots/snapshot_20250905_working_etl/08_procedures.sql

-- =====================================================
-- STEP 7: Deploy Views
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Deploying Views...
PROMPT ========================================

-- Note: Views already exist
-- @@06_views/all_views.sql

PROMPT Views already exist - skipping view deployment
PROMPT To recreate views, run: @Database/Snapshots/snapshot_20250905_working_etl/04_views.sql

-- =====================================================
-- STEP 8: Load Control Data
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Loading Control Data...
PROMPT ========================================

-- Note: Control data already loaded
-- @@07_control_data/load_control_data.sql

PROMPT Control data already loaded - skipping
PROMPT To reload control data, run: @Database/Snapshots/snapshot_20250905_working_etl/10_control_data.sql

-- =====================================================
-- STEP 9: Compile Invalid Objects
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Compiling Invalid Objects...
PROMPT ========================================

BEGIN
    FOR cur IN (SELECT object_name, object_type 
                FROM user_objects 
                WHERE status = 'INVALID'
                ORDER BY object_type, object_name)
    LOOP
        BEGIN
            IF cur.object_type = 'PACKAGE BODY' THEN
                EXECUTE IMMEDIATE 'ALTER PACKAGE ' || cur.object_name || ' COMPILE BODY';
            ELSIF cur.object_type = 'PACKAGE' THEN
                EXECUTE IMMEDIATE 'ALTER PACKAGE ' || cur.object_name || ' COMPILE';
            ELSIF cur.object_type = 'PROCEDURE' THEN
                EXECUTE IMMEDIATE 'ALTER PROCEDURE ' || cur.object_name || ' COMPILE';
            ELSIF cur.object_type = 'FUNCTION' THEN
                EXECUTE IMMEDIATE 'ALTER FUNCTION ' || cur.object_name || ' COMPILE';
            ELSIF cur.object_type = 'TRIGGER' THEN
                EXECUTE IMMEDIATE 'ALTER TRIGGER ' || cur.object_name || ' COMPILE';
            ELSIF cur.object_type = 'VIEW' THEN
                EXECUTE IMMEDIATE 'ALTER VIEW ' || cur.object_name || ' COMPILE';
            END IF;
            DBMS_OUTPUT.PUT_LINE('Compiled: ' || cur.object_type || ' ' || cur.object_name);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Failed to compile: ' || cur.object_type || ' ' || cur.object_name);
        END;
    END LOOP;
END;
/

-- =====================================================
-- STEP 10: Verify Deployment
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Deployment Verification
PROMPT ========================================

-- Check for invalid objects
SELECT object_type, object_name, status
FROM user_objects
WHERE status = 'INVALID'
ORDER BY object_type, object_name;

-- Show deployed packages
PROMPT
PROMPT Deployed Packages:
SELECT object_name, status, 
       TO_CHAR(last_ddl_time, 'YYYY-MM-DD HH24:MI:SS') as last_modified
FROM user_objects
WHERE object_type = 'PACKAGE'
AND object_name LIKE 'PKG_%'
ORDER BY object_name;

PROMPT
PROMPT ========================================
PROMPT Deployment Complete!
PROMPT ========================================
PROMPT
PROMPT Key improvements deployed:
PROMPT - PKG_ETL_VALIDATION: Safe data conversions with error logging
PROMPT - PKG_API_CLIENT: Clean deployment version
PROMPT
PROMPT Next steps:
PROMPT 1. Test ETL with: EXEC PKG_MAIN_ETL_CONTROL.run_main_etl;
PROMPT 2. Check conversion errors: SELECT * FROM ETL_ERROR_LOG WHERE error_type LIKE 'DATA_CONVERSION_%';
PROMPT 3. Get stats: SELECT PKG_ETL_VALIDATION.get_conversion_stats FROM dual;
PROMPT
PROMPT ========================================
/

EXIT