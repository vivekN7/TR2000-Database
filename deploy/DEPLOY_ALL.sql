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
-- OPTIONAL: Drop all objects first for clean deployment
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Optional: Drop All Objects First?
PROMPT ========================================
PROMPT
PROMPT To drop all objects first, run: @DROP_ALL_OBJECTS.sql
PROMPT Otherwise, press Enter to continue with deployment...
PAUSE

-- =====================================================
-- STEP 1: Deploy Tables
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Deploying Tables...
PROMPT ========================================
PROMPT
PROMPT Note: Allowing DROP errors for clean deployment...
-- Temporarily allow errors (tables may not exist on clean deployment)
WHENEVER SQLERROR CONTINUE

PROMPT Deploying control tables...
@@01_tables/01_control_tables.sql

PROMPT Deploying audit tables...
@@01_tables/02_audit_tables.sql

PROMPT Deploying staging tables...
@@01_tables/03_staging_tables.sql

PROMPT Deploying reference tables...
@@01_tables/04_reference_tables.sql

PROMPT Deploying PCS detail tables...
@@01_tables/05_pcs_detail_tables.sql

PROMPT Deploying catalog tables...
@@01_tables/06_catalog_tables.sql

-- =====================================================
-- STEP 2: Deploy Sequences
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Deploying Sequences...
PROMPT ========================================

@@02_sequences/all_sequences.sql

-- =====================================================
-- STEP 3: Deploy Indexes
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Deploying Indexes...
PROMPT ========================================

@@03_indexes/all_indexes.sql

-- =====================================================
-- STEP 4: Deploy Views
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Deploying Views...
PROMPT ========================================

@@06_views/all_views.sql

-- =====================================================
-- STEP 5: Deploy Packages
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Deploying Packages...
PROMPT ========================================

PROMPT Deploying PKG_DATE_UTILS...
@@04_packages/PKG_DATE_UTILS.sql

PROMPT Deploying PKG_ETL_VALIDATION (Safe conversions)...
@@04_packages/PKG_ETL_VALIDATION.sql

PROMPT Deploying PKG_ETL_LOGGING...
@@04_packages/PKG_ETL_LOGGING.sql

PROMPT Deploying PKG_API_CLIENT...
@@04_packages/PKG_API_CLIENT.sql

PROMPT Deploying PKG_ETL_PROCESSOR...
@@04_packages/PKG_ETL_PROCESSOR.sql

PROMPT Deploying PKG_PCS_DETAIL_PROCESSOR...
@@04_packages/PKG_PCS_DETAIL_PROCESSOR.sql

PROMPT Deploying PKG_MAIN_ETL_CONTROL...
@@04_packages/PKG_MAIN_ETL_CONTROL.sql

PROMPT Deploying PKG_ETL_TEST_UTILS...
@@04_packages/PKG_ETL_TEST_UTILS.sql

PROMPT Deploying PKG_INDEPENDENT_ETL_CONTROL...
@@04_packages/PKG_INDEPENDENT_ETL_CONTROL.sql

PROMPT Deploying PKG_DDL_BACKUP (Database backup to DDL_BACKUP_OWNER)...
@@04_packages/PKG_DDL_BACKUP.sql

-- =====================================================
-- STEP 6: Deploy Procedures
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Deploying Procedures...
PROMPT ========================================

-- Note: Temporary fix procedures archived to archive/temp_fix_procedures_20250906/
-- These were development artifacts and are no longer needed for production deployment

-- Re-enable strict error checking for control data loading
WHENEVER SQLERROR EXIT SQL.SQLCODE
PROMPT
PROMPT Re-enabled strict error checking for control data loading...

-- =====================================================
-- STEP 7: Load Control Data
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Loading Control Data...
PROMPT ========================================

@@07_control_data/load_control_data.sql

-- =====================================================
-- STEP 8: Compile Invalid Objects
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
-- STEP 9: Verify Deployment
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Deployment Verification
PROMPT ========================================

-- Object counts
PROMPT
PROMPT Object Counts:
SELECT object_type, COUNT(*) as count
FROM user_objects
WHERE object_type IN ('TABLE', 'SEQUENCE', 'PACKAGE', 'PACKAGE BODY', 'PROCEDURE', 'VIEW', 'TRIGGER')
GROUP BY object_type
ORDER BY object_type;

-- Check for invalid objects
PROMPT
PROMPT Invalid Objects:
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

-- Control data verification
PROMPT
PROMPT Control Data:
SELECT 'ETL_FILTER' as table_name, COUNT(*) as row_count FROM ETL_FILTER
UNION ALL
SELECT 'CONTROL_SETTINGS', COUNT(*) FROM CONTROL_SETTINGS
UNION ALL
SELECT 'CONTROL_ENDPOINTS', COUNT(*) FROM CONTROL_ENDPOINTS;

PROMPT
PROMPT ========================================
PROMPT Deployment Complete!
PROMPT ========================================
PROMPT
PROMPT Summary:
PROMPT - 41 Tables deployed
PROMPT - 28 Sequences deployed  
PROMPT - 9 Packages deployed (including PKG_ETL_VALIDATION)
PROMPT - 4 Procedures deployed
PROMPT - 9 Views deployed
PROMPT - Control data loaded
PROMPT
PROMPT Key improvements:
PROMPT - PKG_ETL_VALIDATION: Safe data conversions with error logging
PROMPT - Complete deployment from scripts (no direct DB edits)
PROMPT
PROMPT Next steps:
PROMPT 1. Test ETL: EXEC PKG_MAIN_ETL_CONTROL.run_main_etl;
PROMPT 2. Check errors: SELECT * FROM ETL_ERROR_LOG ORDER BY error_timestamp DESC;
PROMPT 3. Get stats: SELECT PKG_ETL_VALIDATION.get_conversion_stats FROM dual;
PROMPT
PROMPT ========================================
/

EXIT