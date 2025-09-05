-- ========================================================================
-- TR2000_STAGING Complete Database Restore Script
-- Date: 2025-01-05
-- Status: Working ETL Milestone (VDS and PCS JSON paths fixed)
-- ========================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET ECHO ON
SET TIMING ON

PROMPT ========================================================================;
PROMPT TR2000_STAGING Database Restore - Working ETL Milestone;
PROMPT ========================================================================;
PROMPT;
PROMPT This will restore the complete TR2000_STAGING schema including:;
PROMPT - 41 Tables (staging, core, control, audit);
PROMPT - 28 Sequences;
PROMPT - 82 Indexes;
PROMPT - 9 Views;
PROMPT - 1 Trigger;
PROMPT - 9 Package Specifications;
PROMPT - 9 Package Bodies (with VDS and PCS JSON path fixes);
PROMPT - 4 Standalone Procedures;
PROMPT - Control data (ETL_FILTER, CONTROL_SETTINGS, CONTROL_ENDPOINTS);
PROMPT;
PROMPT CRITICAL: This contains the WORKING fixes for:;
PROMPT - VDS catalog JSON path: $.getVDS[*];
PROMPT - PCS list JSON path: $.getPCS[*];
PROMPT;

PAUSE Press Enter to continue or Ctrl+C to abort...

-- ========================================================================
-- Step 1: Create Tables
-- ========================================================================
PROMPT;
PROMPT Creating tables...;
@@01_tables.sql

-- ========================================================================
-- Step 2: Create Sequences  
-- ========================================================================
PROMPT;
PROMPT Creating sequences...;
@@02_sequences.sql

-- ========================================================================
-- Step 3: Create Indexes
-- ========================================================================
PROMPT;
PROMPT Creating indexes...;
@@03_indexes.sql

-- ========================================================================
-- Step 4: Create Views
-- ========================================================================
PROMPT;
PROMPT Creating views...;
@@04_views.sql

-- ========================================================================
-- Step 5: Create Triggers
-- ========================================================================
PROMPT;
PROMPT Creating triggers...;
@@05_triggers.sql

-- ========================================================================
-- Step 6: Create Package Specifications
-- ========================================================================
PROMPT;
PROMPT Creating package specifications...;
@@06_package_specs.sql

-- ========================================================================
-- Step 7: Create Package Bodies (Contains JSON path fixes!)
-- ========================================================================
PROMPT;
PROMPT Creating package bodies with working JSON paths...;
@@07_package_bodies.sql

-- ========================================================================
-- Step 8: Create Standalone Procedures
-- ========================================================================
PROMPT;
PROMPT Creating standalone procedures...;
@@08_procedures.sql

-- ========================================================================
-- Step 9: Load Control Data
-- ========================================================================
PROMPT;
PROMPT Loading control data...;
@@10_control_data.sql

-- ========================================================================
-- Step 10: Compile Invalid Objects
-- ========================================================================
PROMPT;
PROMPT Recompiling any invalid objects...;
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

-- ========================================================================
-- Step 11: Verify Restoration
-- ========================================================================
PROMPT;
PROMPT ========================================================================;
PROMPT Verifying restoration...;
PROMPT ========================================================================;

-- Check object counts
SELECT 'Tables' as object_type, COUNT(*) as count 
FROM user_tables WHERE table_name NOT LIKE 'BIN$%'
UNION ALL
SELECT 'Packages', COUNT(*) FROM user_objects WHERE object_type = 'PACKAGE'
UNION ALL
SELECT 'Procedures', COUNT(*) FROM user_procedures WHERE object_type = 'PROCEDURE' AND procedure_name IS NULL
UNION ALL
SELECT 'Views', COUNT(*) FROM user_views
UNION ALL
SELECT 'Sequences', COUNT(*) FROM user_sequences
UNION ALL
SELECT 'Triggers', COUNT(*) FROM user_triggers
UNION ALL
SELECT 'Indexes', COUNT(*) FROM user_indexes WHERE index_type != 'LOB'
ORDER BY 1;

-- Check for invalid objects
SELECT object_type, object_name, status
FROM user_objects
WHERE status = 'INVALID'
ORDER BY object_type, object_name;

-- Check control data
SELECT 'ETL_FILTER' as table_name, COUNT(*) as row_count FROM ETL_FILTER
UNION ALL
SELECT 'CONTROL_SETTINGS', COUNT(*) FROM CONTROL_SETTINGS
UNION ALL
SELECT 'CONTROL_ENDPOINTS', COUNT(*) FROM CONTROL_ENDPOINTS;

PROMPT;
PROMPT ========================================================================;
PROMPT Restoration Complete!;
PROMPT ========================================================================;
PROMPT;
PROMPT Next steps:;
PROMPT 1. Run main ETL: EXEC PKG_MAIN_ETL_CONTROL.run_main_etl;;
PROMPT 2. Run VDS catalog ETL: EXEC PKG_INDEPENDENT_ETL_CONTROL.run_vds_catalog_etl;;
PROMPT;
PROMPT Key fixes included:;
PROMPT - PKG_ETL_PROCESSOR.parse_and_load_vds_catalog uses $.getVDS[*];
PROMPT - PKG_ETL_PROCESSOR.parse_and_load_pcs_list uses $.getPCS[*];
PROMPT;
PROMPT ========================================================================;