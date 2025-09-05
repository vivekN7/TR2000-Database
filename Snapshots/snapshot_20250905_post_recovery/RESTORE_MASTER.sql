-- =====================================================
-- TR2000_STAGING Complete Restore Script
-- Date: 2025-09-05 (Post PKG_ETL_PROCESSOR Recovery)
-- =====================================================
-- This backup was created after successfully recovering from 
-- the NULL stubs disaster where PKG_ETL_PROCESSOR was destroyed.
-- 
-- Key fixes included:
-- 1. PKG_ETL_PROCESSOR fully restored with all 12 procedures
-- 2. PCS_LIST parsing fixed ($.getPCS[*] instead of $.getPlantPcsList[*])
-- 3. run_full_etl renamed to run_main_etl
-- =====================================================

-- Connect as TR2000_STAGING user before running this script
-- sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1

WHENEVER SQLERROR EXIT SQL.SQLCODE

PROMPT ====================================
PROMPT Starting Complete Schema Restore
PROMPT ====================================

-- Drop existing objects (be careful!)
PROMPT Dropping existing objects...
BEGIN
    -- Drop all tables with CASCADE CONSTRAINTS
    FOR rec IN (SELECT table_name FROM user_tables WHERE table_name NOT LIKE 'BIN$%') LOOP
        EXECUTE IMMEDIATE 'DROP TABLE ' || rec.table_name || ' CASCADE CONSTRAINTS';
    END LOOP;
    
    -- Drop all views
    FOR rec IN (SELECT view_name FROM user_views) LOOP
        EXECUTE IMMEDIATE 'DROP VIEW ' || rec.view_name;
    END LOOP;
    
    -- Drop all sequences
    FOR rec IN (SELECT sequence_name FROM user_sequences) LOOP
        EXECUTE IMMEDIATE 'DROP SEQUENCE ' || rec.sequence_name;
    END LOOP;
    
    -- Drop all packages
    FOR rec IN (SELECT object_name FROM user_objects WHERE object_type = 'PACKAGE') LOOP
        EXECUTE IMMEDIATE 'DROP PACKAGE ' || rec.object_name;
    END LOOP;
    
    -- Drop all procedures
    FOR rec IN (SELECT object_name FROM user_procedures 
                WHERE object_type = 'PROCEDURE' AND procedure_name IS NULL) LOOP
        EXECUTE IMMEDIATE 'DROP PROCEDURE ' || rec.object_name;
    END LOOP;
    
    -- Drop all triggers
    FOR rec IN (SELECT trigger_name FROM user_triggers) LOOP
        EXECUTE IMMEDIATE 'DROP TRIGGER ' || rec.trigger_name;
    END LOOP;
END;
/

-- Create tables
PROMPT Creating tables...
@@all_tables.sql

-- Create sequences
PROMPT Creating sequences...
@@all_sequences.sql

-- Create views
PROMPT Creating views...
@@all_views.sql

-- Create indexes
PROMPT Creating indexes...
@@all_indexes.sql

-- Create packages
PROMPT Creating packages...
@@all_packages.sql

-- Create procedures
PROMPT Creating procedures...
@@all_procedures.sql

-- Create triggers
PROMPT Creating triggers...
@@all_triggers.sql

-- Load control data
PROMPT Loading control data...
@@control_data.sql

-- Verify restoration
PROMPT ====================================
PROMPT Verifying Restoration
PROMPT ====================================

-- Check objects count
SELECT object_type, COUNT(*) as count
FROM user_objects
WHERE object_type IN ('TABLE', 'VIEW', 'PACKAGE', 'PACKAGE BODY', 'SEQUENCE', 'TRIGGER', 'PROCEDURE')
AND object_name NOT LIKE 'BIN$%'
GROUP BY object_type
ORDER BY object_type;

-- Check package status
SELECT object_name, object_type, status
FROM user_objects
WHERE object_type IN ('PACKAGE', 'PACKAGE BODY')
ORDER BY object_name, object_type;

-- Check control data
SELECT 'ETL_FILTER' as table_name, COUNT(*) as records FROM ETL_FILTER
UNION ALL
SELECT 'CONTROL_SETTINGS', COUNT(*) FROM CONTROL_SETTINGS
UNION ALL
SELECT 'CONTROL_ENDPOINTS', COUNT(*) FROM CONTROL_ENDPOINTS;

PROMPT ====================================
PROMPT Restoration Complete!
PROMPT ====================================
PROMPT 
PROMPT Key Commands:
PROMPT - Run ETL: EXEC PKG_MAIN_ETL_CONTROL.run_main_etl;
PROMPT - Reset for testing: EXEC PKG_ETL_TEST_UTILS.reset_for_testing;
PROMPT - Run VDS catalog ETL: EXEC PKG_INDEPENDENT_ETL_CONTROL.run_vds_catalog_etl;
PROMPT 
PROMPT Note: PKG_ETL_PROCESSOR has been fully restored with all procedures
PROMPT ====================================