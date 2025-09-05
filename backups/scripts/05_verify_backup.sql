-- =====================================================
-- Backup Verification Script
-- Date: 2025-01-05
-- Purpose: Verify all backup methods succeeded
-- =====================================================

SET SERVEROUTPUT ON
SET LINESIZE 200

PROMPT ========================================
PROMPT Backup Verification Report
PROMPT ========================================
PROMPT

-- Check what we have in the database
PROMPT Current Database Object Counts:
PROMPT -------------------------------
SELECT 'Tables' as object_type, COUNT(*) as count 
FROM user_tables WHERE table_name NOT LIKE 'BIN$%'
UNION ALL
SELECT 'Packages', COUNT(*) FROM user_objects WHERE object_type = 'PACKAGE'
UNION ALL
SELECT 'Package Bodies', COUNT(*) FROM user_objects WHERE object_type = 'PACKAGE BODY'
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

PROMPT
PROMPT Invalid Objects:
PROMPT ----------------
SELECT object_type, object_name, status
FROM user_objects
WHERE status = 'INVALID'
ORDER BY object_type, object_name;

PROMPT
PROMPT Control Data Counts:
PROMPT --------------------
SELECT 'ETL_FILTER' as table_name, COUNT(*) as row_count FROM ETL_FILTER
UNION ALL
SELECT 'CONTROL_SETTINGS', COUNT(*) FROM CONTROL_SETTINGS
UNION ALL
SELECT 'CONTROL_ENDPOINTS', COUNT(*) FROM CONTROL_ENDPOINTS
UNION ALL
SELECT 'RAW_JSON', COUNT(*) FROM RAW_JSON
UNION ALL
SELECT 'ETL_RUN_LOG', COUNT(*) FROM ETL_RUN_LOG
UNION ALL
SELECT 'ETL_ERROR_LOG', COUNT(*) FROM ETL_ERROR_LOG;

PROMPT
PROMPT Key Packages Status:
PROMPT --------------------
SELECT object_name, object_type, status, 
       TO_CHAR(last_ddl_time, 'YYYY-MM-DD HH24:MI:SS') as last_modified
FROM user_objects
WHERE object_name IN (
    'PKG_API_CLIENT',
    'PKG_ETL_PROCESSOR', 
    'PKG_MAIN_ETL_CONTROL',
    'PKG_PCS_DETAIL_PROCESSOR',
    'PKG_ETL_LOGGING',
    'PKG_DATE_UTILS'
)
ORDER BY object_name, object_type;

PROMPT
PROMPT ========================================
PROMPT Backup Summary:
PROMPT ========================================
PROMPT
PROMPT 1. Data Pump Backup: Database/backups/datapump/TR2000_FULL_20250105.dmp
PROMPT 2. SQL Extracts: Database/backups/20250105_pre_refactor/
PROMPT    - all_packages.sql
PROMPT    - all_procedures.sql  
PROMPT    - all_tables.sql
PROMPT    - control_data_export.sql
PROMPT 3. Existing Snapshot: Database/Snapshots/snapshot_20250905_working_etl/
PROMPT
PROMPT ========================================

EXIT;