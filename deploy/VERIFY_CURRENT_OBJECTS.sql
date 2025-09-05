-- =====================================================
-- VERIFY CURRENT DATABASE OBJECTS
-- Compare current database with deployment scripts
-- =====================================================

SET PAGESIZE 1000
SET LINESIZE 200
COLUMN object_name FORMAT A40
COLUMN object_type FORMAT A20
COLUMN status FORMAT A10

PROMPT ========================================
PROMPT CURRENT DATABASE OBJECTS ANALYSIS
PROMPT ========================================

PROMPT
PROMPT 1. ALL TABLES IN DATABASE:
PROMPT ========================================
SELECT 'TABLE' as object_category, table_name as object_name, 'VALID' as status
FROM user_tables
ORDER BY table_name;

PROMPT
PROMPT 2. ALL PACKAGES IN DATABASE:
PROMPT ========================================
SELECT 'PACKAGE' as object_category, object_name, status
FROM user_objects
WHERE object_type = 'PACKAGE'
ORDER BY object_name;

PROMPT
PROMPT 3. ALL PROCEDURES IN DATABASE (Standalone):
PROMPT ========================================
SELECT 'PROCEDURE' as object_category, object_name, status
FROM user_objects
WHERE object_type = 'PROCEDURE'
ORDER BY object_name;

PROMPT
PROMPT 4. ALL VIEWS IN DATABASE:
PROMPT ========================================
SELECT 'VIEW' as object_category, view_name as object_name, 'VALID' as status
FROM user_views
ORDER BY view_name;

PROMPT
PROMPT 5. ALL SEQUENCES IN DATABASE:
PROMPT ========================================
SELECT 'SEQUENCE' as object_category, sequence_name as object_name, 'VALID' as status
FROM user_sequences
ORDER BY sequence_name;

PROMPT
PROMPT 6. ALL INDEXES IN DATABASE (Non-system):
PROMPT ========================================
SELECT 'INDEX' as object_category, index_name as object_name, status
FROM user_indexes
WHERE index_name NOT LIKE 'SYS_%'
  AND index_name NOT LIKE 'BIN$%'
ORDER BY index_name;

PROMPT
PROMPT 7. OBJECT COUNT SUMMARY:
PROMPT ========================================
SELECT object_type, COUNT(*) as count, 
       SUM(CASE WHEN status = 'VALID' THEN 1 ELSE 0 END) as valid_count,
       SUM(CASE WHEN status = 'INVALID' THEN 1 ELSE 0 END) as invalid_count
FROM user_objects
WHERE object_type IN ('TABLE', 'PACKAGE', 'PACKAGE BODY', 'PROCEDURE', 'VIEW', 'SEQUENCE', 'INDEX')
GROUP BY object_type
ORDER BY object_type;

PROMPT
PROMPT 8. INVALID OBJECTS (IF ANY):
PROMPT ========================================
SELECT object_type, object_name, status
FROM user_objects
WHERE status = 'INVALID'
ORDER BY object_type, object_name;

PROMPT
PROMPT 9. CONTROL DATA VERIFICATION:
PROMPT ========================================
SELECT 'ETL_FILTER' as table_name, COUNT(*) as row_count FROM ETL_FILTER
UNION ALL
SELECT 'CONTROL_SETTINGS', COUNT(*) FROM CONTROL_SETTINGS
UNION ALL
SELECT 'CONTROL_ENDPOINTS', COUNT(*) FROM CONTROL_ENDPOINTS
UNION ALL
SELECT 'RAW_JSON', COUNT(*) FROM RAW_JSON
ORDER BY table_name;

PROMPT
PROMPT 10. TABLE CATEGORIES (By Prefix):
PROMPT ========================================
SELECT 
    CASE 
        WHEN table_name LIKE 'STG_%' THEN 'STAGING'
        WHEN table_name IN ('ETL_FILTER', 'CONTROL_SETTINGS', 'CONTROL_ENDPOINTS') THEN 'CONTROL'
        WHEN table_name IN ('ETL_ERROR_LOG', 'ETL_RUN_LOG', 'ETL_STATISTICS', 'RAW_JSON') THEN 'AUDIT'
        WHEN table_name LIKE '%_REFERENCES' THEN 'REFERENCE'
        WHEN table_name LIKE 'PCS_%' AND table_name NOT LIKE 'PCS_REFERENCES' THEN 'PCS_DETAIL'
        WHEN table_name IN ('PCS_LIST', 'VDS_LIST') THEN 'CATALOG'
        ELSE 'OTHER'
    END as category,
    COUNT(*) as table_count
FROM user_tables
GROUP BY 
    CASE 
        WHEN table_name LIKE 'STG_%' THEN 'STAGING'
        WHEN table_name IN ('ETL_FILTER', 'CONTROL_SETTINGS', 'CONTROL_ENDPOINTS') THEN 'CONTROL'
        WHEN table_name IN ('ETL_ERROR_LOG', 'ETL_RUN_LOG', 'ETL_STATISTICS', 'RAW_JSON') THEN 'AUDIT'
        WHEN table_name LIKE '%_REFERENCES' THEN 'REFERENCE'
        WHEN table_name LIKE 'PCS_%' AND table_name NOT LIKE 'PCS_REFERENCES' THEN 'PCS_DETAIL'
        WHEN table_name IN ('PCS_LIST', 'VDS_LIST') THEN 'CATALOG'
        ELSE 'OTHER'
    END
ORDER BY category;

PROMPT
PROMPT ========================================
PROMPT ANALYSIS COMPLETE
PROMPT ========================================