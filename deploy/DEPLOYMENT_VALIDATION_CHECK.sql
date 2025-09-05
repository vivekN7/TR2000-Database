-- =====================================================
-- DEPLOYMENT VALIDATION CHECK
-- Run AFTER deployment to verify all objects match expectations
-- =====================================================

SET PAGESIZE 1000
SET LINESIZE 200
COLUMN object_name FORMAT A40
COLUMN expected FORMAT A10
COLUMN actual FORMAT A10
COLUMN status FORMAT A10

PROMPT ========================================
PROMPT TR2000 DEPLOYMENT VALIDATION
PROMPT ========================================

-- Create temporary comparison table
CREATE GLOBAL TEMPORARY TABLE temp_deployment_check (
    object_type VARCHAR2(50),
    object_name VARCHAR2(100), 
    expected_status VARCHAR2(20),
    actual_status VARCHAR2(20),
    match_status VARCHAR2(20)
) ON COMMIT PRESERVE ROWS;

-- Expected Tables (41 total)
INSERT INTO temp_deployment_check VALUES ('TABLE', 'CONTROL_ENDPOINTS', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'CONTROL_SETTINGS', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'ETL_FILTER', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'ETL_ERROR_LOG', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'ETL_RUN_LOG', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'ETL_STATISTICS', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'RAW_JSON', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_EDS_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_ESK_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_MDS_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_PCS_EMBEDDED_NOTES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_PCS_HEADER_PROPERTIES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_PCS_LIST', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_PCS_PIPE_ELEMENTS', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_PCS_PIPE_SIZES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_PCS_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_PCS_TEMP_PRESSURES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_PCS_VALVE_ELEMENTS', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_PIPE_ELEMENT_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_SC_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_VDS_LIST', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_VDS_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_VSK_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'STG_VSM_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'EDS_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'ESK_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'MDS_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'PCS_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'PIPE_ELEMENT_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'SC_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'VDS_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'VSK_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'VSM_REFERENCES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'PCS_EMBEDDED_NOTES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'PCS_HEADER_PROPERTIES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'PCS_PIPE_ELEMENTS', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'PCS_PIPE_SIZES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'PCS_TEMP_PRESSURES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'PCS_VALVE_ELEMENTS', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'PCS_LIST', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('TABLE', 'VDS_LIST', 'EXISTS', NULL, 'PENDING');

-- Expected Packages (10 total)
INSERT INTO temp_deployment_check VALUES ('PACKAGE', 'PKG_API_CLIENT', 'VALID', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('PACKAGE', 'PKG_DATE_UTILS', 'VALID', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('PACKAGE', 'PKG_DDL_BACKUP', 'VALID', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('PACKAGE', 'PKG_ETL_LOGGING', 'VALID', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('PACKAGE', 'PKG_ETL_PROCESSOR', 'VALID', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('PACKAGE', 'PKG_ETL_TEST_UTILS', 'VALID', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('PACKAGE', 'PKG_ETL_VALIDATION', 'VALID', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('PACKAGE', 'PKG_INDEPENDENT_ETL_CONTROL', 'VALID', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('PACKAGE', 'PKG_MAIN_ETL_CONTROL', 'VALID', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('PACKAGE', 'PKG_PCS_DETAIL_PROCESSOR', 'VALID', NULL, 'PENDING');

-- Expected Procedures (4 total)
INSERT INTO temp_deployment_check VALUES ('PROCEDURE', 'FIX_EMBEDDED_NOTES_PARSER', 'VALID', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('PROCEDURE', 'FIX_PCS_LIST_PARSER', 'VALID', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('PROCEDURE', 'FIX_VDS_CATALOG_PARSER', 'VALID', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('PROCEDURE', 'TEMP_FIX_VDS_PARSE', 'VALID', NULL, 'PENDING');

-- Expected Views (9 total)
INSERT INTO temp_deployment_check VALUES ('VIEW', 'V_API_CALLS_PER_RUN', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('VIEW', 'V_API_CALL_STATISTICS', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('VIEW', 'V_API_OPTIMIZATION_CANDIDATES', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('VIEW', 'V_ENDPOINT_TABLE_STATISTICS', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('VIEW', 'V_ETL_RUN_SUMMARY', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('VIEW', 'V_ETL_STATISTICS_SUMMARY', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('VIEW', 'V_OPERATION_STATISTICS', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('VIEW', 'V_RAW_JSON', 'EXISTS', NULL, 'PENDING');
INSERT INTO temp_deployment_check VALUES ('VIEW', 'V_RAW_JSON_SUMMARY', 'EXISTS', NULL, 'PENDING');

-- Update actual status for tables
UPDATE temp_deployment_check c
SET actual_status = CASE WHEN EXISTS (
    SELECT 1 FROM user_tables t WHERE t.table_name = c.object_name
) THEN 'EXISTS' ELSE 'MISSING' END
WHERE c.object_type = 'TABLE';

-- Update actual status for packages  
UPDATE temp_deployment_check c
SET actual_status = (
    SELECT CASE WHEN o.status = 'VALID' THEN 'VALID' 
                WHEN o.status = 'INVALID' THEN 'INVALID'
                ELSE 'MISSING' END
    FROM user_objects o 
    WHERE o.object_name = c.object_name 
    AND o.object_type = 'PACKAGE'
)
WHERE c.object_type = 'PACKAGE';

-- Update missing packages
UPDATE temp_deployment_check c
SET actual_status = 'MISSING'
WHERE c.object_type = 'PACKAGE' 
AND c.actual_status IS NULL;

-- Update actual status for procedures
UPDATE temp_deployment_check c
SET actual_status = (
    SELECT CASE WHEN o.status = 'VALID' THEN 'VALID'
                WHEN o.status = 'INVALID' THEN 'INVALID' 
                ELSE 'MISSING' END
    FROM user_objects o
    WHERE o.object_name = c.object_name
    AND o.object_type = 'PROCEDURE'
)
WHERE c.object_type = 'PROCEDURE';

-- Update missing procedures
UPDATE temp_deployment_check c
SET actual_status = 'MISSING'
WHERE c.object_type = 'PROCEDURE'
AND c.actual_status IS NULL;

-- Update actual status for views
UPDATE temp_deployment_check c
SET actual_status = CASE WHEN EXISTS (
    SELECT 1 FROM user_views v WHERE v.view_name = c.object_name
) THEN 'EXISTS' ELSE 'MISSING' END
WHERE c.object_type = 'VIEW';

-- Update match status
UPDATE temp_deployment_check
SET match_status = CASE 
    WHEN expected_status = actual_status THEN 'MATCH'
    ELSE 'MISMATCH'
END;

PROMPT
PROMPT 1. DEPLOYMENT VALIDATION SUMMARY:
PROMPT ========================================
SELECT 
    object_type,
    COUNT(*) as total_expected,
    SUM(CASE WHEN match_status = 'MATCH' THEN 1 ELSE 0 END) as matched,
    SUM(CASE WHEN match_status = 'MISMATCH' THEN 1 ELSE 0 END) as mismatched,
    ROUND(SUM(CASE WHEN match_status = 'MATCH' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as match_percentage
FROM temp_deployment_check
GROUP BY object_type
ORDER BY object_type;

PROMPT
PROMPT 2. MISSING OR INVALID OBJECTS:
PROMPT ========================================
SELECT object_type, object_name, expected_status, actual_status, match_status
FROM temp_deployment_check
WHERE match_status = 'MISMATCH'
ORDER BY object_type, object_name;

PROMPT
PROMPT 3. CONTROL DATA VALIDATION:
PROMPT ========================================
SELECT 
    'ETL_FILTER' as table_name,
    1 as expected_rows,
    COUNT(*) as actual_rows,
    CASE WHEN COUNT(*) = 1 THEN 'OK' ELSE 'FAIL' END as status
FROM ETL_FILTER
UNION ALL
SELECT 
    'CONTROL_SETTINGS',
    4,
    COUNT(*),
    CASE WHEN COUNT(*) = 4 THEN 'OK' ELSE 'FAIL' END
FROM CONTROL_SETTINGS  
UNION ALL
SELECT 
    'CONTROL_ENDPOINTS',
    17,
    COUNT(*),
    CASE WHEN COUNT(*) = 17 THEN 'OK' ELSE 'FAIL' END
FROM CONTROL_ENDPOINTS;

PROMPT
PROMPT 4. OVERALL DEPLOYMENT STATUS:
PROMPT ========================================
SELECT 
    CASE WHEN (
        SELECT COUNT(*) FROM temp_deployment_check WHERE match_status = 'MISMATCH'
    ) = 0 THEN 'SUCCESS - All objects deployed correctly!'
    ELSE 'FAILURE - ' || (SELECT COUNT(*) FROM temp_deployment_check WHERE match_status = 'MISMATCH') || ' objects missing/invalid'
    END as deployment_status
FROM dual;

-- Clean up
DROP TABLE temp_deployment_check;

PROMPT
PROMPT ========================================
PROMPT VALIDATION COMPLETE
PROMPT ========================================