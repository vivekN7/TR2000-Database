-- Export control table data
SET PAGESIZE 0
SET LINESIZE 10000
SET FEEDBACK OFF
SET ECHO OFF
SET TERMOUT OFF
SET TRIMSPOOL ON

SPOOL control_data.sql

SELECT '-- Control table data export from TR2000_STAGING' || CHR(10) ||
       '-- Generated: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || CHR(10) || CHR(10)
FROM dual;

-- ETL_FILTER data
SELECT '-- ETL_FILTER data' || CHR(10) FROM dual;
SELECT 'DELETE FROM ETL_FILTER;' || CHR(10) FROM dual;

SELECT 'INSERT INTO ETL_FILTER (filter_id, plant_id, plant_name, issue_revision, added_date, added_by_user_id) VALUES (' ||
       filter_id || ', ''' || 
       plant_id || ''', ' ||
       CASE WHEN plant_name IS NULL THEN 'NULL' ELSE '''' || plant_name || '''' END || ', ''' ||
       issue_revision || ''', ' ||
       'TO_DATE(''' || TO_CHAR(added_date, 'YYYY-MM-DD HH24:MI:SS') || ''', ''YYYY-MM-DD HH24:MI:SS''), ' ||
       CASE WHEN added_by_user_id IS NULL THEN 'NULL' ELSE '''' || added_by_user_id || '''' END || ');'
FROM ETL_FILTER
ORDER BY filter_id;

SELECT CHR(10) FROM dual;

-- CONTROL_SETTINGS data  
SELECT '-- CONTROL_SETTINGS data' || CHR(10) FROM dual;
SELECT 'DELETE FROM CONTROL_SETTINGS;' || CHR(10) FROM dual;

SELECT 'INSERT INTO CONTROL_SETTINGS (setting_key, setting_value, description, last_modified_date, modified_by) VALUES (''' ||
       setting_key || ''', ''' || 
       setting_value || ''', ' ||
       CASE WHEN description IS NULL THEN 'NULL' ELSE '''' || REPLACE(description, '''', '''''') || '''' END || ', ' ||
       CASE WHEN last_modified_date IS NULL THEN 'NULL' 
            ELSE 'TO_DATE(''' || TO_CHAR(last_modified_date, 'YYYY-MM-DD HH24:MI:SS') || ''', ''YYYY-MM-DD HH24:MI:SS'')' END || ', ' ||
       CASE WHEN modified_by IS NULL THEN 'NULL' ELSE '''' || modified_by || '''' END || ');'
FROM CONTROL_SETTINGS
ORDER BY setting_key;

SELECT CHR(10) FROM dual;

-- CONTROL_ENDPOINTS data
SELECT '-- CONTROL_ENDPOINTS data' || CHR(10) FROM dual;
SELECT 'DELETE FROM CONTROL_ENDPOINTS;' || CHR(10) FROM dual;

SELECT 'INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template, description, created_date, last_modified_date) VALUES (' ||
       endpoint_id || ', ''' || 
       endpoint_key || ''', ''' ||
       endpoint_template || ''', ' ||
       CASE WHEN description IS NULL THEN 'NULL' ELSE '''' || REPLACE(description, '''', '''''') || '''' END || ', ' ||
       CASE WHEN created_date IS NULL THEN 'NULL' 
            ELSE 'TO_DATE(''' || TO_CHAR(created_date, 'YYYY-MM-DD HH24:MI:SS') || ''', ''YYYY-MM-DD HH24:MI:SS'')' END || ', ' ||
       CASE WHEN last_modified_date IS NULL THEN 'NULL' 
            ELSE 'TO_DATE(''' || TO_CHAR(last_modified_date, 'YYYY-MM-DD HH24:MI:SS') || ''', ''YYYY-MM-DD HH24:MI:SS'')' END || ');'
FROM CONTROL_ENDPOINTS
ORDER BY endpoint_id;

SELECT CHR(10) || 'COMMIT;' || CHR(10) FROM dual;

SPOOL OFF
EXIT;
