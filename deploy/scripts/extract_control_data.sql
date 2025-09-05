-- Script to extract control data
-- Run as: sqlplus TR2000_STAGING/piping @extract_control_data.sql

SET PAGESIZE 0
SET LINESIZE 10000
SET FEEDBACK OFF
SET ECHO OFF
SET HEADING OFF
SET TERMOUT OFF

SPOOL ../07_control_data/load_control_data.sql

PROMPT -- Control Data Load Script;
PROMPT -- Generated from database;
PROMPT;
PROMPT -- Clear existing data;
PROMPT DELETE FROM ETL_FILTER;;
PROMPT DELETE FROM CONTROL_SETTINGS;;
PROMPT DELETE FROM CONTROL_ENDPOINTS;;
PROMPT;

PROMPT -- Load ETL_FILTER;
SELECT 'INSERT INTO ETL_FILTER (filter_id, plant_id, plant_name, issue_revision, added_date, added_by_user_id) VALUES (' ||
    filter_id || ',' ||
    '''' || plant_id || ''',' ||
    '''' || plant_name || ''',' ||
    '''' || issue_revision || ''',' ||
    CASE WHEN added_date IS NULL THEN 'NULL' 
         ELSE 'TO_DATE(''' || TO_CHAR(added_date, 'YYYY-MM-DD HH24:MI:SS') || ''', ''YYYY-MM-DD HH24:MI:SS'')' 
    END || ',' ||
    '''' || added_by_user_id || ''');'
FROM ETL_FILTER
ORDER BY filter_id;

PROMPT;
PROMPT -- Load CONTROL_SETTINGS;
SELECT 'INSERT INTO CONTROL_SETTINGS (setting_key, setting_value, description) VALUES (' ||
    '''' || setting_key || ''',' ||
    '''' || setting_value || ''',' ||
    '''' || REPLACE(description, '''', '''''') || ''');'
FROM CONTROL_SETTINGS
ORDER BY setting_key;

PROMPT;
PROMPT -- Load CONTROL_ENDPOINTS;
SELECT 'INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template, description) VALUES (' ||
    endpoint_id || ',' ||
    '''' || endpoint_key || ''',' ||
    '''' || endpoint_template || ''',' ||
    '''' || REPLACE(description, '''', '''''') || ''');'
FROM CONTROL_ENDPOINTS
ORDER BY endpoint_id;

PROMPT;
PROMPT COMMIT;;
PROMPT;
PROMPT -- Control data loaded successfully;

SPOOL OFF

EXIT;