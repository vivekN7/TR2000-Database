-- =====================================================
-- Complete Database Objects Extraction Script
-- Date: 2025-01-05
-- Purpose: Extract ALL objects from TR2000_STAGING to SQL
-- =====================================================
-- Run as: sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1

SET PAGESIZE 0
SET LINESIZE 10000
SET LONG 999999999
SET LONGCHUNKSIZE 999999
SET TRIMSPOOL ON
SET FEEDBACK OFF
SET ECHO OFF
SET HEADING OFF
SET TERMOUT OFF
SET SERVEROUTPUT ON

-- Start spooling to file
SPOOL Database/backups/20250105_pre_refactor/complete_database_export.sql

PROMPT -- =====================================================;
PROMPT -- TR2000_STAGING Complete Database Export;
PROMPT -- Generated: 2025-01-05;
PROMPT -- =====================================================;
PROMPT;

-- =====================================================
-- TABLES (Structure only - data will be separate)
-- =====================================================
PROMPT -- =====================================================;
PROMPT -- TABLES;
PROMPT -- =====================================================;
BEGIN
    FOR rec IN (SELECT table_name FROM user_tables ORDER BY table_name) LOOP
        DBMS_OUTPUT.PUT_LINE('-- Table: ' || rec.table_name);
        FOR line IN (
            SELECT DBMS_METADATA.GET_DDL('TABLE', rec.table_name) AS ddl FROM dual
        ) LOOP
            DBMS_OUTPUT.PUT_LINE(REPLACE(line.ddl, '"TR2000_STAGING".', ''));
            DBMS_OUTPUT.PUT_LINE('/');
            DBMS_OUTPUT.PUT_LINE('');
        END LOOP;
    END LOOP;
END;
/

-- =====================================================
-- SEQUENCES
-- =====================================================
PROMPT -- =====================================================;
PROMPT -- SEQUENCES;
PROMPT -- =====================================================;
SELECT DBMS_METADATA.GET_DDL('SEQUENCE', sequence_name) || CHR(10) || '/' || CHR(10)
FROM user_sequences
ORDER BY sequence_name;

-- =====================================================
-- INDEXES
-- =====================================================
PROMPT -- =====================================================;
PROMPT -- INDEXES;
PROMPT -- =====================================================;
SELECT DBMS_METADATA.GET_DDL('INDEX', index_name) || CHR(10) || '/' || CHR(10)
FROM user_indexes
WHERE index_type != 'LOB'
AND index_name NOT LIKE 'SYS_%'
ORDER BY index_name;

-- =====================================================
-- VIEWS
-- =====================================================
PROMPT -- =====================================================;
PROMPT -- VIEWS;
PROMPT -- =====================================================;
SELECT DBMS_METADATA.GET_DDL('VIEW', view_name) || CHR(10) || '/' || CHR(10)
FROM user_views
ORDER BY view_name;

-- =====================================================
-- TRIGGERS
-- =====================================================
PROMPT -- =====================================================;
PROMPT -- TRIGGERS;
PROMPT -- =====================================================;
SELECT DBMS_METADATA.GET_DDL('TRIGGER', trigger_name) || CHR(10) || '/' || CHR(10)
FROM user_triggers
ORDER BY trigger_name;

-- =====================================================
-- PACKAGE SPECIFICATIONS
-- =====================================================
PROMPT -- =====================================================;
PROMPT -- PACKAGE SPECIFICATIONS;
PROMPT -- =====================================================;
SELECT DBMS_METADATA.GET_DDL('PACKAGE', object_name) || CHR(10) || '/' || CHR(10)
FROM user_objects 
WHERE object_type = 'PACKAGE'
ORDER BY object_name;

-- =====================================================
-- PACKAGE BODIES
-- =====================================================
PROMPT -- =====================================================;
PROMPT -- PACKAGE BODIES;
PROMPT -- =====================================================;
SELECT DBMS_METADATA.GET_DDL('PACKAGE_BODY', object_name) || CHR(10) || '/' || CHR(10)
FROM user_objects 
WHERE object_type = 'PACKAGE BODY'
ORDER BY object_name;

-- =====================================================
-- PROCEDURES
-- =====================================================
PROMPT -- =====================================================;
PROMPT -- PROCEDURES;
PROMPT -- =====================================================;
SELECT DBMS_METADATA.GET_DDL('PROCEDURE', object_name) || CHR(10) || '/' || CHR(10)
FROM user_objects 
WHERE object_type = 'PROCEDURE'
ORDER BY object_name;

-- =====================================================
-- FUNCTIONS
-- =====================================================
PROMPT -- =====================================================;
PROMPT -- FUNCTIONS;
PROMPT -- =====================================================;
SELECT DBMS_METADATA.GET_DDL('FUNCTION', object_name) || CHR(10) || '/' || CHR(10)
FROM user_objects 
WHERE object_type = 'FUNCTION'
ORDER BY object_name;

SPOOL OFF

-- Now create a separate data export for control tables
SPOOL Database/backups/20250105_pre_refactor/control_data_export.sql

PROMPT -- Control Data Export;
PROMPT -- ETL_FILTER data;
SELECT 'INSERT INTO ETL_FILTER VALUES(' ||
    filter_id || ',' ||
    '''' || plant_id || ''',' ||
    '''' || plant_name || ''',' ||
    '''' || issue_revision || ''',' ||
    CASE WHEN added_date IS NULL THEN 'NULL' ELSE 'TO_DATE(''' || TO_CHAR(added_date, 'YYYY-MM-DD HH24:MI:SS') || ''', ''YYYY-MM-DD HH24:MI:SS'')' END || ',' ||
    '''' || added_by_user_id || '''' ||
    ');'
FROM ETL_FILTER;

PROMPT -- CONTROL_SETTINGS data;
SELECT 'INSERT INTO CONTROL_SETTINGS VALUES(' ||
    '''' || setting_key || ''',' ||
    '''' || setting_value || ''',' ||
    '''' || description || '''' ||
    ');'
FROM CONTROL_SETTINGS;

PROMPT -- CONTROL_ENDPOINTS data;
SELECT 'INSERT INTO CONTROL_ENDPOINTS VALUES(' ||
    endpoint_id || ',' ||
    '''' || endpoint_key || ''',' ||
    '''' || endpoint_template || ''',' ||
    '''' || description || '''' ||
    ');'
FROM CONTROL_ENDPOINTS;

SPOOL OFF

SET FEEDBACK ON
SET ECHO ON
SET HEADING ON
SET TERMOUT ON