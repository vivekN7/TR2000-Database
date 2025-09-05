-- Script to extract all database objects to deployment scripts
-- Run as: sqlplus TR2000_STAGING/piping @extract_all_objects.sql

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

-- Extract Control Tables
SPOOL ../01_tables/01_control_tables.sql

PROMPT -- Control Tables;
PROMPT -- Generated from database;
PROMPT;

SELECT '-- Table: ' || table_name || CHR(10) ||
       'DROP TABLE ' || table_name || ' CASCADE CONSTRAINTS PURGE;' || CHR(10) ||
       '/' || CHR(10) ||
       DBMS_METADATA.GET_DDL('TABLE', table_name) || CHR(10) || '/' || CHR(10)
FROM user_tables
WHERE table_name IN ('ETL_FILTER', 'CONTROL_SETTINGS', 'CONTROL_ENDPOINTS')
ORDER BY table_name;

SPOOL OFF

-- Extract Audit Tables
SPOOL ../01_tables/02_audit_tables.sql

PROMPT -- Audit and Logging Tables;
PROMPT -- Generated from database;
PROMPT;

SELECT '-- Table: ' || table_name || CHR(10) ||
       'DROP TABLE ' || table_name || ' CASCADE CONSTRAINTS PURGE;' || CHR(10) ||
       '/' || CHR(10) ||
       DBMS_METADATA.GET_DDL('TABLE', table_name) || CHR(10) || '/' || CHR(10)
FROM user_tables
WHERE table_name IN ('RAW_JSON', 'ETL_RUN_LOG', 'ETL_ERROR_LOG', 'ETL_STATISTICS', 'ETL_PERFORMANCE_METRICS')
ORDER BY table_name;

SPOOL OFF

-- Extract Staging Tables
SPOOL ../01_tables/03_staging_tables.sql

PROMPT -- Staging Tables (STG_*);
PROMPT -- Generated from database;
PROMPT;

SELECT '-- Table: ' || table_name || CHR(10) ||
       'DROP TABLE ' || table_name || ' CASCADE CONSTRAINTS PURGE;' || CHR(10) ||
       '/' || CHR(10) ||
       DBMS_METADATA.GET_DDL('TABLE', table_name) || CHR(10) || '/' || CHR(10)
FROM user_tables
WHERE table_name LIKE 'STG_%'
ORDER BY table_name;

SPOOL OFF

-- Extract Reference Tables
SPOOL ../01_tables/04_reference_tables.sql

PROMPT -- Reference Tables;
PROMPT -- Generated from database;
PROMPT;

SELECT '-- Table: ' || table_name || CHR(10) ||
       'DROP TABLE ' || table_name || ' CASCADE CONSTRAINTS PURGE;' || CHR(10) ||
       '/' || CHR(10) ||
       DBMS_METADATA.GET_DDL('TABLE', table_name) || CHR(10) || '/' || CHR(10)
FROM user_tables
WHERE table_name LIKE '%_REFERENCES'
ORDER BY table_name;

SPOOL OFF

-- Extract PCS Detail Tables
SPOOL ../01_tables/05_pcs_detail_tables.sql

PROMPT -- PCS Detail Tables;
PROMPT -- Generated from database;
PROMPT;

SELECT '-- Table: ' || table_name || CHR(10) ||
       'DROP TABLE ' || table_name || ' CASCADE CONSTRAINTS PURGE;' || CHR(10) ||
       '/' || CHR(10) ||
       DBMS_METADATA.GET_DDL('TABLE', table_name) || CHR(10) || '/' || CHR(10)
FROM user_tables
WHERE table_name LIKE 'PCS_%'
AND table_name NOT LIKE '%_REFERENCES'
AND table_name != 'PCS_LIST'
ORDER BY table_name;

SPOOL OFF

-- Extract Catalog Tables
SPOOL ../01_tables/06_catalog_tables.sql

PROMPT -- Catalog Tables;
PROMPT -- Generated from database;
PROMPT;

SELECT '-- Table: ' || table_name || CHR(10) ||
       'DROP TABLE ' || table_name || ' CASCADE CONSTRAINTS PURGE;' || CHR(10) ||
       '/' || CHR(10) ||
       DBMS_METADATA.GET_DDL('TABLE', table_name) || CHR(10) || '/' || CHR(10)
FROM user_tables
WHERE table_name IN ('VDS_LIST', 'PCS_LIST')
ORDER BY table_name;

SPOOL OFF

-- Extract All Sequences
SPOOL ../02_sequences/all_sequences.sql

PROMPT -- All Sequences;
PROMPT -- Generated from database;
PROMPT;

SELECT 'DROP SEQUENCE ' || sequence_name || ';' || CHR(10) ||
       '/' || CHR(10) ||
       DBMS_METADATA.GET_DDL('SEQUENCE', sequence_name) || CHR(10) || '/' || CHR(10)
FROM user_sequences
ORDER BY sequence_name;

SPOOL OFF

-- Extract All Indexes
SPOOL ../03_indexes/all_indexes.sql

PROMPT -- All Indexes;
PROMPT -- Generated from database;
PROMPT;

SELECT DBMS_METADATA.GET_DDL('INDEX', index_name) || CHR(10) || '/' || CHR(10)
FROM user_indexes
WHERE index_type != 'LOB'
AND generated = 'N'
AND index_name NOT LIKE 'SYS_%'
ORDER BY index_name;

SPOOL OFF

-- Extract All Views
SPOOL ../06_views/all_views.sql

PROMPT -- All Views;
PROMPT -- Generated from database;
PROMPT;

SELECT 'DROP VIEW ' || view_name || ';' || CHR(10) ||
       '/' || CHR(10) ||
       DBMS_METADATA.GET_DDL('VIEW', view_name) || CHR(10) || '/' || CHR(10)
FROM user_views
ORDER BY view_name;

SPOOL OFF

EXIT;