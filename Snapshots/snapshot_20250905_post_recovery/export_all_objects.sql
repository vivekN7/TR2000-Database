-- Export all database objects from TR2000_STAGING schema
-- Date: 2025-09-05 (Post PKG_ETL_PROCESSOR recovery)
-- This script exports all objects with their DDL

SET PAGESIZE 0
SET LINESIZE 10000
SET LONG 999999999
SET LONGCHUNKSIZE 999999999
SET TRIMSPOOL ON
SET FEEDBACK OFF
SET ECHO OFF
SET TERMOUT OFF

-- Export all tables
SPOOL all_tables.sql

SELECT '-- Tables export from TR2000_STAGING' || CHR(10) ||
       '-- Generated: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || CHR(10) || CHR(10)
FROM dual;

SELECT DBMS_METADATA.GET_DDL('TABLE', table_name) || ';' || CHR(10) || '/' || CHR(10)
FROM user_tables
WHERE table_name NOT LIKE 'BIN$%'
ORDER BY table_name;

SPOOL OFF

-- Export all packages
SPOOL all_packages.sql

SELECT '-- Packages export from TR2000_STAGING' || CHR(10) ||
       '-- Generated: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || CHR(10) || CHR(10)
FROM dual;

-- Package specs first
SELECT DBMS_METADATA.GET_DDL('PACKAGE_SPEC', object_name) || CHR(10) || '/' || CHR(10)
FROM user_objects
WHERE object_type = 'PACKAGE'
AND object_name NOT LIKE 'BIN$%'
ORDER BY object_name;

-- Then package bodies
SELECT DBMS_METADATA.GET_DDL('PACKAGE_BODY', object_name) || CHR(10) || '/' || CHR(10)
FROM user_objects
WHERE object_type = 'PACKAGE BODY'
AND object_name NOT LIKE 'BIN$%'
ORDER BY object_name;

SPOOL OFF

-- Export all views
SPOOL all_views.sql

SELECT '-- Views export from TR2000_STAGING' || CHR(10) ||
       '-- Generated: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || CHR(10) || CHR(10)
FROM dual;

SELECT DBMS_METADATA.GET_DDL('VIEW', view_name) || ';' || CHR(10) || '/' || CHR(10)
FROM user_views
ORDER BY view_name;

SPOOL OFF

-- Export all sequences
SPOOL all_sequences.sql

SELECT '-- Sequences export from TR2000_STAGING' || CHR(10) ||
       '-- Generated: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || CHR(10) || CHR(10)
FROM dual;

SELECT DBMS_METADATA.GET_DDL('SEQUENCE', sequence_name) || ';' || CHR(10) || '/' || CHR(10)
FROM user_sequences
ORDER BY sequence_name;

SPOOL OFF

-- Export all triggers
SPOOL all_triggers.sql

SELECT '-- Triggers export from TR2000_STAGING' || CHR(10) ||
       '-- Generated: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || CHR(10) || CHR(10)
FROM dual;

SELECT DBMS_METADATA.GET_DDL('TRIGGER', trigger_name) || CHR(10) || '/' || CHR(10)
FROM user_triggers
WHERE trigger_name NOT LIKE 'BIN$%'
ORDER BY trigger_name;

SPOOL OFF

-- Export all indexes
SPOOL all_indexes.sql  

SELECT '-- Indexes export from TR2000_STAGING' || CHR(10) ||
       '-- Generated: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || CHR(10) || CHR(10)
FROM dual;

SELECT DBMS_METADATA.GET_DDL('INDEX', index_name) || ';' || CHR(10) || '/' || CHR(10)
FROM user_indexes
WHERE index_type != 'LOB'
AND index_name NOT LIKE 'SYS_%'
AND index_name NOT LIKE 'BIN$%'
ORDER BY index_name;

SPOOL OFF

-- Export all procedures
SPOOL all_procedures.sql

SELECT '-- Procedures export from TR2000_STAGING' || CHR(10) ||
       '-- Generated: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || CHR(10) || CHR(10)
FROM dual;

SELECT DBMS_METADATA.GET_DDL('PROCEDURE', object_name) || CHR(10) || '/' || CHR(10)
FROM user_procedures
WHERE object_type = 'PROCEDURE'
AND procedure_name IS NULL
ORDER BY object_name;

SPOOL OFF

EXIT;
