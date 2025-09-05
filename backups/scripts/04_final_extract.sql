-- Final extraction script with correct paths
SET PAGESIZE 0
SET LINESIZE 10000
SET LONG 999999999
SET LONGCHUNKSIZE 999999
SET TRIMSPOOL ON
SET FEEDBACK OFF
SET ECHO OFF
SET HEADING OFF

-- Extract all package specs and bodies
SPOOL 20250105_pre_refactor/all_packages.sql

SELECT '-- Package: ' || object_name || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE', object_name) || CHR(10) || '/' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE_BODY', object_name) || CHR(10) || '/' || CHR(10)
FROM user_objects 
WHERE object_type = 'PACKAGE'
ORDER BY object_name;

SPOOL OFF

-- Extract all procedures
SPOOL 20250105_pre_refactor/all_procedures.sql

SELECT '-- Procedure: ' || object_name || CHR(10) ||
       DBMS_METADATA.GET_DDL('PROCEDURE', object_name) || CHR(10) || '/' || CHR(10)
FROM user_objects 
WHERE object_type = 'PROCEDURE'
ORDER BY object_name;

SPOOL OFF

-- Extract all tables
SPOOL 20250105_pre_refactor/all_tables.sql

SELECT '-- Table: ' || table_name || CHR(10) ||
       DBMS_METADATA.GET_DDL('TABLE', table_name) || CHR(10) || '/' || CHR(10)
FROM user_tables
ORDER BY table_name;

SPOOL OFF

EXIT;