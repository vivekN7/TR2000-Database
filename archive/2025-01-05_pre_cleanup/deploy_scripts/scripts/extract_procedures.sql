-- Script to extract procedures to deployment scripts
-- Run as: sqlplus TR2000_STAGING/piping @extract_procedures.sql

SET PAGESIZE 0
SET LINESIZE 10000
SET LONG 999999999
SET LONGCHUNKSIZE 999999
SET TRIMSPOOL ON
SET FEEDBACK OFF
SET ECHO OFF
SET HEADING OFF
SET TERMOUT OFF

-- Extract each procedure to its own file
BEGIN
    FOR rec IN (SELECT object_name 
                FROM user_objects 
                WHERE object_type = 'PROCEDURE'
                ORDER BY object_name)
    LOOP
        DBMS_OUTPUT.PUT_LINE('Processing: ' || rec.object_name);
    END LOOP;
END;
/

-- Extract FIX_EMBEDDED_NOTES_PARSER
SPOOL ../05_procedures/FIX_EMBEDDED_NOTES_PARSER.sql
SELECT '-- Procedure: FIX_EMBEDDED_NOTES_PARSER' || CHR(10) ||
       'DROP PROCEDURE FIX_EMBEDDED_NOTES_PARSER;' || CHR(10) ||
       '/' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PROCEDURE', 'FIX_EMBEDDED_NOTES_PARSER') || CHR(10) || '/' || CHR(10)
FROM dual
WHERE EXISTS (SELECT 1 FROM user_procedures WHERE object_name = 'FIX_EMBEDDED_NOTES_PARSER');
SPOOL OFF

-- Extract FIX_PCS_LIST_PARSER
SPOOL ../05_procedures/FIX_PCS_LIST_PARSER.sql
SELECT '-- Procedure: FIX_PCS_LIST_PARSER' || CHR(10) ||
       'DROP PROCEDURE FIX_PCS_LIST_PARSER;' || CHR(10) ||
       '/' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PROCEDURE', 'FIX_PCS_LIST_PARSER') || CHR(10) || '/' || CHR(10)
FROM dual
WHERE EXISTS (SELECT 1 FROM user_procedures WHERE object_name = 'FIX_PCS_LIST_PARSER');
SPOOL OFF

-- Extract FIX_VDS_CATALOG_PARSER
SPOOL ../05_procedures/FIX_VDS_CATALOG_PARSER.sql
SELECT '-- Procedure: FIX_VDS_CATALOG_PARSER' || CHR(10) ||
       'DROP PROCEDURE FIX_VDS_CATALOG_PARSER;' || CHR(10) ||
       '/' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PROCEDURE', 'FIX_VDS_CATALOG_PARSER') || CHR(10) || '/' || CHR(10)
FROM dual
WHERE EXISTS (SELECT 1 FROM user_procedures WHERE object_name = 'FIX_VDS_CATALOG_PARSER');
SPOOL OFF

-- Extract TEMP_FIX_VDS_PARSE
SPOOL ../05_procedures/TEMP_FIX_VDS_PARSE.sql
SELECT '-- Procedure: TEMP_FIX_VDS_PARSE' || CHR(10) ||
       'DROP PROCEDURE TEMP_FIX_VDS_PARSE;' || CHR(10) ||
       '/' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PROCEDURE', 'TEMP_FIX_VDS_PARSE') || CHR(10) || '/' || CHR(10)
FROM dual
WHERE EXISTS (SELECT 1 FROM user_procedures WHERE object_name = 'TEMP_FIX_VDS_PARSE');
SPOOL OFF

EXIT;