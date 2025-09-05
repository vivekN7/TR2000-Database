-- Script to extract individual packages to deployment scripts
-- Run as: sqlplus TR2000_STAGING/piping @extract_packages.sql

SET PAGESIZE 0
SET LINESIZE 10000
SET LONG 999999999
SET LONGCHUNKSIZE 999999
SET TRIMSPOOL ON
SET FEEDBACK OFF
SET ECHO OFF
SET HEADING OFF
SET TERMOUT OFF

-- Extract PKG_DATE_UTILS
SPOOL ../04_packages/PKG_DATE_UTILS.sql

SELECT '-- Package: PKG_DATE_UTILS' || CHR(10) ||
       '-- Purpose: Date parsing utilities' || CHR(10) ||
       CHR(10) ||
       '-- Drop existing package' || CHR(10) ||
       'BEGIN' || CHR(10) ||
       '    EXECUTE IMMEDIATE ''DROP PACKAGE BODY PKG_DATE_UTILS'';' || CHR(10) ||
       'EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;' || CHR(10) ||
       '/' || CHR(10) ||
       'BEGIN' || CHR(10) ||
       '    EXECUTE IMMEDIATE ''DROP PACKAGE PKG_DATE_UTILS'';' || CHR(10) ||
       'EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;' || CHR(10) ||
       '/' || CHR(10) || CHR(10) ||
       '-- Create package specification' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE', 'PKG_DATE_UTILS') || CHR(10) || '/' || CHR(10) ||
       '-- Create package body' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE_BODY', 'PKG_DATE_UTILS') || CHR(10) || '/' || CHR(10) ||
       'SHOW ERRORS' || CHR(10)
FROM dual;

SPOOL OFF

-- Extract PKG_ETL_LOGGING
SPOOL ../04_packages/PKG_ETL_LOGGING.sql

SELECT '-- Package: PKG_ETL_LOGGING' || CHR(10) ||
       '-- Purpose: ETL run and statistics logging' || CHR(10) ||
       CHR(10) ||
       '-- Drop existing package' || CHR(10) ||
       'BEGIN' || CHR(10) ||
       '    EXECUTE IMMEDIATE ''DROP PACKAGE BODY PKG_ETL_LOGGING'';' || CHR(10) ||
       'EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;' || CHR(10) ||
       '/' || CHR(10) ||
       'BEGIN' || CHR(10) ||
       '    EXECUTE IMMEDIATE ''DROP PACKAGE PKG_ETL_LOGGING'';' || CHR(10) ||
       'EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;' || CHR(10) ||
       '/' || CHR(10) || CHR(10) ||
       '-- Create package specification' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE', 'PKG_ETL_LOGGING') || CHR(10) || '/' || CHR(10) ||
       '-- Create package body' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE_BODY', 'PKG_ETL_LOGGING') || CHR(10) || '/' || CHR(10) ||
       'SHOW ERRORS' || CHR(10)
FROM dual;

SPOOL OFF

-- Extract PKG_ETL_PROCESSOR
SPOOL ../04_packages/PKG_ETL_PROCESSOR.sql

SELECT '-- Package: PKG_ETL_PROCESSOR' || CHR(10) ||
       '-- Purpose: JSON parsing and loading for reference data' || CHR(10) ||
       CHR(10) ||
       '-- Drop existing package' || CHR(10) ||
       'BEGIN' || CHR(10) ||
       '    EXECUTE IMMEDIATE ''DROP PACKAGE BODY PKG_ETL_PROCESSOR'';' || CHR(10) ||
       'EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;' || CHR(10) ||
       '/' || CHR(10) ||
       'BEGIN' || CHR(10) ||
       '    EXECUTE IMMEDIATE ''DROP PACKAGE PKG_ETL_PROCESSOR'';' || CHR(10) ||
       'EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;' || CHR(10) ||
       '/' || CHR(10) || CHR(10) ||
       '-- Create package specification' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE', 'PKG_ETL_PROCESSOR') || CHR(10) || '/' || CHR(10) ||
       '-- Create package body' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE_BODY', 'PKG_ETL_PROCESSOR') || CHR(10) || '/' || CHR(10) ||
       'SHOW ERRORS' || CHR(10)
FROM dual
WHERE EXISTS (SELECT 1 FROM user_objects WHERE object_name = 'PKG_ETL_PROCESSOR' AND object_type = 'PACKAGE');

SPOOL OFF

-- Extract PKG_PCS_DETAIL_PROCESSOR
SPOOL ../04_packages/PKG_PCS_DETAIL_PROCESSOR.sql

SELECT '-- Package: PKG_PCS_DETAIL_PROCESSOR' || CHR(10) ||
       '-- Purpose: Process PCS detail endpoints' || CHR(10) ||
       CHR(10) ||
       '-- Drop existing package' || CHR(10) ||
       'BEGIN' || CHR(10) ||
       '    EXECUTE IMMEDIATE ''DROP PACKAGE BODY PKG_PCS_DETAIL_PROCESSOR'';' || CHR(10) ||
       'EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;' || CHR(10) ||
       '/' || CHR(10) ||
       'BEGIN' || CHR(10) ||
       '    EXECUTE IMMEDIATE ''DROP PACKAGE PKG_PCS_DETAIL_PROCESSOR'';' || CHR(10) ||
       'EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;' || CHR(10) ||
       '/' || CHR(10) || CHR(10) ||
       '-- Create package specification' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE', 'PKG_PCS_DETAIL_PROCESSOR') || CHR(10) || '/' || CHR(10) ||
       '-- Create package body' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE_BODY', 'PKG_PCS_DETAIL_PROCESSOR') || CHR(10) || '/' || CHR(10) ||
       'SHOW ERRORS' || CHR(10)
FROM dual
WHERE EXISTS (SELECT 1 FROM user_objects WHERE object_name = 'PKG_PCS_DETAIL_PROCESSOR' AND object_type = 'PACKAGE');

SPOOL OFF

-- Extract PKG_MAIN_ETL_CONTROL
SPOOL ../04_packages/PKG_MAIN_ETL_CONTROL.sql

SELECT '-- Package: PKG_MAIN_ETL_CONTROL' || CHR(10) ||
       '-- Purpose: Main ETL orchestration' || CHR(10) ||
       CHR(10) ||
       '-- Drop existing package' || CHR(10) ||
       'BEGIN' || CHR(10) ||
       '    EXECUTE IMMEDIATE ''DROP PACKAGE BODY PKG_MAIN_ETL_CONTROL'';' || CHR(10) ||
       'EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;' || CHR(10) ||
       '/' || CHR(10) ||
       'BEGIN' || CHR(10) ||
       '    EXECUTE IMMEDIATE ''DROP PACKAGE PKG_MAIN_ETL_CONTROL'';' || CHR(10) ||
       'EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;' || CHR(10) ||
       '/' || CHR(10) || CHR(10) ||
       '-- Create package specification' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE', 'PKG_MAIN_ETL_CONTROL') || CHR(10) || '/' || CHR(10) ||
       '-- Create package body' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE_BODY', 'PKG_MAIN_ETL_CONTROL') || CHR(10) || '/' || CHR(10) ||
       'SHOW ERRORS' || CHR(10)
FROM dual;

SPOOL OFF

-- Extract PKG_ETL_TEST_UTILS
SPOOL ../04_packages/PKG_ETL_TEST_UTILS.sql

SELECT '-- Package: PKG_ETL_TEST_UTILS' || CHR(10) ||
       '-- Purpose: Testing utilities' || CHR(10) ||
       CHR(10) ||
       '-- Drop existing package' || CHR(10) ||
       'BEGIN' || CHR(10) ||
       '    EXECUTE IMMEDIATE ''DROP PACKAGE BODY PKG_ETL_TEST_UTILS'';' || CHR(10) ||
       'EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;' || CHR(10) ||
       '/' || CHR(10) ||
       'BEGIN' || CHR(10) ||
       '    EXECUTE IMMEDIATE ''DROP PACKAGE PKG_ETL_TEST_UTILS'';' || CHR(10) ||
       'EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;' || CHR(10) ||
       '/' || CHR(10) || CHR(10) ||
       '-- Create package specification' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE', 'PKG_ETL_TEST_UTILS') || CHR(10) || '/' || CHR(10) ||
       '-- Create package body' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE_BODY', 'PKG_ETL_TEST_UTILS') || CHR(10) || '/' || CHR(10) ||
       'SHOW ERRORS' || CHR(10)
FROM dual;

SPOOL OFF

-- Extract PKG_INDEPENDENT_ETL_CONTROL
SPOOL ../04_packages/PKG_INDEPENDENT_ETL_CONTROL.sql

SELECT '-- Package: PKG_INDEPENDENT_ETL_CONTROL' || CHR(10) ||
       '-- Purpose: VDS catalog ETL control' || CHR(10) ||
       CHR(10) ||
       '-- Drop existing package' || CHR(10) ||
       'BEGIN' || CHR(10) ||
       '    EXECUTE IMMEDIATE ''DROP PACKAGE BODY PKG_INDEPENDENT_ETL_CONTROL'';' || CHR(10) ||
       'EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;' || CHR(10) ||
       '/' || CHR(10) ||
       'BEGIN' || CHR(10) ||
       '    EXECUTE IMMEDIATE ''DROP PACKAGE PKG_INDEPENDENT_ETL_CONTROL'';' || CHR(10) ||
       'EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;' || CHR(10) ||
       '/' || CHR(10) || CHR(10) ||
       '-- Create package specification' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE', 'PKG_INDEPENDENT_ETL_CONTROL') || CHR(10) || '/' || CHR(10) ||
       '-- Create package body' || CHR(10) ||
       DBMS_METADATA.GET_DDL('PACKAGE_BODY', 'PKG_INDEPENDENT_ETL_CONTROL') || CHR(10) || '/' || CHR(10) ||
       'SHOW ERRORS' || CHR(10)
FROM dual;

SPOOL OFF

EXIT;