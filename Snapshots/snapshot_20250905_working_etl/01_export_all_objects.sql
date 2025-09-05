-- Export all database objects for TR2000_STAGING schema
-- Date: 2025-01-05
-- Status: Working ETL Milestone (both VDS and PCS fixes applied)

SET PAGESIZE 0
SET LINESIZE 32767
SET LONG 999999999
SET LONGCHUNKSIZE 999999999
SET FEEDBACK OFF
SET HEADING OFF
SET TRIMSPOOL ON
SET SERVEROUTPUT ON

-- Export all tables
SPOOL 01_tables.sql
PROMPT -- Tables DDL for TR2000_STAGING;
PROMPT -- Generated: 2025-01-05;
PROMPT;

BEGIN
    FOR r IN (SELECT table_name FROM user_tables WHERE table_name NOT LIKE 'BIN$%' ORDER BY table_name) LOOP
        DBMS_OUTPUT.PUT_LINE(DBMS_METADATA.GET_DDL('TABLE', r.table_name, USER) || ';');
        DBMS_OUTPUT.PUT_LINE('/');
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;
END;
/

SPOOL OFF

-- Export all sequences
SPOOL 02_sequences.sql
PROMPT -- Sequences for TR2000_STAGING;
PROMPT -- Generated: 2025-01-05;
PROMPT;

BEGIN
    FOR r IN (SELECT sequence_name FROM user_sequences ORDER BY sequence_name) LOOP
        DBMS_OUTPUT.PUT_LINE(DBMS_METADATA.GET_DDL('SEQUENCE', r.sequence_name, USER) || ';');
        DBMS_OUTPUT.PUT_LINE('/');
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;
END;
/

SPOOL OFF

-- Export all indexes
SPOOL 03_indexes.sql
PROMPT -- Indexes for TR2000_STAGING;
PROMPT -- Generated: 2025-01-05;
PROMPT;

BEGIN
    FOR r IN (SELECT index_name FROM user_indexes 
              WHERE index_type != 'LOB' 
              AND generated = 'N'
              ORDER BY index_name) LOOP
        DBMS_OUTPUT.PUT_LINE(DBMS_METADATA.GET_DDL('INDEX', r.index_name, USER) || ';');
        DBMS_OUTPUT.PUT_LINE('/');
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;
END;
/

SPOOL OFF

-- Export all views
SPOOL 04_views.sql
PROMPT -- Views for TR2000_STAGING;
PROMPT -- Generated: 2025-01-05;
PROMPT;

BEGIN
    FOR r IN (SELECT view_name FROM user_views ORDER BY view_name) LOOP
        DBMS_OUTPUT.PUT_LINE(DBMS_METADATA.GET_DDL('VIEW', r.view_name, USER) || ';');
        DBMS_OUTPUT.PUT_LINE('/');
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;
END;
/

SPOOL OFF

-- Export all triggers
SPOOL 05_triggers.sql
PROMPT -- Triggers for TR2000_STAGING;
PROMPT -- Generated: 2025-01-05;
PROMPT;

BEGIN
    FOR r IN (SELECT trigger_name FROM user_triggers ORDER BY trigger_name) LOOP
        DBMS_OUTPUT.PUT_LINE(DBMS_METADATA.GET_DDL('TRIGGER', r.trigger_name, USER) || ';');
        DBMS_OUTPUT.PUT_LINE('/');
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;
END;
/

SPOOL OFF

-- Export all package specs
SPOOL 06_package_specs.sql
PROMPT -- Package Specifications for TR2000_STAGING;
PROMPT -- Generated: 2025-01-05;
PROMPT;

BEGIN
    FOR r IN (SELECT object_name FROM user_objects 
              WHERE object_type = 'PACKAGE' 
              ORDER BY object_name) LOOP
        DBMS_OUTPUT.PUT_LINE(DBMS_METADATA.GET_DDL('PACKAGE', r.object_name, USER) || ';');
        DBMS_OUTPUT.PUT_LINE('/');
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;
END;
/

SPOOL OFF

-- Export all package bodies
SPOOL 07_package_bodies.sql
PROMPT -- Package Bodies for TR2000_STAGING;
PROMPT -- Generated: 2025-01-05;
PROMPT -- CRITICAL: Contains fixed JSON paths for VDS ($.getVDS) and PCS ($.getPCS);
PROMPT;

BEGIN
    FOR r IN (SELECT object_name FROM user_objects 
              WHERE object_type = 'PACKAGE BODY' 
              ORDER BY object_name) LOOP
        DBMS_OUTPUT.PUT_LINE(DBMS_METADATA.GET_DDL('PACKAGE_BODY', r.object_name, USER) || ';');
        DBMS_OUTPUT.PUT_LINE('/');
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;
END;
/

SPOOL OFF

-- Export all procedures
SPOOL 08_procedures.sql
PROMPT -- Standalone Procedures for TR2000_STAGING;
PROMPT -- Generated: 2025-01-05;
PROMPT;

BEGIN
    FOR r IN (SELECT object_name FROM user_procedures 
              WHERE object_type = 'PROCEDURE' 
              AND procedure_name IS NULL
              ORDER BY object_name) LOOP
        DBMS_OUTPUT.PUT_LINE(DBMS_METADATA.GET_DDL('PROCEDURE', r.object_name, USER) || ';');
        DBMS_OUTPUT.PUT_LINE('/');
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;
END;
/

SPOOL OFF

-- Export control data
SPOOL 10_control_data.sql
PROMPT -- Control Data for TR2000_STAGING;
PROMPT -- Generated: 2025-01-05;
PROMPT;
PROMPT -- ETL_FILTER;
SELECT 'INSERT INTO ETL_FILTER (filter_id, plant_id, plant_name, issue_revision, added_by_user_id, added_timestamp) VALUES (' ||
       filter_id || ', ''' || plant_id || ''', ''' || plant_name || ''', ''' || 
       issue_revision || ''', ''' || added_by_user_id || ''', ' ||
       'TO_DATE(''' || TO_CHAR(added_timestamp, 'YYYY-MM-DD HH24:MI:SS') || ''', ''YYYY-MM-DD HH24:MI:SS''));'
FROM ETL_FILTER;

PROMPT;
PROMPT -- CONTROL_SETTINGS;
SELECT 'INSERT INTO CONTROL_SETTINGS (setting_key, setting_value, description) VALUES (''' ||
       setting_key || ''', ''' || setting_value || ''', ''' || description || ''');'
FROM CONTROL_SETTINGS;

PROMPT;
PROMPT -- CONTROL_ENDPOINTS;
SELECT 'INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template, description) VALUES (' ||
       endpoint_id || ', ''' || endpoint_key || ''', ''' || endpoint_template || ''', ''' || 
       NVL(description, '') || ''');'
FROM CONTROL_ENDPOINTS
ORDER BY endpoint_id;

PROMPT;
PROMPT COMMIT;;
SPOOL OFF

EXIT;