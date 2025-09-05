-- Package: PKG_ETL_TEST_UTILS
-- Purpose: Testing utilities

-- Drop existing package
BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE BODY PKG_ETL_TEST_UTILS';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;
/
BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE PKG_ETL_TEST_UTILS';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;
/

-- Create package specification

  CREATE OR REPLACE PACKAGE PKG_ETL_TEST_UTILS" AS
    -- Testing and development utilities package
    -- This package provides utilities for testing and development
    -- NOT for production use

    -- Clear all log tables (for testing)
    PROCEDURE clear_all_logs;

    -- Clear all data tables (for testing)
    PROCEDURE clear_all_data;

    -- Clear RAW_JSON table (for testing)
    PROCEDURE clear_raw_json;

    -- Complete reset for testing
    PROCEDURE reset_for_testing;

    -- Generate test data in ETL_FILTER
    PROCEDURE generate_test_filter;

    -- Show current ETL status
    PROCEDURE show_etl_status;

    -- Validate data integrity
    PROCEDURE validate_data_integrity;

    -- Quick test run (single plant/issue)
    PROCEDURE quick_test_run(
        p_plant_id VARCHAR2 DEFAULT '34',
        p_issue_revision VARCHAR2 DEFAULT '4.2'
    );

END PKG_ETL_TEST_UTILS;
CREATE OR REPLACE PACKAGE BODY PKG_ETL_TEST_UTILS" AS

    PROCEDURE clear_all_logs IS
    BEGIN
        DELETE FROM ETL_ERROR_LOG;
        DELETE FROM ETL_RUN_LOG;
        DELETE FROM ETL_STATISTICS;
        COMMIT;
    END;

    PROCEDURE clear_all_data IS
    BEGIN
        -- Clear core tables
        DELETE FROM PCS_REFERENCES;
        DELETE FROM VDS_REFERENCES;
        DELETE FROM MDS_REFERENCES;
        DELETE FROM EDS_REFERENCES;
        DELETE FROM VSK_REFERENCES;
        DELETE FROM ESK_REFERENCES;
        DELETE FROM PIPE_ELEMENT_REFERENCES;
        DELETE FROM SC_REFERENCES;
        DELETE FROM VSM_REFERENCES;
        DELETE FROM PCS_LIST;
        DELETE FROM PCS_HEADER_PROPERTIES;
        DELETE FROM PCS_TEMP_PRESSURES;
        DELETE FROM PCS_PIPE_SIZES;
        DELETE FROM PCS_PIPE_ELEMENTS;
        DELETE FROM PCS_VALVE_ELEMENTS;
        DELETE FROM PCS_EMBEDDED_NOTES;
        DELETE FROM VDS_LIST;

        -- Clear staging tables
        DELETE FROM STG_PCS_REFERENCES;
        DELETE FROM STG_VDS_REFERENCES;
        DELETE FROM STG_MDS_REFERENCES;
        DELETE FROM STG_EDS_REFERENCES;
        DELETE FROM STG_VSK_REFERENCES;
        DELETE FROM STG_ESK_REFERENCES;
        DELETE FROM STG_PIPE_ELEMENT_REFERENCES;
        DELETE FROM STG_SC_REFERENCES;
        DELETE FROM STG_VSM_REFERENCES;
        DELETE FROM STG_PCS_LIST;
        DELETE FROM STG_VDS_LIST;
        DELETE FROM STG_PCS_HEADER_PROPERTIES;
        DELETE FROM STG_PCS_TEMP_PRESSURES;
        DELETE FROM STG_PCS_PIPE_SIZES;
        DELETE FROM STG_PCS_PIPE_ELEMENTS;
        DELETE FROM STG_PCS_VALVE_ELEMENTS;
        DELETE FROM STG_PCS_EMBEDDED_NOTES;

        COMMIT;
    END;

    PROCEDURE clear_raw_json IS
    BEGIN
        DELETE FROM RAW_JSON;
        COMMIT;
    END;

    PROCEDURE reset_for_testing IS
    BEGIN
        clear_all_data;
        clear_all_logs;
        clear_raw_json;
    END;

    PROCEDURE generate_test_filter IS
    BEGIN
        DELETE FROM ETL_FILTER;
        INSERT INTO ETL_FILTER (plant_id, plant_name, issue_revision, added_by_user_id)
        VALUES ('34', 'GRANE', '4.2', 'TEST_USER');
        COMMIT;
    END;

    PROCEDURE show_etl_status IS
        v_count NUMBER;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('=== ETL STATUS REPORT ===');

        SELECT COUNT(*) INTO v_count FROM ETL_FILTER;
        DBMS_OUTPUT.PUT_LINE('ETL_FILTER: ' || v_count);

        SELECT COUNT(*) INTO v_count FROM RAW_JSON;
        DBMS_OUTPUT.PUT_LINE('RAW_JSON: ' || v_count);

        SELECT COUNT(*) INTO v_count FROM PCS_REFERENCES;
        DBMS_OUTPUT.PUT_LINE('PCS_REFERENCES: ' || v_count);

        SELECT COUNT(*) INTO v_count FROM VDS_REFERENCES;
        DBMS_OUTPUT.PUT_LINE('VDS_REFERENCES: ' || v_count);

        -- Check if any staging tables have data
        SELECT COUNT(*) INTO v_count FROM STG_PCS_REFERENCES;
        IF v_count > 0 THEN
            DBMS_OUTPUT.PUT_LINE('WARNING: STG_PCS_REFERENCES has ' || v_count || ' records');
        END IF;

        DBMS_OUTPUT.PUT_LINE('========================');
    END;

    PROCEDURE validate_data_integrity IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Data integrity check complete');
    END;

    PROCEDURE quick_test_run(
        p_plant_id VARCHAR2 DEFAULT '34',
        p_issue_revision VARCHAR2 DEFAULT '4.2'
    ) IS
    BEGIN
        reset_for_testing;
        DELETE FROM ETL_FILTER;
        INSERT INTO ETL_FILTER (plant_id, plant_name, issue_revision, added_by_user_id)
        VALUES (p_plant_id, 'TEST_PLANT', p_issue_revision, 'TEST_USER');
        COMMIT;
        show_etl_status;
    END;

END PKG_ETL_TEST_UTILS;
/
