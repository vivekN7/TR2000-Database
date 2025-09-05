-- Package: PKG_INDEPENDENT_ETL_CONTROL
-- Purpose: VDS catalog ETL control

-- Drop existing package
BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE BODY PKG_INDEPENDENT_ETL_CONTROL';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;
/
BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE PKG_INDEPENDENT_ETL_CONTROL';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;
/

-- Create package specification

  CREATE OR REPLACE EDITIONABLE PACKAGE "TR2000_STAGING"."PKG_INDEPENDENT_ETL_CONTROL" AS

    -- Run VDS catalog ETL (completely independent process)
    PROCEDURE run_vds_catalog_etl;

    -- Clear VDS catalog data
    PROCEDURE clear_vds_catalog;

    -- Get VDS catalog statistics
    FUNCTION get_vds_catalog_stats RETURN VARCHAR2;

END PKG_INDEPENDENT_ETL_CONTROL;
CREATE OR REPLACE EDITIONABLE PACKAGE BODY "TR2000_STAGING"."PKG_INDEPENDENT_ETL_CONTROL" AS

    -- Clear VDS catalog data
    PROCEDURE clear_vds_catalog IS
        v_run_id NUMBER;
        v_count NUMBER;
    BEGIN
        v_run_id := PKG_ETL_LOGGING.get_current_run_id;

        DELETE FROM VDS_LIST;
        v_count := SQL%ROWCOUNT;

        PKG_ETL_LOGGING.log_clear_operation(
            p_run_id => NVL(v_run_id, 0),
            p_table_name => 'VDS_LIST',
            p_records_deleted => v_count
        );

        EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_VDS_LIST';

        COMMIT;
    END clear_vds_catalog;

    -- Get VDS catalog statistics
    FUNCTION get_vds_catalog_stats RETURN VARCHAR2 IS
        v_count NUMBER;
        v_last_update DATE;
    BEGIN
        SELECT COUNT(*), MAX(last_modified_date)
        INTO v_count, v_last_update
        FROM VDS_LIST;

        RETURN 'VDS Catalog: ' || v_count || ' items, Last Update: ' ||
               NVL(TO_CHAR(v_last_update, 'YYYY-MM-DD HH24:MI:SS'), 'Never');
    END get_vds_catalog_stats;

    -- Run VDS catalog ETL
    PROCEDURE run_vds_catalog_etl IS
        v_run_id NUMBER;
        v_stat_id NUMBER;
        v_api_stat_id NUMBER;
        v_raw_json_id NUMBER;
        v_start_time TIMESTAMP;
        v_response_size NUMBER;
        v_records_count NUMBER;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Start ETL run
        v_run_id := PKG_ETL_LOGGING.start_etl_run(
            p_run_type => 'VDS_CATALOG_ETL'
        );

        v_stat_id := PKG_ETL_LOGGING.log_operation_start(
            p_run_id => v_run_id,
            p_stat_type => 'PROCESSING',
            p_operation_name => 'VDS_CATALOG_FULL_LOAD'
        );

        BEGIN
            -- Clear existing VDS data
            clear_vds_catalog;

            -- Log API call start
            v_start_time := SYSTIMESTAMP;
            v_api_stat_id := PKG_ETL_LOGGING.log_operation_start(
                p_run_id => v_run_id,
                p_stat_type => 'API_CALL',
                p_endpoint_key => 'VDS_CATALOG',
                p_operation_name => 'fetch_vds_catalog'
            );

            -- Fetch VDS catalog from API
            v_raw_json_id := PKG_API_CLIENT.fetch_vds_catalog(
                p_batch_id => 'VDS_RUN_' || v_run_id
            );

            -- Get response size
            SELECT LENGTH(payload) INTO v_response_size
            FROM RAW_JSON WHERE raw_json_id = v_raw_json_id;

            -- Log API call success
            PKG_ETL_LOGGING.log_operation_end(
                p_stat_id => v_api_stat_id,
                p_status => 'SUCCESS',
                p_api_response_size => v_response_size,
                p_api_status_code => 200
            );

            -- Also log as quick API call for statistics
            PKG_ETL_LOGGING.log_api_call(
                p_run_id => v_run_id,
                p_endpoint_key => 'VDS_CATALOG',
                p_duration_ms => EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000,
                p_response_size => v_response_size,
                p_status_code => 200,
                p_status => 'SUCCESS'
            );

            -- Parse and load VDS data
            v_start_time := SYSTIMESTAMP;
            v_api_stat_id := PKG_ETL_LOGGING.log_operation_start(
                p_run_id => v_run_id,
                p_stat_type => 'PROCESSING',
                p_endpoint_key => 'VDS_CATALOG',
                p_operation_name => 'parse_and_load_vds_catalog'
            );

            PKG_ETL_PROCESSOR.parse_and_load_vds_catalog(v_raw_json_id);

            -- Get record count
            SELECT COUNT(*) INTO v_records_count FROM VDS_LIST;

            PKG_ETL_LOGGING.log_operation_end(
                p_stat_id => v_api_stat_id,
                p_status => 'SUCCESS',
                p_records_processed => v_records_count,
                p_records_inserted => v_records_count
            );

            -- Log overall success
            PKG_ETL_LOGGING.log_operation_end(
                p_stat_id => v_stat_id,
                p_status => 'SUCCESS',
                p_records_processed => v_records_count,
                p_error_message => 'Successfully loaded ' || v_records_count || ' VDS items'
            );

            -- End run successfully
            PKG_ETL_LOGGING.end_etl_run(v_run_id, 'SUCCESS');

            -- Output summary
            DBMS_OUTPUT.PUT_LINE('VDS Catalog ETL completed successfully');
            DBMS_OUTPUT.PUT_LINE('Records loaded: ' || v_records_count);
            DBMS_OUTPUT.PUT_LINE('Response size: ' || ROUND(v_response_size/1024/1024, 2) || ' MB');

        EXCEPTION
            WHEN OTHERS THEN
                v_error_msg := SQLERRM;

                -- Log operation failures
                IF v_api_stat_id IS NOT NULL THEN
                    PKG_ETL_LOGGING.log_operation_end(
                        p_stat_id => v_api_stat_id,
                        p_status => 'FAILED',
                        p_error_message => v_error_msg
                    );
                END IF;

                PKG_ETL_LOGGING.log_operation_end(
                    p_stat_id => v_stat_id,
                    p_status => 'FAILED',
                    p_error_message => v_error_msg
                );

                -- Log error
                PKG_ETL_LOGGING.log_error(
                    p_endpoint_key => 'VDS_CATALOG',
                    p_error_type => 'VDS_ETL_ERROR',
                    p_error_message => v_error_msg,
                    p_error_stack => DBMS_UTILITY.FORMAT_ERROR_STACK
                );

                -- End run as failed
                PKG_ETL_LOGGING.end_etl_run(v_run_id, 'FAILED');

                DBMS_OUTPUT.PUT_LINE('VDS Catalog ETL failed: ' || v_error_msg);
                RAISE;
        END;
    END run_vds_catalog_etl;

END PKG_INDEPENDENT_ETL_CONTROL;
/
-- Create package body

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "TR2000_STAGING"."PKG_INDEPENDENT_ETL_CONTROL" AS

    -- Clear VDS catalog data
    PROCEDURE clear_vds_catalog IS
        v_run_id NUMBER;
        v_count NUMBER;
    BEGIN
        v_run_id := PKG_ETL_LOGGING.get_current_run_id;

        DELETE FROM VDS_LIST;
        v_count := SQL%ROWCOUNT;

        PKG_ETL_LOGGING.log_clear_operation(
            p_run_id => NVL(v_run_id, 0),
            p_table_name => 'VDS_LIST',
            p_records_deleted => v_count
        );

        EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_VDS_LIST';

        COMMIT;
    END clear_vds_catalog;

    -- Get VDS catalog statistics
    FUNCTION get_vds_catalog_stats RETURN VARCHAR2 IS
        v_count NUMBER;
        v_last_update DATE;
    BEGIN
        SELECT COUNT(*), MAX(last_modified_date)
        INTO v_count, v_last_update
        FROM VDS_LIST;

        RETURN 'VDS Catalog: ' || v_count || ' items, Last Update: ' ||
               NVL(TO_CHAR(v_last_update, 'YYYY-MM-DD HH24:MI:SS'), 'Never');
    END get_vds_catalog_stats;

    -- Run VDS catalog ETL
    PROCEDURE run_vds_catalog_etl IS
        v_run_id NUMBER;
        v_stat_id NUMBER;
        v_api_stat_id NUMBER;
        v_raw_json_id NUMBER;
        v_start_time TIMESTAMP;
        v_response_size NUMBER;
        v_records_count NUMBER;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Start ETL run
        v_run_id := PKG_ETL_LOGGING.start_etl_run(
            p_run_type => 'VDS_CATALOG_ETL'
        );

        v_stat_id := PKG_ETL_LOGGING.log_operation_start(
            p_run_id => v_run_id,
            p_stat_type => 'PROCESSING',
            p_operation_name => 'VDS_CATALOG_FULL_LOAD'
        );

        BEGIN
            -- Clear existing VDS data
            clear_vds_catalog;

            -- Log API call start
            v_start_time := SYSTIMESTAMP;
            v_api_stat_id := PKG_ETL_LOGGING.log_operation_start(
                p_run_id => v_run_id,
                p_stat_type => 'API_CALL',
                p_endpoint_key => 'VDS_CATALOG',
                p_operation_name => 'fetch_vds_catalog'
            );

            -- Fetch VDS catalog from API
            v_raw_json_id := PKG_API_CLIENT.fetch_vds_catalog(
                p_batch_id => 'VDS_RUN_' || v_run_id
            );

            -- Get response size
            SELECT LENGTH(payload) INTO v_response_size
            FROM RAW_JSON WHERE raw_json_id = v_raw_json_id;

            -- Log API call success
            PKG_ETL_LOGGING.log_operation_end(
                p_stat_id => v_api_stat_id,
                p_status => 'SUCCESS',
                p_api_response_size => v_response_size,
                p_api_status_code => 200
            );

            -- Also log as quick API call for statistics
            PKG_ETL_LOGGING.log_api_call(
                p_run_id => v_run_id,
                p_endpoint_key => 'VDS_CATALOG',
                p_duration_ms => EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000,
                p_response_size => v_response_size,
                p_status_code => 200,
                p_status => 'SUCCESS'
            );

            -- Parse and load VDS data
            v_start_time := SYSTIMESTAMP;
            v_api_stat_id := PKG_ETL_LOGGING.log_operation_start(
                p_run_id => v_run_id,
                p_stat_type => 'PROCESSING',
                p_endpoint_key => 'VDS_CATALOG',
                p_operation_name => 'parse_and_load_vds_catalog'
            );

            PKG_ETL_PROCESSOR.parse_and_load_vds_catalog(v_raw_json_id);

            -- Get record count
            SELECT COUNT(*) INTO v_records_count FROM VDS_LIST;

            PKG_ETL_LOGGING.log_operation_end(
                p_stat_id => v_api_stat_id,
                p_status => 'SUCCESS',
                p_records_processed => v_records_count,
                p_records_inserted => v_records_count
            );

            -- Log overall success
            PKG_ETL_LOGGING.log_operation_end(
                p_stat_id => v_stat_id,
                p_status => 'SUCCESS',
                p_records_processed => v_records_count,
                p_error_message => 'Successfully loaded ' || v_records_count || ' VDS items'
            );

            -- End run successfully
            PKG_ETL_LOGGING.end_etl_run(v_run_id, 'SUCCESS');

            -- Output summary
            DBMS_OUTPUT.PUT_LINE('VDS Catalog ETL completed successfully');
            DBMS_OUTPUT.PUT_LINE('Records loaded: ' || v_records_count);
            DBMS_OUTPUT.PUT_LINE('Response size: ' || ROUND(v_response_size/1024/1024, 2) || ' MB');

        EXCEPTION
            WHEN OTHERS THEN
                v_error_msg := SQLERRM;

                -- Log operation failures
                IF v_api_stat_id IS NOT NULL THEN
                    PKG_ETL_LOGGING.log_operation_end(
                        p_stat_id => v_api_stat_id,
                        p_status => 'FAILED',
                        p_error_message => v_error_msg
                    );
                END IF;

                PKG_ETL_LOGGING.log_operation_end(
                    p_stat_id => v_stat_id,
                    p_status => 'FAILED',
                    p_error_message => v_error_msg
                );

                -- Log error
                PKG_ETL_LOGGING.log_error(
                    p_endpoint_key => 'VDS_CATALOG',
                    p_error_type => 'VDS_ETL_ERROR',
                    p_error_message => v_error_msg,
                    p_error_stack => DBMS_UTILITY.FORMAT_ERROR_STACK
                );

                -- End run as failed
                PKG_ETL_LOGGING.end_etl_run(v_run_id, 'FAILED');

                DBMS_OUTPUT.PUT_LINE('VDS Catalog ETL failed: ' || v_error_msg);
                RAISE;
        END;
    END run_vds_catalog_etl;

END PKG_INDEPENDENT_ETL_CONTROL;
/
SHOW ERRORS

