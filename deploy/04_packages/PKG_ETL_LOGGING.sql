-- Package: PKG_ETL_LOGGING
-- Purpose: ETL run and statistics logging

-- Drop existing package
BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE BODY PKG_ETL_LOGGING';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;
/
BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE PKG_ETL_LOGGING';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;
/

-- Create package specification

  CREATE OR REPLACE PACKAGE PKG_ETL_LOGGING AS

    -- Run logging procedures
    FUNCTION start_etl_run(
        p_run_type      VARCHAR2,
        p_initiated_by  VARCHAR2 DEFAULT USER
    ) RETURN NUMBER;

    PROCEDURE end_etl_run(
        p_run_id    NUMBER,
        p_status    VARCHAR2  -- SUCCESS, FAILED, WARNING
    );

    -- Statistics logging
    FUNCTION log_operation_start(
        p_run_id            NUMBER,
        p_stat_type         VARCHAR2,  -- API_CALL, PROCESSING, CLEAR, SUMMARY
        p_endpoint_key      VARCHAR2 DEFAULT NULL,
        p_operation_name    VARCHAR2 DEFAULT NULL,
        p_plant_id          VARCHAR2 DEFAULT NULL,
        p_issue_revision    VARCHAR2 DEFAULT NULL,
        p_pcs_name          VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER;

    PROCEDURE log_operation_end(
        p_stat_id           NUMBER,
        p_status            VARCHAR2,  -- SUCCESS, FAILED, WARNING
        p_records_processed NUMBER DEFAULT 0,
        p_records_inserted  NUMBER DEFAULT 0,
        p_records_updated   NUMBER DEFAULT 0,
        p_records_deleted   NUMBER DEFAULT 0,
        p_records_failed    NUMBER DEFAULT 0,
        p_api_response_size NUMBER DEFAULT NULL,
        p_api_status_code   NUMBER DEFAULT NULL,
        p_error_message     VARCHAR2 DEFAULT NULL
    );

    -- Error logging
    PROCEDURE log_error(
        p_endpoint_key      VARCHAR2,
        p_plant_id          VARCHAR2 DEFAULT NULL,
        p_issue_revision    VARCHAR2 DEFAULT NULL,
        p_error_type        VARCHAR2 DEFAULT 'PROCESSING_ERROR',
        p_error_code        VARCHAR2 DEFAULT NULL,
        p_error_message     VARCHAR2,
        p_error_stack       CLOB DEFAULT NULL,
        p_raw_data          CLOB DEFAULT NULL
    );

    -- Quick logging procedures for common operations
    PROCEDURE log_api_call(
        p_run_id            NUMBER,
        p_endpoint_key      VARCHAR2,
        p_plant_id          VARCHAR2 DEFAULT NULL,
        p_issue_revision    VARCHAR2 DEFAULT NULL,
        p_duration_ms       NUMBER,
        p_response_size     NUMBER,
        p_status_code       NUMBER,
        p_status            VARCHAR2
    );

    PROCEDURE log_clear_operation(
        p_run_id            NUMBER,
        p_table_name        VARCHAR2,
        p_records_deleted   NUMBER,
        p_duration_ms       NUMBER DEFAULT NULL
    );

    -- Summary statistics
    PROCEDURE log_run_summary(
        p_run_id            NUMBER
    );

    -- Utility to get current run_id (for nested procedures)
    FUNCTION get_current_run_id RETURN NUMBER;
    PROCEDURE set_current_run_id(p_run_id NUMBER);

END PKG_ETL_LOGGING;
/

CREATE OR REPLACE PACKAGE BODY PKG_ETL_LOGGING AS

    -- Package variable to store current run_id
    g_current_run_id NUMBER;

    -- Start ETL run
    FUNCTION start_etl_run(
        p_run_type      VARCHAR2,
        p_initiated_by  VARCHAR2 DEFAULT USER
    ) RETURN NUMBER IS
        v_run_id NUMBER;
    BEGIN
        -- Insert into ETL_RUN_LOG (uses IDENTITY column for run_id)
        INSERT INTO ETL_RUN_LOG (
            run_type, start_time, status, initiated_by
        ) VALUES (
            p_run_type, SYSTIMESTAMP, 'RUNNING', p_initiated_by
        ) RETURNING run_id INTO v_run_id;

        -- Store run_id in package variable
        g_current_run_id := v_run_id;

        COMMIT;
        RETURN v_run_id;
    END start_etl_run;

    -- End ETL run
    PROCEDURE end_etl_run(
        p_run_id    NUMBER,
        p_status    VARCHAR2  -- SUCCESS, FAILED, WARNING
    ) IS
    BEGIN
        -- Update ETL_RUN_LOG
        UPDATE ETL_RUN_LOG
        SET end_time = SYSTIMESTAMP,
            status = p_status
        WHERE run_id = p_run_id;

        -- Log summary statistics
        log_run_summary(p_run_id);

        COMMIT;
    END end_etl_run;

    -- Statistics logging
    FUNCTION log_operation_start(
        p_run_id            NUMBER,
        p_stat_type         VARCHAR2,  -- API_CALL, PROCESSING, CLEAR, SUMMARY
        p_endpoint_key      VARCHAR2 DEFAULT NULL,
        p_operation_name    VARCHAR2 DEFAULT NULL,
        p_plant_id          VARCHAR2 DEFAULT NULL,
        p_issue_revision    VARCHAR2 DEFAULT NULL,
        p_pcs_name          VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER IS
        v_stat_id NUMBER;
    BEGIN
        INSERT INTO ETL_STATISTICS (
            run_id, stat_type, endpoint_key, operation_name,
            plant_id, issue_revision, pcs_name,
            start_time, status
        ) VALUES (
            p_run_id, p_stat_type, p_endpoint_key, p_operation_name,
            p_plant_id, p_issue_revision, p_pcs_name,
            SYSTIMESTAMP, 'RUNNING'
        ) RETURNING stat_id INTO v_stat_id;

        COMMIT;
        RETURN v_stat_id;
    END log_operation_start;

    PROCEDURE log_operation_end(
        p_stat_id           NUMBER,
        p_status            VARCHAR2,  -- SUCCESS, FAILED, WARNING
        p_records_processed NUMBER DEFAULT 0,
        p_records_inserted  NUMBER DEFAULT 0,
        p_records_updated   NUMBER DEFAULT 0,
        p_records_deleted   NUMBER DEFAULT 0,
        p_records_failed    NUMBER DEFAULT 0,
        p_api_response_size NUMBER DEFAULT NULL,
        p_api_status_code   NUMBER DEFAULT NULL,
        p_error_message     VARCHAR2 DEFAULT NULL
    ) IS
    BEGIN
        UPDATE ETL_STATISTICS
        SET end_time = SYSTIMESTAMP,
            status = p_status,
            records_processed = p_records_processed,
            records_inserted = p_records_inserted,
            records_updated = p_records_updated,
            records_deleted = p_records_deleted,
            records_failed = p_records_failed,
            api_response_size = p_api_response_size,
            api_status_code = p_api_status_code,
            error_message = p_error_message
        WHERE stat_id = p_stat_id;

        COMMIT;
    END log_operation_end;

    -- Error logging
    PROCEDURE log_error(
        p_endpoint_key      VARCHAR2,
        p_plant_id          VARCHAR2 DEFAULT NULL,
        p_issue_revision    VARCHAR2 DEFAULT NULL,
        p_error_type        VARCHAR2 DEFAULT 'PROCESSING_ERROR',
        p_error_code        VARCHAR2 DEFAULT NULL,
        p_error_message     VARCHAR2,
        p_error_stack       CLOB DEFAULT NULL,
        p_raw_data          CLOB DEFAULT NULL
    ) IS
    BEGIN
        INSERT INTO ETL_ERROR_LOG (
            error_id, endpoint_key, plant_id, issue_revision,
            error_timestamp, error_type, error_code, error_message,
            error_stack, raw_data
        ) VALUES (
            DEFAULT, p_endpoint_key, p_plant_id, p_issue_revision,  -- Uses IDENTITY column
            SYSTIMESTAMP, p_error_type, p_error_code, p_error_message,
            p_error_stack, p_raw_data
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            NULL; -- Don't let logging errors break the main process
    END log_error;

    -- Quick logging procedures for common operations
    PROCEDURE log_api_call(
        p_run_id            NUMBER,
        p_endpoint_key      VARCHAR2,
        p_plant_id          VARCHAR2 DEFAULT NULL,
        p_issue_revision    VARCHAR2 DEFAULT NULL,
        p_duration_ms       NUMBER,
        p_response_size     NUMBER,
        p_status_code       NUMBER,
        p_status            VARCHAR2
    ) IS
        v_stat_id NUMBER;
    BEGIN
        INSERT INTO ETL_STATISTICS (
            run_id, stat_type, endpoint_key, plant_id, issue_revision,
            start_time, end_time, status,
            api_response_size, api_status_code
        ) VALUES (
            p_run_id, 'API_CALL', p_endpoint_key, p_plant_id, p_issue_revision,
            SYSTIMESTAMP - NUMTODSINTERVAL(p_duration_ms/1000, 'SECOND'),
            SYSTIMESTAMP, p_status,
            p_response_size, p_status_code
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            NULL; -- Don't let logging errors break the main process
    END log_api_call;

    PROCEDURE log_clear_operation(
        p_run_id            NUMBER,
        p_table_name        VARCHAR2,
        p_records_deleted   NUMBER,
        p_duration_ms       NUMBER DEFAULT NULL
    ) IS
    BEGIN
        INSERT INTO ETL_STATISTICS (
            run_id, stat_type, operation_name,
            start_time, end_time, status,
            records_deleted
        ) VALUES (
            p_run_id, 'CLEAR', 'Clear ' || p_table_name,
            SYSTIMESTAMP - NUMTODSINTERVAL(NVL(p_duration_ms, 100)/1000, 'SECOND'),
            SYSTIMESTAMP, 'SUCCESS',
            p_records_deleted
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            NULL; -- Don't let logging errors break the main process
    END log_clear_operation;

    -- Summary statistics - PROPERLY IMPLEMENTED
    PROCEDURE log_run_summary(
        p_run_id            NUMBER
    ) IS
        v_total_operations NUMBER;
        v_successful_ops NUMBER;
        v_failed_ops NUMBER;
        v_total_records NUMBER;
        v_total_api_calls NUMBER;
        v_run_duration_seconds NUMBER;
    BEGIN
        -- Calculate summary statistics
        SELECT
            COUNT(*),
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END),
            SUM(NVL(records_processed, 0) + NVL(records_inserted, 0) +
                NVL(records_updated, 0) + NVL(records_deleted, 0)),
            SUM(CASE WHEN stat_type = 'API_CALL' THEN 1 ELSE 0 END)
        INTO
            v_total_operations,
            v_successful_ops,
            v_failed_ops,
            v_total_records,
            v_total_api_calls
        FROM ETL_STATISTICS
        WHERE run_id = p_run_id;

        -- Calculate run duration
        SELECT EXTRACT(SECOND FROM (end_time - start_time)) +
               EXTRACT(MINUTE FROM (end_time - start_time)) * 60 +
               EXTRACT(HOUR FROM (end_time - start_time)) * 3600
        INTO v_run_duration_seconds
        FROM ETL_RUN_LOG
        WHERE run_id = p_run_id;

        -- Insert summary as a SUMMARY type statistic
        INSERT INTO ETL_STATISTICS (
            run_id, stat_type, operation_name,
            start_time, end_time, status,
            records_processed,
            api_status_code,  -- Using this for total operations count
            api_response_size -- Using this for API call count
        ) VALUES (
            p_run_id, 'SUMMARY', 'ETL Run Summary',
            SYSTIMESTAMP - NUMTODSINTERVAL(v_run_duration_seconds, 'SECOND'),
            SYSTIMESTAMP,
            CASE WHEN v_failed_ops = 0 THEN 'SUCCESS' ELSE 'WARNING' END,
            v_total_records,
            v_total_operations,
            v_total_api_calls
        );

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            NULL; -- Don't let summary errors break the main process
    END log_run_summary;

    -- Utility to get current run_id (for nested procedures)
    FUNCTION get_current_run_id RETURN NUMBER IS
    BEGIN
        RETURN g_current_run_id;
    END get_current_run_id;

    PROCEDURE set_current_run_id(p_run_id NUMBER) IS
    BEGIN
        g_current_run_id := p_run_id;
    END set_current_run_id;

END PKG_ETL_LOGGING;
/
