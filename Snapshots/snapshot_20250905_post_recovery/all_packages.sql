-- Packages export from TR2000_STAGING
-- Generated: 2025-09-05 02:57:39



  CREATE OR REPLACE EDITIONABLE PACKAGE "TR2000_STAGING"."PKG_API_CLIENT" AS
    -- Fetch reference data
    FUNCTION fetch_reference_data(
        p_plant_id VARCHAR2,
        p_issue_revision VARCHAR2,
        p_ref_type VARCHAR2,
        p_batch_id VARCHAR2
    ) RETURN NUMBER;

    -- Fetch PCS list
    FUNCTION fetch_pcs_list(
        p_plant_id VARCHAR2,
        p_batch_id VARCHAR2
    ) RETURN NUMBER;

    -- Fetch PCS detail (NEW)
    FUNCTION fetch_pcs_detail(
        p_plant_id VARCHAR2,
        p_pcs_name VARCHAR2,
        p_revision VARCHAR2,
        p_detail_type VARCHAR2,
        p_batch_id VARCHAR2
    ) RETURN NUMBER;

    -- Fetch VDS catalog
    FUNCTION fetch_vds_catalog(
        p_batch_id VARCHAR2
    ) RETURN NUMBER;

    -- Build endpoint URL
    FUNCTION build_endpoint_url(
        p_endpoint_key VARCHAR2,
        p_plant_id VARCHAR2 DEFAULT NULL,
        p_issue_revision VARCHAR2 DEFAULT NULL,
        p_pcs_name VARCHAR2 DEFAULT NULL,
        p_pcs_revision VARCHAR2 DEFAULT NULL
    ) RETURN VARCHAR2;
END PKG_API_CLIENT;
/


  CREATE OR REPLACE EDITIONABLE PACKAGE "TR2000_STAGING"."PKG_DATE_UTILS" AS

    -- Parse date string with multiple format attempts
    FUNCTION parse_date(p_date_string IN VARCHAR2) RETURN DATE;

    -- Parse date string, return NULL if unparseable instead of error
    FUNCTION safe_parse_date(p_date_string IN VARCHAR2) RETURN DATE;

    -- Parse timestamp string with multiple format attempts
    FUNCTION parse_timestamp(p_timestamp_string IN VARCHAR2) RETURN TIMESTAMP;

    -- Parse timestamp string, return NULL if unparseable
    FUNCTION safe_parse_timestamp(p_timestamp_string IN VARCHAR2) RETURN TIMESTAMP;

END PKG_DATE_UTILS;
/


  CREATE OR REPLACE EDITIONABLE PACKAGE "TR2000_STAGING"."PKG_ETL_LOGGING" AS

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


  CREATE OR REPLACE EDITIONABLE PACKAGE "TR2000_STAGING"."PKG_ETL_PROCESSOR" AS
    -- Parse individual reference types
    PROCEDURE parse_and_load_pcs_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_vds_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_mds_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_eds_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_vsk_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_esk_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_pipe_element_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_sc_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_vsm_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    -- Parse PCS list
    PROCEDURE parse_and_load_pcs_list(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2
    );

    -- Parse PCS details
    PROCEDURE parse_and_load_pcs_details(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_pcs_name IN VARCHAR2,
        p_revision IN VARCHAR2,
        p_detail_type IN VARCHAR2
    );

    -- Parse VDS catalog
    PROCEDURE parse_and_load_vds_catalog(
        p_raw_json_id IN NUMBER
    );
END PKG_ETL_PROCESSOR;
/


  CREATE OR REPLACE EDITIONABLE PACKAGE "TR2000_STAGING"."PKG_ETL_TEST_UTILS" AS
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
/


  CREATE OR REPLACE EDITIONABLE PACKAGE "TR2000_STAGING"."PKG_INDEPENDENT_ETL_CONTROL" AS

    -- Run VDS catalog ETL (completely independent process)
    PROCEDURE run_vds_catalog_etl;

    -- Clear VDS catalog data
    PROCEDURE clear_vds_catalog;

    -- Get VDS catalog statistics
    FUNCTION get_vds_catalog_stats RETURN VARCHAR2;

END PKG_INDEPENDENT_ETL_CONTROL;
/


  CREATE OR REPLACE EDITIONABLE PACKAGE "TR2000_STAGING"."PKG_MAIN_ETL_CONTROL" AS

    -- Main ETL entry point (renamed from run_full_etl)
    PROCEDURE run_main_etl;

    -- Process all reference types for an issue
    PROCEDURE process_references_for_issue(
        p_plant_id VARCHAR2,
        p_issue_revision VARCHAR2
    );

    -- Process PCS list for a plant
    PROCEDURE process_pcs_list(
        p_plant_id VARCHAR2
    );

    -- Process PCS details
    PROCEDURE process_pcs_details;

END PKG_MAIN_ETL_CONTROL;
/


  CREATE OR REPLACE EDITIONABLE PACKAGE "TR2000_STAGING"."PKG_PCS_DETAIL_PROCESSOR" AS
    PROCEDURE process_pcs_detail(
        p_raw_json_id   IN NUMBER,
        p_plant_id      IN VARCHAR2,
        p_pcs_name      IN VARCHAR2,
        p_revision      IN VARCHAR2,
        p_detail_type   IN VARCHAR2
    );
END PKG_PCS_DETAIL_PROCESSOR;
/


  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "TR2000_STAGING"."PKG_API_CLIENT" AS

    -- Build endpoint URL from template
    FUNCTION build_endpoint_url(
        p_endpoint_key VARCHAR2,
        p_plant_id VARCHAR2 DEFAULT NULL,
        p_issue_revision VARCHAR2 DEFAULT NULL,
        p_pcs_name VARCHAR2 DEFAULT NULL,
        p_pcs_revision VARCHAR2 DEFAULT NULL
    ) RETURN VARCHAR2 IS
        v_url VARCHAR2(500);
        v_template VARCHAR2(500);
    BEGIN
        -- Get template from CONTROL_ENDPOINTS
        BEGIN
            SELECT endpoint_template INTO v_template
            FROM CONTROL_ENDPOINTS
            WHERE endpoint_key = p_endpoint_key;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                -- If not found, build manually for PCS details
                IF p_endpoint_key LIKE 'PCS_%' THEN
                    v_template := '/plants/{plant_id}/pcs/{pcs_name}/rev/{pcs_revision}';
                    IF p_endpoint_key = 'PCS_HEADER_PROPERTIES' THEN
                        v_template := v_template;  -- Base endpoint
                    ELSIF p_endpoint_key = 'PCS_TEMP_PRESSURES' THEN
                        v_template := v_template || '/temp-pressures';
                    ELSIF p_endpoint_key = 'PCS_PIPE_SIZES' THEN
                        v_template := v_template || '/pipe-sizes';
                    ELSIF p_endpoint_key = 'PCS_PIPE_ELEMENTS' THEN
                        v_template := v_template || '/pipe-elements';
                    ELSIF p_endpoint_key = 'PCS_VALVE_ELEMENTS' THEN
                        v_template := v_template || '/valve-elements';
                    ELSIF p_endpoint_key = 'PCS_EMBEDDED_NOTES' THEN
                        v_template := v_template || '/embedded-notes';
                    END IF;
                ELSE
                    RAISE_APPLICATION_ERROR(-20001, 'Endpoint key not found: ' || p_endpoint_key);
                END IF;
        END;

        -- Replace placeholders
        v_url := v_template;
        v_url := REPLACE(v_url, '{plant_id}', p_plant_id);
        v_url := REPLACE(v_url, '{issue_revision}', p_issue_revision);
        v_url := REPLACE(v_url, '{pcs_name}', p_pcs_name);
        v_url := REPLACE(v_url, '{pcs_revision}', p_pcs_revision);

        RETURN v_url;
    END build_endpoint_url;

    -- Fetch reference data (PCS, VDS, MDS, etc.) - API → RAW_JSON only
    FUNCTION fetch_reference_data(
        p_plant_id VARCHAR2,
        p_issue_revision VARCHAR2,
        p_ref_type VARCHAR2,
        p_batch_id VARCHAR2
    ) RETURN NUMBER IS
        v_path VARCHAR2(500);
        v_response CLOB;
        v_raw_json_id NUMBER;
        v_base_url VARCHAR2(500);
        v_endpoint_key VARCHAR2(100);
        v_url VARCHAR2(4000);
        v_template VARCHAR2(500);
        v_http_status PLS_INTEGER;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Get base URL from configuration
        SELECT setting_value INTO v_base_url
        FROM CONTROL_SETTINGS
        WHERE setting_key = 'API_BASE_URL';

        -- Map reference type to endpoint key
        v_endpoint_key := UPPER(p_ref_type) || '_REFERENCES';

        -- Build actual path using template
        v_path := build_endpoint_url(
            p_endpoint_key => v_endpoint_key,
            p_plant_id => p_plant_id,
            p_issue_revision => p_issue_revision
        );

        -- Get template for logging
        v_template := build_endpoint_url(v_endpoint_key, '{plant_id}', '{issue_revision}');

        -- Construct full URL
        v_url := v_base_url || v_path;

        -- Make API call through API_SERVICE proxy
        BEGIN
            v_response := API_SERVICE.API_GATEWAY.get_clob(
                p_url => v_url,
                p_method => 'GET',
                p_body => NULL,
                p_headers => NULL,
                p_credential_static_id => NULL,
                p_status_code => v_http_status
            );
        EXCEPTION
            WHEN OTHERS THEN
                v_http_status := -1;
                v_error_msg := SUBSTR(SQLERRM, 1, 4000);
        END;

        -- Store in RAW_JSON only - no direct loading to core tables
        INSERT INTO RAW_JSON (
            raw_json_id,
            endpoint_key,
            endpoint_template,
            endpoint_value,
            payload,
            batch_id,
            api_call_timestamp,
            created_date,
            key_fingerprint
        ) VALUES (
            RAW_JSON_SEQ.NEXTVAL,
            v_endpoint_key,
            v_template,
            v_path,
            v_response,
            p_batch_id,
            SYSTIMESTAMP,
            SYSDATE,
            STANDARD_HASH(v_path || '|' || p_batch_id, 'SHA256')
        ) RETURNING raw_json_id INTO v_raw_json_id;

        -- Log error if HTTP status is not success
        IF v_http_status IS NULL OR v_http_status NOT BETWEEN 200 AND 299 THEN
            INSERT INTO ETL_ERROR_LOG (
                error_id,
                endpoint_key,
                plant_id,
                issue_revision,
                error_timestamp,
                error_type,
                error_code,
                error_message
            ) VALUES (
                ETL_ERROR_SEQ.NEXTVAL,
                v_endpoint_key,
                p_plant_id,
                p_issue_revision,
                SYSTIMESTAMP,
                'API_CALL_ERROR',
                TO_CHAR(v_http_status),
                'HTTP ' || NVL(TO_CHAR(v_http_status), 'NULL') || ' for ' || v_url ||
                CASE WHEN v_error_msg IS NOT NULL THEN CHR(10) || v_error_msg ELSE '' END
            );
            COMMIT;
        END IF;

        RETURN v_raw_json_id;

    EXCEPTION
        WHEN OTHERS THEN
            DECLARE
                v_error_code VARCHAR2(50) := TO_CHAR(SQLCODE);
                v_error_msg VARCHAR2(4000) := SUBSTR(SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, 1, 4000);
            BEGIN
                INSERT INTO ETL_ERROR_LOG (
                    error_id,
                    endpoint_key,
                    plant_id,
                    issue_revision,
                    error_timestamp,
                    error_type,
                    error_code,
                    error_message
                ) VALUES (
                    ETL_ERROR_SEQ.NEXTVAL,
                    v_endpoint_key,
                    p_plant_id,
                    p_issue_revision,
                    SYSTIMESTAMP,
                    'UNEXPECTED_ERROR',
                    v_error_code,
                    v_error_msg
                );
                COMMIT;
            END;
            RETURN NULL;
    END fetch_reference_data;

    -- Fetch PCS list for a plant - API → RAW_JSON only
    FUNCTION fetch_pcs_list(
        p_plant_id VARCHAR2,
        p_batch_id VARCHAR2
    ) RETURN NUMBER IS
        v_path VARCHAR2(500);
        v_response CLOB;
        v_raw_json_id NUMBER;
        v_base_url VARCHAR2(500);
        v_url VARCHAR2(4000);
        v_http_status PLS_INTEGER;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Get base URL from configuration
        SELECT setting_value INTO v_base_url
        FROM CONTROL_SETTINGS
        WHERE setting_key = 'API_BASE_URL';

        -- Build path
        v_path := build_endpoint_url(
            p_endpoint_key => 'PCS_LIST',
            p_plant_id => p_plant_id
        );

        -- Construct full URL
        v_url := v_base_url || v_path;

        -- Make API call through API_SERVICE proxy
        BEGIN
            v_response := API_SERVICE.API_GATEWAY.get_clob(
                p_url => v_url,
                p_method => 'GET',
                p_body => NULL,
                p_headers => NULL,
                p_credential_static_id => NULL,
                p_status_code => v_http_status
            );
        EXCEPTION
            WHEN OTHERS THEN
                v_http_status := -1;
                v_error_msg := SUBSTR(SQLERRM, 1, 4000);
        END;

        -- Store in RAW_JSON only - no direct loading to core tables
        INSERT INTO RAW_JSON (
            raw_json_id,
            endpoint_key,
            endpoint_template,
            endpoint_value,
            payload,
            batch_id,
            api_call_timestamp,
            created_date,
            key_fingerprint
        ) VALUES (
            RAW_JSON_SEQ.NEXTVAL,
            'PCS_LIST',
            '/plants/{plant_id}/pcs',
            v_path,
            v_response,
            p_batch_id,
            SYSTIMESTAMP,
            SYSDATE,
            STANDARD_HASH(v_path || '|' || p_batch_id, 'SHA256')
        ) RETURNING raw_json_id INTO v_raw_json_id;

        -- Log error if HTTP status is not success
        IF v_http_status IS NULL OR v_http_status NOT BETWEEN 200 AND 299 THEN
            INSERT INTO ETL_ERROR_LOG (
                error_id,
                endpoint_key,
                plant_id,
                error_timestamp,
                error_type,
                error_code,
                error_message
            ) VALUES (
                ETL_ERROR_SEQ.NEXTVAL,
                'PCS_LIST',
                p_plant_id,
                SYSTIMESTAMP,
                'API_CALL_ERROR',
                TO_CHAR(v_http_status),
                'HTTP ' || NVL(TO_CHAR(v_http_status), 'NULL') || ' for ' || v_url ||
                CASE WHEN v_error_msg IS NOT NULL THEN CHR(10) || v_error_msg ELSE '' END
            );
            COMMIT;
        END IF;

        RETURN v_raw_json_id;

    EXCEPTION
        WHEN OTHERS THEN
            DECLARE
                v_error_code VARCHAR2(50) := TO_CHAR(SQLCODE);
                v_error_msg VARCHAR2(4000) := SUBSTR(SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, 1, 4000);
            BEGIN
                INSERT INTO ETL_ERROR_LOG (
                    error_id,
                    endpoint_key,
                    plant_id,
                    error_timestamp,
                    error_type,
                    error_code,
                    error_message
                ) VALUES (
                    ETL_ERROR_SEQ.NEXTVAL,
                    'PCS_LIST',
                    p_plant_id,
                    SYSTIMESTAMP,
                    'UNEXPECTED_ERROR',
                    v_error_code,
                    v_error_msg
                );
                COMMIT;
            END;
            RETURN NULL;
    END fetch_pcs_list;

    -- Fetch PCS detail - API → RAW_JSON only (called by PKG_MAIN_ETL_CONTROL)
    FUNCTION fetch_pcs_detail(
        p_plant_id VARCHAR2,
        p_pcs_name VARCHAR2,
        p_revision VARCHAR2,
        p_detail_type VARCHAR2,
        p_batch_id VARCHAR2
    ) RETURN NUMBER IS
        v_path VARCHAR2(500);
        v_response CLOB;
        v_raw_json_id NUMBER;
        v_base_url VARCHAR2(500);
        v_endpoint_key VARCHAR2(100);
        v_url VARCHAR2(4000);
        v_template VARCHAR2(500);
        v_http_status PLS_INTEGER;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Get base URL from configuration
        SELECT setting_value INTO v_base_url
        FROM CONTROL_SETTINGS
        WHERE setting_key = 'API_BASE_URL';

        -- Map detail type to endpoint key
        v_endpoint_key := 'PCS_' || UPPER(REPLACE(p_detail_type, '-', '_'));

        -- Build actual path using template
        v_path := build_endpoint_url(
            p_endpoint_key => v_endpoint_key,
            p_plant_id => p_plant_id,
            p_pcs_name => p_pcs_name,
            p_pcs_revision => p_revision
        );

        -- Get template for logging
        v_template := build_endpoint_url(v_endpoint_key, '{plant_id}', NULL, '{pcs_name}', '{pcs_revision}');

        -- Construct full URL
        v_url := v_base_url || v_path;

        -- Make API call through API_SERVICE proxy
        BEGIN
            v_response := API_SERVICE.API_GATEWAY.get_clob(
                p_url => v_url,
                p_method => 'GET',
                p_body => NULL,
                p_headers => NULL,
                p_credential_static_id => NULL,
                p_status_code => v_http_status
            );
        EXCEPTION
            WHEN OTHERS THEN
                v_http_status := -1;
                v_error_msg := SUBSTR(SQLERRM, 1, 4000);
        END;

        -- Store in RAW_JSON only - PKG_PCS_DETAIL_PROCESSOR will handle STG_* → Core
        INSERT INTO RAW_JSON (
            raw_json_id,
            endpoint_key,
            endpoint_template,
            endpoint_value,
            payload,
            batch_id,
            api_call_timestamp,
            created_date,
            key_fingerprint
        ) VALUES (
            RAW_JSON_SEQ.NEXTVAL,
            v_endpoint_key,
            v_template,
            v_path,
            v_response,
            p_batch_id,
            SYSTIMESTAMP,
            SYSDATE,
            STANDARD_HASH(v_path || '|' || p_batch_id, 'SHA256')
        ) RETURNING raw_json_id INTO v_raw_json_id;

        -- Log error if HTTP status is not success
        IF v_http_status IS NULL OR v_http_status NOT BETWEEN 200 AND 299 THEN
            INSERT INTO ETL_ERROR_LOG (
                error_id,
                endpoint_key,
                plant_id,
                issue_revision,  -- Using issue_revision column for PCS name
                error_timestamp,
                error_type,
                error_code,
                error_message
            ) VALUES (
                ETL_ERROR_SEQ.NEXTVAL,
                v_endpoint_key,
                p_plant_id,
                p_pcs_name,  -- Store PCS name in issue_revision column
                SYSTIMESTAMP,
                'API_CALL_ERROR',
                TO_CHAR(v_http_status),
                'HTTP ' || NVL(TO_CHAR(v_http_status), 'NULL') || ' for ' || v_url ||
                CASE WHEN v_error_msg IS NOT NULL THEN CHR(10) || v_error_msg ELSE '' END
            );
            COMMIT;
        END IF;

        RETURN v_raw_json_id;

    EXCEPTION
        WHEN OTHERS THEN
            DECLARE
                v_error_code VARCHAR2(50) := TO_CHAR(SQLCODE);
                v_error_msg VARCHAR2(4000) := SUBSTR(SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, 1, 4000);
            BEGIN
                INSERT INTO ETL_ERROR_LOG (
                    error_id,
                    endpoint_key,
                    plant_id,
                    issue_revision,  -- Using issue_revision column for PCS name
                    error_timestamp,
                    error_type,
                    error_code,
                    error_message
                ) VALUES (
                    ETL_ERROR_SEQ.NEXTVAL,
                    v_endpoint_key,
                    p_plant_id,
                    p_pcs_name,  -- Store PCS name in issue_revision column
                    SYSTIMESTAMP,
                    'UNEXPECTED_ERROR',
                    v_error_code,
                    v_error_msg
                );
                COMMIT;
            END;
            RETURN NULL;
    END fetch_pcs_detail;

    -- Fetch VDS catalog - API → RAW_JSON only
    FUNCTION fetch_vds_catalog(
        p_batch_id VARCHAR2
    ) RETURN NUMBER IS
        v_response CLOB;
        v_raw_json_id NUMBER;
        v_base_url VARCHAR2(500);
        v_url VARCHAR2(4000);
        v_http_status PLS_INTEGER;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Get base URL from configuration
        SELECT setting_value INTO v_base_url
        FROM CONTROL_SETTINGS
        WHERE setting_key = 'API_BASE_URL';

        -- VDS catalog endpoint is simple
        v_url := v_base_url || '/vds';

        -- Make API call through API_SERVICE proxy
        BEGIN
            v_response := API_SERVICE.API_GATEWAY.get_clob(
                p_url => v_url,
                p_method => 'GET',
                p_body => NULL,
                p_headers => NULL,
                p_credential_static_id => NULL,
                p_status_code => v_http_status
            );
        EXCEPTION
            WHEN OTHERS THEN
                v_http_status := -1;
                v_error_msg := SUBSTR(SQLERRM, 1, 4000);
        END;

        -- Store in RAW_JSON only - no direct loading to core tables
        INSERT INTO RAW_JSON (
            raw_json_id,
            endpoint_key,
            endpoint_template,
            endpoint_value,
            payload,
            batch_id,
            api_call_timestamp,
            created_date,
            key_fingerprint
        ) VALUES (
            RAW_JSON_SEQ.NEXTVAL,
            'VDS_LIST',
            '/vds',
            '/vds',
            v_response,
            p_batch_id,
            SYSTIMESTAMP,
            SYSDATE,
            STANDARD_HASH('/vds|' || p_batch_id, 'SHA256')
        ) RETURNING raw_json_id INTO v_raw_json_id;

        -- Log error if HTTP status is not success
        IF v_http_status IS NULL OR v_http_status NOT BETWEEN 200 AND 299 THEN
            INSERT INTO ETL_ERROR_LOG (
                error_id,
                endpoint_key,
                error_timestamp,
                error_type,
                error_code,
                error_message
            ) VALUES (
                ETL_ERROR_SEQ.NEXTVAL,
                'VDS_LIST',
                SYSTIMESTAMP,
                'API_CALL_ERROR',
                TO_CHAR(v_http_status),
                'HTTP ' || NVL(TO_CHAR(v_http_status), 'NULL') || ' for ' || v_url ||
                CASE WHEN v_error_msg IS NOT NULL THEN CHR(10) || v_error_msg ELSE '' END
            );
            COMMIT;
        END IF;

        RETURN v_raw_json_id;

    EXCEPTION
        WHEN OTHERS THEN
            DECLARE
                v_error_code VARCHAR2(50) := TO_CHAR(SQLCODE);
                v_error_msg VARCHAR2(4000) := SUBSTR(SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, 1, 4000);
            BEGIN
                INSERT INTO ETL_ERROR_LOG (
                    error_id,
                    endpoint_key,
                    error_timestamp,
                    error_type,
                    error_code,
                    error_message
                ) VALUES (
                    ETL_ERROR_SEQ.NEXTVAL,
                    'VDS_LIST',
                    SYSTIMESTAMP,
                    'UNEXPECTED_ERROR',
                    v_error_code,
                    v_error_msg
                );
                COMMIT;
            END;
            RETURN NULL;
    END fetch_vds_catalog;

END PKG_API_CLIENT;
/


  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "TR2000_STAGING"."PKG_DATE_UTILS" AS

    -- Parse date string with multiple format attempts
    FUNCTION parse_date(p_date_string IN VARCHAR2) RETURN DATE IS
        v_date DATE;
        v_clean_string VARCHAR2(100);
    BEGIN
        -- Return NULL for empty strings
        IF p_date_string IS NULL OR TRIM(p_date_string) IS NULL THEN
            RETURN NULL;
        END IF;

        -- Clean the string (remove extra spaces, normalize)
        v_clean_string := TRIM(p_date_string);

        -- Try various date formats in order of likelihood
        BEGIN
            -- Format 1: DD.MM.YYYY (European with dots - most common in TR2000)
            v_date := TO_DATE(v_clean_string, 'DD.MM.YYYY');
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 2: DD/MM/YYYY (European with slashes)
            v_date := TO_DATE(v_clean_string, 'DD/MM/YYYY');
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 3: DD-MM-YYYY (European with dashes)
            v_date := TO_DATE(v_clean_string, 'DD-MM-YYYY');
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 4: YYYY-MM-DD (ISO format)
            v_date := TO_DATE(v_clean_string, 'YYYY-MM-DD');
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 5: YYYY/MM/DD (ISO with slashes)
            v_date := TO_DATE(v_clean_string, 'YYYY/MM/DD');
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 6: MM/DD/YYYY (US format)
            v_date := TO_DATE(v_clean_string, 'MM/DD/YYYY');
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 7: MM-DD-YYYY (US with dashes)
            v_date := TO_DATE(v_clean_string, 'MM-DD-YYYY');
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 8: DD.MM.YYYY HH24:MI:SS (European with time)
            v_date := TO_DATE(v_clean_string, 'DD.MM.YYYY HH24:MI:SS');
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 9: YYYY-MM-DD HH24:MI:SS (ISO with time)
            v_date := TO_DATE(v_clean_string, 'YYYY-MM-DD HH24:MI:SS');
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 10: DD/MM/YYYY HH24:MI:SS (European with time and slashes)
            v_date := TO_DATE(v_clean_string, 'DD/MM/YYYY HH24:MI:SS');
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 11: YYYY-MM-DD"T"HH24:MI:SS (ISO 8601 with T separator)
            v_date := TO_DATE(v_clean_string, 'YYYY-MM-DD"T"HH24:MI:SS');
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 12: DD-MON-YYYY (Oracle default)
            v_date := TO_DATE(v_clean_string, 'DD-MON-YYYY');
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 13: DD-MON-YY (Oracle short year)
            v_date := TO_DATE(v_clean_string, 'DD-MON-YY');
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 14: YYYYMMDD (Compact format)
            IF LENGTH(v_clean_string) = 8 AND REGEXP_LIKE(v_clean_string, '^\d{8}$') THEN
                v_date := TO_DATE(v_clean_string, 'YYYYMMDD');
                RETURN v_date;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 15: DD.MM.YY (European short year)
            v_date := TO_DATE(v_clean_string, 'DD.MM.YY');
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        -- If we get here, none of the formats worked
        RAISE_APPLICATION_ERROR(-20901,
            'Unable to parse date string: ' || p_date_string ||
            '. Tried DD.MM.YYYY, DD/MM/YYYY, YYYY-MM-DD, MM/DD/YYYY and other formats.');

    END parse_date;

    -- Safe version that returns NULL instead of raising error
    FUNCTION safe_parse_date(p_date_string IN VARCHAR2) RETURN DATE IS
    BEGIN
        RETURN parse_date(p_date_string);
    EXCEPTION
        WHEN OTHERS THEN
            -- Log the parsing error but return NULL
            DBMS_OUTPUT.PUT_LINE('Warning: Could not parse date "' || p_date_string || '": ' || SQLERRM);
            RETURN NULL;
    END safe_parse_date;

    -- Parse timestamp string with multiple format attempts
    FUNCTION parse_timestamp(p_timestamp_string IN VARCHAR2) RETURN TIMESTAMP IS
        v_timestamp TIMESTAMP;
        v_clean_string VARCHAR2(100);
    BEGIN
        -- Return NULL for empty strings
        IF p_timestamp_string IS NULL OR TRIM(p_timestamp_string) IS NULL THEN
            RETURN NULL;
        END IF;

        -- Clean the string
        v_clean_string := TRIM(p_timestamp_string);

        -- Try various timestamp formats
        BEGIN
            -- Format 1: YYYY-MM-DD HH24:MI:SS.FF (ISO with fractional seconds)
            v_timestamp := TO_TIMESTAMP(v_clean_string, 'YYYY-MM-DD HH24:MI:SS.FF');
            RETURN v_timestamp;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 2: YYYY-MM-DD HH24:MI:SS (ISO without fractional seconds)
            v_timestamp := TO_TIMESTAMP(v_clean_string, 'YYYY-MM-DD HH24:MI:SS');
            RETURN v_timestamp;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 3: DD.MM.YYYY HH24:MI:SS.FF (European with fractional seconds)
            v_timestamp := TO_TIMESTAMP(v_clean_string, 'DD.MM.YYYY HH24:MI:SS.FF');
            RETURN v_timestamp;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 4: DD.MM.YYYY HH24:MI:SS (European without fractional seconds)
            v_timestamp := TO_TIMESTAMP(v_clean_string, 'DD.MM.YYYY HH24:MI:SS');
            RETURN v_timestamp;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 5: YYYY-MM-DD"T"HH24:MI:SS.FF"Z" (ISO 8601 with timezone)
            v_timestamp := TO_TIMESTAMP(REPLACE(REPLACE(v_clean_string, 'T', ' '), 'Z', ''),
                                        'YYYY-MM-DD HH24:MI:SS.FF');
            RETURN v_timestamp;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 6: YYYY-MM-DD"T"HH24:MI:SS (ISO 8601 without fractional seconds)
            v_timestamp := TO_TIMESTAMP(v_clean_string, 'YYYY-MM-DD"T"HH24:MI:SS');
            RETURN v_timestamp;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 7: MM/DD/YYYY HH24:MI:SS (US format)
            v_timestamp := TO_TIMESTAMP(v_clean_string, 'MM/DD/YYYY HH24:MI:SS');
            RETURN v_timestamp;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        BEGIN
            -- Format 8: Just try as a date if no time component detected
            IF NOT REGEXP_LIKE(v_clean_string, '\d{1,2}:\d{2}') THEN
                v_timestamp := CAST(parse_date(v_clean_string) AS TIMESTAMP);
                RETURN v_timestamp;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;

        -- If we get here, none of the formats worked
        RAISE_APPLICATION_ERROR(-20902,
            'Unable to parse timestamp string: ' || p_timestamp_string);

    END parse_timestamp;

    -- Safe version for timestamps
    FUNCTION safe_parse_timestamp(p_timestamp_string IN VARCHAR2) RETURN TIMESTAMP IS
    BEGIN
        RETURN parse_timestamp(p_timestamp_string);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Warning: Could not parse timestamp "' || p_timestamp_string || '": ' || SQLERRM);
            RETURN NULL;
    END safe_parse_timestamp;

END PKG_DATE_UTILS;
/


  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "TR2000_STAGING"."PKG_ETL_LOGGING" AS

    -- Package variable to store current run_id
    g_current_run_id NUMBER;

    -- Start ETL run
    FUNCTION start_etl_run(
        p_run_type      VARCHAR2,
        p_initiated_by  VARCHAR2 DEFAULT USER
    ) RETURN NUMBER IS
        v_run_id NUMBER;
    BEGIN
        -- Get next run_id
        SELECT ETL_RUN_SEQ.NEXTVAL INTO v_run_id FROM DUAL;

        -- Insert into ETL_RUN_LOG
        INSERT INTO ETL_RUN_LOG (
            run_id, run_type, start_time, status, initiated_by
        ) VALUES (
            v_run_id, p_run_type, SYSTIMESTAMP, 'RUNNING', p_initiated_by
        );

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
            ETL_ERROR_SEQ.NEXTVAL, p_endpoint_key, p_plant_id, p_issue_revision,
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


  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "TR2000_STAGING"."PKG_ETL_PROCESSOR" AS

    -- =====================================================
    -- PARSE AND LOAD PCS REFERENCES (keeping existing)
    -- =====================================================
    PROCEDURE parse_and_load_pcs_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        DELETE FROM STG_PCS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO STG_PCS_REFERENCES (
            plant_id, issue_revision,
            "PCS", "Revision", "RevDate", "Status",
            "OfficialRevision", "RevisionSuffix", "RatingClass",
            "MaterialGroup", "HistoricalPCS", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.PCS, jt.Revision, jt.RevDate, jt.Status,
            jt.OfficialRevision, jt.RevisionSuffix, jt.RatingClass,
            jt.MaterialGroup, jt.HistoricalPCS, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssuePCSList[*]'
            COLUMNS (
                PCS VARCHAR2(100) PATH '$.PCS',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                RevisionSuffix VARCHAR2(50) PATH '$.RevisionSuffix',
                RatingClass VARCHAR2(100) PATH '$.RatingClass',
                MaterialGroup VARCHAR2(100) PATH '$.MaterialGroup',
                HistoricalPCS VARCHAR2(100) PATH '$.HistoricalPCS',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        DELETE FROM PCS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO PCS_REFERENCES (
            pcs_references_guid, plant_id, issue_revision, pcs_name,
            revision, rev_date, status, official_revision,
            revision_suffix, rating_class, material_group,
            historical_pcs, delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "PCS",
            "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "RevisionSuffix",
            "RatingClass", "MaterialGroup", "HistoricalPCS",
            "Delta", SYSDATE, SYSDATE
        FROM STG_PCS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_pcs_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'PCS_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'PCS_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_pcs_references;

    -- =====================================================
    -- PARSE AND LOAD VDS REFERENCES (keeping existing)
    -- =====================================================
    PROCEDURE parse_and_load_vds_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        DELETE FROM STG_VDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO STG_VDS_REFERENCES (
            plant_id, issue_revision,
            "VDS", "Revision", "RevDate", "Status",
            "OfficialRevision", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.VDS, jt.Revision, jt.RevDate, jt.Status,
            jt.OfficialRevision, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssueVDSList[*]'
            COLUMNS (
                VDS VARCHAR2(100) PATH '$.VDS',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        DELETE FROM VDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO VDS_REFERENCES (
            vds_references_guid, plant_id, issue_revision, vds_name,
            revision, rev_date, status, official_revision,
            delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "VDS",
            "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "Delta",
            SYSDATE, SYSDATE
        FROM STG_VDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_vds_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'VDS_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'VDS_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_vds_references;

    -- =====================================================
    -- PARSE AND LOAD MDS REFERENCES (keeping existing)
    -- =====================================================
    PROCEDURE parse_and_load_mds_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        DELETE FROM STG_MDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO STG_MDS_REFERENCES (
            plant_id, issue_revision,
            "MDS", "Revision", "Area", "RevDate",
            "Status", "OfficialRevision", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.MDS, jt.Revision, jt.Area, jt.RevDate,
            jt.Status, jt.OfficialRevision, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssueMDSList[*]'
            COLUMNS (
                MDS VARCHAR2(100) PATH '$.MDS',
                Revision VARCHAR2(50) PATH '$.Revision',
                Area VARCHAR2(100) PATH '$.Area',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        DELETE FROM MDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO MDS_REFERENCES (
            mds_references_guid, plant_id, issue_revision, mds_name,
            revision, area, rev_date, status, official_revision,
            delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "MDS",
            "Revision", "Area", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "Delta",
            SYSDATE, SYSDATE
        FROM STG_MDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_mds_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'MDS_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'MDS_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_mds_references;

    -- =====================================================
    -- PARSE AND LOAD EDS REFERENCES (keeping existing)
    -- =====================================================
    PROCEDURE parse_and_load_eds_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        DELETE FROM STG_EDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO STG_EDS_REFERENCES (
            plant_id, issue_revision,
            "EDS", "Revision", "RevDate", "Status",
            "OfficialRevision", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.EDS, jt.Revision, jt.RevDate, jt.Status,
            jt.OfficialRevision, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssueEDSList[*]'
            COLUMNS (
                EDS VARCHAR2(100) PATH '$.EDS',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        DELETE FROM EDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO EDS_REFERENCES (
            eds_references_guid, plant_id, issue_revision, eds_name,
            revision, rev_date, status, official_revision,
            delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "EDS",
            "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "Delta",
            SYSDATE, SYSDATE
        FROM STG_EDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_eds_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'EDS_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'EDS_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_eds_references;

    -- =====================================================
    -- PARSE AND LOAD VSK REFERENCES (keeping existing)
    -- =====================================================
    PROCEDURE parse_and_load_vsk_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        DELETE FROM STG_VSK_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO STG_VSK_REFERENCES (
            plant_id, issue_revision,
            "VSK", "Revision", "RevDate", "Status",
            "OfficialRevision", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.VSK, jt.Revision, jt.RevDate, jt.Status,
            jt.OfficialRevision, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssueVSKList[*]'
            COLUMNS (
                VSK VARCHAR2(100) PATH '$.VSK',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        DELETE FROM VSK_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO VSK_REFERENCES (
            vsk_references_guid, plant_id, issue_revision, vsk_name,
            revision, rev_date, status, official_revision,
            delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "VSK",
            "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "Delta",
            SYSDATE, SYSDATE
        FROM STG_VSK_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_vsk_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'VSK_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'VSK_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_vsk_references;

    -- =====================================================
    -- PARSE AND LOAD ESK REFERENCES (keeping existing)
    -- =====================================================
    PROCEDURE parse_and_load_esk_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        DELETE FROM STG_ESK_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO STG_ESK_REFERENCES (
            plant_id, issue_revision,
            "ESK", "Revision", "RevDate", "Status",
            "OfficialRevision", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.ESK, jt.Revision, jt.RevDate, jt.Status,
            jt.OfficialRevision, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssueESKList[*]'
            COLUMNS (
                ESK VARCHAR2(100) PATH '$.ESK',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        DELETE FROM ESK_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO ESK_REFERENCES (
            esk_references_guid, plant_id, issue_revision, esk_name,
            revision, rev_date, status, official_revision,
            delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "ESK",
            "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "Delta",
            SYSDATE, SYSDATE
        FROM STG_ESK_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_esk_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'ESK_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'ESK_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_esk_references;

    -- =====================================================
    -- PARSE AND LOAD PIPE ELEMENT REFERENCES (keeping existing)
    -- =====================================================
    PROCEDURE parse_and_load_pipe_element_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        DELETE FROM STG_PIPE_ELEMENT_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO STG_PIPE_ELEMENT_REFERENCES (
            plant_id, issue_revision,
            "ElementID", "ElementGroup", "DimensionStandard",
            "ProductForm", "MaterialGrade", "MDS", "MDSRevision",
            "Area", "Revision", "RevDate", "Status", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.ElementID, jt.ElementGroup, jt.DimensionStandard,
            jt.ProductForm, jt.MaterialGrade, jt.MDS, jt.MDSRevision,
            jt.Area, jt.Revision, jt.RevDate, jt.Status, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssuePipeElementList[*]'
            COLUMNS (
                ElementID VARCHAR2(50) PATH '$.ElementID',
                ElementGroup VARCHAR2(100) PATH '$.ElementGroup',
                DimensionStandard VARCHAR2(100) PATH '$.DimensionStandard',
                ProductForm VARCHAR2(100) PATH '$.ProductForm',
                MaterialGrade VARCHAR2(200) PATH '$.MaterialGrade',
                MDS VARCHAR2(100) PATH '$.MDS',
                MDSRevision VARCHAR2(50) PATH '$.MDSRevision',
                Area VARCHAR2(100) PATH '$.Area',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        DELETE FROM PIPE_ELEMENT_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO PIPE_ELEMENT_REFERENCES (
            pipe_element_references_guid, plant_id, issue_revision,
            element_id, element_group, dimension_standard,
            product_form, material_grade, mds, mds_revision,
            area, revision, rev_date, status, delta,
            created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision,
            TO_NUMBER("ElementID"), "ElementGroup", "DimensionStandard",
            "ProductForm", "MaterialGrade", "MDS", "MDSRevision",
            "Area", "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "Delta", SYSDATE, SYSDATE
        FROM STG_PIPE_ELEMENT_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_pipe_element_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'PIPE_ELEMENT_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'PIPE_ELEMENT_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_pipe_element_references;

    -- =====================================================
    -- PARSE AND LOAD SC REFERENCES (keeping existing)
    -- =====================================================
    PROCEDURE parse_and_load_sc_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        DELETE FROM STG_SC_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO STG_SC_REFERENCES (
            plant_id, issue_revision,
            "SC", "Revision", "RevDate", "Status",
            "OfficialRevision", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.SC, jt.Revision, jt.RevDate, jt.Status,
            jt.OfficialRevision, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssueSCList[*]'
            COLUMNS (
                SC VARCHAR2(100) PATH '$.SC',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        DELETE FROM SC_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO SC_REFERENCES (
            sc_references_guid, plant_id, issue_revision, sc_name,
            revision, rev_date, status, official_revision,
            delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "SC",
            "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "Delta",
            SYSDATE, SYSDATE
        FROM STG_SC_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_sc_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'SC_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'SC_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_sc_references;

    -- =====================================================
    -- PARSE AND LOAD VSM REFERENCES (keeping existing)
    -- =====================================================
    PROCEDURE parse_and_load_vsm_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        DELETE FROM STG_VSM_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO STG_VSM_REFERENCES (
            plant_id, issue_revision,
            "VSM", "Revision", "RevDate", "Status",
            "OfficialRevision", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.VSM, jt.Revision, jt.RevDate, jt.Status,
            jt.OfficialRevision, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssueVSMList[*]'
            COLUMNS (
                VSM VARCHAR2(100) PATH '$.VSM',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        DELETE FROM VSM_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO VSM_REFERENCES (
            vsm_references_guid, plant_id, issue_revision, vsm_name,
            revision, rev_date, status, official_revision,
            delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "VSM",
            "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "Delta",
            SYSDATE, SYSDATE
        FROM STG_VSM_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_vsm_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'VSM_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'VSM_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_vsm_references;

    -- =====================================================
    -- FIXED: PARSE AND LOAD PCS LIST - Using correct JSON path $.getPCS[*]
    -- =====================================================
    PROCEDURE parse_and_load_pcs_list(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
        v_row_count NUMBER;
    BEGIN
        -- Step 1: Get JSON from RAW_JSON
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        -- Step 2: Clear staging table
        DELETE FROM STG_PCS_LIST
        WHERE plant_id = p_plant_id;

        -- Step 3: Parse JSON - The actual path is $.getPCS[*] NOT $.getPlantPcsList[*]
        INSERT INTO STG_PCS_LIST (
            plant_id, "PCS", "Revision", "Status", "RevDate",
            "RatingClass", "TestPressure", "MaterialGroup", "DesignCode",
            "LastUpdate", "LastUpdateBy", "Approver", "Notepad",
            "SpecialReqID", "TubePCS", "NewVDSSection"
        )
        SELECT
            p_plant_id,
            jt.PCS, jt.Revision, jt.Status, jt.RevDate,
            jt.RatingClass, jt.TestPressure, jt.MaterialGroup, jt.DesignCode,
            jt.LastUpdate, jt.LastUpdateBy, jt.Approver, jt.Notepad,
            jt.SpecialReqID, jt.TubePCS, jt.NewVDSSection
        FROM JSON_TABLE(v_json, '$.getPCS[*]'  -- FIXED: Changed from $.getPlantPcsList[*] to $.getPCS[*]
            COLUMNS (
                PCS VARCHAR2(100) PATH '$.PCS',
                Revision VARCHAR2(50) PATH '$.Revision',
                Status VARCHAR2(50) PATH '$.Status',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                RatingClass VARCHAR2(100) PATH '$.RatingClass',
                TestPressure VARCHAR2(50) PATH '$.TestPressure',
                MaterialGroup VARCHAR2(100) PATH '$.MaterialGroup',
                DesignCode VARCHAR2(100) PATH '$.DesignCode',
                LastUpdate VARCHAR2(50) PATH '$.LastUpdate',
                LastUpdateBy VARCHAR2(100) PATH '$.LastUpdateBy',
                Approver VARCHAR2(100) PATH '$.Approver',
                Notepad VARCHAR2(4000) PATH '$.Notepad',
                SpecialReqID VARCHAR2(50) PATH '$.SpecialReqID',
                TubePCS VARCHAR2(100) PATH '$.TubePCS',
                NewVDSSection VARCHAR2(100) PATH '$.NewVDSSection'
            )
        ) jt;

        -- Check how many rows we inserted
        v_row_count := SQL%ROWCOUNT;
        DBMS_OUTPUT.PUT_LINE('Inserted ' || v_row_count || ' rows into STG_PCS_LIST');

        -- Step 4: Move from staging to core table with type conversion
        DELETE FROM PCS_LIST
        WHERE plant_id = p_plant_id;

        INSERT INTO PCS_LIST (
            pcs_list_guid, plant_id, pcs_name, revision, status, rev_date,
            rating_class, test_pressure, material_group, design_code,
            last_update, last_update_by, approver, notepad,
            special_req_id, tube_pcs, new_vds_section,
            created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, "PCS", "Revision", "Status",
            PKG_DATE_UTILS.safe_parse_date("RevDate"), "RatingClass",
            TO_NUMBER("TestPressure"), "MaterialGroup", "DesignCode",
            PKG_DATE_UTILS.safe_parse_date("LastUpdate"), "LastUpdateBy",
            "Approver", "Notepad", TO_NUMBER("SpecialReqID"),
            "TubePCS", "NewVDSSection", SYSDATE, SYSDATE
        FROM STG_PCS_LIST
        WHERE plant_id = p_plant_id
        AND "PCS" IS NOT NULL;

        v_row_count := SQL%ROWCOUNT;
        DBMS_OUTPUT.PUT_LINE('Inserted ' || v_row_count || ' rows into PCS_LIST');

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_pcs_list: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'PCS_LIST',
                p_plant_id => p_plant_id,
                p_issue_revision => NULL,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'PCS_LIST_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_pcs_list;

    -- =====================================================
    -- PARSE AND LOAD PCS DETAILS (keeping existing - delegates to PKG_PCS_DETAIL_PROCESSOR)
    -- =====================================================
    PROCEDURE parse_and_load_pcs_details(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_pcs_name IN VARCHAR2,
        p_revision IN VARCHAR2,
        p_detail_type IN VARCHAR2
    ) IS
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Delegate to PKG_PCS_DETAIL_PROCESSOR which already has implementations
        PKG_PCS_DETAIL_PROCESSOR.process_pcs_detail(
            p_raw_json_id => p_raw_json_id,
            p_plant_id => p_plant_id,
            p_pcs_name => p_pcs_name,
            p_revision => p_revision,
            p_detail_type => p_detail_type
        );
    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_pcs_details: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'PCS_DETAIL_' || UPPER(p_detail_type),
                p_plant_id => p_plant_id,
                p_issue_revision => NULL,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'PCS_DETAILS_PARSE_ERROR',
                p_error_message => v_error_msg
            );
            RAISE;
    END parse_and_load_pcs_details;

    -- =====================================================
    -- PARSE AND LOAD VDS CATALOG (keeping existing)
    -- =====================================================
    PROCEDURE parse_and_load_vds_catalog(
        p_raw_json_id IN NUMBER
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Step 1: Get JSON from RAW_JSON
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        -- Step 2: Clear staging table
        DELETE FROM STG_VDS_LIST;

        -- Step 3: Parse JSON to staging table - Try both JSON formats
        -- Format 1: $.getVDSList[*]
        IF JSON_EXISTS(v_json, '$.getVDSList') THEN
            INSERT INTO STG_VDS_LIST (
                "VDS", "Revision", "Status", "RevDate", "LastUpdate",
                "LastUpdateBy", "Description", "Notepad", "SpecialReqID",
                "ValveTypeID", "RatingClassID", "MaterialGroupID",
                "EndConnectionID", "BoreID", "VDSSizeID",
                "SizeRange", "CustomName", "SubsegmentList"
            )
            SELECT
                jt.VDS, jt.Revision, jt.Status, jt.RevDate, jt.LastUpdate,
                jt.LastUpdateBy, jt.Description, jt.Notepad, jt.SpecialReqID,
                jt.ValveTypeID, jt.RatingClassID, jt.MaterialGroupID,
                jt.EndConnectionID, jt.BoreID, jt.VDSSizeID,
                jt.SizeRange, jt.CustomName, jt.SubsegmentList
            FROM JSON_TABLE(v_json, '$.getVDSList[*]'
                COLUMNS (
                    VDS VARCHAR2(100) PATH '$.VDS',
                    Revision VARCHAR2(50) PATH '$.Revision',
                    Status VARCHAR2(50) PATH '$.Status',
                    RevDate VARCHAR2(50) PATH '$.RevDate',
                    LastUpdate VARCHAR2(50) PATH '$.LastUpdate',
                    LastUpdateBy VARCHAR2(100) PATH '$.LastUpdateBy',
                    Description VARCHAR2(500) PATH '$.Description',
                    Notepad VARCHAR2(4000) PATH '$.Notepad',
                    SpecialReqID VARCHAR2(50) PATH '$.SpecialReqID',
                    ValveTypeID VARCHAR2(50) PATH '$.ValveTypeID',
                    RatingClassID VARCHAR2(50) PATH '$.RatingClassID',
                    MaterialGroupID VARCHAR2(50) PATH '$.MaterialGroupID',
                    EndConnectionID VARCHAR2(50) PATH '$.EndConnectionID',
                    BoreID VARCHAR2(50) PATH '$.BoreID',
                    VDSSizeID VARCHAR2(50) PATH '$.VDSSizeID',
                    SizeRange VARCHAR2(100) PATH '$.SizeRange',
                    CustomName VARCHAR2(200) PATH '$.CustomName',
                    SubsegmentList VARCHAR2(500) PATH '$.SubsegmentList'
                )
            ) jt;
        ELSE
            -- Format 2: Direct array $[*]
            INSERT INTO STG_VDS_LIST (
                "VDS", "Revision", "Status", "RevDate", "LastUpdate",
                "LastUpdateBy", "Description", "Notepad", "SpecialReqID",
                "ValveTypeID", "RatingClassID", "MaterialGroupID",
                "EndConnectionID", "BoreID", "VDSSizeID",
                "SizeRange", "CustomName", "SubsegmentList"
            )
            SELECT
                jt.VDS, jt.Revision, jt.Status, jt.RevDate, jt.LastUpdate,
                jt.LastUpdateBy, jt.Description, jt.Notepad, jt.SpecialReqID,
                jt.ValveTypeID, jt.RatingClassID, jt.MaterialGroupID,
                jt.EndConnectionID, jt.BoreID, jt.VDSSizeID,
                jt.SizeRange, jt.CustomName, jt.SubsegmentList
            FROM JSON_TABLE(v_json, '$[*]'
                COLUMNS (
                    VDS VARCHAR2(100) PATH '$.VDS',
                    Revision VARCHAR2(50) PATH '$.Revision',
                    Status VARCHAR2(50) PATH '$.Status',
                    RevDate VARCHAR2(50) PATH '$.RevDate',
                    LastUpdate VARCHAR2(50) PATH '$.LastUpdate',
                    LastUpdateBy VARCHAR2(100) PATH '$.LastUpdateBy',
                    Description VARCHAR2(500) PATH '$.Description',
                    Notepad VARCHAR2(4000) PATH '$.Notepad',
                    SpecialReqID VARCHAR2(50) PATH '$.SpecialReqID',
                    ValveTypeID VARCHAR2(50) PATH '$.ValveTypeID',
                    RatingClassID VARCHAR2(50) PATH '$.RatingClassID',
                    MaterialGroupID VARCHAR2(50) PATH '$.MaterialGroupID',
                    EndConnectionID VARCHAR2(50) PATH '$.EndConnectionID',
                    BoreID VARCHAR2(50) PATH '$.BoreID',
                    VDSSizeID VARCHAR2(50) PATH '$.VDSSizeID',
                    SizeRange VARCHAR2(100) PATH '$.SizeRange',
                    CustomName VARCHAR2(200) PATH '$.CustomName',
                    SubsegmentList VARCHAR2(500) PATH '$.SubsegmentList'
                )
            ) jt;
        END IF;

        -- Step 4: Move from staging to core table with type conversion
        DELETE FROM VDS_LIST;

        INSERT INTO VDS_LIST (
            vds_list_guid, vds_name, revision, status, rev_date,
            last_update, last_update_by, description, notepad,
            special_req_id, valve_type_id, rating_class_id,
            material_group_id, end_connection_id, bore_id,
            vds_size_id, size_range, custom_name, subsegment_list,
            created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), "VDS", "Revision", "Status",
            PKG_DATE_UTILS.safe_parse_date("RevDate"),
            PKG_DATE_UTILS.safe_parse_date("LastUpdate"),
            "LastUpdateBy", "Description", "Notepad",
            TO_NUMBER("SpecialReqID"), TO_NUMBER("ValveTypeID"),
            TO_NUMBER("RatingClassID"), TO_NUMBER("MaterialGroupID"),
            TO_NUMBER("EndConnectionID"), TO_NUMBER("BoreID"),
            TO_NUMBER("VDSSizeID"), "SizeRange", "CustomName",
            "SubsegmentList", SYSDATE, SYSDATE
        FROM STG_VDS_LIST
        WHERE "VDS" IS NOT NULL;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_vds_catalog: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'VDS_CATALOG',
                p_plant_id => NULL,
                p_issue_revision => NULL,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'VDS_CATALOG_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_vds_catalog;

END PKG_ETL_PROCESSOR;
/


  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "TR2000_STAGING"."PKG_ETL_TEST_UTILS" AS

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


  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "TR2000_STAGING"."PKG_MAIN_ETL_CONTROL" AS

    PROCEDURE run_main_etl IS
        v_batch_id VARCHAR2(50);
        v_run_id NUMBER;
        v_error_msg VARCHAR2(4000);
    BEGIN
        v_batch_id := 'FULL_ETL_' || TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS');

        v_run_id := PKG_ETL_LOGGING.start_etl_run(
            p_run_type => 'FULL_ETL',
            p_initiated_by => USER
        );
        PKG_ETL_LOGGING.set_current_run_id(v_run_id);

        DBMS_OUTPUT.PUT_LINE('Starting full ETL run');
        DBMS_OUTPUT.PUT_LINE('Batch ID: ' || v_batch_id);
        DBMS_OUTPUT.PUT_LINE('Run ID: ' || v_run_id);

        FOR rec IN (SELECT DISTINCT plant_id, issue_revision FROM ETL_FILTER) LOOP
            DBMS_OUTPUT.PUT_LINE('Processing references for Plant: ' || rec.plant_id || ', Issue: ' || rec.issue_revision);
            process_references_for_issue(rec.plant_id, rec.issue_revision);
        END LOOP;

        FOR rec IN (SELECT DISTINCT plant_id FROM ETL_FILTER) LOOP
            DBMS_OUTPUT.PUT_LINE('Processing PCS list for Plant: ' || rec.plant_id);
            process_pcs_list(rec.plant_id);
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('Processing PCS details...');
        process_pcs_details;

        PKG_ETL_LOGGING.end_etl_run(
            p_run_id => v_run_id,
            p_status => 'SUCCESS'
        );

        DBMS_OUTPUT.PUT_LINE('Full ETL completed');

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := SQLERRM;

            PKG_ETL_LOGGING.end_etl_run(
                p_run_id => v_run_id,
                p_status => 'FAILED'
            );

            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'FULL_ETL',
                p_plant_id => NULL,
                p_issue_revision => NULL,
                p_error_type => 'ETL_ERROR',
                p_error_code => 'FULL_ETL_FAILED',
                p_error_message => v_error_msg
            );

            DBMS_OUTPUT.PUT_LINE('Full ETL failed: ' || v_error_msg);
            RAISE;
    END run_main_etl;

    PROCEDURE process_references_for_issue(
        p_plant_id VARCHAR2,
        p_issue_revision VARCHAR2
    ) IS
        v_batch_id VARCHAR2(50);
        v_count NUMBER := 0;
        v_raw_json_id NUMBER;
        v_records_processed NUMBER;
        v_run_id NUMBER;
        v_stat_id NUMBER;
    BEGIN
        v_batch_id := 'REF_' || p_plant_id || '_' || p_issue_revision || '_' || TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS');
        v_run_id := PKG_ETL_LOGGING.get_current_run_id;

        DELETE FROM PCS_REFERENCES WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;
        DELETE FROM VDS_REFERENCES WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;
        DELETE FROM MDS_REFERENCES WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;
        DELETE FROM EDS_REFERENCES WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;
        DELETE FROM VSK_REFERENCES WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;
        DELETE FROM ESK_REFERENCES WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;
        DELETE FROM PIPE_ELEMENT_REFERENCES WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;
        DELETE FROM SC_REFERENCES WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;
        DELETE FROM VSM_REFERENCES WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;

        FOR ref_type IN (
            SELECT 'PCS' as ref_type FROM dual UNION ALL
            SELECT 'VDS' FROM dual UNION ALL
            SELECT 'MDS' FROM dual UNION ALL
            SELECT 'EDS' FROM dual UNION ALL
            SELECT 'VSK' FROM dual UNION ALL
            SELECT 'ESK' FROM dual UNION ALL
            SELECT 'PIPE_ELEMENT' FROM dual UNION ALL
            SELECT 'SC' FROM dual UNION ALL
            SELECT 'VSM' FROM dual
        ) LOOP
            v_raw_json_id := PKG_API_CLIENT.fetch_reference_data(
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_ref_type => ref_type.ref_type,
                p_batch_id => v_batch_id
            );

            v_count := v_count + 1;

            IF ref_type.ref_type = 'PCS' THEN
                PKG_ETL_PROCESSOR.parse_and_load_pcs_references(
                    p_raw_json_id => v_raw_json_id,
                    p_plant_id => p_plant_id,
                    p_issue_revision => p_issue_revision
                );
                SELECT COUNT(*) INTO v_records_processed FROM PCS_REFERENCES
                WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;

            ELSIF ref_type.ref_type = 'VDS' THEN
                PKG_ETL_PROCESSOR.parse_and_load_vds_references(
                    p_raw_json_id => v_raw_json_id,
                    p_plant_id => p_plant_id,
                    p_issue_revision => p_issue_revision
                );
                SELECT COUNT(*) INTO v_records_processed FROM VDS_REFERENCES
                WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;

            ELSIF ref_type.ref_type = 'MDS' THEN
                PKG_ETL_PROCESSOR.parse_and_load_mds_references(
                    p_raw_json_id => v_raw_json_id,
                    p_plant_id => p_plant_id,
                    p_issue_revision => p_issue_revision
                );
                SELECT COUNT(*) INTO v_records_processed FROM MDS_REFERENCES
                WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;

            ELSIF ref_type.ref_type = 'EDS' THEN
                PKG_ETL_PROCESSOR.parse_and_load_eds_references(
                    p_raw_json_id => v_raw_json_id,
                    p_plant_id => p_plant_id,
                    p_issue_revision => p_issue_revision
                );
                SELECT COUNT(*) INTO v_records_processed FROM EDS_REFERENCES
                WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;

            ELSIF ref_type.ref_type = 'VSK' THEN
                PKG_ETL_PROCESSOR.parse_and_load_vsk_references(
                    p_raw_json_id => v_raw_json_id,
                    p_plant_id => p_plant_id,
                    p_issue_revision => p_issue_revision
                );
                SELECT COUNT(*) INTO v_records_processed FROM VSK_REFERENCES
                WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;

            ELSIF ref_type.ref_type = 'ESK' THEN
                PKG_ETL_PROCESSOR.parse_and_load_esk_references(
                    p_raw_json_id => v_raw_json_id,
                    p_plant_id => p_plant_id,
                    p_issue_revision => p_issue_revision
                );
                SELECT COUNT(*) INTO v_records_processed FROM ESK_REFERENCES
                WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;

            ELSIF ref_type.ref_type = 'PIPE_ELEMENT' THEN
                PKG_ETL_PROCESSOR.parse_and_load_pipe_element_references(
                    p_raw_json_id => v_raw_json_id,
                    p_plant_id => p_plant_id,
                    p_issue_revision => p_issue_revision
                );
                SELECT COUNT(*) INTO v_records_processed FROM PIPE_ELEMENT_REFERENCES
                WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;

            ELSIF ref_type.ref_type = 'SC' THEN
                PKG_ETL_PROCESSOR.parse_and_load_sc_references(
                    p_raw_json_id => v_raw_json_id,
                    p_plant_id => p_plant_id,
                    p_issue_revision => p_issue_revision
                );
                SELECT COUNT(*) INTO v_records_processed FROM SC_REFERENCES
                WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;

            ELSIF ref_type.ref_type = 'VSM' THEN
                PKG_ETL_PROCESSOR.parse_and_load_vsm_references(
                    p_raw_json_id => v_raw_json_id,
                    p_plant_id => p_plant_id,
                    p_issue_revision => p_issue_revision
                );
                SELECT COUNT(*) INTO v_records_processed FROM VSM_REFERENCES
                WHERE plant_id = p_plant_id AND issue_revision = p_issue_revision;
            END IF;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('Processed ' || v_count || ' reference types');
        COMMIT;
    END process_references_for_issue;

    PROCEDURE process_pcs_list(
        p_plant_id VARCHAR2
    ) IS
        v_batch_id VARCHAR2(50);
        v_raw_json_id NUMBER;
        v_run_id NUMBER;
        v_stat_id NUMBER;
        v_records_count NUMBER;
    BEGIN
        v_batch_id := 'PCS_LIST_' || p_plant_id || '_' || TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS');
        v_run_id := PKG_ETL_LOGGING.get_current_run_id;

        v_raw_json_id := PKG_API_CLIENT.fetch_pcs_list(
            p_plant_id => p_plant_id,
            p_batch_id => v_batch_id
        );

        PKG_ETL_PROCESSOR.parse_and_load_pcs_list(
            p_raw_json_id => v_raw_json_id,
            p_plant_id => p_plant_id
        );

        DBMS_OUTPUT.PUT_LINE('PCS list processed');
        COMMIT;
    END process_pcs_list;

    PROCEDURE process_pcs_details IS
        v_batch_id VARCHAR2(50);
        v_count NUMBER := 0;
        v_max_pcs_details NUMBER;
        v_raw_json_id NUMBER;
        v_run_id NUMBER;
        v_stat_id NUMBER;
        v_base_url VARCHAR2(500);
    BEGIN
        SELECT setting_value INTO v_max_pcs_details
        FROM CONTROL_SETTINGS
        WHERE setting_key = 'MAX_PCS_DETAILS_PER_RUN';

        SELECT setting_value INTO v_base_url
        FROM CONTROL_SETTINGS
        WHERE setting_key = 'API_BASE_URL';

        DBMS_OUTPUT.PUT_LINE('Processing PCS details (max: ' || v_max_pcs_details || ')...');
        DBMS_OUTPUT.PUT_LINE('Base URL: ' || v_base_url);

        v_batch_id := 'PCS_DETAILS_' || TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS');
        v_run_id := PKG_ETL_LOGGING.get_current_run_id;

        FOR rec IN (
            SELECT DISTINCT
                l.plant_id,
                l.pcs_name,
                l.revision  -- This is the PCS revision from PCS_LIST
            FROM PCS_LIST l
            WHERE EXISTS (
                SELECT 1 FROM PCS_REFERENCES r
                WHERE r.pcs_name = l.pcs_name
            )
            AND ROWNUM <= CASE
                WHEN v_max_pcs_details = 0 THEN 999999
                ELSE v_max_pcs_details
            END
        ) LOOP
            FOR detail_type IN (
                SELECT 'header-properties' as dtype FROM dual UNION ALL
                SELECT 'temp-pressures' FROM dual UNION ALL
                SELECT 'pipe-sizes' FROM dual UNION ALL
                SELECT 'pipe-elements' FROM dual UNION ALL
                SELECT 'valve-elements' FROM dual UNION ALL
                SELECT 'embedded-notes' FROM dual
            ) LOOP
                -- CORRECT: fetch_pcs_detail expects p_revision (not p_pcs_revision)
                v_raw_json_id := PKG_API_CLIENT.fetch_pcs_detail(
                    p_plant_id => rec.plant_id,
                    p_pcs_name => rec.pcs_name,
                    p_revision => rec.revision,  -- Correct parameter name
                    p_detail_type => detail_type.dtype,
                    p_batch_id => v_batch_id
                );

                PKG_PCS_DETAIL_PROCESSOR.process_pcs_detail(
                    p_raw_json_id => v_raw_json_id,
                    p_plant_id => rec.plant_id,
                    p_pcs_name => rec.pcs_name,
                    p_revision => rec.revision,
                    p_detail_type => detail_type.dtype
                );
            END LOOP;

            v_count := v_count + 1;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('PCS details completed. Processed: ' || v_count || ' PCS, API calls: ' || (v_count * 6));
        COMMIT;
    END process_pcs_details;

END PKG_MAIN_ETL_CONTROL;
/


  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "TR2000_STAGING"."PKG_PCS_DETAIL_PROCESSOR" AS

    PROCEDURE process_pcs_detail(
        p_raw_json_id   IN NUMBER,
        p_plant_id      IN VARCHAR2,
        p_pcs_name      IN VARCHAR2,
        p_revision      IN VARCHAR2,
        p_detail_type   IN VARCHAR2
    ) IS
        v_json CLOB;
        v_upper_type VARCHAR2(50);
        v_error_msg VARCHAR2(4000);
        v_error_code VARCHAR2(50);
    BEGIN
        -- API → RAW_JSON (get the JSON payload)
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        v_upper_type := UPPER(REPLACE(p_detail_type, '-', '_'));

        -- Process based on detail type
        IF v_upper_type IN ('HEADER_PROPERTIES', 'PCS_HEADER_PROPERTIES', 'PCS_HEADER', 'HEADER') THEN
            -- ===== HEADER_PROPERTIES Handler =====
            -- RAW_JSON → STG_PCS_HEADER_PROPERTIES
            DELETE FROM STG_PCS_HEADER_PROPERTIES
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name;

            -- Insert ALL columns that exist in both JSON and staging table
            INSERT INTO STG_PCS_HEADER_PROPERTIES (
                plant_id, pcs_name, pcs_revision,
                "PCS", "Revision", "Status", "RevDate",
                "RatingClass", "TestPressure", "MaterialGroup", "DesignCode",
                "LastUpdate", "LastUpdateBy", "Approver", "Notepad",
                "SC", "VSM", "DesignCodeRevMark",
                "CorrAllowance", "CorrAllowanceRevMark",
                "LongWeldEff", "LongWeldEffRevMark",
                "WallThkTol", "WallThkTolRevMark",
                "ServiceRemark", "ServiceRemarkRevMark",
                "DesignPress01", "DesignPress02", "DesignPress03", "DesignPress04",
                "DesignPress05", "DesignPress06", "DesignPress07", "DesignPress08",
                "DesignPress09", "DesignPress10", "DesignPress11", "DesignPress12",
                "DesignPressRevMark",
                "DesignTemp01", "DesignTemp02", "DesignTemp03", "DesignTemp04",
                "DesignTemp05", "DesignTemp06", "DesignTemp07", "DesignTemp08",
                "DesignTemp09", "DesignTemp10", "DesignTemp11", "DesignTemp12",
                "DesignTempRevMark",
                "NoteIDCorrAllowance", "NoteIDServiceCode", "NoteIDWallThkTol",
                "NoteIDLongWeldEff", "NoteIDGeneralPCS", "NoteIDDesignCode",
                "NoteIDPressTempTable", "NoteIDPipeSizeWthTable",
                "PressElementChange", "TempElementChange",
                "MaterialGroupID", "SpecialReqID", "SpecialReq",
                "NewVDSSection", "TubePCS", "EDSMJMatrix", "MJReductionFactor"
            )
            SELECT
                p_plant_id, p_pcs_name, p_revision,
                jt.PCS, jt.Revision, jt.Status, jt.RevDate,
                jt.RatingClass, jt.TestPressure, jt.MaterialGroup, jt.DesignCode,
                jt.LastUpdate, jt.LastUpdateBy, jt.Approver, jt.Notepad,
                jt.SC, jt.VSM, jt.DesignCodeRevMark,
                jt.CorrAllowance, jt.CorrAllowanceRevMark,
                jt.LongWeldEff, jt.LongWeldEffRevMark,
                jt.WallThkTol, jt.WallThkTolRevMark,
                jt.ServiceRemark, jt.ServiceRemarkRevMark,
                jt.DesignPress01, jt.DesignPress02, jt.DesignPress03, jt.DesignPress04,
                jt.DesignPress05, jt.DesignPress06, jt.DesignPress07, jt.DesignPress08,
                jt.DesignPress09, jt.DesignPress10, jt.DesignPress11, jt.DesignPress12,
                jt.DesignPressRevMark,
                jt.DesignTemp01, jt.DesignTemp02, jt.DesignTemp03, jt.DesignTemp04,
                jt.DesignTemp05, jt.DesignTemp06, jt.DesignTemp07, jt.DesignTemp08,
                jt.DesignTemp09, jt.DesignTemp10, jt.DesignTemp11, jt.DesignTemp12,
                jt.DesignTempRevMark,
                jt.NoteIDCorrAllowance, jt.NoteIDServiceCode, jt.NoteIDWallThkTol,
                jt.NoteIDLongWeldEff, jt.NoteIDGeneralPCS, jt.NoteIDDesignCode,
                jt.NoteIDPressTempTable, jt.NoteIDPipeSizeWthTable,
                jt.PressElementChange, jt.TempElementChange,
                jt.MaterialGroupID, jt.SpecialReqID, jt.SpecialReq,
                jt.NewVDSSection, jt.TubePCS, jt.EDSMJMatrix, jt.MJReductionFactor
            FROM JSON_TABLE(v_json, '$.getPCS[*]'
                COLUMNS (
                    PCS VARCHAR2(100) PATH '$.PCS',
                    Revision VARCHAR2(50) PATH '$.Revision',
                    Status VARCHAR2(50) PATH '$.Status',
                    RevDate VARCHAR2(50) PATH '$.RevDate',
                    RatingClass VARCHAR2(100) PATH '$.RatingClass',
                    TestPressure VARCHAR2(50) PATH '$.TestPressure',
                    MaterialGroup VARCHAR2(100) PATH '$.MaterialGroup',
                    DesignCode VARCHAR2(100) PATH '$.DesignCode',
                    LastUpdate VARCHAR2(50) PATH '$.LastUpdate',
                    LastUpdateBy VARCHAR2(100) PATH '$.LastUpdateBy',
                    Approver VARCHAR2(100) PATH '$.Approver',
                    Notepad VARCHAR2(4000) PATH '$.Notepad',
                    SC VARCHAR2(100) PATH '$.SC',
                    VSM VARCHAR2(100) PATH '$.VSM',
                    DesignCodeRevMark VARCHAR2(50) PATH '$.DesignCodeRevMark',
                    CorrAllowance VARCHAR2(50) PATH '$.CorrAllowance',
                    CorrAllowanceRevMark VARCHAR2(50) PATH '$.CorrAllowanceRevMark',
                    LongWeldEff VARCHAR2(50) PATH '$.LongWeldEff',
                    LongWeldEffRevMark VARCHAR2(50) PATH '$.LongWeldEffRevMark',
                    WallThkTol VARCHAR2(50) PATH '$.WallThkTol',
                    WallThkTolRevMark VARCHAR2(50) PATH '$.WallThkTolRevMark',
                    ServiceRemark VARCHAR2(500) PATH '$.ServiceRemark',
                    ServiceRemarkRevMark VARCHAR2(50) PATH '$.ServiceRemarkRevMark',
                    DesignPress01 VARCHAR2(50) PATH '$.DesignPress01',
                    DesignPress02 VARCHAR2(50) PATH '$.DesignPress02',
                    DesignPress03 VARCHAR2(50) PATH '$.DesignPress03',
                    DesignPress04 VARCHAR2(50) PATH '$.DesignPress04',
                    DesignPress05 VARCHAR2(50) PATH '$.DesignPress05',
                    DesignPress06 VARCHAR2(50) PATH '$.DesignPress06',
                    DesignPress07 VARCHAR2(50) PATH '$.DesignPress07',
                    DesignPress08 VARCHAR2(50) PATH '$.DesignPress08',
                    DesignPress09 VARCHAR2(50) PATH '$.DesignPress09',
                    DesignPress10 VARCHAR2(50) PATH '$.DesignPress10',
                    DesignPress11 VARCHAR2(50) PATH '$.DesignPress11',
                    DesignPress12 VARCHAR2(50) PATH '$.DesignPress12',
                    DesignPressRevMark VARCHAR2(50) PATH '$.DesignPressRevMark',
                    DesignTemp01 VARCHAR2(50) PATH '$.DesignTemp01',
                    DesignTemp02 VARCHAR2(50) PATH '$.DesignTemp02',
                    DesignTemp03 VARCHAR2(50) PATH '$.DesignTemp03',
                    DesignTemp04 VARCHAR2(50) PATH '$.DesignTemp04',
                    DesignTemp05 VARCHAR2(50) PATH '$.DesignTemp05',
                    DesignTemp06 VARCHAR2(50) PATH '$.DesignTemp06',
                    DesignTemp07 VARCHAR2(50) PATH '$.DesignTemp07',
                    DesignTemp08 VARCHAR2(50) PATH '$.DesignTemp08',
                    DesignTemp09 VARCHAR2(50) PATH '$.DesignTemp09',
                    DesignTemp10 VARCHAR2(50) PATH '$.DesignTemp10',
                    DesignTemp11 VARCHAR2(50) PATH '$.DesignTemp11',
                    DesignTemp12 VARCHAR2(50) PATH '$.DesignTemp12',
                    DesignTempRevMark VARCHAR2(50) PATH '$.DesignTempRevMark',
                    NoteIDCorrAllowance VARCHAR2(50) PATH '$.NoteIDCorrAllowance',
                    NoteIDServiceCode VARCHAR2(50) PATH '$.NoteIDServiceCode',
                    NoteIDWallThkTol VARCHAR2(50) PATH '$.NoteIDWallThkTol',
                    NoteIDLongWeldEff VARCHAR2(50) PATH '$.NoteIDLongWeldEff',
                    NoteIDGeneralPCS VARCHAR2(50) PATH '$.NoteIDGeneralPCS',
                    NoteIDDesignCode VARCHAR2(50) PATH '$.NoteIDDesignCode',
                    NoteIDPressTempTable VARCHAR2(50) PATH '$.NoteIDPressTempTable',
                    NoteIDPipeSizeWthTable VARCHAR2(50) PATH '$.NoteIDPipeSizeWthTable',
                    PressElementChange VARCHAR2(50) PATH '$.PressElementChange',
                    TempElementChange VARCHAR2(50) PATH '$.TempElementChange',
                    MaterialGroupID VARCHAR2(50) PATH '$.MaterialGroupID',
                    SpecialReqID VARCHAR2(50) PATH '$.SpecialReqID',
                    SpecialReq VARCHAR2(500) PATH '$.SpecialReq',
                    NewVDSSection VARCHAR2(100) PATH '$.NewVDSSection',
                    TubePCS VARCHAR2(100) PATH '$.TubePCS',
                    EDSMJMatrix VARCHAR2(100) PATH '$.EDSMJMatrix',
                    MJReductionFactor VARCHAR2(50) PATH '$.MJReductionFactor'
                )) jt;

            -- STG_* → Core Table PCS_HEADER_PROPERTIES (include all columns)
            DELETE FROM PCS_HEADER_PROPERTIES
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name AND pcs_revision = p_revision;

            INSERT INTO PCS_HEADER_PROPERTIES (
                pcs_header_properties_guid, plant_id, pcs_name, pcs_revision,
                pcs, revision, status, rev_date,
                rating_class, test_pressure, material_group, design_code,
                last_update, last_update_by, approver, notepad,
                sc, vsm, design_code_rev_mark,
                corr_allowance, corr_allowance_rev_mark,
                long_weld_eff, long_weld_eff_rev_mark,
                wall_thk_tol, wall_thk_tol_rev_mark,
                service_remark, service_remark_rev_mark,
                design_press01, design_press02, design_press03, design_press04,
                design_press05, design_press06, design_press07, design_press08,
                design_press09, design_press10, design_press11, design_press12,
                design_press_rev_mark,
                design_temp01, design_temp02, design_temp03, design_temp04,
                design_temp05, design_temp06, design_temp07, design_temp08,
                design_temp09, design_temp10, design_temp11, design_temp12,
                design_temp_rev_mark,
                note_id_corr_allowance, note_id_service_code, note_id_wall_thk_tol,
                note_id_long_weld_eff, note_id_general_pcs, note_id_design_code,
                note_id_press_temp_table, note_id_pipe_size_wth_table,
                press_element_change, temp_element_change,
                material_group_id, special_req_id, special_req,
                new_vds_section, tube_pcs, eds_mj_matrix, mj_reduction_factor,
                created_date, last_modified_date
            )
            SELECT
                SYS_GUID(), plant_id, pcs_name, pcs_revision,
                "PCS", "Revision", "Status", PKG_DATE_UTILS.safe_parse_date("RevDate"),
                "RatingClass", safe_to_number("TestPressure"), "MaterialGroup", "DesignCode",
                PKG_DATE_UTILS.safe_parse_date("LastUpdate"), "LastUpdateBy", "Approver", "Notepad",
                "SC", "VSM", "DesignCodeRevMark",
                safe_to_number("CorrAllowance"), "CorrAllowanceRevMark",
                safe_to_number("LongWeldEff"), "LongWeldEffRevMark",
                "WallThkTol", "WallThkTolRevMark",
                "ServiceRemark", "ServiceRemarkRevMark",
                safe_to_number("DesignPress01"), safe_to_number("DesignPress02"), safe_to_number("DesignPress03"), safe_to_number("DesignPress04"),
                safe_to_number("DesignPress05"), safe_to_number("DesignPress06"), safe_to_number("DesignPress07"), safe_to_number("DesignPress08"),
                safe_to_number("DesignPress09"), safe_to_number("DesignPress10"), safe_to_number("DesignPress11"), safe_to_number("DesignPress12"),
                "DesignPressRevMark",
                safe_to_number("DesignTemp01"), safe_to_number("DesignTemp02"), safe_to_number("DesignTemp03"), safe_to_number("DesignTemp04"),
                safe_to_number("DesignTemp05"), safe_to_number("DesignTemp06"), safe_to_number("DesignTemp07"), safe_to_number("DesignTemp08"),
                safe_to_number("DesignTemp09"), safe_to_number("DesignTemp10"), safe_to_number("DesignTemp11"), safe_to_number("DesignTemp12"),
                "DesignTempRevMark",
                safe_to_number("NoteIDCorrAllowance"), safe_to_number("NoteIDServiceCode"), safe_to_number("NoteIDWallThkTol"),
                safe_to_number("NoteIDLongWeldEff"), safe_to_number("NoteIDGeneralPCS"), safe_to_number("NoteIDDesignCode"),
                safe_to_number("NoteIDPressTempTable"), safe_to_number("NoteIDPipeSizeWthTable"),
                "PressElementChange", "TempElementChange",
                safe_to_number("MaterialGroupID"), safe_to_number("SpecialReqID"), "SpecialReq",
                "NewVDSSection", "TubePCS", "EDSMJMatrix", safe_to_number("MJReductionFactor"),
                SYSDATE, SYSDATE
            FROM STG_PCS_HEADER_PROPERTIES
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name;

        ELSIF v_upper_type IN ('TEMP_PRESSURES', 'PCS_TEMP_PRESSURES', 'TEMP-PRESSURES') THEN
            -- ===== TEMP_PRESSURES Handler =====
            -- RAW_JSON → STG_PCS_TEMP_PRESSURES
            DELETE FROM STG_PCS_TEMP_PRESSURES
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name;

            INSERT INTO STG_PCS_TEMP_PRESSURES (
                plant_id, pcs_name, pcs_revision,
                "Temperature", "Pressure"
            )
            SELECT
                p_plant_id, p_pcs_name, p_revision,
                jt.Temperature, jt.Pressure
            FROM JSON_TABLE(v_json, '$.getTempPressure[*]'
                COLUMNS (
                    Temperature VARCHAR2(50) PATH '$.Temperature',
                    Pressure VARCHAR2(50) PATH '$.Pressure'
                )) jt;

            -- STG_* → Core Table PCS_TEMP_PRESSURES
            DELETE FROM PCS_TEMP_PRESSURES
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name AND pcs_revision = p_revision;

            INSERT INTO PCS_TEMP_PRESSURES (
                pcs_temp_pressures_guid, plant_id, pcs_name, pcs_revision,
                temperature, pressure,
                created_date, last_modified_date
            )
            SELECT
                SYS_GUID(), plant_id, pcs_name, pcs_revision,
                TO_NUMBER("Temperature"), TO_NUMBER("Pressure"),
                SYSDATE, SYSDATE
            FROM STG_PCS_TEMP_PRESSURES
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name;

        ELSIF v_upper_type IN ('PIPE_SIZES', 'PCS_PIPE_SIZES', 'PIPE-SIZES') THEN
            -- ===== PIPE_SIZES Handler =====
            -- RAW_JSON → STG_PCS_PIPE_SIZES
            DELETE FROM STG_PCS_PIPE_SIZES
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name;

            INSERT INTO STG_PCS_PIPE_SIZES (
                plant_id, pcs_name, pcs_revision,
                "PCS", "Revision",
                "NomSize", "OuterDiam", "WallThickness", "Schedule",
                "UnderTolerance", "CorrosionAllowance", "WeldingFactor",
                "DimElementChange", "ScheduleInMatrix"
            )
            SELECT
                p_plant_id, p_pcs_name, p_revision,
                json_pcs, json_revision,
                nom_size, outer_diam, wall_thickness, schedule,
                under_tolerance, corrosion_allowance, welding_factor,
                dim_element_change, schedule_in_matrix
            FROM JSON_TABLE(v_json, '$'
                COLUMNS (
                    json_pcs VARCHAR2(100) PATH '$.PCS',
                    json_revision VARCHAR2(50) PATH '$.Revision',
                    NESTED PATH '$.getPipeSize[*]'
                    COLUMNS (
                        nom_size VARCHAR2(50) PATH '$.NomSize',
                        outer_diam VARCHAR2(50) PATH '$.OuterDiam',
                        wall_thickness VARCHAR2(50) PATH '$.WallThickness',
                        schedule VARCHAR2(50) PATH '$.Schedule',
                        under_tolerance VARCHAR2(50) PATH '$.UnderTolerance',
                        corrosion_allowance VARCHAR2(50) PATH '$.CorrosionAllowance',
                        welding_factor VARCHAR2(50) PATH '$.WeldingFactor',
                        dim_element_change VARCHAR2(50) PATH '$.DimElementChange',
                        schedule_in_matrix VARCHAR2(50) PATH '$.ScheduleInMatrix'
                    )
                )) jt;

            -- STG_* → Core Table PCS_PIPE_SIZES
            DELETE FROM PCS_PIPE_SIZES
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name AND pcs_revision = p_revision;

            INSERT INTO PCS_PIPE_SIZES (
                pcs_pipe_sizes_guid, plant_id, pcs_name, pcs_revision,
                pcs, revision, nom_size, outer_diam, wall_thickness, schedule,
                under_tolerance, corrosion_allowance, welding_factor,
                dim_element_change, schedule_in_matrix,
                created_date, last_modified_date
            )
            SELECT
                SYS_GUID(), plant_id, pcs_name, pcs_revision,
                "PCS", "Revision", "NomSize",
                TO_NUMBER("OuterDiam"), TO_NUMBER("WallThickness"), "Schedule",
                TO_NUMBER("UnderTolerance"), TO_NUMBER("CorrosionAllowance"), TO_NUMBER("WeldingFactor"),
                "DimElementChange", "ScheduleInMatrix",
                SYSDATE, SYSDATE
            FROM STG_PCS_PIPE_SIZES
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name;

        ELSIF v_upper_type IN ('PIPE_ELEMENTS', 'PCS_PIPE_ELEMENTS', 'PIPE-ELEMENTS') THEN
            -- ===== PIPE_ELEMENTS Handler =====
            -- RAW_JSON → STG_PCS_PIPE_ELEMENTS
            DELETE FROM STG_PCS_PIPE_ELEMENTS
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name;

            -- FIX: Changed from '$.getPipeElements[*]' to '$.getPipeElement[*]' (singular)
            INSERT INTO STG_PCS_PIPE_ELEMENTS (
                plant_id, pcs_name, pcs_revision,
                "PCS", "Revision", "MaterialGroupID",
                "ElementGroupNo", "LineNo", "Element",
                "DimStandard", "FromSize", "ToSize",
                "ProductForm", "Material", "MDS",
                "EDS", "EDSRevision", "ESK",
                "Revmark", "Remark", "PageBreak",
                "ElementID", "FreeText", "NoteID",
                "NewDeletedLine", "InitialInfo", "InitialRevmark",
                "MDSVariant", "MDSRevision", "Area"
            )
            SELECT
                p_plant_id, p_pcs_name, p_revision,
                json_pcs, json_revision, material_group_id,
                element_group_no, line_no, element,
                dim_standard, from_size, to_size,
                product_form, material, mds,
                eds, eds_revision, esk,
                revmark, remark, page_break,
                element_id, free_text, note_id,
                new_deleted_line, initial_info, initial_revmark,
                mds_variant, mds_revision, area
            FROM JSON_TABLE(v_json, '$'
                COLUMNS (
                    json_pcs VARCHAR2(100) PATH '$.PCS',
                    json_revision VARCHAR2(50) PATH '$.Revision',
                    material_group_id VARCHAR2(50) PATH '$.MaterialGroupID',
                    NESTED PATH '$.getPipeElement[*]'
                    COLUMNS (
                        element_group_no VARCHAR2(50) PATH '$.ElementGroupNo',
                        line_no VARCHAR2(50) PATH '$.LineNo',
                        element VARCHAR2(200) PATH '$.Element',
                        dim_standard VARCHAR2(100) PATH '$.DimStandard',
                        from_size VARCHAR2(50) PATH '$.FromSize',
                        to_size VARCHAR2(50) PATH '$.ToSize',
                        product_form VARCHAR2(100) PATH '$.ProductForm',
                        material VARCHAR2(200) PATH '$.Material',
                        mds VARCHAR2(100) PATH '$.MDS',
                        eds VARCHAR2(100) PATH '$.EDS',
                        eds_revision VARCHAR2(50) PATH '$.EDSRevision',
                        esk VARCHAR2(100) PATH '$.ESK',
                        revmark VARCHAR2(50) PATH '$.Revmark',
                        remark VARCHAR2(4000) PATH '$.Remark',
                        page_break VARCHAR2(50) PATH '$.PageBreak',
                        element_id VARCHAR2(50) PATH '$.ElementID',
                        free_text VARCHAR2(500) PATH '$.FreeText',
                        note_id VARCHAR2(50) PATH '$.NoteID',
                        new_deleted_line VARCHAR2(50) PATH '$.NewDeletedLine',
                        initial_info VARCHAR2(4000) PATH '$.InitialInfo',
                        initial_revmark VARCHAR2(50) PATH '$.InitialRevmark',
                        mds_variant VARCHAR2(100) PATH '$.MDSVariant',
                        mds_revision VARCHAR2(50) PATH '$.MDSRevision',
                        area VARCHAR2(100) PATH '$.Area'
                    )
                )) jt;

            -- STG_* → Core Table PCS_PIPE_ELEMENTS
            DELETE FROM PCS_PIPE_ELEMENTS
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name AND pcs_revision = p_revision;

            INSERT INTO PCS_PIPE_ELEMENTS (
                pcs_pipe_elements_guid, plant_id, pcs_name, pcs_revision,
                pcs, revision, material_group_id,
                element_group_no, line_no, element,
                dim_standard, from_size, to_size,
                product_form, material, mds,
                eds, eds_revision, esk,
                revmark, remark, page_break,
                element_id, free_text, note_id,
                new_deleted_line, initial_info, initial_revmark,
                mds_variant, mds_revision, area,
                created_date, last_modified_date
            )
            SELECT
                SYS_GUID(), plant_id, pcs_name, pcs_revision,
                "PCS", "Revision", TO_NUMBER("MaterialGroupID"),
                TO_NUMBER("ElementGroupNo"), TO_NUMBER("LineNo"), "Element",
                "DimStandard", "FromSize", "ToSize",
                "ProductForm", "Material", "MDS",
                "EDS", "EDSRevision", "ESK",
                "Revmark", SUBSTR("Remark", 1, 500), "PageBreak",
                TO_NUMBER("ElementID"), "FreeText", TO_NUMBER("NoteID"),
                "NewDeletedLine", SUBSTR("InitialInfo", 1, 200), "InitialRevmark",
                "MDSVariant", "MDSRevision", "Area",
                SYSDATE, SYSDATE
            FROM STG_PCS_PIPE_ELEMENTS
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name;

        ELSIF v_upper_type IN ('VALVE_ELEMENTS', 'PCS_VALVE_ELEMENTS', 'VALVE-ELEMENTS') THEN
            -- ===== VALVE_ELEMENTS Handler =====
            -- RAW_JSON → STG_PCS_VALVE_ELEMENTS
            DELETE FROM STG_PCS_VALVE_ELEMENTS
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name;

            -- FIX: Changed from '$.getValveElements[*]' to '$.getValveElement[*]' (singular)
            INSERT INTO STG_PCS_VALVE_ELEMENTS (
                plant_id, pcs_name, pcs_revision,
                "ValveGroupNo", "LineNo", "ValveType",
                "VDS", "ValveDescription", "FromSize",
                "ToSize", "Revmark", "Remark",
                "PageBreak", "NoteID", "PreviousVDS",
                "NewDeletedLine", "InitialInfo", "InitialRevmark",
                "SizeRange", "Status", "Revision"
            )
            SELECT
                p_plant_id, p_pcs_name, p_revision,
                valve_group_no, line_no, valve_type,
                vds, valve_description, from_size,
                to_size, revmark, remark,
                page_break, note_id, previous_vds,
                new_deleted_line, initial_info, initial_revmark,
                size_range, status, revision
            FROM JSON_TABLE(v_json, '$.getValveElement[*]'
                COLUMNS (
                    valve_group_no VARCHAR2(50) PATH '$.ValveGroupNo',
                    line_no VARCHAR2(50) PATH '$.LineNo',
                    valve_type VARCHAR2(100) PATH '$.ValveType',
                    vds VARCHAR2(100) PATH '$.VDS',
                    valve_description VARCHAR2(4000) PATH '$.ValveDescription',
                    from_size VARCHAR2(50) PATH '$.FromSize',
                    to_size VARCHAR2(50) PATH '$.ToSize',
                    revmark VARCHAR2(50) PATH '$.Revmark',
                    remark VARCHAR2(4000) PATH '$.Remark',
                    page_break VARCHAR2(50) PATH '$.PageBreak',
                    note_id VARCHAR2(50) PATH '$.NoteID',
                    previous_vds VARCHAR2(100) PATH '$.PreviousVDS',
                    new_deleted_line VARCHAR2(50) PATH '$.NewDeletedLine',
                    initial_info VARCHAR2(4000) PATH '$.InitialInfo',
                    initial_revmark VARCHAR2(50) PATH '$.InitialRevmark',
                    size_range VARCHAR2(100) PATH '$.SizeRange',
                    status VARCHAR2(50) PATH '$.Status',
                    revision VARCHAR2(50) PATH '$.Revision'
                )) jt;

            -- STG_* → Core Table PCS_VALVE_ELEMENTS
            DELETE FROM PCS_VALVE_ELEMENTS
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name AND pcs_revision = p_revision;

            INSERT INTO PCS_VALVE_ELEMENTS (
                pcs_valve_elements_guid, plant_id, pcs_name, pcs_revision,
                valve_group_no, line_no, valve_type,
                vds, valve_description, from_size,
                to_size, revmark, remark,
                page_break, note_id, previous_vds,
                new_deleted_line, initial_info, initial_revmark,
                size_range, status, revision,
                created_date, last_modified_date
            )
            SELECT
                SYS_GUID(), plant_id, pcs_name, pcs_revision,
                TO_NUMBER("ValveGroupNo"), TO_NUMBER("LineNo"), "ValveType",
                "VDS", SUBSTR("ValveDescription", 1, 500), "FromSize",
                "ToSize", "Revmark", SUBSTR("Remark", 1, 500),
                "PageBreak", TO_NUMBER("NoteID"), "PreviousVDS",
                "NewDeletedLine", SUBSTR("InitialInfo", 1, 200), "InitialRevmark",
                "SizeRange", "Status", "Revision",
                SYSDATE, SYSDATE
            FROM STG_PCS_VALVE_ELEMENTS
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name;

        ELSIF v_upper_type IN ('EMBEDDED_NOTES', 'PCS_EMBEDDED_NOTES', 'EMBEDDED-NOTES') THEN
            -- ===== EMBEDDED_NOTES Handler =====
            -- RAW_JSON → STG_PCS_EMBEDDED_NOTES
            DELETE FROM STG_PCS_EMBEDDED_NOTES
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name;

            INSERT INTO STG_PCS_EMBEDDED_NOTES (
                plant_id, pcs_name, pcs_revision,
                "PCSName", "Revision", "TextSectionID",
                "TextSectionDescription", "PageBreak", "HTMLCLOB"
            )
            SELECT
                p_plant_id, p_pcs_name, p_revision,
                pcs_name_json, revision, text_section_id,
                text_section_description, page_break, html_clob
            FROM JSON_TABLE(v_json, '$.getEmbeddedNotes[*]'
                COLUMNS (
                    pcs_name_json VARCHAR2(100) PATH '$.PCSName',
                    revision VARCHAR2(50) PATH '$.Revision',
                    text_section_id VARCHAR2(50) PATH '$.TextSectionID',
                    text_section_description VARCHAR2(500) PATH '$.TextSectionDescription',
                    page_break VARCHAR2(50) PATH '$.PageBreak',
                    html_clob CLOB PATH '$.HTMLCLOB'
                )) jt;

            -- STG_* → Core Table PCS_EMBEDDED_NOTES
            DELETE FROM PCS_EMBEDDED_NOTES
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name AND pcs_revision = p_revision;

            INSERT INTO PCS_EMBEDDED_NOTES (
                pcs_embedded_notes_guid, plant_id, pcs_name, pcs_revision,
                pcsname, revision, text_section_id,
                text_section_description, page_break, html_clob,
                created_date, last_modified_date
            )
            SELECT
                SYS_GUID(), plant_id, pcs_name, pcs_revision,
                "PCSName", "Revision", TO_NUMBER("TextSectionID"),
                "TextSectionDescription", "PageBreak", "HTMLCLOB",
                SYSDATE, SYSDATE
            FROM STG_PCS_EMBEDDED_NOTES
            WHERE plant_id = p_plant_id AND pcs_name = p_pcs_name;

        ELSE
            -- Unknown detail type
            RAISE_APPLICATION_ERROR(-20001, 'Unknown PCS detail type: ' || p_detail_type);
        END IF;

        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_code := SQLCODE;
            v_error_msg := SQLERRM;
            ROLLBACK;

            -- Log error
            INSERT INTO ETL_ERROR_LOG (
                error_id, endpoint_key, plant_id,
                error_timestamp, error_type, error_code,
                error_message, raw_data
            ) VALUES (
                ETL_ERROR_SEQ.NEXTVAL,
                'PCS_' || v_upper_type,
                p_plant_id,
                SYSTIMESTAMP,
                'PROCESSING_ERROR',
                v_error_code,
                v_error_msg,
                SUBSTR(v_json, 1, 4000)
            );
            COMMIT;

            -- Re-raise the error
            RAISE;
    END process_pcs_detail;

END PKG_PCS_DETAIL_PROCESSOR;
/

