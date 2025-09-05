-- Package Bodies for TR2000_STAGING
-- Generated: 2025-01-05
-- CRITICAL: Contains fixed JSON paths for VDS ($.getVDS) and PCS ($.getPCS)


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

END PKG_API_CLIENT;;
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

END PKG_DATE_UTILS;;
/

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "TR2000_STAGING"."PKG_DDL_BACKUP" AS

    -- Main backup procedure with fixed LOB handling
    PROCEDURE backup_schema(
        p_schema_name VARCHAR2 DEFAULT USER,
        p_notes VARCHAR2 DEFAULT NULL,
        p_include_control_data BOOLEAN DEFAULT TRUE
    ) IS
        v_ddl_content CLOB;
        v_object_count NUMBER := 0;
        v_view_count NUMBER := 0;
        v_package_count NUMBER := 0;
        v_table_count NUMBER := 0;
        v_sequence_count NUMBER := 0;
        v_trigger_count NUMBER := 0;
        v_ddl_backup_id NUMBER;
        v_start_time TIMESTAMP := SYSTIMESTAMP;
        v_end_time TIMESTAMP;
        v_duration_seconds NUMBER;
        v_hash VARCHAR2(64);
        v_temp_ddl CLOB;
        v_ddl_size NUMBER := 0;  -- Store size before freeing LOB

        CURSOR c_objects IS
            SELECT object_type, object_name
            FROM user_objects
            WHERE object_type IN ('TABLE', 'VIEW', 'PACKAGE', 'PACKAGE BODY',
                                  'PROCEDURE', 'FUNCTION', 'SEQUENCE', 'TRIGGER',
                                  'SYNONYM', 'TYPE', 'TYPE BODY', 'INDEX')
            AND object_name NOT LIKE 'BIN$%'
            AND object_name NOT IN ('DDL_BACKUP', 'CONTROL_DATA_BACKUP')
            AND object_name NOT LIKE 'SYS_%'
            AND object_name NOT LIKE 'ISEQ$$%'
            ORDER BY
                DECODE(object_type,
                       'TABLE', 1,
                       'SEQUENCE', 2,
                       'TYPE', 3,
                       'TYPE BODY', 4,
                       'FUNCTION', 5,
                       'PROCEDURE', 6,
                       'PACKAGE', 7,
                       'PACKAGE BODY', 8,
                       'VIEW', 9,
                       'TRIGGER', 10,
                       'INDEX', 11,
                       'SYNONYM', 12,
                       99),
                object_name;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Starting full DDL backup for schema: ' || p_schema_name);
        DBMS_OUTPUT.PUT_LINE('This will extract actual DDL for all objects...');

        -- Initialize DDL content
        DBMS_LOB.CREATETEMPORARY(v_ddl_content, TRUE);
        DBMS_LOB.APPEND(v_ddl_content,
            '-- Full DDL Backup for schema: ' || p_schema_name || CHR(10) ||
            '-- Generated: ' || TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM') || CHR(10) ||
            '-- Notes: ' || NVL(p_notes, 'No notes provided') || CHR(10) ||
            '-- WARNING: Execute this script carefully!' || CHR(10) || CHR(10)
        );

        -- Set DBMS_METADATA parameters
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', TRUE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);

        -- Extract DDL for each object
        FOR obj IN c_objects LOOP
            BEGIN
                -- Skip backup tables and certain object types
                IF obj.object_name IN ('DDL_BACKUP', 'CONTROL_DATA_BACKUP') THEN
                    CONTINUE;
                END IF;

                IF obj.object_type = 'PACKAGE BODY' THEN
                    CONTINUE;  -- Will be included with PACKAGE
                END IF;

                IF obj.object_type = 'INDEX' THEN
                    -- Skip constraint-based indexes
                    DECLARE
                        v_constraint_type VARCHAR2(1);
                    BEGIN
                        SELECT constraint_type INTO v_constraint_type
                        FROM user_constraints
                        WHERE constraint_name = obj.object_name
                        AND constraint_type IN ('P', 'U');
                        CONTINUE;
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                            NULL; -- Not a constraint index
                    END;
                END IF;

                -- Extract DDL (simplified to avoid errors)
                BEGIN
                    DBMS_LOB.APPEND(v_ddl_content,
                        CHR(10) || '-- ' || obj.object_type || ': ' || obj.object_name || CHR(10));

                    -- Count object types
                    IF obj.object_type = 'TABLE' THEN
                        v_table_count := v_table_count + 1;
                    ELSIF obj.object_type = 'VIEW' THEN
                        v_view_count := v_view_count + 1;
                    ELSIF obj.object_type = 'PACKAGE' THEN
                        v_package_count := v_package_count + 1;
                    ELSIF obj.object_type = 'SEQUENCE' THEN
                        v_sequence_count := v_sequence_count + 1;
                    ELSIF obj.object_type = 'TRIGGER' THEN
                        v_trigger_count := v_trigger_count + 1;
                    END IF;

                    -- Try to extract DDL
                    BEGIN
                        DBMS_LOB.CREATETEMPORARY(v_temp_ddl, TRUE);

                        IF obj.object_type IN ('TABLE', 'VIEW', 'SEQUENCE', 'PROCEDURE', 'FUNCTION', 'TRIGGER', 'SYNONYM') THEN
                            v_temp_ddl := DBMS_METADATA.GET_DDL(obj.object_type, obj.object_name, p_schema_name);
                        ELSIF obj.object_type = 'PACKAGE' THEN
                            v_temp_ddl := DBMS_METADATA.GET_DDL('PACKAGE', obj.object_name, p_schema_name);
                            -- Try to get body
                            BEGIN
                                DBMS_LOB.APPEND(v_temp_ddl, CHR(10) || '/' || CHR(10));
                                DBMS_LOB.APPEND(v_temp_ddl,
                                    DBMS_METADATA.GET_DDL('PACKAGE_BODY', obj.object_name, p_schema_name));
                            EXCEPTION
                                WHEN OTHERS THEN
                                    NULL;
                            END;
                        ELSIF obj.object_type = 'TYPE' THEN
                            v_temp_ddl := DBMS_METADATA.GET_DDL('TYPE', obj.object_name, p_schema_name);
                            -- Try to get body
                            BEGIN
                                DBMS_LOB.APPEND(v_temp_ddl, CHR(10) || '/' || CHR(10));
                                DBMS_LOB.APPEND(v_temp_ddl,
                                    DBMS_METADATA.GET_DDL('TYPE_BODY', obj.object_name, p_schema_name));
                            EXCEPTION
                                WHEN OTHERS THEN
                                    NULL;
                            END;
                        END IF;

                        -- Append to main DDL
                        IF DBMS_LOB.GETLENGTH(v_temp_ddl) > 0 THEN
                            DBMS_LOB.APPEND(v_ddl_content, v_temp_ddl);
                            DBMS_LOB.APPEND(v_ddl_content, CHR(10) || '/' || CHR(10));
                        END IF;

                        DBMS_LOB.FREETEMPORARY(v_temp_ddl);

                    EXCEPTION
                        WHEN OTHERS THEN
                            -- Log but continue
                            DBMS_LOB.APPEND(v_ddl_content,
                                '-- Error extracting DDL: ' || SUBSTR(SQLERRM, 1, 200) || CHR(10));
                            IF DBMS_LOB.ISTEMPORARY(v_temp_ddl) = 1 THEN
                                DBMS_LOB.FREETEMPORARY(v_temp_ddl);
                            END IF;
                    END;

                    v_object_count := v_object_count + 1;

                    IF MOD(v_object_count, 10) = 0 THEN
                        DBMS_OUTPUT.PUT_LINE('  Processed ' || v_object_count || ' objects...');
                    END IF;

                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('  Warning: Error processing ' || obj.object_type ||
                                           ' ' || obj.object_name || ': ' || SUBSTR(SQLERRM, 1, 100));
                END;
            END;
        END LOOP;

        -- Add footer
        DBMS_LOB.APPEND(v_ddl_content, CHR(10) || CHR(10) ||
            '-- ============================================' || CHR(10) ||
            '-- End of DDL Backup' || CHR(10) ||
            '-- Total Objects: ' || v_object_count || CHR(10) ||
            '-- ============================================' || CHR(10));

        -- Calculate values BEFORE insert
        v_end_time := SYSTIMESTAMP;
        v_duration_seconds := EXTRACT(SECOND FROM (v_end_time - v_start_time)) +
                             EXTRACT(MINUTE FROM (v_end_time - v_start_time)) * 60 +
                             EXTRACT(HOUR FROM (v_end_time - v_start_time)) * 3600;

        v_ddl_size := DBMS_LOB.GETLENGTH(v_ddl_content);
        v_hash := 'SIZE:' || v_ddl_size || ':COUNT:' || v_object_count;

        -- Get sequence value
        SELECT DDL_BACKUP_SEQ.NEXTVAL INTO v_ddl_backup_id FROM DUAL;

        -- Insert into backup table
        INSERT INTO DDL_BACKUP_OWNER.DDL_BACKUP (
            backup_id,
            backup_timestamp,
            schema_name,
            backup_type,
            object_count,
            view_count,
            package_count,
            table_count,
            sequence_count,
            trigger_count,
            ddl_content,
            ddl_size_bytes,
            ddl_hash,
            backup_notes,
            backup_user,
            os_user,
            ip_address,
            program,
            duration_seconds
        ) VALUES (
            v_ddl_backup_id,
            v_start_time,
            p_schema_name,
            'FULL_SCHEMA_DDL',
            v_object_count,
            v_view_count,
            v_package_count,
            v_table_count,
            v_sequence_count,
            v_trigger_count,
            v_ddl_content,
            v_ddl_size,
            v_hash,
            p_notes,
            USER,
            SYS_CONTEXT('USERENV', 'OS_USER'),
            SYS_CONTEXT('USERENV', 'IP_ADDRESS'),
            SYS_CONTEXT('USERENV', 'MODULE'),
            v_duration_seconds
        );

        -- Backup control data if requested
        IF p_include_control_data THEN
            backup_control_data(
                p_schema_name => p_schema_name,
                p_notes => p_notes,
                p_ddl_backup_id => v_ddl_backup_id
            );
        END IF;

        COMMIT;

        -- Cleanup AFTER commit
        DBMS_LOB.FREETEMPORARY(v_ddl_content);

        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('===== Backup Completed Successfully! =====');
        DBMS_OUTPUT.PUT_LINE('Backup ID: ' || v_ddl_backup_id);
        DBMS_OUTPUT.PUT_LINE('Total objects backed up: ' || v_object_count);
        DBMS_OUTPUT.PUT_LINE('  Tables: ' || v_table_count);
        DBMS_OUTPUT.PUT_LINE('  Views: ' || v_view_count);
        DBMS_OUTPUT.PUT_LINE('  Packages: ' || v_package_count);
        DBMS_OUTPUT.PUT_LINE('  Sequences: ' || v_sequence_count);
        DBMS_OUTPUT.PUT_LINE('  Triggers: ' || v_trigger_count);
        DBMS_OUTPUT.PUT_LINE('DDL Size: ' || ROUND(v_ddl_size/1024) || ' KB');
        DBMS_OUTPUT.PUT_LINE('Duration: ' || ROUND(v_duration_seconds, 2) || ' seconds');
        DBMS_OUTPUT.PUT_LINE('==========================================');

    EXCEPTION
        WHEN OTHERS THEN
            IF DBMS_LOB.ISTEMPORARY(v_ddl_content) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_ddl_content);
            END IF;
            DBMS_OUTPUT.PUT_LINE('BACKUP FAILED: ' || SQLERRM);
            RAISE_APPLICATION_ERROR(-20001, 'Backup failed: ' || SQLERRM);
    END backup_schema;

    -- Keep all other procedures unchanged
    PROCEDURE backup_control_data(
        p_schema_name VARCHAR2 DEFAULT USER,
        p_notes VARCHAR2 DEFAULT NULL,
        p_ddl_backup_id NUMBER DEFAULT NULL
    ) IS
        v_control_settings_json CLOB;
        v_control_endpoints_json CLOB;
        v_etl_filter_json CLOB;
        v_settings_count NUMBER := 0;
        v_endpoints_count NUMBER := 0;
        v_filter_count NUMBER := 0;
        v_backup_id NUMBER;
    BEGIN
        -- Initialize CLOBs
        DBMS_LOB.CREATETEMPORARY(v_control_settings_json, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_control_endpoints_json, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_etl_filter_json, TRUE);

        -- Build JSON for CONTROL_SETTINGS
        DBMS_LOB.APPEND(v_control_settings_json, '[');
        FOR rec IN (SELECT setting_key, setting_value, description
                   FROM CONTROL_SETTINGS ORDER BY setting_key) LOOP
            IF v_settings_count > 0 THEN
                DBMS_LOB.APPEND(v_control_settings_json, ',');
            END IF;
            DBMS_LOB.APPEND(v_control_settings_json,
                '{"setting_key":"' || rec.setting_key ||
                '","setting_value":"' || REPLACE(rec.setting_value, '"', '\"') ||
                '","description":"' || REPLACE(NVL(rec.description, ''), '"', '\"') || '"}');
            v_settings_count := v_settings_count + 1;
        END LOOP;
        DBMS_LOB.APPEND(v_control_settings_json, ']');

        -- Build JSON for CONTROL_ENDPOINTS
        DBMS_LOB.APPEND(v_control_endpoints_json, '[');
        FOR rec IN (SELECT endpoint_id, endpoint_key, endpoint_template, comments
                   FROM CONTROL_ENDPOINTS ORDER BY endpoint_id) LOOP
            IF v_endpoints_count > 0 THEN
                DBMS_LOB.APPEND(v_control_endpoints_json, ',');
            END IF;
            DBMS_LOB.APPEND(v_control_endpoints_json,
                '{"endpoint_id":' || rec.endpoint_id ||
                ',"endpoint_key":"' || rec.endpoint_key ||
                '","endpoint_template":"' || REPLACE(rec.endpoint_template, '"', '\"') ||
                '","comments":"' || REPLACE(NVL(rec.comments, ''), '"', '\"') || '"}');
            v_endpoints_count := v_endpoints_count + 1;
        END LOOP;
        DBMS_LOB.APPEND(v_control_endpoints_json, ']');

        -- Build JSON for ETL_FILTER
        DBMS_LOB.APPEND(v_etl_filter_json, '[');
        FOR rec IN (SELECT filter_id, plant_id, plant_name, issue_revision,
                          added_date, added_by_user_id, notes
                   FROM ETL_FILTER ORDER BY filter_id) LOOP
            IF v_filter_count > 0 THEN
                DBMS_LOB.APPEND(v_etl_filter_json, ',');
            END IF;
            DBMS_LOB.APPEND(v_etl_filter_json,
                '{"filter_id":' || rec.filter_id ||
                ',"plant_id":"' || rec.plant_id ||
                '","plant_name":"' || REPLACE(rec.plant_name, '"', '\"') ||
                '","issue_revision":"' || rec.issue_revision ||
                '","added_date":"' || TO_CHAR(rec.added_date, 'YYYY-MM-DD HH24:MI:SS') ||
                '","added_by_user_id":"' || rec.added_by_user_id ||
                '","notes":"' || REPLACE(NVL(rec.notes, ''), '"', '\"') || '"}');
            v_filter_count := v_filter_count + 1;
        END LOOP;
        DBMS_LOB.APPEND(v_etl_filter_json, ']');

        -- Get sequence value
        SELECT CONTROL_DATA_BACKUP_SEQ.NEXTVAL INTO v_backup_id FROM DUAL;

        -- Insert into backup table
        INSERT INTO DDL_BACKUP_OWNER.CONTROL_DATA_BACKUP (
            backup_id,
            backup_timestamp,
            schema_name,
            control_settings_json,
            control_endpoints_json,
            etl_filter_json,
            settings_count,
            endpoints_count,
            filter_count,
            backup_notes,
            backup_user,
            ddl_backup_id
        ) VALUES (
            v_backup_id,
            SYSTIMESTAMP,
            p_schema_name,
            v_control_settings_json,
            v_control_endpoints_json,
            v_etl_filter_json,
            v_settings_count,
            v_endpoints_count,
            v_filter_count,
            p_notes,
            USER,
            p_ddl_backup_id
        );

        COMMIT;

        -- Cleanup
        DBMS_LOB.FREETEMPORARY(v_control_settings_json);
        DBMS_LOB.FREETEMPORARY(v_control_endpoints_json);
        DBMS_LOB.FREETEMPORARY(v_etl_filter_json);

        DBMS_OUTPUT.PUT_LINE('Control data backup completed. Backup ID: ' || v_backup_id);
        DBMS_OUTPUT.PUT_LINE('  Settings: ' || v_settings_count);
        DBMS_OUTPUT.PUT_LINE('  Endpoints: ' || v_endpoints_count);
        DBMS_OUTPUT.PUT_LINE('  Filters: ' || v_filter_count);

    EXCEPTION
        WHEN OTHERS THEN
            IF DBMS_LOB.ISTEMPORARY(v_control_settings_json) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_control_settings_json);
            END IF;
            IF DBMS_LOB.ISTEMPORARY(v_control_endpoints_json) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_control_endpoints_json);
            END IF;
            IF DBMS_LOB.ISTEMPORARY(v_etl_filter_json) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_etl_filter_json);
            END IF;
            RAISE_APPLICATION_ERROR(-20002, 'Control data backup failed: ' || SQLERRM);
    END backup_control_data;

    FUNCTION get_ddl_from_backup(p_backup_id NUMBER) RETURN CLOB IS
        v_ddl_content CLOB;
    BEGIN
        SELECT ddl_content INTO v_ddl_content
        FROM DDL_BACKUP_OWNER.DDL_BACKUP
        WHERE backup_id = p_backup_id;

        RETURN v_ddl_content;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20003, 'Backup ID ' || p_backup_id || ' not found');
    END get_ddl_from_backup;

    FUNCTION restore_control_data(
        p_backup_id NUMBER,
        p_dry_run BOOLEAN DEFAULT TRUE
    ) RETURN VARCHAR2 IS
        v_control_settings_json CLOB;
        v_control_endpoints_json CLOB;
        v_etl_filter_json CLOB;
        v_result VARCHAR2(4000);
        v_restored_count NUMBER := 0;
    BEGIN
        SELECT control_settings_json, control_endpoints_json, etl_filter_json
        INTO v_control_settings_json, v_control_endpoints_json, v_etl_filter_json
        FROM DDL_BACKUP_OWNER.CONTROL_DATA_BACKUP
        WHERE backup_id = p_backup_id;

        IF p_dry_run THEN
            v_result := 'DRY RUN - No changes made. Would restore:' || CHR(10);

            SELECT COUNT(*) INTO v_restored_count
            FROM JSON_TABLE(v_control_settings_json, '$[*]'
                COLUMNS (setting_key VARCHAR2(100) PATH '$.setting_key'));
            v_result := v_result || 'CONTROL_SETTINGS: ' || v_restored_count || ' records' || CHR(10);

            SELECT COUNT(*) INTO v_restored_count
            FROM JSON_TABLE(v_control_endpoints_json, '$[*]'
                COLUMNS (endpoint_id NUMBER PATH '$.endpoint_id'));
            v_result := v_result || 'CONTROL_ENDPOINTS: ' || v_restored_count || ' records' || CHR(10);

            SELECT COUNT(*) INTO v_restored_count
            FROM JSON_TABLE(v_etl_filter_json, '$[*]'
                COLUMNS (filter_id NUMBER PATH '$.filter_id'));
            v_result := v_result || 'ETL_FILTER: ' || v_restored_count || ' records';

        ELSE
            v_result := 'RESTORE COMPLETED:' || CHR(10);

            DELETE FROM CONTROL_SETTINGS;
            DELETE FROM CONTROL_ENDPOINTS;
            DELETE FROM ETL_FILTER;

            INSERT INTO CONTROL_SETTINGS (setting_key, setting_value, description)
            SELECT setting_key, setting_value, description
            FROM JSON_TABLE(v_control_settings_json, '$[*]'
                COLUMNS (
                    setting_key VARCHAR2(100) PATH '$.setting_key',
                    setting_value VARCHAR2(4000) PATH '$.setting_value',
                    description VARCHAR2(4000) PATH '$.description'
                ));
            v_result := v_result || 'CONTROL_SETTINGS: ' || SQL%ROWCOUNT || ' records' || CHR(10);

            INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template, comments)
            SELECT endpoint_id, endpoint_key, endpoint_template, comments
            FROM JSON_TABLE(v_control_endpoints_json, '$[*]'
                COLUMNS (
                    endpoint_id NUMBER PATH '$.endpoint_id',
                    endpoint_key VARCHAR2(100) PATH '$.endpoint_key',
                    endpoint_template VARCHAR2(500) PATH '$.endpoint_template',
                    comments VARCHAR2(500) PATH '$.comments'
                ));
            v_result := v_result || 'CONTROL_ENDPOINTS: ' || SQL%ROWCOUNT || ' records' || CHR(10);

            INSERT INTO ETL_FILTER (filter_id, plant_id, plant_name, issue_revision,
                                   added_date, added_by_user_id, notes)
            SELECT filter_id, plant_id, plant_name, issue_revision,
                   TO_DATE(added_date, 'YYYY-MM-DD HH24:MI:SS'), added_by_user_id, notes
            FROM JSON_TABLE(v_etl_filter_json, '$[*]'
                COLUMNS (
                    filter_id NUMBER PATH '$.filter_id',
                    plant_id VARCHAR2(50) PATH '$.plant_id',
                    plant_name VARCHAR2(100) PATH '$.plant_name',
                    issue_revision VARCHAR2(50) PATH '$.issue_revision',
                    added_date VARCHAR2(30) PATH '$.added_date',
                    added_by_user_id VARCHAR2(50) PATH '$.added_by_user_id',
                    notes VARCHAR2(500) PATH '$.notes'
                ));
            v_result := v_result || 'ETL_FILTER: ' || SQL%ROWCOUNT || ' records';

            COMMIT;
        END IF;

        RETURN v_result;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20004, 'Control data backup ID ' || p_backup_id || ' not found');
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE_APPLICATION_ERROR(-20005, 'Restore failed: ' || SQLERRM);
    END restore_control_data;

    PROCEDURE list_backups(
        p_days_back NUMBER DEFAULT 7,
        p_schema_name VARCHAR2 DEFAULT USER
    ) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('=== DDL Backups for ' || p_schema_name || ' (Last ' || p_days_back || ' days) ===');
        DBMS_OUTPUT.PUT_LINE(RPAD('ID', 5) || RPAD('Timestamp', 25) || RPAD('Type', 20) ||
                           RPAD('Objects', 10) || RPAD('Size(KB)', 10) || 'Notes');
        DBMS_OUTPUT.PUT_LINE(RPAD('-', 100, '-'));

        FOR rec IN (
            SELECT backup_id, backup_timestamp, backup_type, object_count,
                   ROUND(ddl_size_bytes/1024) as size_kb, backup_notes
            FROM DDL_BACKUP_OWNER.DDL_BACKUP
            WHERE schema_name = p_schema_name
            AND backup_timestamp >= SYSTIMESTAMP - p_days_back
            ORDER BY backup_timestamp DESC
        ) LOOP
            DBMS_OUTPUT.PUT_LINE(
                RPAD(rec.backup_id, 5) ||
                RPAD(TO_CHAR(rec.backup_timestamp, 'YYYY-MM-DD HH24:MI:SS'), 25) ||
                RPAD(NVL(rec.backup_type, 'UNKNOWN'), 20) ||
                RPAD(rec.object_count, 10) ||
                RPAD(rec.size_kb || 'KB', 10) ||
                NVL(SUBSTR(rec.backup_notes, 1, 30), 'No notes')
            );
        END LOOP;
    END list_backups;

    PROCEDURE compare_backups(
        p_backup_id_1 NUMBER,
        p_backup_id_2 NUMBER
    ) IS
        v_count1 NUMBER;
        v_count2 NUMBER;
        v_size1 NUMBER;
        v_size2 NUMBER;
        v_date1 TIMESTAMP;
        v_date2 TIMESTAMP;
    BEGIN
        SELECT object_count, ddl_size_bytes, backup_timestamp
        INTO v_count1, v_size1, v_date1
        FROM DDL_BACKUP_OWNER.DDL_BACKUP
        WHERE backup_id = p_backup_id_1;

        SELECT object_count, ddl_size_bytes, backup_timestamp
        INTO v_count2, v_size2, v_date2
        FROM DDL_BACKUP_OWNER.DDL_BACKUP
        WHERE backup_id = p_backup_id_2;

        DBMS_OUTPUT.PUT_LINE('=== Backup Comparison ===');
        DBMS_OUTPUT.PUT_LINE('Backup ' || p_backup_id_1 || ' (' || TO_CHAR(v_date1, 'YYYY-MM-DD HH24:MI') || ')');
        DBMS_OUTPUT.PUT_LINE('  Objects: ' || v_count1 || ', Size: ' || ROUND(v_size1/1024) || 'KB');
        DBMS_OUTPUT.PUT_LINE('Backup ' || p_backup_id_2 || ' (' || TO_CHAR(v_date2, 'YYYY-MM-DD HH24:MI') || ')');
        DBMS_OUTPUT.PUT_LINE('  Objects: ' || v_count2 || ', Size: ' || ROUND(v_size2/1024) || 'KB');
        DBMS_OUTPUT.PUT_LINE('Differences:');
        DBMS_OUTPUT.PUT_LINE('  Object count: ' || (v_count2 - v_count1));
        DBMS_OUTPUT.PUT_LINE('  Size change: ' || ROUND((v_size2 - v_size1)/1024) || 'KB');
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('Error: One or both backup IDs not found');
    END compare_backups;

    FUNCTION get_latest_backup_id(
        p_schema_name VARCHAR2 DEFAULT USER
    ) RETURN NUMBER IS
        v_backup_id NUMBER;
    BEGIN
        SELECT MAX(backup_id) INTO v_backup_id
        FROM DDL_BACKUP_OWNER.DDL_BACKUP
        WHERE schema_name = p_schema_name;

        RETURN v_backup_id;
    END get_latest_backup_id;

END PKG_DDL_BACKUP;;
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

END PKG_ETL_LOGGING;;
/
BEGIN
*
ERROR at line 1:
ORA-06502: PL/SQL: numeric or value error
ORA-06512: at line 5
ORA-06512: at line 5


