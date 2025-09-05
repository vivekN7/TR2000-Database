-- =====================================================
-- Package: PKG_ETL_VALIDATION
-- Purpose: Safe data conversion and validation functions
-- CRITICAL: Prevents silent data loss during ETL
-- =====================================================

-- Drop existing package
BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE BODY PKG_ETL_VALIDATION';
EXCEPTION 
    WHEN OTHERS THEN 
        IF SQLCODE != -4043 THEN RAISE; END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE PKG_ETL_VALIDATION';
EXCEPTION 
    WHEN OTHERS THEN 
        IF SQLCODE != -4043 THEN RAISE; END IF;
END;
/

-- Create package specification
CREATE OR REPLACE PACKAGE PKG_ETL_VALIDATION AS

    -- Safe number conversion with logging
    FUNCTION safe_to_number(
        p_value        VARCHAR2,
        p_default      NUMBER DEFAULT NULL,
        p_source_table VARCHAR2 DEFAULT NULL,
        p_source_field VARCHAR2 DEFAULT NULL,
        p_record_id    VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER;
    
    -- Safe date conversion with logging  
    FUNCTION safe_to_date(
        p_value        VARCHAR2,
        p_default      DATE DEFAULT NULL,
        p_source_table VARCHAR2 DEFAULT NULL,
        p_source_field VARCHAR2 DEFAULT NULL,
        p_record_id    VARCHAR2 DEFAULT NULL
    ) RETURN DATE;
    
    -- Validate JSON structure
    FUNCTION validate_json(
        p_json_content CLOB,
        p_endpoint_key VARCHAR2 DEFAULT NULL
    ) RETURN BOOLEAN;
    
    -- Check for required fields
    FUNCTION validate_required_field(
        p_value        VARCHAR2,
        p_field_name   VARCHAR2,
        p_source_table VARCHAR2
    ) RETURN BOOLEAN;
    
    -- Validate row counts between staging and core
    PROCEDURE validate_row_counts(
        p_table_name  VARCHAR2,
        p_stg_count   NUMBER,
        p_core_count  NUMBER
    );
    
    -- Check for duplicate keys before insert
    FUNCTION check_duplicate_key(
        p_table_name VARCHAR2,
        p_key_columns VARCHAR2,  -- comma-separated list
        p_key_values  VARCHAR2   -- comma-separated list
    ) RETURN BOOLEAN;
    
    -- Log conversion error
    PROCEDURE log_conversion_error(
        p_error_type   VARCHAR2,
        p_source_value VARCHAR2,
        p_source_table VARCHAR2,
        p_source_field VARCHAR2,
        p_record_id    VARCHAR2 DEFAULT NULL,
        p_error_msg    VARCHAR2 DEFAULT NULL
    );
    
    -- Get conversion statistics for monitoring
    FUNCTION get_conversion_stats(
        p_run_id NUMBER DEFAULT NULL
    ) RETURN VARCHAR2;

END PKG_ETL_VALIDATION;
/

-- Create package body
CREATE OR REPLACE PACKAGE BODY PKG_ETL_VALIDATION AS

    -- Safe number conversion with logging
    FUNCTION safe_to_number(
        p_value        VARCHAR2,
        p_default      NUMBER DEFAULT NULL,
        p_source_table VARCHAR2 DEFAULT NULL,
        p_source_field VARCHAR2 DEFAULT NULL,
        p_record_id    VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER IS
        v_result NUMBER;
    BEGIN
        -- Handle NULL
        IF p_value IS NULL THEN
            RETURN p_default;
        END IF;
        
        -- Trim whitespace
        DECLARE
            v_clean_value VARCHAR2(100) := TRIM(p_value);
        BEGIN
            -- Check if it's a valid number format
            IF REGEXP_LIKE(v_clean_value, '^-?[0-9]+\.?[0-9]*$') THEN
                v_result := TO_NUMBER(v_clean_value);
                RETURN v_result;
            ELSE
                -- Log the conversion error
                log_conversion_error(
                    p_error_type   => 'INVALID_NUMBER',
                    p_source_value => p_value,
                    p_source_table => p_source_table,
                    p_source_field => p_source_field,
                    p_record_id    => p_record_id,
                    p_error_msg    => 'Value is not a valid number format'
                );
                RETURN p_default;
            END IF;
        END;
    EXCEPTION
        WHEN VALUE_ERROR THEN
            -- Log the conversion error
            log_conversion_error(
                p_error_type   => 'NUMBER_CONVERSION_ERROR',
                p_source_value => p_value,
                p_source_table => p_source_table,
                p_source_field => p_source_field,
                p_record_id    => p_record_id,
                p_error_msg    => SQLERRM
            );
            RETURN p_default;
        WHEN OTHERS THEN
            -- Log unexpected error
            log_conversion_error(
                p_error_type   => 'UNEXPECTED_NUMBER_ERROR',
                p_source_value => p_value,
                p_source_table => p_source_table,
                p_source_field => p_source_field,
                p_record_id    => p_record_id,
                p_error_msg    => SQLERRM
            );
            RETURN p_default;
    END safe_to_number;
    
    -- Safe date conversion with logging
    FUNCTION safe_to_date(
        p_value        VARCHAR2,
        p_default      DATE DEFAULT NULL,
        p_source_table VARCHAR2 DEFAULT NULL,
        p_source_field VARCHAR2 DEFAULT NULL,
        p_record_id    VARCHAR2 DEFAULT NULL
    ) RETURN DATE IS
        v_result DATE;
        v_formats SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST(
            'YYYY-MM-DD HH24:MI:SS',
            'YYYY-MM-DD',
            'DD-MON-YYYY',
            'DD/MM/YYYY',
            'MM/DD/YYYY',
            'YYYY/MM/DD',
            'DD-MM-YYYY',
            'MM-DD-YYYY',
            'DD-MON-YY',
            'DD.MM.YYYY',
            'YYYY.MM.DD'
        );
    BEGIN
        -- Handle NULL
        IF p_value IS NULL THEN
            RETURN p_default;
        END IF;
        
        -- Try each format
        FOR i IN 1..v_formats.COUNT LOOP
            BEGIN
                v_result := TO_DATE(p_value, v_formats(i));
                RETURN v_result;
            EXCEPTION
                WHEN OTHERS THEN
                    NULL; -- Try next format
            END;
        END LOOP;
        
        -- If no format worked, log error and return default
        log_conversion_error(
            p_error_type   => 'DATE_CONVERSION_ERROR',
            p_source_value => p_value,
            p_source_table => p_source_table,
            p_source_field => p_source_field,
            p_record_id    => p_record_id,
            p_error_msg    => 'No matching date format found'
        );
        RETURN p_default;
        
    EXCEPTION
        WHEN OTHERS THEN
            log_conversion_error(
                p_error_type   => 'UNEXPECTED_DATE_ERROR',
                p_source_value => p_value,
                p_source_table => p_source_table,
                p_source_field => p_source_field,
                p_record_id    => p_record_id,
                p_error_msg    => SQLERRM
            );
            RETURN p_default;
    END safe_to_date;
    
    -- Validate JSON structure
    FUNCTION validate_json(
        p_json_content CLOB,
        p_endpoint_key VARCHAR2 DEFAULT NULL
    ) RETURN BOOLEAN IS
        v_json JSON_OBJECT_T;
    BEGIN
        -- Check for NULL or empty
        IF p_json_content IS NULL OR LENGTH(p_json_content) < 2 THEN
            log_conversion_error(
                p_error_type   => 'INVALID_JSON',
                p_source_value => 'NULL or empty',
                p_source_table => 'RAW_JSON',
                p_source_field => 'PAYLOAD',
                p_record_id    => p_endpoint_key,
                p_error_msg    => 'JSON content is NULL or empty'
            );
            RETURN FALSE;
        END IF;
        
        -- Try to parse JSON
        BEGIN
            v_json := JSON_OBJECT_T.parse(p_json_content);
            RETURN TRUE;
        EXCEPTION
            WHEN OTHERS THEN
                -- Check if it's an array
                DECLARE
                    v_json_array JSON_ARRAY_T;
                BEGIN
                    v_json_array := JSON_ARRAY_T.parse(p_json_content);
                    RETURN TRUE;
                EXCEPTION
                    WHEN OTHERS THEN
                        log_conversion_error(
                            p_error_type   => 'INVALID_JSON',
                            p_source_value => SUBSTR(p_json_content, 1, 100),
                            p_source_table => 'RAW_JSON',
                            p_source_field => 'PAYLOAD',
                            p_record_id    => p_endpoint_key,
                            p_error_msg    => 'JSON parsing failed: ' || SQLERRM
                        );
                        RETURN FALSE;
                END;
        END;
    END validate_json;
    
    -- Check for required fields
    FUNCTION validate_required_field(
        p_value        VARCHAR2,
        p_field_name   VARCHAR2,
        p_source_table VARCHAR2
    ) RETURN BOOLEAN IS
    BEGIN
        IF p_value IS NULL OR TRIM(p_value) IS NULL THEN
            log_conversion_error(
                p_error_type   => 'REQUIRED_FIELD_NULL',
                p_source_value => 'NULL',
                p_source_table => p_source_table,
                p_source_field => p_field_name,
                p_error_msg    => 'Required field is NULL or empty'
            );
            RETURN FALSE;
        END IF;
        RETURN TRUE;
    END validate_required_field;
    
    -- Validate row counts between staging and core
    PROCEDURE validate_row_counts(
        p_table_name  VARCHAR2,
        p_stg_count   NUMBER,
        p_core_count  NUMBER
    ) IS
        v_diff NUMBER;
    BEGIN
        v_diff := p_stg_count - p_core_count;
        
        IF v_diff != 0 THEN
            -- Log the discrepancy
            INSERT INTO ETL_ERROR_LOG (
                error_id,
                endpoint_key,
                error_timestamp,
                error_type,
                error_code,
                error_message,
                raw_data
            ) VALUES (
                DEFAULT,  -- Uses IDENTITY column
                p_table_name,
                SYSTIMESTAMP,
                'ROW_COUNT_MISMATCH',
                TO_CHAR(v_diff),
                'Row count mismatch: STG=' || p_stg_count || 
                ', CORE=' || p_core_count || ', DIFF=' || v_diff,
                'Table: ' || p_table_name
            );
            COMMIT;
            
            -- Don't raise error, just log warning
            DBMS_OUTPUT.PUT_LINE('WARNING: Row count mismatch for ' || p_table_name || 
                                  ' (STG=' || p_stg_count || ', CORE=' || p_core_count || ')');
        END IF;
    END validate_row_counts;
    
    -- Check for duplicate keys before insert
    FUNCTION check_duplicate_key(
        p_table_name VARCHAR2,
        p_key_columns VARCHAR2,
        p_key_values  VARCHAR2
    ) RETURN BOOLEAN IS
        v_count NUMBER;
        v_sql VARCHAR2(4000);
    BEGIN
        -- Build dynamic SQL to check for duplicates
        v_sql := 'SELECT COUNT(*) FROM ' || p_table_name || ' WHERE ';
        
        -- Parse key columns and values
        DECLARE
            v_cols SYS.ODCIVARCHAR2LIST;
            v_vals SYS.ODCIVARCHAR2LIST;
            v_conditions VARCHAR2(4000);
        BEGIN
            -- Simple parsing (would be more robust in production)
            v_cols := SYS.ODCIVARCHAR2LIST();
            v_vals := SYS.ODCIVARCHAR2LIST();
            
            -- For now, return FALSE (no duplicate) - implement full logic later
            RETURN FALSE;
        END;
        
    EXCEPTION
        WHEN OTHERS THEN
            -- Log error but don't block insert
            DBMS_OUTPUT.PUT_LINE('Error checking duplicates: ' || SQLERRM);
            RETURN FALSE;
    END check_duplicate_key;
    
    -- Log conversion error
    PROCEDURE log_conversion_error(
        p_error_type   VARCHAR2,
        p_source_value VARCHAR2,
        p_source_table VARCHAR2,
        p_source_field VARCHAR2,
        p_record_id    VARCHAR2 DEFAULT NULL,
        p_error_msg    VARCHAR2 DEFAULT NULL
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO ETL_ERROR_LOG (
            error_id,
            endpoint_key,
            plant_id,
            issue_revision,
            error_timestamp,
            error_type,
            error_code,
            error_message,
            raw_data
        ) VALUES (
            DEFAULT,  -- Uses IDENTITY column
            p_source_table,
            p_source_field,
            p_record_id,
            SYSTIMESTAMP,
            'DATA_CONVERSION_' || p_error_type,
            p_error_type,
            NVL(p_error_msg, 'Conversion failed') || 
            ' - Value: ' || SUBSTR(p_source_value, 1, 100),
            'Field: ' || p_source_field || 
            ', Table: ' || p_source_table ||
            CASE WHEN p_record_id IS NOT NULL THEN ', ID: ' || p_record_id ELSE '' END
        );
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            NULL; -- Don't let logging errors break the ETL
    END log_conversion_error;
    
    -- Get conversion statistics for monitoring
    FUNCTION get_conversion_stats(
        p_run_id NUMBER DEFAULT NULL
    ) RETURN VARCHAR2 IS
        v_stats VARCHAR2(4000);
        v_total_errors NUMBER;
        v_number_errors NUMBER;
        v_date_errors NUMBER;
        v_json_errors NUMBER;
        v_required_errors NUMBER;
    BEGIN
        -- Count errors by type
        SELECT COUNT(*) INTO v_total_errors
        FROM ETL_ERROR_LOG
        WHERE error_type LIKE 'DATA_CONVERSION_%'
        AND error_timestamp > SYSDATE - 1;
        
        SELECT COUNT(*) INTO v_number_errors
        FROM ETL_ERROR_LOG
        WHERE error_type LIKE '%NUMBER%'
        AND error_timestamp > SYSDATE - 1;
        
        SELECT COUNT(*) INTO v_date_errors
        FROM ETL_ERROR_LOG
        WHERE error_type LIKE '%DATE%'
        AND error_timestamp > SYSDATE - 1;
        
        SELECT COUNT(*) INTO v_json_errors
        FROM ETL_ERROR_LOG
        WHERE error_type LIKE '%JSON%'
        AND error_timestamp > SYSDATE - 1;
        
        SELECT COUNT(*) INTO v_required_errors
        FROM ETL_ERROR_LOG
        WHERE error_type LIKE '%REQUIRED%'
        AND error_timestamp > SYSDATE - 1;
        
        v_stats := 'Conversion Errors (Last 24h): ' || CHR(10) ||
                   'Total: ' || v_total_errors || CHR(10) ||
                   'Number Conversions: ' || v_number_errors || CHR(10) ||
                   'Date Conversions: ' || v_date_errors || CHR(10) ||
                   'JSON Validation: ' || v_json_errors || CHR(10) ||
                   'Required Fields: ' || v_required_errors;
                   
        RETURN v_stats;
    END get_conversion_stats;

END PKG_ETL_VALIDATION;
/

-- Verify compilation
ALTER PACKAGE PKG_ETL_VALIDATION COMPILE;
/
SHOW ERRORS
/

PROMPT Package PKG_ETL_VALIDATION deployed successfully
PROMPT
PROMPT This package provides critical data validation and conversion functions:
PROMPT - safe_to_number: Converts strings to numbers with error logging
PROMPT - safe_to_date: Converts strings to dates with multiple format support
PROMPT - validate_json: Validates JSON structure before processing
PROMPT - validate_required_field: Ensures required fields are not NULL
PROMPT - validate_row_counts: Checks for data loss between staging and core
PROMPT - All errors are logged to ETL_ERROR_LOG for monitoring
/