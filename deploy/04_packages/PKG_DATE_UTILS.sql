-- Package: PKG_DATE_UTILS
-- Purpose: Date parsing utilities

-- Drop existing package
BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE BODY PKG_DATE_UTILS';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;
/
BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE PKG_DATE_UTILS';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;
/

-- Create package specification
CREATE OR REPLACE PACKAGE PKG_DATE_UTILS AS

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

CREATE OR REPLACE PACKAGE BODY PKG_DATE_UTILS AS

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
