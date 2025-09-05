-- Control Data for TR2000_STAGING
-- Generated: 2025-01-05

-- ETL_FILTER
       'TO_DATE(''' || TO_CHAR(added_timestamp, 'YYYY-MM-DD HH24:MI:SS') || ''', ''YYYY-MM-DD HH24:MI:SS''));'
                               *
ERROR at line 4:
ORA-00904: "ADDED_TIMESTAMP": invalid identifier



-- CONTROL_SETTINGS
INSERT INTO CONTROL_SETTINGS (setting_key, setting_value, description) VALUES ('API_BASE_URL', 'https://equinor.pipespec-api.presight.com', 'Base URL for TR2000 API');
INSERT INTO CONTROL_SETTINGS (setting_key, setting_value, description) VALUES ('MAX_PCS_DETAILS_PER_RUN', '0', 'Maximum number of PCS to process for each PCS Details table in a single ETL run. Set to 0 or NULL to process all.');

-- CONTROL_ENDPOINTS
       NVL(description, '') || ''');'
           *
ERROR at line 3:
ORA-00904: "DESCRIPTION": invalid identifier



COMMIT;
