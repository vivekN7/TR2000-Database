-- Control Data Export
-- ETL_FILTER data
INSERT INTO ETL_FILTER VALUES(65,'34','GRANE','4.2',TO_DATE('2025-09-05 00:25:32', 'YYYY-MM-DD HH24:MI:SS'),'TEST_USER');
-- CONTROL_SETTINGS data
INSERT INTO CONTROL_SETTINGS VALUES('API_BASE_URL','https://equinor.pipespec-api.presight.com','Base URL for TR2000 API');
INSERT INTO CONTROL_SETTINGS VALUES('MAX_PCS_DETAILS_PER_RUN','0','Maximum number of PCS to process for each PCS Details table in a single ETL run. Set to 0 or NULL to process all.');
-- CONTROL_ENDPOINTS data
    '''' || description || '''' ||
            *
ERROR at line 5:
ORA-00904: "DESCRIPTION": invalid identifier


