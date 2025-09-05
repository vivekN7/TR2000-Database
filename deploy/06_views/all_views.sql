-- All Views
-- Generated from database

DROP VIEW V_API_CALLS_PER_RUN;
/

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "TR2000_STAGING"."V_API_CALLS_PER_RUN" ("RUN_ID", "RUN_TYPE", "RUN_START", "RUN_END", "RUN_STATUS", "UNIQUE_ENDPOINTS_CALLED", "TOTAL_API_CALLS", "API_CALL_COUNT", "PROCESSING_COUNT", "TOTAL_DATA_MB", "AVG_DURATION_MS", "TOTAL_DURATION_SECONDS", "TOTAL_RECORDS_PROCESSED", "TOTAL_RECORDS_INSERTED") AS
  SELECT
    r.run_id,
    r.run_type,
    r.start_time as run_start,
    r.end_time as run_end,
    r.status as run_status,
    COUNT(DISTINCT s.endpoint_key) as unique_endpoints_called,
    COUNT(*) as total_api_calls,
    SUM(CASE WHEN s.stat_type = 'API_CALL' THEN 1 ELSE 0 END) as api_call_count,
    SUM(CASE WHEN s.stat_type = 'PROCESSING' THEN 1 ELSE 0 END) as processing_count,
    SUM(s.api_response_size)/1024/1024 as total_data_mb,
    AVG(s.duration_ms) as avg_duration_ms,
    SUM(s.duration_ms)/1000 as total_duration_seconds,
    SUM(s.records_processed) as total_records_processed,
    SUM(s.records_inserted) as total_records_inserted
FROM ETL_RUN_LOG r
LEFT JOIN ETL_STATISTICS s ON r.run_id = s.run_id
GROUP BY r.run_id, r.run_type, r.start_time, r.end_time, r.status
ORDER BY r.run_id DESC
/

DROP VIEW V_API_CALL_STATISTICS;
/

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "TR2000_STAGING"."V_API_CALL_STATISTICS" ("ENDPOINT_KEY", "TOTAL_CALLS", "AVG_RESPONSE_TIME_MS", "MIN_RESPONSE_TIME_MS", "MAX_RESPONSE_TIME_MS", "TOTAL_DATA_MB", "AVG_RESPONSE_KB", "SUCCESSFUL_CALLS", "FAILED_CALLS", "LAST_CALL_TIME") AS
  SELECT
    endpoint_key,
    COUNT(*) as total_calls,
    AVG(duration_ms) as avg_response_time_ms,
    MIN(duration_ms) as min_response_time_ms,
    MAX(duration_ms) as max_response_time_ms,
    SUM(api_response_size)/1024/1024 as total_data_mb,
    AVG(api_response_size)/1024 as avg_response_kb,
    SUM(CASE WHEN api_status_code BETWEEN 200 AND 299 THEN 1 ELSE 0 END) as successful_calls,
    SUM(CASE WHEN api_status_code >= 400 THEN 1 ELSE 0 END) as failed_calls,
    MAX(start_time) as last_call_time
FROM ETL_STATISTICS
WHERE stat_type = 'API_CALL'
GROUP BY endpoint_key
ORDER BY total_calls DESC
/

DROP VIEW V_API_OPTIMIZATION_CANDIDATES;
/

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "TR2000_STAGING"."V_API_OPTIMIZATION_CANDIDATES" ("ENDPOINT_KEY", "CALL_COUNT", "RUN_COUNT", "AVG_CALLS_PER_RUN", "AVG_DURATION_MS", "TOTAL_TIME_SECONDS", "PCT_OF_TOTAL_TIME", "VOLUME_CATEGORY", "OPTIMIZATION_PRIORITY") AS
  WITH endpoint_stats AS (
    SELECT
        endpoint_key,
        COUNT(*) as call_count,
        AVG(duration_ms) as avg_duration_ms,
        SUM(duration_ms) as total_duration_ms,
        COUNT(DISTINCT run_id) as run_count
    FROM ETL_STATISTICS
    WHERE stat_type = 'API_CALL'
    AND endpoint_key IS NOT NULL
    GROUP BY endpoint_key
)
SELECT
    endpoint_key,
    call_count,
    run_count,
    ROUND(call_count / NULLIF(run_count, 0), 2) as avg_calls_per_run,
    ROUND(avg_duration_ms, 2) as avg_duration_ms,
    ROUND(total_duration_ms/1000, 2) as total_time_seconds,
    ROUND(total_duration_ms * 100.0 / SUM(total_duration_ms) OVER(), 2) as pct_of_total_time,
    CASE
        WHEN call_count / NULLIF(run_count, 0) > 50 THEN 'HIGH_VOLUME'
        WHEN call_count / NULLIF(run_count, 0) > 10 THEN 'MEDIUM_VOLUME'
        ELSE 'LOW_VOLUME'
    END as volume_category,
    CASE
        WHEN total_duration_ms > 60000 THEN 'OPTIMIZE_CRITICAL'
        WHEN total_duration_ms > 30000 THEN 'OPTIMIZE_RECOMMENDED'
        ELSE 'OK'
    END as optimization_priority
FROM endpoint_stats
ORDER BY call_count DESC
/

DROP VIEW V_ENDPOINT_TABLE_STATISTICS;
/

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "TR2000_STAGING"."V_ENDPOINT_TABLE_STATISTICS" ("ENDPOINT_KEY", "TARGET_TABLE", "TOTAL_CALLS", "RUNS_WITH_THIS_ENDPOINT", "AVG_CALLS_PER_RUN", "TOTAL_RECORDS_PROCESSED", "TOTAL_RECORDS_INSERTED", "AVG_DURATION_MS", "MAX_DURATION_MS", "TOTAL_DATA_MB", "LAST_CALLED") AS
  SELECT
    endpoint_key,
    CASE
        WHEN endpoint_key = 'PCS_REFERENCES' THEN 'PCS_REFERENCES'
        WHEN endpoint_key = 'VDS_REFERENCES' THEN 'VDS_REFERENCES'
        WHEN endpoint_key = 'MDS_REFERENCES' THEN 'MDS_REFERENCES'
        WHEN endpoint_key = 'EDS_REFERENCES' THEN 'EDS_REFERENCES'
        WHEN endpoint_key = 'VSK_REFERENCES' THEN 'VSK_REFERENCES'
        WHEN endpoint_key = 'ESK_REFERENCES' THEN 'ESK_REFERENCES'
        WHEN endpoint_key = 'PIPE_ELEMENT_REFERENCES' THEN 'PIPE_ELEMENT_REFERENCES'
        WHEN endpoint_key = 'SC_REFERENCES' THEN 'SC_REFERENCES'
        WHEN endpoint_key = 'VSM_REFERENCES' THEN 'VSM_REFERENCES'
        WHEN endpoint_key = 'PCS_LIST' THEN 'PCS_LIST'
        WHEN endpoint_key = 'PCS_HEADER_PROPERTIES' THEN 'PCS_HEADER_PROPERTIES'
        WHEN endpoint_key = 'PCS_TEMP_PRESSURES' THEN 'PCS_TEMP_PRESSURES'
        WHEN endpoint_key = 'PCS_PIPE_SIZES' THEN 'PCS_PIPE_SIZES'
        WHEN endpoint_key = 'PCS_PIPE_ELEMENTS' THEN 'PCS_PIPE_ELEMENTS'
        WHEN endpoint_key = 'PCS_VALVE_ELEMENTS' THEN 'PCS_VALVE_ELEMENTS'
        WHEN endpoint_key = 'PCS_EMBEDDED_NOTES' THEN 'PCS_EMBEDDED_NOTES'
        WHEN endpoint_key = 'VDS_CATALOG' THEN 'VDS_LIST'
        ELSE 'UNKNOWN'
    END as target_table,
    COUNT(*) as total_calls,
    COUNT(DISTINCT run_id) as runs_with_this_endpoint,
    ROUND(COUNT(*) / COUNT(DISTINCT run_id), 2) as avg_calls_per_run,
    SUM(records_processed) as total_records_processed,
    SUM(records_inserted) as total_records_inserted,
    AVG(duration_ms) as avg_duration_ms,
    MAX(duration_ms) as max_duration_ms,
    SUM(api_response_size)/1024/1024 as total_data_mb,
    MAX(start_time) as last_called
FROM ETL_STATISTICS
WHERE endpoint_key IS NOT NULL
GROUP BY endpoint_key
ORDER BY total_calls DESC
/

DROP VIEW V_ETL_RUN_SUMMARY;
/

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "TR2000_STAGING"."V_ETL_RUN_SUMMARY" ("RUN_ID", "RUN_TYPE", "START_TIME", "END_TIME", "RUN_DURATION_SECONDS", "STATUS", "UNIQUE_API_ENDPOINTS", "TOTAL_API_CALLS", "TOTAL_DATA_MB", "TOTAL_RECORDS", "ERROR_COUNT") AS
  SELECT
    r.run_id,
    r.run_type,
    r.start_time,
    r.end_time,
    ROUND(EXTRACT(MINUTE FROM (r.end_time - r.start_time)) * 60 +
          EXTRACT(SECOND FROM (r.end_time - r.start_time)), 2) as run_duration_seconds,
    r.status,
    (SELECT COUNT(DISTINCT endpoint_key) FROM ETL_STATISTICS WHERE run_id = r.run_id AND stat_type = 'API_CALL') as unique_api_endpoints,
    (SELECT COUNT(*) FROM ETL_STATISTICS WHERE run_id = r.run_id AND stat_type = 'API_CALL') as total_api_calls,
    (SELECT SUM(api_response_size)/1024/1024 FROM ETL_STATISTICS WHERE run_id = r.run_id) as total_data_mb,
    (SELECT SUM(records_processed) FROM ETL_STATISTICS WHERE run_id = r.run_id) as total_records,
    (SELECT COUNT(*) FROM ETL_ERROR_LOG WHERE error_timestamp BETWEEN r.start_time AND NVL(r.end_time, SYSTIMESTAMP)) as error_count
FROM ETL_RUN_LOG r
ORDER BY r.run_id DESC
/

DROP VIEW V_ETL_STATISTICS_SUMMARY;
/

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "TR2000_STAGING"."V_ETL_STATISTICS_SUMMARY" ("RUN_ID", "ENDPOINT_KEY", "OPERATION_COUNT", "TOTAL_RECORDS_PROCESSED", "TOTAL_RECORDS_INSERTED", "TOTAL_RECORDS_FAILED", "AVG_DURATION_MS", "MIN_DURATION_MS", "MAX_DURATION_MS", "TOTAL_DURATION_MS", "FIRST_OPERATION", "LAST_OPERATION", "SUCCESS_COUNT", "FAILURE_COUNT", "SUCCESS_RATE_PCT") AS
  SELECT
    run_id,
    endpoint_key,
    COUNT(*) as operation_count,
    SUM(records_processed) as total_records_processed,
    SUM(records_inserted) as total_records_inserted,
    SUM(records_failed) as total_records_failed,
    AVG(duration_ms) as avg_duration_ms,
    MIN(duration_ms) as min_duration_ms,
    MAX(duration_ms) as max_duration_ms,
    SUM(duration_ms) as total_duration_ms,
    MIN(start_time) as first_operation,
    MAX(end_time) as last_operation,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failure_count,
    ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate_pct
FROM ETL_STATISTICS
GROUP BY run_id, endpoint_key
/

DROP VIEW V_OPERATION_STATISTICS;
/

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "TR2000_STAGING"."V_OPERATION_STATISTICS" ("OPERATION_NAME", "EXECUTION_COUNT", "AVG_DURATION_MS", "MIN_DURATION_MS", "MAX_DURATION_MS", "TOTAL_RECORDS_PROCESSED", "TOTAL_RECORDS_INSERTED", "TOTAL_RECORDS_UPDATED", "TOTAL_RECORDS_DELETED", "TOTAL_RECORDS_FAILED", "SUCCESS_COUNT", "FAILURE_COUNT", "SUCCESS_RATE") AS
  SELECT
    operation_name,
    COUNT(*) as execution_count,
    AVG(duration_ms) as avg_duration_ms,
    MIN(duration_ms) as min_duration_ms,
    MAX(duration_ms) as max_duration_ms,
    SUM(records_processed) as total_records_processed,
    SUM(records_inserted) as total_records_inserted,
    SUM(records_updated) as total_records_updated,
    SUM(records_deleted) as total_records_deleted,
    SUM(records_failed) as total_records_failed,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failure_count,
    ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
FROM ETL_STATISTICS
WHERE operation_name IS NOT NULL
GROUP BY operation_name
ORDER BY execution_count DESC
/

DROP VIEW V_RAW_JSON;
/

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "TR2000_STAGING"."V_RAW_JSON" ("RAW_JSON_ID", "ENDPOINT_KEY", "ENDPOINT_TEMPLATE", "ENDPOINT_VALUE", "PLANT_ID", "ISSUE_REVISION", "PCS_NAME", "PCS_REVISION", "ENDPOINT_CATEGORY", "DATA_STATUS", "BATCH_ID", "API_CALL_TIMESTAMP", "CREATED_DATE", "PAYLOAD_SIZE", "PAYLOAD_PREVIEW") AS
  SELECT
    raw_json_id,
    endpoint_key,
    endpoint_template,
    endpoint_value,
    -- Extract plant_id from endpoint_value
    CASE
        WHEN endpoint_value LIKE '/plants/%' THEN
            REGEXP_SUBSTR(endpoint_value, '/plants/([^/]+)', 1, 1, NULL, 1)
        ELSE NULL
    END AS plant_id,
    -- Extract issue_revision from endpoint_value
    CASE
        WHEN endpoint_value LIKE '%/issues/rev/%' THEN
            REGEXP_SUBSTR(endpoint_value, '/issues/rev/([^/]+)', 1, 1, NULL, 1)
        ELSE NULL
    END AS issue_revision,
    -- Extract pcs_name from endpoint_value
    CASE
        WHEN endpoint_value LIKE '%/pcs/%/rev/%' THEN
            REGEXP_SUBSTR(endpoint_value, '/pcs/([^/]+)/rev/', 1, 1, NULL, 1)
        ELSE NULL
    END AS pcs_name,
    -- Extract pcs_revision from endpoint_value
    CASE
        WHEN endpoint_value LIKE '%/pcs/%/rev/%' THEN
            REGEXP_SUBSTR(endpoint_value, '/pcs/[^/]+/rev/([^/]+)', 1, 1, NULL, 1)
        ELSE NULL
    END AS pcs_revision,
    -- Categorize endpoint type
    CASE
        WHEN endpoint_key LIKE '%_REFERENCES' THEN 'Reference Data'
        WHEN endpoint_key LIKE 'PCS_%' AND endpoint_key != 'PCS_LIST' THEN 'PCS Details'
        WHEN endpoint_key = 'PCS_LIST' THEN 'PCS List'
        WHEN endpoint_key = 'VDS_CATALOG' THEN 'VDS Catalog'
        WHEN endpoint_key = 'PLANTS' THEN 'Plants'
        WHEN endpoint_key = 'ISSUES' THEN 'Issues'
        ELSE 'Other'
    END AS endpoint_category,
    -- Processing status based on payload
    CASE
        WHEN payload IS NULL THEN 'No Data'
        WHEN DBMS_LOB.GETLENGTH(payload) < 50 THEN 'Empty/Error'
        WHEN DBMS_LOB.INSTR(payload, '"error"') > 0 THEN 'API Error'
        WHEN DBMS_LOB.SUBSTR(payload, 2, 1) = '[]' OR DBMS_LOB.SUBSTR(payload, 2, 1) = '{}' THEN 'Empty Result'
        ELSE 'Has Data'
    END AS data_status,
    batch_id,
    api_call_timestamp,
    created_date,
    -- Payload info
    DBMS_LOB.GETLENGTH(payload) AS payload_size,
    -- Payload preview (handle CLOB properly)
    CASE
        WHEN payload IS NULL THEN 'NULL'
        WHEN DBMS_LOB.GETLENGTH(payload) < 50 THEN TO_CHAR(DBMS_LOB.SUBSTR(payload, 50, 1))
        ELSE DBMS_LOB.SUBSTR(payload, 100, 1) || '...'
    END AS payload_preview
FROM RAW_JSON
/

DROP VIEW V_RAW_JSON_SUMMARY;
/

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "TR2000_STAGING"."V_RAW_JSON_SUMMARY" ("ENDPOINT_KEY", "ENDPOINT_CATEGORY", "CALL_COUNT", "BATCH_COUNT", "PLANT_COUNT", "ISSUE_COUNT", "PCS_COUNT", "SUCCESSFUL_CALLS", "FAILED_CALLS", "MIN_PAYLOAD_SIZE", "MAX_PAYLOAD_SIZE", "AVG_PAYLOAD_SIZE", "TOTAL_PAYLOAD_SIZE", "FIRST_CALL", "LAST_CALL") AS
  WITH raw_data AS (
    SELECT
        endpoint_key,
        CASE
            WHEN endpoint_key LIKE '%_REFERENCES' THEN 'Reference Data'
            WHEN endpoint_key LIKE 'PCS_%' AND endpoint_key != 'PCS_LIST' THEN 'PCS Details'
            WHEN endpoint_key = 'PCS_LIST' THEN 'PCS List'
            WHEN endpoint_key = 'VDS_CATALOG' THEN 'VDS Catalog'
            ELSE 'Other'
        END AS endpoint_category,
        CASE
            WHEN endpoint_value LIKE '/plants/%' THEN
                REGEXP_SUBSTR(endpoint_value, '/plants/([^/]+)', 1, 1, NULL, 1)
            ELSE NULL
        END AS plant_id,
        CASE
            WHEN endpoint_value LIKE '%/issues/rev/%' THEN
                REGEXP_SUBSTR(endpoint_value, '/issues/rev/([^/]+)', 1, 1, NULL, 1)
            ELSE NULL
        END AS issue_revision,
        CASE
            WHEN endpoint_value LIKE '%/pcs/%/rev/%' THEN
                REGEXP_SUBSTR(endpoint_value, '/pcs/([^/]+)/rev/', 1, 1, NULL, 1)
            ELSE NULL
        END AS pcs_name,
        CASE
            WHEN payload IS NULL THEN 'No Data'
            WHEN DBMS_LOB.GETLENGTH(payload) < 50 THEN 'Empty/Error'
            ELSE 'Has Data'
        END AS data_status,
        DBMS_LOB.GETLENGTH(payload) AS payload_size,
        batch_id,
        api_call_timestamp
    FROM RAW_JSON
)
SELECT
    endpoint_key,
    endpoint_category,
    COUNT(*) as call_count,
    COUNT(DISTINCT batch_id) as batch_count,
    COUNT(DISTINCT plant_id) as plant_count,
    COUNT(DISTINCT issue_revision) as issue_count,
    COUNT(DISTINCT pcs_name) as pcs_count,
    SUM(CASE WHEN data_status = 'Has Data' THEN 1 ELSE 0 END) as successful_calls,
    SUM(CASE WHEN data_status != 'Has Data' THEN 1 ELSE 0 END) as failed_calls,
    MIN(payload_size) as min_payload_size,
    MAX(payload_size) as max_payload_size,
    ROUND(AVG(payload_size)) as avg_payload_size,
    SUM(payload_size) as total_payload_size,
    MIN(api_call_timestamp) as first_call,
    MAX(api_call_timestamp) as last_call
FROM raw_data
GROUP BY endpoint_key, endpoint_category
ORDER BY endpoint_category, endpoint_key
/

