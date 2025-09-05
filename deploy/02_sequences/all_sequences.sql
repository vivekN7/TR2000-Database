-- All Sequences for TR2000 ETL System
-- Most tables use IDENTITY columns, so minimal sequences needed

-- Note: Tables with IDENTITY columns (auto-generated sequences):
-- - ETL_FILTER (filter_id)
-- - CONTROL_ENDPOINTS (endpoint_id) 
-- - ETL_ERROR_LOG (error_id)
-- - ETL_RUN_LOG (run_id)
-- - ETL_STATISTICS (stat_id)
-- - RAW_JSON (raw_json_id)
-- - All reference tables (*_REFERENCES_GUID uses SYS_GUID())
-- - All PCS detail tables (*_GUID uses SYS_GUID()) 
-- - All catalog tables (*_GUID uses SYS_GUID())

-- Only create sequences if explicitly needed for custom logic
-- Currently, all tables use IDENTITY columns or SYS_GUID() defaults

PROMPT No manual sequences required - all tables use IDENTITY columns or SYS_GUID();