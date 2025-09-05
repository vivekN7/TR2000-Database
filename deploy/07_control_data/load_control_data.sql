-- Control Data Load Script
-- TR2000 ETL System
-- Date: 2025-01-05

-- Clear existing data
DELETE FROM ETL_FILTER;
DELETE FROM CONTROL_SETTINGS;
DELETE FROM CONTROL_ENDPOINTS;

-- Load ETL_FILTER (Test plant GRANE)
INSERT INTO ETL_FILTER (plant_id, plant_name, issue_revision, added_by_user_id, notes)
VALUES ('34', 'GRANE', '4.2', 'SYSTEM', 'Test configuration for GRANE plant');

-- Load CONTROL_SETTINGS
INSERT INTO CONTROL_SETTINGS (setting_key, setting_value, setting_type, description)
VALUES ('API_BASE_URL', 'https://equinor.pipespec-api.presight.com', 'URL', 'Base URL for TR2000 API');

INSERT INTO CONTROL_SETTINGS (setting_key, setting_value, setting_type, description)
VALUES ('MAX_PCS_DETAILS_PER_RUN', '5', 'NUMBER', 'Maximum number of PCS to process for each PCS Details table in a single ETL run. Set to 0 or NULL to process all.');

INSERT INTO CONTROL_SETTINGS (setting_key, setting_value, setting_type, description)
VALUES ('API_TIMEOUT_SECONDS', '300', 'NUMBER', 'API call timeout in seconds');

INSERT INTO CONTROL_SETTINGS (setting_key, setting_value, setting_type, description)
VALUES ('LOG_RETENTION_DAYS', '30', 'NUMBER', 'Number of days to retain ETL logs');

-- Load CONTROL_ENDPOINTS (Reference endpoints)
INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('PCS_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/pcs', 'REFERENCE', '$.getIssuePCSList[*]', 'STG_PCS_REFERENCES', 'PCS_REFERENCES', 'PCS reference data for issue');

INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('VDS_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/vds', 'REFERENCE', '$.getIssueVDSList[*]', 'STG_VDS_REFERENCES', 'VDS_REFERENCES', 'VDS reference data for issue');

INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('MDS_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/mds', 'REFERENCE', '$.getIssueMDSList[*]', 'STG_MDS_REFERENCES', 'MDS_REFERENCES', 'MDS reference data for issue');

INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('EDS_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/eds', 'REFERENCE', '$.getIssueEDSList[*]', 'STG_EDS_REFERENCES', 'EDS_REFERENCES', 'EDS reference data for issue');

INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('VSK_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/vsk', 'REFERENCE', '$.getIssueVSKList[*]', 'STG_VSK_REFERENCES', 'VSK_REFERENCES', 'VSK reference data for issue');

INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('ESK_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/esk', 'REFERENCE', '$.getIssueESKList[*]', 'STG_ESK_REFERENCES', 'ESK_REFERENCES', 'ESK reference data for issue');

INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('PIPE_ELEMENT_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/pipe-elements', 'REFERENCE', '$.getIssuePipeElementList[*]', 'STG_PIPE_ELEMENT_REFERENCES', 'PIPE_ELEMENT_REFERENCES', 'Pipe Element reference data for issue');

INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('SC_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/sc', 'REFERENCE', '$.getIssueSCList[*]', 'STG_SC_REFERENCES', 'SC_REFERENCES', 'SC reference data for issue');

INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('VSM_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/vsm', 'REFERENCE', '$.getIssueVSMList[*]', 'STG_VSM_REFERENCES', 'VSM_REFERENCES', 'VSM reference data for issue');

-- Load CONTROL_ENDPOINTS (PCS and VDS catalogs)
INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('PCS_LIST', '/plants/{plant_id}/pcs', 'CATALOG', '$.getPCSList[*]', 'STG_PCS_LIST', 'PCS_LIST', 'PCS catalog for plant');

INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('VDS_LIST', '/vds', 'CATALOG', '$.getVDSList[*]', 'STG_VDS_LIST', 'VDS_LIST', 'Global VDS catalog');

-- Load CONTROL_ENDPOINTS (PCS Detail endpoints)
INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('PCS_HEADER_PROPERTIES', '/plants/{plant_id}/pcs/{pcs_name}/rev/{pcs_revision}', 'PCS_DETAIL', '$.getPCSHeaderProperties[*]', 'STG_PCS_HEADER_PROPERTIES', 'PCS_HEADER_PROPERTIES', 'PCS header and properties');

INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('PCS_TEMP_PRESSURES', '/plants/{plant_id}/pcs/{pcs_name}/rev/{pcs_revision}/temp-pressures', 'PCS_DETAIL', '$.getPCSTempPressures[*]', 'STG_PCS_TEMP_PRESSURES', 'PCS_TEMP_PRESSURES', 'PCS temperature and pressure combinations');

INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('PCS_PIPE_SIZES', '/plants/{plant_id}/pcs/{pcs_name}/rev/{pcs_revision}/pipe-sizes', 'PCS_DETAIL', '$.getPCSPipeSizes[*]', 'STG_PCS_PIPE_SIZES', 'PCS_PIPE_SIZES', 'PCS pipe sizing specifications');

INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('PCS_PIPE_ELEMENTS', '/plants/{plant_id}/pcs/{pcs_name}/rev/{pcs_revision}/pipe-elements', 'PCS_DETAIL', '$.getPCSPipeElements[*]', 'STG_PCS_PIPE_ELEMENTS', 'PCS_PIPE_ELEMENTS', 'PCS pipe element details');

INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('PCS_VALVE_ELEMENTS', '/plants/{plant_id}/pcs/{pcs_name}/rev/{pcs_revision}/valve-elements', 'PCS_DETAIL', '$.getPCSValveElements[*]', 'STG_PCS_VALVE_ELEMENTS', 'PCS_VALVE_ELEMENTS', 'PCS valve element details');

INSERT INTO CONTROL_ENDPOINTS (endpoint_key, endpoint_template, endpoint_type, json_root_path, staging_table, core_table, comments)
VALUES ('PCS_EMBEDDED_NOTES', '/plants/{plant_id}/pcs/{pcs_name}/rev/{pcs_revision}/embedded-notes', 'PCS_DETAIL', '$.getPCSEmbeddedNotes[*]', 'STG_PCS_EMBEDDED_NOTES', 'PCS_EMBEDDED_NOTES', 'PCS embedded notes and documentation');

COMMIT;

-- Verification
SELECT 'ETL_FILTER' as table_name, COUNT(*) as rows FROM ETL_FILTER
UNION ALL
SELECT 'CONTROL_SETTINGS', COUNT(*) FROM CONTROL_SETTINGS  
UNION ALL
SELECT 'CONTROL_ENDPOINTS', COUNT(*) FROM CONTROL_ENDPOINTS;

PROMPT Control data loaded successfully:
PROMPT - 1 plant/issue filter (GRANE 34/4.2)
PROMPT - 4 control settings  
PROMPT - 17 endpoint configurations