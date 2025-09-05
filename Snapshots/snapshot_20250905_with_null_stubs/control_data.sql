-- ETL_FILTER data
INSERT INTO ETL_FILTER (filter_id, plant_id, plant_name, issue_revision, added_date, added_by_user_id) VALUES (65, '34', 'GRANE', '4.2', SYSDATE, 'TEST_USER');
-- CONTROL_SETTINGS data
INSERT INTO CONTROL_SETTINGS (setting_key, setting_value, description) VALUES ('API_BASE_URL', 'https://equinor.pipespec-api.presight.com', 'Base URL for TR2000 API');
INSERT INTO CONTROL_SETTINGS (setting_key, setting_value, description) VALUES ('MAX_PCS_DETAILS_PER_RUN', '8', 'Maximum number of PCS to process for each PCS Details table in a single ETL run. Set to 0 or NULL to process all.');
-- CONTROL_ENDPOINTS data
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (3, 'PLANTS', '/plants');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (4, 'ISSUES', '/plants/{plant_id}/issues');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (5, 'PCS_LIST', '/plants/{plant_id}/pcs');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (6, 'PCS_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/pcs');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (7, 'VDS_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/vds');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (8, 'MDS_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/mds');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (9, 'EDS_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/eds');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (10, 'VSK_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/vsk');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (11, 'ESK_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/esk');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (12, 'SC_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/sc');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (13, 'VSM_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/vsm');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (14, 'PIPE_ELEMENT_REFERENCES', '/plants/{plant_id}/issues/rev/{issue_revision}/pipe-elements');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (15, 'PCS_HEADER_PROPERTIES', '/plants/{plant_id}/pcs/{pcs_name}/rev/{pcs_revision}');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (16, 'PCS_TEMP_PRESSURES', '/plants/{plant_id}/pcs/{pcs_name}/rev/{pcs_revision}/temp-pressures');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (17, 'PCS_PIPE_SIZES', '/plants/{plant_id}/pcs/{pcs_name}/rev/{pcs_revision}/pipe-sizes');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (18, 'PCS_PIPE_ELEMENTS', '/plants/{plant_id}/pcs/{pcs_name}/rev/{pcs_revision}/pipe-elements');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (19, 'PCS_VALVE_ELEMENTS', '/plants/{plant_id}/pcs/{pcs_name}/rev/{pcs_revision}/valve-elements');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (20, 'PCS_EMBEDDED_NOTES', '/plants/{plant_id}/pcs/{pcs_name}/rev/{pcs_revision}/embedded-notes');
INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template) VALUES (21, 'VDS_CATALOG', '/vds');
