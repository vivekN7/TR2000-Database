-- Package: PKG_MAIN_ETL_CONTROL
-- Purpose: Main ETL orchestration

-- Drop existing package
BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE BODY PKG_MAIN_ETL_CONTROL';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;
/
BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE PKG_MAIN_ETL_CONTROL';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;
/

-- Create package specification

  CREATE OR REPLACE PACKAGE PKG_MAIN_ETL_CONTROL" AS

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
CREATE OR REPLACE PACKAGE BODY PKG_MAIN_ETL_CONTROL" AS

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

        -- CRITICAL FIX: Clear all data tables at start
        DBMS_OUTPUT.PUT_LINE('Clearing all data tables...');
        PKG_ETL_TEST_UTILS.clear_all_data();
        DBMS_OUTPUT.PUT_LINE('Data tables cleared');

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

        -- No need to DELETE - tables already cleared in run_main_etl

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

        DBMS_OUTPUT.PUT_LINE('Processing PCS details for OFFICIAL revisions only...');
        DBMS_OUTPUT.PUT_LINE('Max PCS details per run: ' || v_max_pcs_details);

        v_batch_id := 'PCS_DETAILS_' || TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS');
        v_run_id := PKG_ETL_LOGGING.get_current_run_id;

        -- CRITICAL FIX: Only process official revisions from PCS_REFERENCES
        FOR rec IN (
            SELECT DISTINCT
                r.plant_id,
                r.pcs_name,
                r.official_revision as revision  -- Use OFFICIAL_REVISION
            FROM PCS_REFERENCES r
            WHERE r.official_revision IS NOT NULL
            -- Ensure the official revision exists in PCS_LIST
            AND EXISTS (
                SELECT 1 FROM PCS_LIST l
                WHERE l.plant_id = r.plant_id
                  AND l.pcs_name = r.pcs_name
                  AND l.revision = r.official_revision
            )
            AND ROWNUM <= CASE
                WHEN v_max_pcs_details = 0 THEN 999999
                ELSE v_max_pcs_details
            END
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('Processing PCS: ' || rec.pcs_name || ' Official Rev: ' || rec.revision);

            FOR detail_type IN (
                SELECT 'header-properties' as dtype FROM dual UNION ALL
                SELECT 'temp-pressures' FROM dual UNION ALL
                SELECT 'pipe-sizes' FROM dual UNION ALL
                SELECT 'pipe-elements' FROM dual UNION ALL
                SELECT 'valve-elements' FROM dual UNION ALL
                SELECT 'embedded-notes' FROM dual
            ) LOOP
                v_raw_json_id := PKG_API_CLIENT.fetch_pcs_detail(
                    p_plant_id => rec.plant_id,
                    p_pcs_name => rec.pcs_name,
                    p_revision => rec.revision,  -- Official revision
                    p_detail_type => detail_type.dtype,
                    p_batch_id => v_batch_id
                );

                PKG_PCS_DETAIL_PROCESSOR.process_pcs_detail(
                    p_raw_json_id => v_raw_json_id,
                    p_plant_id => rec.plant_id,
                    p_pcs_name => rec.pcs_name,
                    p_revision => rec.revision,  -- Official revision
                    p_detail_type => detail_type.dtype
                );
            END LOOP;

            v_count := v_count + 1;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('PCS details completed. Processed: ' || v_count || ' official PCS revisions');
        DBMS_OUTPUT.PUT_LINE('Total API calls: ' || (v_count * 6));
        COMMIT;
    END process_pcs_details;

END PKG_MAIN_ETL_CONTROL;
/
