-- Package: PKG_ETL_PROCESSOR
-- Purpose: JSON parsing and loading for reference data

-- Drop existing package
BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE BODY PKG_ETL_PROCESSOR';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;
/
BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE PKG_ETL_PROCESSOR';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;
/

-- Create package specification

  CREATE OR REPLACE PACKAGE PKG_ETL_PROCESSOR" AS
    -- Parse individual reference types
    PROCEDURE parse_and_load_pcs_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_vds_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_mds_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_eds_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_vsk_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_esk_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_pipe_element_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_sc_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    PROCEDURE parse_and_load_vsm_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    );

    -- Parse PCS list
    PROCEDURE parse_and_load_pcs_list(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2
    );

    -- Parse PCS details
    PROCEDURE parse_and_load_pcs_details(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_pcs_name IN VARCHAR2,
        p_revision IN VARCHAR2,
        p_detail_type IN VARCHAR2
    );

    -- Parse VDS catalog
    PROCEDURE parse_and_load_vds_catalog(
        p_raw_json_id IN NUMBER
    );
END PKG_ETL_PROCESSOR;
CREATE OR REPLACE PACKAGE BODY PKG_ETL_PROCESSOR" AS

    -- =====================================================
    -- PARSE AND LOAD PCS REFERENCES
    -- =====================================================
    PROCEDURE parse_and_load_pcs_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Step 1: Get JSON from RAW_JSON
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        -- Step 2: Clear staging table
        DELETE FROM STG_PCS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        -- Step 3: Parse JSON to staging table
        INSERT INTO STG_PCS_REFERENCES (
            plant_id, issue_revision,
            "PCS", "Revision", "RevDate", "Status",
            "OfficialRevision", "RevisionSuffix", "RatingClass",
            "MaterialGroup", "HistoricalPCS", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.PCS, jt.Revision, jt.RevDate, jt.Status,
            jt.OfficialRevision, jt.RevisionSuffix, jt.RatingClass,
            jt.MaterialGroup, jt.HistoricalPCS, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssuePCSList[*]'
            COLUMNS (
                PCS VARCHAR2(100) PATH '$.PCS',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                RevisionSuffix VARCHAR2(50) PATH '$.RevisionSuffix',
                RatingClass VARCHAR2(100) PATH '$.RatingClass',
                MaterialGroup VARCHAR2(100) PATH '$.MaterialGroup',
                HistoricalPCS VARCHAR2(100) PATH '$.HistoricalPCS',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        -- Step 4: Move from staging to core table with type conversion
        DELETE FROM PCS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO PCS_REFERENCES (
            pcs_references_guid, plant_id, issue_revision, pcs_name,
            revision, rev_date, status, official_revision,
            revision_suffix, rating_class, material_group,
            historical_pcs, delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "PCS",
            "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "RevisionSuffix",
            "RatingClass", "MaterialGroup", "HistoricalPCS",
            "Delta", SYSDATE, SYSDATE
        FROM STG_PCS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_pcs_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'PCS_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'PCS_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_pcs_references;

    -- =====================================================
    -- PARSE AND LOAD VDS REFERENCES
    -- =====================================================
    PROCEDURE parse_and_load_vds_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Step 1: Get JSON from RAW_JSON
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        -- Step 2: Clear staging table
        DELETE FROM STG_VDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        -- Step 3: Parse JSON to staging table
        INSERT INTO STG_VDS_REFERENCES (
            plant_id, issue_revision,
            "VDS", "Revision", "RevDate", "Status",
            "OfficialRevision", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.VDS, jt.Revision, jt.RevDate, jt.Status,
            jt.OfficialRevision, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssueVDSList[*]'
            COLUMNS (
                VDS VARCHAR2(100) PATH '$.VDS',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        -- Step 4: Move from staging to core table with type conversion
        DELETE FROM VDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO VDS_REFERENCES (
            vds_references_guid, plant_id, issue_revision, vds_name,
            revision, rev_date, status, official_revision,
            delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "VDS",
            "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "Delta",
            SYSDATE, SYSDATE
        FROM STG_VDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_vds_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'VDS_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'VDS_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_vds_references;

    -- =====================================================
    -- PARSE AND LOAD MDS REFERENCES
    -- =====================================================
    PROCEDURE parse_and_load_mds_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Step 1: Get JSON from RAW_JSON
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        -- Step 2: Clear staging table
        DELETE FROM STG_MDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        -- Step 3: Parse JSON to staging table
        INSERT INTO STG_MDS_REFERENCES (
            plant_id, issue_revision,
            "MDS", "Revision", "Area", "RevDate",
            "Status", "OfficialRevision", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.MDS, jt.Revision, jt.Area, jt.RevDate,
            jt.Status, jt.OfficialRevision, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssueMDSList[*]'
            COLUMNS (
                MDS VARCHAR2(100) PATH '$.MDS',
                Revision VARCHAR2(50) PATH '$.Revision',
                Area VARCHAR2(100) PATH '$.Area',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        -- Step 4: Move from staging to core table with type conversion
        DELETE FROM MDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO MDS_REFERENCES (
            mds_references_guid, plant_id, issue_revision, mds_name,
            revision, area, rev_date, status, official_revision,
            delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "MDS",
            "Revision", "Area", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "Delta",
            SYSDATE, SYSDATE
        FROM STG_MDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_mds_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'MDS_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'MDS_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_mds_references;

    -- =====================================================
    -- PARSE AND LOAD EDS REFERENCES
    -- =====================================================
    PROCEDURE parse_and_load_eds_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Step 1: Get JSON from RAW_JSON
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        -- Step 2: Clear staging table
        DELETE FROM STG_EDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        -- Step 3: Parse JSON to staging table
        INSERT INTO STG_EDS_REFERENCES (
            plant_id, issue_revision,
            "EDS", "Revision", "RevDate", "Status",
            "OfficialRevision", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.EDS, jt.Revision, jt.RevDate, jt.Status,
            jt.OfficialRevision, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssueEDSList[*]'
            COLUMNS (
                EDS VARCHAR2(100) PATH '$.EDS',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        -- Step 4: Move from staging to core table with type conversion
        DELETE FROM EDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO EDS_REFERENCES (
            eds_references_guid, plant_id, issue_revision, eds_name,
            revision, rev_date, status, official_revision,
            delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "EDS",
            "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "Delta",
            SYSDATE, SYSDATE
        FROM STG_EDS_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_eds_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'EDS_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'EDS_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_eds_references;

    -- =====================================================
    -- PARSE AND LOAD VSK REFERENCES
    -- =====================================================
    PROCEDURE parse_and_load_vsk_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Step 1: Get JSON from RAW_JSON
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        -- Step 2: Clear staging table
        DELETE FROM STG_VSK_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        -- Step 3: Parse JSON to staging table
        INSERT INTO STG_VSK_REFERENCES (
            plant_id, issue_revision,
            "VSK", "Revision", "RevDate", "Status",
            "OfficialRevision", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.VSK, jt.Revision, jt.RevDate, jt.Status,
            jt.OfficialRevision, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssueVSKList[*]'
            COLUMNS (
                VSK VARCHAR2(100) PATH '$.VSK',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        -- Step 4: Move from staging to core table with type conversion
        DELETE FROM VSK_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO VSK_REFERENCES (
            vsk_references_guid, plant_id, issue_revision, vsk_name,
            revision, rev_date, status, official_revision,
            delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "VSK",
            "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "Delta",
            SYSDATE, SYSDATE
        FROM STG_VSK_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_vsk_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'VSK_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'VSK_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_vsk_references;

    -- =====================================================
    -- PARSE AND LOAD ESK REFERENCES
    -- =====================================================
    PROCEDURE parse_and_load_esk_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Step 1: Get JSON from RAW_JSON
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        -- Step 2: Clear staging table
        DELETE FROM STG_ESK_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        -- Step 3: Parse JSON to staging table
        INSERT INTO STG_ESK_REFERENCES (
            plant_id, issue_revision,
            "ESK", "Revision", "RevDate", "Status",
            "OfficialRevision", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.ESK, jt.Revision, jt.RevDate, jt.Status,
            jt.OfficialRevision, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssueESKList[*]'
            COLUMNS (
                ESK VARCHAR2(100) PATH '$.ESK',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        -- Step 4: Move from staging to core table with type conversion
        DELETE FROM ESK_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO ESK_REFERENCES (
            esk_references_guid, plant_id, issue_revision, esk_name,
            revision, rev_date, status, official_revision,
            delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "ESK",
            "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "Delta",
            SYSDATE, SYSDATE
        FROM STG_ESK_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_esk_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'ESK_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'ESK_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_esk_references;

    -- =====================================================
    -- PARSE AND LOAD PIPE ELEMENT REFERENCES
    -- =====================================================
    PROCEDURE parse_and_load_pipe_element_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Step 1: Get JSON from RAW_JSON
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        -- Step 2: Clear staging table
        DELETE FROM STG_PIPE_ELEMENT_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        -- Step 3: Parse JSON to staging table
        INSERT INTO STG_PIPE_ELEMENT_REFERENCES (
            plant_id, issue_revision,
            "ElementID", "ElementGroup", "DimensionStandard",
            "ProductForm", "MaterialGrade", "MDS", "MDSRevision",
            "Area", "Revision", "RevDate", "Status", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.ElementID, jt.ElementGroup, jt.DimensionStandard,
            jt.ProductForm, jt.MaterialGrade, jt.MDS, jt.MDSRevision,
            jt.Area, jt.Revision, jt.RevDate, jt.Status, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssuePipeElementList[*]'
            COLUMNS (
                ElementID VARCHAR2(50) PATH '$.ElementID',
                ElementGroup VARCHAR2(100) PATH '$.ElementGroup',
                DimensionStandard VARCHAR2(100) PATH '$.DimensionStandard',
                ProductForm VARCHAR2(100) PATH '$.ProductForm',
                MaterialGrade VARCHAR2(200) PATH '$.MaterialGrade',
                MDS VARCHAR2(100) PATH '$.MDS',
                MDSRevision VARCHAR2(50) PATH '$.MDSRevision',
                Area VARCHAR2(100) PATH '$.Area',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        -- Step 4: Move from staging to core table with type conversion
        DELETE FROM PIPE_ELEMENT_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO PIPE_ELEMENT_REFERENCES (
            pipe_element_references_guid, plant_id, issue_revision,
            element_id, element_group, dimension_standard,
            product_form, material_grade, mds, mds_revision,
            area, revision, rev_date, status, delta,
            created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision,
            TO_NUMBER("ElementID"), "ElementGroup", "DimensionStandard",
            "ProductForm", "MaterialGrade", "MDS", "MDSRevision",
            "Area", "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "Delta", SYSDATE, SYSDATE
        FROM STG_PIPE_ELEMENT_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_pipe_element_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'PIPE_ELEMENT_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'PIPE_ELEMENT_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_pipe_element_references;

    -- =====================================================
    -- PARSE AND LOAD SC REFERENCES
    -- =====================================================
    PROCEDURE parse_and_load_sc_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Step 1: Get JSON from RAW_JSON
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        -- Step 2: Clear staging table
        DELETE FROM STG_SC_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        -- Step 3: Parse JSON to staging table
        INSERT INTO STG_SC_REFERENCES (
            plant_id, issue_revision,
            "SC", "Revision", "RevDate", "Status",
            "OfficialRevision", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.SC, jt.Revision, jt.RevDate, jt.Status,
            jt.OfficialRevision, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssueSCList[*]'
            COLUMNS (
                SC VARCHAR2(100) PATH '$.SC',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        -- Step 4: Move from staging to core table with type conversion
        DELETE FROM SC_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO SC_REFERENCES (
            sc_references_guid, plant_id, issue_revision, sc_name,
            revision, rev_date, status, official_revision,
            delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "SC",
            "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "Delta",
            SYSDATE, SYSDATE
        FROM STG_SC_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_sc_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'SC_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'SC_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_sc_references;

    -- =====================================================
    -- PARSE AND LOAD VSM REFERENCES
    -- =====================================================
    PROCEDURE parse_and_load_vsm_references(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_issue_revision IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Step 1: Get JSON from RAW_JSON
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        -- Step 2: Clear staging table
        DELETE FROM STG_VSM_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        -- Step 3: Parse JSON to staging table
        INSERT INTO STG_VSM_REFERENCES (
            plant_id, issue_revision,
            "VSM", "Revision", "RevDate", "Status",
            "OfficialRevision", "Delta"
        )
        SELECT
            p_plant_id, p_issue_revision,
            jt.VSM, jt.Revision, jt.RevDate, jt.Status,
            jt.OfficialRevision, jt.Delta
        FROM JSON_TABLE(v_json, '$.getIssueVSMList[*]'
            COLUMNS (
                VSM VARCHAR2(100) PATH '$.VSM',
                Revision VARCHAR2(50) PATH '$.Revision',
                RevDate VARCHAR2(50) PATH '$.RevDate',
                Status VARCHAR2(50) PATH '$.Status',
                OfficialRevision VARCHAR2(50) PATH '$.OfficialRevision',
                Delta VARCHAR2(50) PATH '$.Delta'
            )
        ) jt;

        -- Step 4: Move from staging to core table with type conversion
        DELETE FROM VSM_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

        INSERT INTO VSM_REFERENCES (
            vsm_references_guid, plant_id, issue_revision, vsm_name,
            revision, rev_date, status, official_revision,
            delta, created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, issue_revision, "VSM",
            "Revision", PKG_DATE_UTILS.safe_parse_date("RevDate"),
            "Status", "OfficialRevision", "Delta",
            SYSDATE, SYSDATE
        FROM STG_VSM_REFERENCES
        WHERE plant_id = p_plant_id
        AND issue_revision = p_issue_revision;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_vsm_references: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'VSM_REFERENCES',
                p_plant_id => p_plant_id,
                p_issue_revision => p_issue_revision,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'VSM_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_vsm_references;

    -- =====================================================
    -- PARSE AND LOAD PCS LIST
    -- =====================================================
    PROCEDURE parse_and_load_pcs_list(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Step 1: Get JSON from RAW_JSON
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        -- Step 2: Clear staging table
        DELETE FROM STG_PCS_LIST
        WHERE plant_id = p_plant_id;

        -- Step 3: Parse JSON to staging table - FIXED: $.getPCS[*]
        INSERT INTO STG_PCS_LIST (
            plant_id, "PCS", "Revision", "Status", "RevDate",
            "RatingClass", "TestPressure", "MaterialGroup", "DesignCode",
            "LastUpdate", "LastUpdateBy", "Approver", "Notepad",
            "SpecialReqID", "TubePCS", "NewVDSSection"
        )
        SELECT
            p_plant_id,
            jt.PCS, jt.Revision, jt.Status, jt.RevDate,
            jt.RatingClass, jt.TestPressure, jt.MaterialGroup, jt.DesignCode,
            jt.LastUpdate, jt.LastUpdateBy, jt.Approver, jt.Notepad,
            jt.SpecialReqID, jt.TubePCS, jt.NewVDSSection
        FROM JSON_TABLE(v_json, '$.getPCS[*]'
                COLUMNS (
                    PCS VARCHAR2(100) PATH '$.PCS',
                    Revision VARCHAR2(50) PATH '$.Revision',
                    Status VARCHAR2(50) PATH '$.Status',
                    RevDate VARCHAR2(50) PATH '$.RevDate',
                    RatingClass VARCHAR2(100) PATH '$.RatingClass',
                    TestPressure VARCHAR2(50) PATH '$.TestPressure',
                    MaterialGroup VARCHAR2(100) PATH '$.MaterialGroup',
                    DesignCode VARCHAR2(100) PATH '$.DesignCode',
                    LastUpdate VARCHAR2(50) PATH '$.LastUpdate',
                    LastUpdateBy VARCHAR2(100) PATH '$.LastUpdateBy',
                    Approver VARCHAR2(100) PATH '$.Approver',
                    Notepad VARCHAR2(4000) PATH '$.Notepad',
                    SpecialReqID VARCHAR2(50) PATH '$.SpecialReqID',
                    TubePCS VARCHAR2(100) PATH '$.TubePCS',
                    NewVDSSection VARCHAR2(100) PATH '$.NewVDSSection'
                )
            ) jt;

        -- Step 4: Move from staging to core table with type conversion
        DELETE FROM PCS_LIST
        WHERE plant_id = p_plant_id;

        INSERT INTO PCS_LIST (
            pcs_list_guid, plant_id, pcs_name, revision, status, rev_date,
            rating_class, test_pressure, material_group, design_code,
            last_update, last_update_by, approver, notepad,
            special_req_id, tube_pcs, new_vds_section,
            created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), plant_id, "PCS", "Revision", "Status",
            PKG_DATE_UTILS.safe_parse_date("RevDate"), "RatingClass",
            TO_NUMBER("TestPressure"), "MaterialGroup", "DesignCode",
            PKG_DATE_UTILS.safe_parse_date("LastUpdate"), "LastUpdateBy",
            "Approver", "Notepad", TO_NUMBER("SpecialReqID"),
            "TubePCS", "NewVDSSection", SYSDATE, SYSDATE
        FROM STG_PCS_LIST
        WHERE plant_id = p_plant_id
        AND "PCS" IS NOT NULL;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_pcs_list: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'PCS_LIST',
                p_plant_id => p_plant_id,
                p_issue_revision => NULL,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'PCS_LIST_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_pcs_list;

    -- =====================================================
    -- PARSE AND LOAD PCS DETAILS
    -- =====================================================
    PROCEDURE parse_and_load_pcs_details(
        p_raw_json_id IN NUMBER,
        p_plant_id IN VARCHAR2,
        p_pcs_name IN VARCHAR2,
        p_revision IN VARCHAR2,
        p_detail_type IN VARCHAR2
    ) IS
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Delegate to PKG_PCS_DETAIL_PROCESSOR which already has implementations
        PKG_PCS_DETAIL_PROCESSOR.process_pcs_detail(
            p_raw_json_id => p_raw_json_id,
            p_plant_id => p_plant_id,
            p_pcs_name => p_pcs_name,
            p_revision => p_revision,
            p_detail_type => p_detail_type
        );
    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_pcs_details: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'PCS_DETAIL_' || UPPER(p_detail_type),
                p_plant_id => p_plant_id,
                p_issue_revision => NULL,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'PCS_DETAILS_PARSE_ERROR',
                p_error_message => v_error_msg
            );
            RAISE;
    END parse_and_load_pcs_details;

    -- =====================================================
    -- PARSE AND LOAD VDS CATALOG
    -- =====================================================
    PROCEDURE parse_and_load_vds_catalog(
        p_raw_json_id IN NUMBER
    ) IS
        v_json CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Step 1: Get JSON from RAW_JSON
        SELECT payload INTO v_json
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        -- Step 2: Clear staging table
        DELETE FROM STG_VDS_LIST;

        -- Step 3: Parse JSON to staging table - FIXED PATH: $.getVDS[*]
        INSERT INTO STG_VDS_LIST (
            "VDS", "Revision", "Status", "RevDate", "LastUpdate",
            "LastUpdateBy", "Description", "Notepad", "SpecialReqID",
            "ValveTypeID", "RatingClassID", "MaterialGroupID",
            "EndConnectionID", "BoreID", "VDSSizeID",
            "SizeRange", "CustomName", "SubsegmentList"
        )
        SELECT
            jt.VDS, jt.Revision, jt.Status, jt.RevDate, jt.LastUpdate,
            jt.LastUpdateBy, jt.Description, jt.Notepad, jt.SpecialReqID,
            jt.ValveTypeID, jt.RatingClassID, jt.MaterialGroupID,
            jt.EndConnectionID, jt.BoreID, jt.VDSSizeID,
            jt.SizeRange, jt.CustomName, jt.SubsegmentList
        FROM JSON_TABLE(v_json, '$.getVDS[*]'
                COLUMNS (
                    VDS VARCHAR2(100) PATH '$.VDS',
                    Revision VARCHAR2(50) PATH '$.Revision',
                    Status VARCHAR2(50) PATH '$.Status',
                    RevDate VARCHAR2(50) PATH '$.RevDate',
                    LastUpdate VARCHAR2(50) PATH '$.LastUpdate',
                    LastUpdateBy VARCHAR2(100) PATH '$.LastUpdateBy',
                    Description VARCHAR2(500) PATH '$.Description',
                    Notepad VARCHAR2(4000) PATH '$.Notepad',
                    SpecialReqID VARCHAR2(50) PATH '$.SpecialReqID',
                    ValveTypeID VARCHAR2(50) PATH '$.ValveTypeID',
                    RatingClassID VARCHAR2(50) PATH '$.RatingClassID',
                    MaterialGroupID VARCHAR2(50) PATH '$.MaterialGroupID',
                    EndConnectionID VARCHAR2(50) PATH '$.EndConnectionID',
                    BoreID VARCHAR2(50) PATH '$.BoreID',
                    VDSSizeID VARCHAR2(50) PATH '$.VDSSizeID',
                    SizeRange VARCHAR2(100) PATH '$.SizeRange',
                    CustomName VARCHAR2(200) PATH '$.CustomName',
                    SubsegmentList VARCHAR2(500) PATH '$.SubsegmentList'
                )
            ) jt;

        -- Step 4: Move from staging to core table with type conversion
        DELETE FROM VDS_LIST;

        INSERT INTO VDS_LIST (
            vds_list_guid, vds_name, revision, status, rev_date,
            last_update, last_update_by, description, notepad,
            special_req_id, valve_type_id, rating_class_id,
            material_group_id, end_connection_id, bore_id,
            vds_size_id, size_range, custom_name, subsegment_list,
            created_date, last_modified_date
        )
        SELECT
            SYS_GUID(), "VDS", "Revision", "Status",
            PKG_DATE_UTILS.safe_parse_date("RevDate"),
            PKG_DATE_UTILS.safe_parse_date("LastUpdate"),
            "LastUpdateBy", "Description", "Notepad",
            TO_NUMBER("SpecialReqID"), TO_NUMBER("ValveTypeID"),
            TO_NUMBER("RatingClassID"), TO_NUMBER("MaterialGroupID"),
            TO_NUMBER("EndConnectionID"), TO_NUMBER("BoreID"),
            TO_NUMBER("VDSSizeID"), "SizeRange", "CustomName",
            "SubsegmentList", SYSDATE, SYSDATE
        FROM STG_VDS_LIST
        WHERE "VDS" IS NOT NULL;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := 'Error in parse_and_load_vds_catalog: ' || SQLERRM;
            PKG_ETL_LOGGING.log_error(
                p_endpoint_key => 'VDS_CATALOG',
                p_plant_id => NULL,
                p_issue_revision => NULL,
                p_error_type => 'PROCESSING_ERROR',
                p_error_code => 'VDS_CATALOG_PARSE_ERROR',
                p_error_message => v_error_msg,
                p_raw_data => v_json
            );
            RAISE;
    END parse_and_load_vds_catalog;

END PKG_ETL_PROCESSOR;
/
