-- Procedure: FIX_PCS_LIST_PARSER
DROP PROCEDURE FIX_PCS_LIST_PARSER;
/

  CREATE OR REPLACE EDITIONABLE PROCEDURE "TR2000_STAGING"."FIX_PCS_LIST_PARSER" AS
    v_sql CLOB;
BEGIN
    -- Extract just the parse_and_load_pcs_list procedure
    v_sql := '
    CREATE OR REPLACE PROCEDURE temp_parse_and_load_pcs_list(
        p_raw_json_id IN NUMBER,
        p_plant_id    IN VARCHAR2
    ) IS
        v_json_content CLOB;
    BEGIN
        SELECT payload INTO v_json_content
        FROM RAW_JSON
        WHERE raw_json_id = p_raw_json_id;

        DELETE FROM STG_PCS_LIST
        WHERE plant_id = p_plant_id;

        -- Parse JSON - API returns PCS not Name
        IF JSON_EXISTS(v_json_content, ''$.getPlantPcsList'') THEN
            INSERT INTO STG_PCS_LIST (
                plant_id, pcs, revision, status, rev_date,
                rating_class, test_pressure, material_group, design_code,
                last_update, last_update_by, approver, notepad,
                special_req_id, tube_pcs, new_vds_section
            )
            SELECT
                p_plant_id,
                jt.pcs,
                jt.revision,
                jt.status,
                jt.rev_date,
                jt.rating_class,
                jt.test_pressure,
                jt.material_group,
                jt.design_code,
                jt.last_update,
                jt.last_update_by,
                jt.approver,
                jt.notepad,
                jt.special_req_id,
                jt.tube_pcs,
                jt.new_vds_section
            FROM JSON_TABLE(
                v_json_content, ''$.getPlantPcsList[*]''
                COLUMNS (
                    pcs              VARCHAR2(100) PATH ''$.PCS'',
                    revision         VARCHAR2(50)  PATH ''$.Revision'',
                    status           VARCHAR2(50)  PATH ''$.Status'',
                    rev_date         VARCHAR2(50)  PATH ''$.RevDate'',
                    rating_class     VARCHAR2(100) PATH ''$.RatingClass'',
                    test_pressure    VARCHAR2(50)  PATH ''$.TestPressure'',
                    material_group   VARCHAR2(100) PATH ''$.MaterialGroup'',
                    design_code      VARCHAR2(100) PATH ''$.DesignCode'',
                    last_update      VARCHAR2(50)  PATH ''$.LastUpdate'',
                    last_update_by   VARCHAR2(100) PATH ''$.LastUpdateBy'',
                    approver         VARCHAR2(100) PATH ''$.Approver'',
                    notepad          VARCHAR2(4000) PATH ''$.Notepad'',
                    special_req_id   VARCHAR2(50)  PATH ''$.SpecialReqID'',
                    tube_pcs         VARCHAR2(100) PATH ''$.TubePCS'',
                    new_vds_section  VARCHAR2(100) PATH ''$.NewVDSSection''
                )
            ) jt;
        ELSE
            -- Try direct array
            INSERT INTO STG_PCS_LIST (
                plant_id, pcs, revision, status, rev_date,
                rating_class, test_pressure, material_group, design_code,
                last_update, last_update_by, approver, notepad,
                special_req_id, tube_pcs, new_vds_section
            )
            SELECT
                p_plant_id,
                jt.pcs,
                jt.revision,
                jt.status,
                jt.rev_date,
                jt.rating_class,
                jt.test_pressure,
                jt.material_group,
                jt.design_code,
                jt.last_update,
                jt.last_update_by,
                jt.approver,
                jt.notepad,
                jt.special_req_id,
                jt.tube_pcs,
                jt.new_vds_section
            FROM JSON_TABLE(
                v_json_content, ''$[*]''
                COLUMNS (
                    pcs              VARCHAR2(100) PATH ''$.PCS'',
                    revision         VARCHAR2(50)  PATH ''$.Revision'',
                    status           VARCHAR2(50)  PATH ''$.Status'',
                    rev_date         VARCHAR2(50)  PATH ''$.RevDate'',
                    rating_class     VARCHAR2(100) PATH ''$.RatingClass'',
                    test_pressure    VARCHAR2(50)  PATH ''$.TestPressure'',
                    material_group   VARCHAR2(100) PATH ''$.MaterialGroup'',
                    design_code      VARCHAR2(100) PATH ''$.DesignCode'',
                    last_update      VARCHAR2(50)  PATH ''$.LastUpdate'',
                    last_update_by   VARCHAR2(100) PATH ''$.LastUpdateBy'',
                    approver         VARCHAR2(100) PATH ''$.Approver'',
                    notepad          VARCHAR2(4000) PATH ''$.Notepad'',
                    special_req_id   VARCHAR2(50)  PATH ''$.SpecialReqID'',
                    tube_pcs         VARCHAR2(100) PATH ''$.TubePCS'',
                    new_vds_section  VARCHAR2(100) PATH ''$.NewVDSSection''
                )
            ) jt;
        END IF;

        -- Load into PCS_LIST with proper column mapping
        INSERT INTO PCS_LIST (
            pcs_list_guid,
            plant_id,
            pcs_name,      -- Map from PCS
            revision,      -- Use revision as-is (not current_revision)
            status,
            rev_date,
            rating_class,
            test_pressure,
            material_group,
            design_code,
            last_update,
            last_update_by,
            approver,
            notepad,
            special_req_id,
            tube_pcs,
            new_vds_section,
            created_date,
            last_modified_date
        )
        SELECT
            SYS_GUID(),
            plant_id,
            pcs,           -- Map PCS to pcs_name
            revision,
            status,
            PKG_DATE_UTILS.safe_parse_date(rev_date),
            rating_class,
            TO_NUMBER(test_pressure),
            material_group,
            design_code,
            PKG_DATE_UTILS.safe_parse_date(last_update),
            last_update_by,
            approver,
            notepad,
            TO_NUMBER(special_req_id),
            tube_pcs,
            new_vds_section,
            SYSDATE,
            SYSDATE
        FROM STG_PCS_LIST
        WHERE plant_id = p_plant_id
          AND pcs IS NOT NULL;
    END temp_parse_and_load_pcs_list;
    ';

    EXECUTE IMMEDIATE v_sql;
    DBMS_OUTPUT.PUT_LINE('Temporary procedure created');
END;
/

