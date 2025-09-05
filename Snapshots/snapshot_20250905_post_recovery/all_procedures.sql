-- Procedures export from TR2000_STAGING
-- Generated: 2025-09-05 02:58:09



  CREATE OR REPLACE EDITIONABLE PROCEDURE "TR2000_STAGING"."FIX_EMBEDDED_NOTES_PARSER" AS
BEGIN
    EXECUTE IMMEDIATE '
    CREATE OR REPLACE PROCEDURE temp_fix_embedded_notes(
        p_json_content IN CLOB,
        p_plant_id     IN VARCHAR2,
        p_pcs_name     IN VARCHAR2,
        p_revision     IN VARCHAR2
    ) AS
    BEGIN
        -- Clear existing data
        DELETE FROM PCS_EMBEDDED_NOTES
        WHERE plant_id = p_plant_id
          AND pcs_name = p_pcs_name
          AND revision = p_revision;

        -- Insert new data
        IF JSON_EXISTS(p_json_content, ''$.getEmbeddedNote'') THEN
            INSERT INTO PCS_EMBEDDED_NOTES (
                pcs_embedded_notes_guid, plant_id, pcs_name, revision,
                text_section_id, text_section_description,
                page_break, html_clob,  -- Correct column name
                created_date, last_modified_date
            )
            SELECT
                SYS_GUID(),
                p_plant_id,
                p_pcs_name,
                p_revision,
                jt.text_section_id,
                jt.text_section_description,
                jt.page_break,
                jt.html_clob,  -- Correct mapping
                SYSDATE,
                SYSDATE
            FROM JSON_TABLE(
                p_json_content, ''$.getEmbeddedNote[*]''
                COLUMNS (
                    text_section_id          VARCHAR2(50)  PATH ''$.TextSectionID'',
                    text_section_description VARCHAR2(500) PATH ''$.TextSectionDescription'',
                    page_break               VARCHAR2(10)  PATH ''$.PageBreak'',
                    html_clob                CLOB          PATH ''$.HTMLCLOB''
                )
            ) jt;
        END IF;

        COMMIT;
    END temp_fix_embedded_notes;
    ';

    DBMS_OUTPUT.PUT_LINE('Embedded notes parser fixed');
END;
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


  CREATE OR REPLACE EDITIONABLE PROCEDURE "TR2000_STAGING"."FIX_VDS_CATALOG_PARSER" AS
BEGIN
    EXECUTE IMMEDIATE '
    CREATE OR REPLACE PROCEDURE temp_fix_vds_catalog(
        p_json_content IN CLOB
    ) AS
    BEGIN
        -- Clear existing VDS catalog
        DELETE FROM VDS_LIST;

        -- Parse and load VDS catalog
        IF JSON_EXISTS(p_json_content, ''$.getVDSList'') THEN
            INSERT INTO VDS_LIST (
                vds_list_guid,  -- Correct column name
                vds_name,
                revision,
                status,
                rev_date,
                description,
                valve_type_id,
                rating_class_id,
                material_group_id,
                end_connection_id,
                bore_id,
                size_range,
                custom_name,
                subsegment_list,
                created_date,
                last_modified_date
            )
            SELECT
                SYS_GUID(),  -- Generate GUID
                jt.vds_name,
                jt.revision,
                jt.status,
                PKG_DATE_UTILS.safe_parse_date(jt.rev_date),
                jt.description,
                TO_NUMBER(jt.valve_type_id),
                TO_NUMBER(jt.rating_class_id),
                TO_NUMBER(jt.material_group_id),
                TO_NUMBER(jt.end_connection_id),
                TO_NUMBER(jt.bore_id),
                jt.size_range,
                jt.custom_name,
                jt.subsegment_list,
                SYSDATE,
                SYSDATE
            FROM JSON_TABLE(
                p_json_content, ''$.getVDSList[*]''
                COLUMNS (
                    vds_name           VARCHAR2(100) PATH ''$.VDS'',
                    revision           VARCHAR2(50)  PATH ''$.Revision'',
                    status             VARCHAR2(50)  PATH ''$.Status'',
                    rev_date           VARCHAR2(50)  PATH ''$.RevDate'',
                    description        VARCHAR2(500) PATH ''$.Description'',
                    valve_type_id      VARCHAR2(50)  PATH ''$.ValveTypeID'',
                    rating_class_id    VARCHAR2(50)  PATH ''$.RatingClassID'',
                    material_group_id  VARCHAR2(50)  PATH ''$.MaterialGroupID'',
                    end_connection_id  VARCHAR2(50)  PATH ''$.EndConnectionID'',
                    bore_id            VARCHAR2(50)  PATH ''$.BoreID'',
                    size_range         VARCHAR2(100) PATH ''$.SizeRange'',
                    custom_name        VARCHAR2(200) PATH ''$.CustomName'',
                    subsegment_list    VARCHAR2(500) PATH ''$.SubsegmentList''
                )
            ) jt;
        END IF;

        COMMIT;
    END temp_fix_vds_catalog;
    ';

    DBMS_OUTPUT.PUT_LINE('VDS catalog parser fixed');
END;
/


  CREATE OR REPLACE EDITIONABLE PROCEDURE "TR2000_STAGING"."TEMP_FIX_VDS_PARSE" (p_raw_json_id IN NUMBER) IS
    v_json CLOB;
BEGIN
    SELECT payload INTO v_json FROM RAW_JSON WHERE raw_json_id = p_raw_json_id;

    -- Clear all VDS catalog data
    DELETE FROM STG_VDS_LIST;
    DELETE FROM VDS_LIST;

    -- Parse JSON with correct path
    INSERT INTO STG_VDS_LIST (
        "VDS", "Revision", "Status", "RevDate", "LastUpdate",
        "LastUpdateBy", "Description", "Notepad", "SpecialReqID",
        "ValveTypeID", "RatingClassID", "MaterialGroupID",
        "EndConnectionID", "BoreID", "VDSSizeID", "SizeRange",
        "CustomName", "SubsegmentList"
    )
    SELECT
        jt."VDS", jt."Revision", jt."Status", jt."RevDate", jt."LastUpdate",
        jt."LastUpdateBy", jt."Description", jt."Notepad", jt."SpecialReqID",
        jt."ValveTypeID", jt."RatingClassID", jt."MaterialGroupID",
        jt."EndConnectionID", jt."BoreID", jt."VDSSizeID", jt."SizeRange",
        jt."CustomName", jt."SubsegmentList"
    FROM JSON_TABLE(v_json, '$.getVDS[*]'
        COLUMNS (
            "VDS" VARCHAR2(100) PATH '$.VDS',
            "Revision" VARCHAR2(50) PATH '$.Revision',
            "Status" VARCHAR2(50) PATH '$.Status',
            "RevDate" VARCHAR2(50) PATH '$.RevDate',
            "LastUpdate" VARCHAR2(50) PATH '$.LastUpdate',
            "LastUpdateBy" VARCHAR2(100) PATH '$.LastUpdateBy',
            "Description" VARCHAR2(500) PATH '$.Description',
            "Notepad" VARCHAR2(4000) PATH '$.Notepad',
            "SpecialReqID" VARCHAR2(50) PATH '$.SpecialReqID',
            "ValveTypeID" VARCHAR2(50) PATH '$.ValveTypeID',
            "RatingClassID" VARCHAR2(50) PATH '$.RatingClassID',
            "MaterialGroupID" VARCHAR2(50) PATH '$.MaterialGroupID',
            "EndConnectionID" VARCHAR2(50) PATH '$.EndConnectionID',
            "BoreID" VARCHAR2(50) PATH '$.BoreID',
            "VDSSizeID" VARCHAR2(50) PATH '$.VDSSizeID',
            "SizeRange" VARCHAR2(100) PATH '$.SizeRange',
            "CustomName" VARCHAR2(200) PATH '$.CustomName',
            "SubsegmentList" VARCHAR2(500) PATH '$.SubsegmentList'
        )) jt;

    -- Move to core tables
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
    FROM STG_VDS_LIST;

    COMMIT;
END;
/

