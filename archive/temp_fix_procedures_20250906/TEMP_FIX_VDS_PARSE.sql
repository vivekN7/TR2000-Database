-- Procedure: TEMP_FIX_VDS_PARSE
DROP PROCEDURE TEMP_FIX_VDS_PARSE;
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

