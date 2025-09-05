-- Procedure: FIX_VDS_CATALOG_PARSER
DROP PROCEDURE FIX_VDS_CATALOG_PARSER;
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

