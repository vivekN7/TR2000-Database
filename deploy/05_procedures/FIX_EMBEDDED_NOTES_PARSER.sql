-- Procedure: FIX_EMBEDDED_NOTES_PARSER
DROP PROCEDURE FIX_EMBEDDED_NOTES_PARSER;
/

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

