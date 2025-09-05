CREATE OR REPLACE PROCEDURE fix_embedded_notes_parser AS
CREATE OR REPLACE BEGIN
CREATE OR REPLACE     EXECUTE IMMEDIATE '
CREATE OR REPLACE     CREATE OR REPLACE PROCEDURE temp_fix_embedded_notes(
CREATE OR REPLACE	  p_json_content IN CLOB,
CREATE OR REPLACE	  p_plant_id	 IN VARCHAR2,
CREATE OR REPLACE	  p_pcs_name	 IN VARCHAR2,
CREATE OR REPLACE	  p_revision	 IN VARCHAR2
CREATE OR REPLACE     ) AS
CREATE OR REPLACE     BEGIN
CREATE OR REPLACE	  -- Clear existing data
CREATE OR REPLACE	  DELETE FROM PCS_EMBEDDED_NOTES
CREATE OR REPLACE	  WHERE plant_id = p_plant_id
CREATE OR REPLACE	    AND pcs_name = p_pcs_name
CREATE OR REPLACE	    AND revision = p_revision;
CREATE OR REPLACE
CREATE OR REPLACE	  -- Insert new data
CREATE OR REPLACE	  IF JSON_EXISTS(p_json_content, ''$.getEmbeddedNote'') THEN
CREATE OR REPLACE	      INSERT INTO PCS_EMBEDDED_NOTES (
CREATE OR REPLACE		  pcs_embedded_notes_guid, plant_id, pcs_name, revision,
CREATE OR REPLACE		  text_section_id, text_section_description,
CREATE OR REPLACE		  page_break, html_clob,  -- Correct column name
CREATE OR REPLACE		  created_date, last_modified_date
CREATE OR REPLACE	      )
CREATE OR REPLACE	      SELECT
CREATE OR REPLACE		  SYS_GUID(),
CREATE OR REPLACE		  p_plant_id,
CREATE OR REPLACE		  p_pcs_name,
CREATE OR REPLACE		  p_revision,
CREATE OR REPLACE		  jt.text_section_id,
CREATE OR REPLACE		  jt.text_section_description,
CREATE OR REPLACE		  jt.page_break,
CREATE OR REPLACE		  jt.html_clob,  -- Correct mapping
CREATE OR REPLACE		  SYSDATE,
CREATE OR REPLACE		  SYSDATE
CREATE OR REPLACE	      FROM JSON_TABLE(
CREATE OR REPLACE		  p_json_content, ''$.getEmbeddedNote[*]''
CREATE OR REPLACE		  COLUMNS (
CREATE OR REPLACE		      text_section_id	       VARCHAR2(50)  PATH ''$.TextSectionID'',
CREATE OR REPLACE		      text_section_description VARCHAR2(500) PATH ''$.TextSectionDescription'',
CREATE OR REPLACE		      page_break	       VARCHAR2(10)  PATH ''$.PageBreak'',
CREATE OR REPLACE		      html_clob 	       CLOB	     PATH ''$.HTMLCLOB''
CREATE OR REPLACE		  )
CREATE OR REPLACE	      ) jt;
CREATE OR REPLACE	  END IF;
CREATE OR REPLACE
CREATE OR REPLACE	  COMMIT;
CREATE OR REPLACE     END temp_fix_embedded_notes;
CREATE OR REPLACE     ';
CREATE OR REPLACE
CREATE OR REPLACE     DBMS_OUTPUT.PUT_LINE('Embedded notes parser fixed');
CREATE OR REPLACE END;
CREATE OR REPLACE PROCEDURE fix_pcs_list_parser AS
CREATE OR REPLACE     v_sql CLOB;
CREATE OR REPLACE BEGIN
CREATE OR REPLACE     -- Extract just the parse_and_load_pcs_list procedure
CREATE OR REPLACE     v_sql := '
CREATE OR REPLACE     CREATE OR REPLACE PROCEDURE temp_parse_and_load_pcs_list(
CREATE OR REPLACE	  p_raw_json_id IN NUMBER,
CREATE OR REPLACE	  p_plant_id	IN VARCHAR2
CREATE OR REPLACE     ) IS
CREATE OR REPLACE	  v_json_content CLOB;
CREATE OR REPLACE     BEGIN
CREATE OR REPLACE	  SELECT payload INTO v_json_content
CREATE OR REPLACE	  FROM RAW_JSON
CREATE OR REPLACE	  WHERE raw_json_id = p_raw_json_id;
CREATE OR REPLACE
CREATE OR REPLACE	  DELETE FROM STG_PCS_LIST
CREATE OR REPLACE	  WHERE plant_id = p_plant_id;
CREATE OR REPLACE
CREATE OR REPLACE	  -- Parse JSON - API returns PCS not Name
CREATE OR REPLACE	  IF JSON_EXISTS(v_json_content, ''$.getPlantPcsList'') THEN
CREATE OR REPLACE	      INSERT INTO STG_PCS_LIST (
CREATE OR REPLACE		  plant_id, pcs, revision, status, rev_date,
CREATE OR REPLACE		  rating_class, test_pressure, material_group, design_code,
CREATE OR REPLACE		  last_update, last_update_by, approver, notepad,
CREATE OR REPLACE		  special_req_id, tube_pcs, new_vds_section
CREATE OR REPLACE	      )
CREATE OR REPLACE	      SELECT
CREATE OR REPLACE		  p_plant_id,
CREATE OR REPLACE		  jt.pcs,
CREATE OR REPLACE		  jt.revision,
CREATE OR REPLACE		  jt.status,
CREATE OR REPLACE		  jt.rev_date,
CREATE OR REPLACE		  jt.rating_class,
CREATE OR REPLACE		  jt.test_pressure,
CREATE OR REPLACE		  jt.material_group,
CREATE OR REPLACE		  jt.design_code,
CREATE OR REPLACE		  jt.last_update,
CREATE OR REPLACE		  jt.last_update_by,
CREATE OR REPLACE		  jt.approver,
CREATE OR REPLACE		  jt.notepad,
CREATE OR REPLACE		  jt.special_req_id,
CREATE OR REPLACE		  jt.tube_pcs,
CREATE OR REPLACE		  jt.new_vds_section
CREATE OR REPLACE	      FROM JSON_TABLE(
CREATE OR REPLACE		  v_json_content, ''$.getPlantPcsList[*]''
CREATE OR REPLACE		  COLUMNS (
CREATE OR REPLACE		      pcs	       VARCHAR2(100) PATH ''$.PCS'',
CREATE OR REPLACE		      revision	       VARCHAR2(50)  PATH ''$.Revision'',
CREATE OR REPLACE		      status	       VARCHAR2(50)  PATH ''$.Status'',
CREATE OR REPLACE		      rev_date	       VARCHAR2(50)  PATH ''$.RevDate'',
CREATE OR REPLACE		      rating_class     VARCHAR2(100) PATH ''$.RatingClass'',
CREATE OR REPLACE		      test_pressure    VARCHAR2(50)  PATH ''$.TestPressure'',
CREATE OR REPLACE		      material_group   VARCHAR2(100) PATH ''$.MaterialGroup'',
CREATE OR REPLACE		      design_code      VARCHAR2(100) PATH ''$.DesignCode'',
CREATE OR REPLACE		      last_update      VARCHAR2(50)  PATH ''$.LastUpdate'',
CREATE OR REPLACE		      last_update_by   VARCHAR2(100) PATH ''$.LastUpdateBy'',
CREATE OR REPLACE		      approver	       VARCHAR2(100) PATH ''$.Approver'',
CREATE OR REPLACE		      notepad	       VARCHAR2(4000) PATH ''$.Notepad'',
CREATE OR REPLACE		      special_req_id   VARCHAR2(50)  PATH ''$.SpecialReqID'',
CREATE OR REPLACE		      tube_pcs	       VARCHAR2(100) PATH ''$.TubePCS'',
CREATE OR REPLACE		      new_vds_section  VARCHAR2(100) PATH ''$.NewVDSSection''
CREATE OR REPLACE		  )
CREATE OR REPLACE	      ) jt;
CREATE OR REPLACE	  ELSE
CREATE OR REPLACE	      -- Try direct array
CREATE OR REPLACE	      INSERT INTO STG_PCS_LIST (
CREATE OR REPLACE		  plant_id, pcs, revision, status, rev_date,
CREATE OR REPLACE		  rating_class, test_pressure, material_group, design_code,
CREATE OR REPLACE		  last_update, last_update_by, approver, notepad,
CREATE OR REPLACE		  special_req_id, tube_pcs, new_vds_section
CREATE OR REPLACE	      )
CREATE OR REPLACE	      SELECT
CREATE OR REPLACE		  p_plant_id,
CREATE OR REPLACE		  jt.pcs,
CREATE OR REPLACE		  jt.revision,
CREATE OR REPLACE		  jt.status,
CREATE OR REPLACE		  jt.rev_date,
CREATE OR REPLACE		  jt.rating_class,
CREATE OR REPLACE		  jt.test_pressure,
CREATE OR REPLACE		  jt.material_group,
CREATE OR REPLACE		  jt.design_code,
CREATE OR REPLACE		  jt.last_update,
CREATE OR REPLACE		  jt.last_update_by,
CREATE OR REPLACE		  jt.approver,
CREATE OR REPLACE		  jt.notepad,
CREATE OR REPLACE		  jt.special_req_id,
CREATE OR REPLACE		  jt.tube_pcs,
CREATE OR REPLACE		  jt.new_vds_section
CREATE OR REPLACE	      FROM JSON_TABLE(
CREATE OR REPLACE		  v_json_content, ''$[*]''
CREATE OR REPLACE		  COLUMNS (
CREATE OR REPLACE		      pcs	       VARCHAR2(100) PATH ''$.PCS'',
CREATE OR REPLACE		      revision	       VARCHAR2(50)  PATH ''$.Revision'',
CREATE OR REPLACE		      status	       VARCHAR2(50)  PATH ''$.Status'',
CREATE OR REPLACE		      rev_date	       VARCHAR2(50)  PATH ''$.RevDate'',
CREATE OR REPLACE		      rating_class     VARCHAR2(100) PATH ''$.RatingClass'',
CREATE OR REPLACE		      test_pressure    VARCHAR2(50)  PATH ''$.TestPressure'',
CREATE OR REPLACE		      material_group   VARCHAR2(100) PATH ''$.MaterialGroup'',
CREATE OR REPLACE		      design_code      VARCHAR2(100) PATH ''$.DesignCode'',
CREATE OR REPLACE		      last_update      VARCHAR2(50)  PATH ''$.LastUpdate'',
CREATE OR REPLACE		      last_update_by   VARCHAR2(100) PATH ''$.LastUpdateBy'',
CREATE OR REPLACE		      approver	       VARCHAR2(100) PATH ''$.Approver'',
CREATE OR REPLACE		      notepad	       VARCHAR2(4000) PATH ''$.Notepad'',
CREATE OR REPLACE		      special_req_id   VARCHAR2(50)  PATH ''$.SpecialReqID'',
CREATE OR REPLACE		      tube_pcs	       VARCHAR2(100) PATH ''$.TubePCS'',
CREATE OR REPLACE		      new_vds_section  VARCHAR2(100) PATH ''$.NewVDSSection''
CREATE OR REPLACE		  )
CREATE OR REPLACE	      ) jt;
CREATE OR REPLACE	  END IF;
CREATE OR REPLACE
CREATE OR REPLACE	  -- Load into PCS_LIST with proper column mapping
CREATE OR REPLACE	  INSERT INTO PCS_LIST (
CREATE OR REPLACE	      pcs_list_guid,
CREATE OR REPLACE	      plant_id,
CREATE OR REPLACE	      pcs_name,      -- Map from PCS
CREATE OR REPLACE	      revision,      -- Use revision as-is (not current_revision)
CREATE OR REPLACE	      status,
CREATE OR REPLACE	      rev_date,
CREATE OR REPLACE	      rating_class,
CREATE OR REPLACE	      test_pressure,
CREATE OR REPLACE	      material_group,
CREATE OR REPLACE	      design_code,
CREATE OR REPLACE	      last_update,
CREATE OR REPLACE	      last_update_by,
CREATE OR REPLACE	      approver,
CREATE OR REPLACE	      notepad,
CREATE OR REPLACE	      special_req_id,
CREATE OR REPLACE	      tube_pcs,
CREATE OR REPLACE	      new_vds_section,
CREATE OR REPLACE	      created_date,
CREATE OR REPLACE	      last_modified_date
CREATE OR REPLACE	  )
CREATE OR REPLACE	  SELECT
CREATE OR REPLACE	      SYS_GUID(),
CREATE OR REPLACE	      plant_id,
CREATE OR REPLACE	      pcs,	     -- Map PCS to pcs_name
CREATE OR REPLACE	      revision,
CREATE OR REPLACE	      status,
CREATE OR REPLACE	      PKG_DATE_UTILS.safe_parse_date(rev_date),
CREATE OR REPLACE	      rating_class,
CREATE OR REPLACE	      TO_NUMBER(test_pressure),
CREATE OR REPLACE	      material_group,
CREATE OR REPLACE	      design_code,
CREATE OR REPLACE	      PKG_DATE_UTILS.safe_parse_date(last_update),
CREATE OR REPLACE	      last_update_by,
CREATE OR REPLACE	      approver,
CREATE OR REPLACE	      notepad,
CREATE OR REPLACE	      TO_NUMBER(special_req_id),
CREATE OR REPLACE	      tube_pcs,
CREATE OR REPLACE	      new_vds_section,
CREATE OR REPLACE	      SYSDATE,
CREATE OR REPLACE	      SYSDATE
CREATE OR REPLACE	  FROM STG_PCS_LIST
CREATE OR REPLACE	  WHERE plant_id = p_plant_id
CREATE OR REPLACE	    AND pcs IS NOT NULL;
CREATE OR REPLACE     END temp_parse_and_load_pcs_list;
CREATE OR REPLACE     ';
CREATE OR REPLACE
CREATE OR REPLACE     EXECUTE IMMEDIATE v_sql;
CREATE OR REPLACE     DBMS_OUTPUT.PUT_LINE('Temporary procedure created');
CREATE OR REPLACE END;
CREATE OR REPLACE PROCEDURE fix_vds_catalog_parser AS
CREATE OR REPLACE BEGIN
CREATE OR REPLACE     EXECUTE IMMEDIATE '
CREATE OR REPLACE     CREATE OR REPLACE PROCEDURE temp_fix_vds_catalog(
CREATE OR REPLACE	  p_json_content IN CLOB
CREATE OR REPLACE     ) AS
CREATE OR REPLACE     BEGIN
CREATE OR REPLACE	  -- Clear existing VDS catalog
CREATE OR REPLACE	  DELETE FROM VDS_LIST;
CREATE OR REPLACE
CREATE OR REPLACE	  -- Parse and load VDS catalog
CREATE OR REPLACE	  IF JSON_EXISTS(p_json_content, ''$.getVDSList'') THEN
CREATE OR REPLACE	      INSERT INTO VDS_LIST (
CREATE OR REPLACE		  vds_list_guid,  -- Correct column name
CREATE OR REPLACE		  vds_name,
CREATE OR REPLACE		  revision,
CREATE OR REPLACE		  status,
CREATE OR REPLACE		  rev_date,
CREATE OR REPLACE		  description,
CREATE OR REPLACE		  valve_type_id,
CREATE OR REPLACE		  rating_class_id,
CREATE OR REPLACE		  material_group_id,
CREATE OR REPLACE		  end_connection_id,
CREATE OR REPLACE		  bore_id,
CREATE OR REPLACE		  size_range,
CREATE OR REPLACE		  custom_name,
CREATE OR REPLACE		  subsegment_list,
CREATE OR REPLACE		  created_date,
CREATE OR REPLACE		  last_modified_date
CREATE OR REPLACE	      )
CREATE OR REPLACE	      SELECT
CREATE OR REPLACE		  SYS_GUID(),  -- Generate GUID
CREATE OR REPLACE		  jt.vds_name,
CREATE OR REPLACE		  jt.revision,
CREATE OR REPLACE		  jt.status,
CREATE OR REPLACE		  PKG_DATE_UTILS.safe_parse_date(jt.rev_date),
CREATE OR REPLACE		  jt.description,
CREATE OR REPLACE		  TO_NUMBER(jt.valve_type_id),
CREATE OR REPLACE		  TO_NUMBER(jt.rating_class_id),
CREATE OR REPLACE		  TO_NUMBER(jt.material_group_id),
CREATE OR REPLACE		  TO_NUMBER(jt.end_connection_id),
CREATE OR REPLACE		  TO_NUMBER(jt.bore_id),
CREATE OR REPLACE		  jt.size_range,
CREATE OR REPLACE		  jt.custom_name,
CREATE OR REPLACE		  jt.subsegment_list,
CREATE OR REPLACE		  SYSDATE,
CREATE OR REPLACE		  SYSDATE
CREATE OR REPLACE	      FROM JSON_TABLE(
CREATE OR REPLACE		  p_json_content, ''$.getVDSList[*]''
CREATE OR REPLACE		  COLUMNS (
CREATE OR REPLACE		      vds_name		 VARCHAR2(100) PATH ''$.VDS'',
CREATE OR REPLACE		      revision		 VARCHAR2(50)  PATH ''$.Revision'',
CREATE OR REPLACE		      status		 VARCHAR2(50)  PATH ''$.Status'',
CREATE OR REPLACE		      rev_date		 VARCHAR2(50)  PATH ''$.RevDate'',
CREATE OR REPLACE		      description	 VARCHAR2(500) PATH ''$.Description'',
CREATE OR REPLACE		      valve_type_id	 VARCHAR2(50)  PATH ''$.ValveTypeID'',
CREATE OR REPLACE		      rating_class_id	 VARCHAR2(50)  PATH ''$.RatingClassID'',
CREATE OR REPLACE		      material_group_id  VARCHAR2(50)  PATH ''$.MaterialGroupID'',
CREATE OR REPLACE		      end_connection_id  VARCHAR2(50)  PATH ''$.EndConnectionID'',
CREATE OR REPLACE		      bore_id		 VARCHAR2(50)  PATH ''$.BoreID'',
CREATE OR REPLACE		      size_range	 VARCHAR2(100) PATH ''$.SizeRange'',
CREATE OR REPLACE		      custom_name	 VARCHAR2(200) PATH ''$.CustomName'',
CREATE OR REPLACE		      subsegment_list	 VARCHAR2(500) PATH ''$.SubsegmentList''
CREATE OR REPLACE		  )
CREATE OR REPLACE	      ) jt;
CREATE OR REPLACE	  END IF;
CREATE OR REPLACE
CREATE OR REPLACE	  COMMIT;
CREATE OR REPLACE     END temp_fix_vds_catalog;
CREATE OR REPLACE     ';
CREATE OR REPLACE
CREATE OR REPLACE     DBMS_OUTPUT.PUT_LINE('VDS catalog parser fixed');
CREATE OR REPLACE END;
CREATE OR REPLACE PROCEDURE temp_fix_vds_parse(p_raw_json_id IN NUMBER) IS
CREATE OR REPLACE     v_json CLOB;
CREATE OR REPLACE BEGIN
CREATE OR REPLACE     SELECT payload INTO v_json FROM RAW_JSON WHERE raw_json_id = p_raw_json_id;
CREATE OR REPLACE
CREATE OR REPLACE     -- Clear all VDS catalog data
CREATE OR REPLACE     DELETE FROM STG_VDS_LIST;
CREATE OR REPLACE     DELETE FROM VDS_LIST;
CREATE OR REPLACE
CREATE OR REPLACE     -- Parse JSON with correct path
CREATE OR REPLACE     INSERT INTO STG_VDS_LIST (
CREATE OR REPLACE	  "VDS", "Revision", "Status", "RevDate", "LastUpdate",
CREATE OR REPLACE	  "LastUpdateBy", "Description", "Notepad", "SpecialReqID",
CREATE OR REPLACE	  "ValveTypeID", "RatingClassID", "MaterialGroupID",
CREATE OR REPLACE	  "EndConnectionID", "BoreID", "VDSSizeID", "SizeRange",
CREATE OR REPLACE	  "CustomName", "SubsegmentList"
CREATE OR REPLACE     )
CREATE OR REPLACE     SELECT
CREATE OR REPLACE	  jt."VDS", jt."Revision", jt."Status", jt."RevDate", jt."LastUpdate",
CREATE OR REPLACE	  jt."LastUpdateBy", jt."Description", jt."Notepad", jt."SpecialReqID",
CREATE OR REPLACE	  jt."ValveTypeID", jt."RatingClassID", jt."MaterialGroupID",
CREATE OR REPLACE	  jt."EndConnectionID", jt."BoreID", jt."VDSSizeID", jt."SizeRange",
CREATE OR REPLACE	  jt."CustomName", jt."SubsegmentList"
CREATE OR REPLACE     FROM JSON_TABLE(v_json, '$.getVDS[*]'
CREATE OR REPLACE	  COLUMNS (
CREATE OR REPLACE	      "VDS" VARCHAR2(100) PATH '$.VDS',
CREATE OR REPLACE	      "Revision" VARCHAR2(50) PATH '$.Revision',
CREATE OR REPLACE	      "Status" VARCHAR2(50) PATH '$.Status',
CREATE OR REPLACE	      "RevDate" VARCHAR2(50) PATH '$.RevDate',
CREATE OR REPLACE	      "LastUpdate" VARCHAR2(50) PATH '$.LastUpdate',
CREATE OR REPLACE	      "LastUpdateBy" VARCHAR2(100) PATH '$.LastUpdateBy',
CREATE OR REPLACE	      "Description" VARCHAR2(500) PATH '$.Description',
CREATE OR REPLACE	      "Notepad" VARCHAR2(4000) PATH '$.Notepad',
CREATE OR REPLACE	      "SpecialReqID" VARCHAR2(50) PATH '$.SpecialReqID',
CREATE OR REPLACE	      "ValveTypeID" VARCHAR2(50) PATH '$.ValveTypeID',
CREATE OR REPLACE	      "RatingClassID" VARCHAR2(50) PATH '$.RatingClassID',
CREATE OR REPLACE	      "MaterialGroupID" VARCHAR2(50) PATH '$.MaterialGroupID',
CREATE OR REPLACE	      "EndConnectionID" VARCHAR2(50) PATH '$.EndConnectionID',
CREATE OR REPLACE	      "BoreID" VARCHAR2(50) PATH '$.BoreID',
CREATE OR REPLACE	      "VDSSizeID" VARCHAR2(50) PATH '$.VDSSizeID',
CREATE OR REPLACE	      "SizeRange" VARCHAR2(100) PATH '$.SizeRange',
CREATE OR REPLACE	      "CustomName" VARCHAR2(200) PATH '$.CustomName',
CREATE OR REPLACE	      "SubsegmentList" VARCHAR2(500) PATH '$.SubsegmentList'
CREATE OR REPLACE	  )) jt;
CREATE OR REPLACE
CREATE OR REPLACE     -- Move to core tables
CREATE OR REPLACE     INSERT INTO VDS_LIST (
CREATE OR REPLACE	  vds_list_guid, vds_name, revision, status, rev_date,
CREATE OR REPLACE	  last_update, last_update_by, description, notepad,
CREATE OR REPLACE	  special_req_id, valve_type_id, rating_class_id,
CREATE OR REPLACE	  material_group_id, end_connection_id, bore_id,
CREATE OR REPLACE	  vds_size_id, size_range, custom_name, subsegment_list,
CREATE OR REPLACE	  created_date, last_modified_date
CREATE OR REPLACE     )
CREATE OR REPLACE     SELECT
CREATE OR REPLACE	  SYS_GUID(), "VDS", "Revision", "Status",
CREATE OR REPLACE	  PKG_DATE_UTILS.safe_parse_date("RevDate"),
CREATE OR REPLACE	  PKG_DATE_UTILS.safe_parse_date("LastUpdate"),
CREATE OR REPLACE	  "LastUpdateBy", "Description", "Notepad",
CREATE OR REPLACE	  TO_NUMBER("SpecialReqID"), TO_NUMBER("ValveTypeID"),
CREATE OR REPLACE	  TO_NUMBER("RatingClassID"), TO_NUMBER("MaterialGroupID"),
CREATE OR REPLACE	  TO_NUMBER("EndConnectionID"), TO_NUMBER("BoreID"),
CREATE OR REPLACE	  TO_NUMBER("VDSSizeID"), "SizeRange", "CustomName",
CREATE OR REPLACE	  "SubsegmentList", SYSDATE, SYSDATE
CREATE OR REPLACE     FROM STG_VDS_LIST;
CREATE OR REPLACE
CREATE OR REPLACE     COMMIT;
CREATE OR REPLACE END;
