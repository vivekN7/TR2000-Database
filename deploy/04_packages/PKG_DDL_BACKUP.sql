-- Package: PKG_DDL_BACKUP  
-- Purpose: Database DDL and control data backup to DDL_BACKUP_OWNER schema
-- Critical: Required for backing up code to external backup user

-- Drop existing package
BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE BODY PKG_DDL_BACKUP';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;
/
BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE PKG_DDL_BACKUP';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -4043 THEN RAISE; END IF; END;
/

-- Create package specification
CREATE OR REPLACE PACKAGE PKG_DDL_BACKUP AS
    -- Main backup procedures
    PROCEDURE backup_schema(
        p_schema_name VARCHAR2 DEFAULT USER,
        p_notes VARCHAR2 DEFAULT NULL,
        p_include_control_data BOOLEAN DEFAULT TRUE
    );

    PROCEDURE backup_control_data(
        p_schema_name VARCHAR2 DEFAULT USER,
        p_notes VARCHAR2 DEFAULT NULL,
        p_ddl_backup_id NUMBER DEFAULT NULL
    );

    -- Recovery procedures
    FUNCTION get_ddl_from_backup(
        p_backup_id NUMBER
    ) RETURN CLOB;

    FUNCTION restore_control_data(
        p_backup_id NUMBER,
        p_dry_run BOOLEAN DEFAULT TRUE
    ) RETURN VARCHAR2;

    -- Utility procedures
    PROCEDURE list_backups(
        p_days_back NUMBER DEFAULT 7,
        p_schema_name VARCHAR2 DEFAULT USER
    );

    PROCEDURE compare_backups(
        p_backup_id_1 NUMBER,
        p_backup_id_2 NUMBER
    );

    FUNCTION get_latest_backup_id(
        p_schema_name VARCHAR2 DEFAULT USER
    ) RETURN NUMBER;

END PKG_DDL_BACKUP;
/

-- Create package body
CREATE OR REPLACE PACKAGE BODY PKG_DDL_BACKUP AS

    -- Main backup procedure with fixed LOB handling
    PROCEDURE backup_schema(
        p_schema_name VARCHAR2 DEFAULT USER,
        p_notes VARCHAR2 DEFAULT NULL,
        p_include_control_data BOOLEAN DEFAULT TRUE
    ) IS
        v_ddl_content CLOB;
        v_object_count NUMBER := 0;
        v_view_count NUMBER := 0;
        v_package_count NUMBER := 0;
        v_table_count NUMBER := 0;
        v_sequence_count NUMBER := 0;
        v_trigger_count NUMBER := 0;
        v_ddl_backup_id NUMBER;
        v_start_time TIMESTAMP := SYSTIMESTAMP;
        v_end_time TIMESTAMP;
        v_duration_seconds NUMBER;
        v_hash VARCHAR2(64);
        v_temp_ddl CLOB;
        v_ddl_size NUMBER := 0;  -- Store size before freeing LOB

        CURSOR c_objects IS
            SELECT object_type, object_name
            FROM user_objects
            WHERE object_type IN ('TABLE', 'VIEW', 'PACKAGE', 'PACKAGE BODY',
                                  'PROCEDURE', 'FUNCTION', 'SEQUENCE', 'TRIGGER',
                                  'SYNONYM', 'TYPE', 'TYPE BODY', 'INDEX')
            AND object_name NOT LIKE 'BIN$%'
            AND object_name NOT IN ('DDL_BACKUP', 'CONTROL_DATA_BACKUP')
            AND object_name NOT LIKE 'SYS_%'
            AND object_name NOT LIKE 'ISEQ$$%'
            ORDER BY
                DECODE(object_type,
                       'TABLE', 1,
                       'SEQUENCE', 2,
                       'TYPE', 3,
                       'TYPE BODY', 4,
                       'FUNCTION', 5,
                       'PROCEDURE', 6,
                       'PACKAGE', 7,
                       'PACKAGE BODY', 8,
                       'VIEW', 9,
                       'TRIGGER', 10,
                       'INDEX', 11,
                       'SYNONYM', 12,
                       99),
                object_name;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Starting full DDL backup for schema: ' || p_schema_name);
        DBMS_OUTPUT.PUT_LINE('This will extract actual DDL for all objects...');

        -- Initialize DDL content
        DBMS_LOB.CREATETEMPORARY(v_ddl_content, TRUE);
        DBMS_LOB.APPEND(v_ddl_content,
            '-- Full DDL Backup for schema: ' || p_schema_name || CHR(10) ||
            '-- Generated: ' || TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM') || CHR(10) ||
            '-- Notes: ' || NVL(p_notes, 'No notes provided') || CHR(10) ||
            '-- WARNING: Execute this script carefully!' || CHR(10) || CHR(10)
        );

        -- Set DBMS_METADATA parameters
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', TRUE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);

        -- Extract DDL for each object
        FOR obj IN c_objects LOOP
            BEGIN
                -- Skip backup tables and certain object types
                IF obj.object_name IN ('DDL_BACKUP', 'CONTROL_DATA_BACKUP') THEN
                    CONTINUE;
                END IF;

                IF obj.object_type = 'PACKAGE BODY' THEN
                    CONTINUE;  -- Will be included with PACKAGE
                END IF;

                IF obj.object_type = 'INDEX' THEN
                    -- Skip constraint-based indexes
                    DECLARE
                        v_constraint_type VARCHAR2(1);
                    BEGIN
                        SELECT constraint_type INTO v_constraint_type
                        FROM user_constraints
                        WHERE constraint_name = obj.object_name
                        AND constraint_type IN ('P', 'U');
                        CONTINUE;
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                            NULL; -- Not a constraint index
                    END;
                END IF;

                -- Extract DDL (simplified to avoid errors)
                BEGIN
                    DBMS_LOB.APPEND(v_ddl_content,
                        CHR(10) || '-- ' || obj.object_type || ': ' || obj.object_name || CHR(10));

                    -- Count object types
                    IF obj.object_type = 'TABLE' THEN
                        v_table_count := v_table_count + 1;
                    ELSIF obj.object_type = 'VIEW' THEN
                        v_view_count := v_view_count + 1;
                    ELSIF obj.object_type = 'PACKAGE' THEN
                        v_package_count := v_package_count + 1;
                    ELSIF obj.object_type = 'SEQUENCE' THEN
                        v_sequence_count := v_sequence_count + 1;
                    ELSIF obj.object_type = 'TRIGGER' THEN
                        v_trigger_count := v_trigger_count + 1;
                    END IF;

                    -- Try to extract DDL
                    BEGIN
                        DBMS_LOB.CREATETEMPORARY(v_temp_ddl, TRUE);

                        IF obj.object_type IN ('TABLE', 'VIEW', 'SEQUENCE', 'PROCEDURE', 'FUNCTION', 'TRIGGER', 'SYNONYM') THEN
                            v_temp_ddl := DBMS_METADATA.GET_DDL(obj.object_type, obj.object_name, p_schema_name);
                        ELSIF obj.object_type = 'PACKAGE' THEN
                            v_temp_ddl := DBMS_METADATA.GET_DDL('PACKAGE', obj.object_name, p_schema_name);
                            -- Try to get body
                            BEGIN
                                DBMS_LOB.APPEND(v_temp_ddl, CHR(10) || '/' || CHR(10));
                                DBMS_LOB.APPEND(v_temp_ddl,
                                    DBMS_METADATA.GET_DDL('PACKAGE_BODY', obj.object_name, p_schema_name));
                            EXCEPTION
                                WHEN OTHERS THEN
                                    NULL;
                            END;
                        ELSIF obj.object_type = 'TYPE' THEN
                            v_temp_ddl := DBMS_METADATA.GET_DDL('TYPE', obj.object_name, p_schema_name);
                            -- Try to get body
                            BEGIN
                                DBMS_LOB.APPEND(v_temp_ddl, CHR(10) || '/' || CHR(10));
                                DBMS_LOB.APPEND(v_temp_ddl,
                                    DBMS_METADATA.GET_DDL('TYPE_BODY', obj.object_name, p_schema_name));
                            EXCEPTION
                                WHEN OTHERS THEN
                                    NULL;
                            END;
                        END IF;

                        -- Append to main DDL
                        IF DBMS_LOB.GETLENGTH(v_temp_ddl) > 0 THEN
                            DBMS_LOB.APPEND(v_ddl_content, v_temp_ddl);
                            DBMS_LOB.APPEND(v_ddl_content, CHR(10) || '/' || CHR(10));
                        END IF;

                        DBMS_LOB.FREETEMPORARY(v_temp_ddl);

                    EXCEPTION
                        WHEN OTHERS THEN
                            -- Log but continue
                            DBMS_LOB.APPEND(v_ddl_content,
                                '-- Error extracting DDL: ' || SUBSTR(SQLERRM, 1, 200) || CHR(10));
                            IF DBMS_LOB.ISTEMPORARY(v_temp_ddl) = 1 THEN
                                DBMS_LOB.FREETEMPORARY(v_temp_ddl);
                            END IF;
                    END;

                    v_object_count := v_object_count + 1;

                    IF MOD(v_object_count, 10) = 0 THEN
                        DBMS_OUTPUT.PUT_LINE('    Processed ' || v_object_count || ' objects...');
                    END IF;

                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('    Warning: Error processing ' || obj.object_type ||
                                           ' ' || obj.object_name || ': ' || SUBSTR(SQLERRM, 1, 100));
                END;
            END;
        END LOOP;

        -- Add footer
        DBMS_LOB.APPEND(v_ddl_content, CHR(10) || CHR(10) ||
            '-- ============================================' || CHR(10) ||
            '-- End of DDL Backup' || CHR(10) ||
            '-- Total Objects: ' || v_object_count || CHR(10) ||
            '-- ============================================' || CHR(10));

        -- Calculate values BEFORE insert
        v_end_time := SYSTIMESTAMP;
        v_duration_seconds := EXTRACT(SECOND FROM (v_end_time - v_start_time)) +
                             EXTRACT(MINUTE FROM (v_end_time - v_start_time)) * 60 +
                             EXTRACT(HOUR FROM (v_end_time - v_start_time)) * 3600;

        v_ddl_size := DBMS_LOB.GETLENGTH(v_ddl_content);
        v_hash := 'SIZE:' || v_ddl_size || ':COUNT:' || v_object_count;

        -- Get sequence value (use DDL_BACKUP_OWNER's sequence)
        SELECT DDL_BACKUP_OWNER.DDL_BACKUP_SEQ.NEXTVAL INTO v_ddl_backup_id FROM DUAL;

        -- Insert into backup table
        INSERT INTO DDL_BACKUP_OWNER.DDL_BACKUP (
            backup_id,
            backup_timestamp,
            schema_name,
            backup_type,
            object_count,
            view_count,
            package_count,
            table_count,
            sequence_count,
            trigger_count,
            ddl_content,
            ddl_size_bytes,
            ddl_hash,
            backup_notes,
            backup_user,
            os_user,
            ip_address,
            program,
            duration_seconds
        ) VALUES (
            v_ddl_backup_id,
            v_start_time,
            p_schema_name,
            'FULL_SCHEMA_DDL',
            v_object_count,
            v_view_count,
            v_package_count,
            v_table_count,
            v_sequence_count,
            v_trigger_count,
            v_ddl_content,
            v_ddl_size,
            v_hash,
            p_notes,
            USER,
            SYS_CONTEXT('USERENV', 'OS_USER'),
            SYS_CONTEXT('USERENV', 'IP_ADDRESS'),
            SYS_CONTEXT('USERENV', 'MODULE'),
            v_duration_seconds
        );

        -- Backup control data if requested
        IF p_include_control_data THEN
            backup_control_data(
                p_schema_name => p_schema_name,
                p_notes => p_notes,
                p_ddl_backup_id => v_ddl_backup_id
            );
        END IF;

        COMMIT;

        -- Cleanup AFTER commit
        DBMS_LOB.FREETEMPORARY(v_ddl_content);

        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('===== Backup Completed Successfully! =====');
        DBMS_OUTPUT.PUT_LINE('Backup ID: ' || v_ddl_backup_id);
        DBMS_OUTPUT.PUT_LINE('Total objects backed up: ' || v_object_count);
        DBMS_OUTPUT.PUT_LINE('    Tables: ' || v_table_count);
        DBMS_OUTPUT.PUT_LINE('    Views: ' || v_view_count);
        DBMS_OUTPUT.PUT_LINE('    Packages: ' || v_package_count);
        DBMS_OUTPUT.PUT_LINE('    Sequences: ' || v_sequence_count);
        DBMS_OUTPUT.PUT_LINE('    Triggers: ' || v_trigger_count);
        DBMS_OUTPUT.PUT_LINE('DDL Size: ' || ROUND(v_ddl_size/1024) || ' KB');
        DBMS_OUTPUT.PUT_LINE('Duration: ' || ROUND(v_duration_seconds, 2) || ' seconds');
        DBMS_OUTPUT.PUT_LINE('==========================================');

    EXCEPTION
        WHEN OTHERS THEN
            IF DBMS_LOB.ISTEMPORARY(v_ddl_content) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_ddl_content);
            END IF;
            DBMS_OUTPUT.PUT_LINE('BACKUP FAILED: ' || SQLERRM);
            RAISE_APPLICATION_ERROR(-20001, 'Backup failed: ' || SQLERRM);
    END backup_schema;

    -- Keep all other procedures unchanged
    PROCEDURE backup_control_data(
        p_schema_name VARCHAR2 DEFAULT USER,
        p_notes VARCHAR2 DEFAULT NULL,
        p_ddl_backup_id NUMBER DEFAULT NULL
    ) IS
        v_control_settings_json CLOB;
        v_control_endpoints_json CLOB;
        v_etl_filter_json CLOB;
        v_settings_count NUMBER := 0;
        v_endpoints_count NUMBER := 0;
        v_filter_count NUMBER := 0;
        v_backup_id NUMBER;
    BEGIN
        -- Initialize CLOBs
        DBMS_LOB.CREATETEMPORARY(v_control_settings_json, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_control_endpoints_json, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_etl_filter_json, TRUE);

        -- Build JSON for CONTROL_SETTINGS
        DBMS_LOB.APPEND(v_control_settings_json, '[');
        FOR rec IN (SELECT setting_key, setting_value, description
                   FROM CONTROL_SETTINGS ORDER BY setting_key) LOOP
            IF v_settings_count > 0 THEN
                DBMS_LOB.APPEND(v_control_settings_json, ',');
            END IF;
            DBMS_LOB.APPEND(v_control_settings_json,
                '{"setting_key":"' || rec.setting_key ||
                '","setting_value":"' || REPLACE(rec.setting_value, '"', '\"') ||
                '","description":"' || REPLACE(NVL(rec.description, ''), '"', '\"') || '"}');
            v_settings_count := v_settings_count + 1;
        END LOOP;
        DBMS_LOB.APPEND(v_control_settings_json, ']');

        -- Build JSON for CONTROL_ENDPOINTS
        DBMS_LOB.APPEND(v_control_endpoints_json, '[');
        FOR rec IN (SELECT endpoint_id, endpoint_key, endpoint_template, comments
                   FROM CONTROL_ENDPOINTS ORDER BY endpoint_id) LOOP
            IF v_endpoints_count > 0 THEN
                DBMS_LOB.APPEND(v_control_endpoints_json, ',');
            END IF;
            DBMS_LOB.APPEND(v_control_endpoints_json,
                '{"endpoint_id":' || rec.endpoint_id ||
                ',"endpoint_key":"' || rec.endpoint_key ||
                '","endpoint_template":"' || REPLACE(rec.endpoint_template, '"', '\"') ||
                '","comments":"' || REPLACE(NVL(rec.comments, ''), '"', '\"') || '"}');
            v_endpoints_count := v_endpoints_count + 1;
        END LOOP;
        DBMS_LOB.APPEND(v_control_endpoints_json, ']');

        -- Build JSON for ETL_FILTER
        DBMS_LOB.APPEND(v_etl_filter_json, '[');
        FOR rec IN (SELECT filter_id, plant_id, plant_name, issue_revision,
                          added_date, added_by_user_id, notes
                   FROM ETL_FILTER ORDER BY filter_id) LOOP
            IF v_filter_count > 0 THEN
                DBMS_LOB.APPEND(v_etl_filter_json, ',');
            END IF;
            DBMS_LOB.APPEND(v_etl_filter_json,
                '{"filter_id":' || rec.filter_id ||
                ',"plant_id":"' || rec.plant_id ||
                '","plant_name":"' || REPLACE(rec.plant_name, '"', '\"') ||
                '","issue_revision":"' || rec.issue_revision ||
                '","added_date":"' || TO_CHAR(rec.added_date, 'YYYY-MM-DD HH24:MI:SS') ||
                '","added_by_user_id":"' || rec.added_by_user_id ||
                '","notes":"' || REPLACE(NVL(rec.notes, ''), '"', '\"') || '"}');
            v_filter_count := v_filter_count + 1;
        END LOOP;
        DBMS_LOB.APPEND(v_etl_filter_json, ']');

        -- Get sequence value (use DDL_BACKUP_OWNER's sequence)
        SELECT DDL_BACKUP_OWNER.CONTROL_DATA_BACKUP_SEQ.NEXTVAL INTO v_backup_id FROM DUAL;

        -- Insert into backup table
        INSERT INTO DDL_BACKUP_OWNER.CONTROL_DATA_BACKUP (
            backup_id,
            backup_timestamp,
            schema_name,
            control_settings_json,
            control_endpoints_json,
            etl_filter_json,
            settings_count,
            endpoints_count,
            filter_count,
            backup_notes,
            backup_user,
            ddl_backup_id
        ) VALUES (
            v_backup_id,
            SYSTIMESTAMP,
            p_schema_name,
            v_control_settings_json,
            v_control_endpoints_json,
            v_etl_filter_json,
            v_settings_count,
            v_endpoints_count,
            v_filter_count,
            p_notes,
            USER,
            p_ddl_backup_id
        );

        COMMIT;

        -- Cleanup
        DBMS_LOB.FREETEMPORARY(v_control_settings_json);
        DBMS_LOB.FREETEMPORARY(v_control_endpoints_json);
        DBMS_LOB.FREETEMPORARY(v_etl_filter_json);

        DBMS_OUTPUT.PUT_LINE('Control data backup completed. Backup ID: ' || v_backup_id);
        DBMS_OUTPUT.PUT_LINE('    Settings: ' || v_settings_count);
        DBMS_OUTPUT.PUT_LINE('    Endpoints: ' || v_endpoints_count);
        DBMS_OUTPUT.PUT_LINE('    Filters: ' || v_filter_count);

    EXCEPTION
        WHEN OTHERS THEN
            IF DBMS_LOB.ISTEMPORARY(v_control_settings_json) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_control_settings_json);
            END IF;
            IF DBMS_LOB.ISTEMPORARY(v_control_endpoints_json) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_control_endpoints_json);
            END IF;
            IF DBMS_LOB.ISTEMPORARY(v_etl_filter_json) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_etl_filter_json);
            END IF;
            RAISE_APPLICATION_ERROR(-20002, 'Control data backup failed: ' || SQLERRM);
    END backup_control_data;

    FUNCTION get_ddl_from_backup(p_backup_id NUMBER) RETURN CLOB IS
        v_ddl_content CLOB;
    BEGIN
        SELECT ddl_content INTO v_ddl_content
        FROM DDL_BACKUP_OWNER.DDL_BACKUP
        WHERE backup_id = p_backup_id;

        RETURN v_ddl_content;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20003, 'Backup ID ' || p_backup_id || ' not found');
    END get_ddl_from_backup;

    FUNCTION restore_control_data(
        p_backup_id NUMBER,
        p_dry_run BOOLEAN DEFAULT TRUE
    ) RETURN VARCHAR2 IS
        v_control_settings_json CLOB;
        v_control_endpoints_json CLOB;
        v_etl_filter_json CLOB;
        v_result VARCHAR2(4000);
        v_restored_count NUMBER := 0;
    BEGIN
        SELECT control_settings_json, control_endpoints_json, etl_filter_json
        INTO v_control_settings_json, v_control_endpoints_json, v_etl_filter_json
        FROM DDL_BACKUP_OWNER.CONTROL_DATA_BACKUP
        WHERE backup_id = p_backup_id;

        IF p_dry_run THEN
            v_result := 'DRY RUN - No changes made. Would restore:' || CHR(10);

            SELECT COUNT(*) INTO v_restored_count
            FROM JSON_TABLE(v_control_settings_json, '$[*]'
                COLUMNS (setting_key VARCHAR2(100) PATH '$.setting_key'));
            v_result := v_result || 'CONTROL_SETTINGS: ' || v_restored_count || ' records' || CHR(10);

            SELECT COUNT(*) INTO v_restored_count
            FROM JSON_TABLE(v_control_endpoints_json, '$[*]'
                COLUMNS (endpoint_id NUMBER PATH '$.endpoint_id'));
            v_result := v_result || 'CONTROL_ENDPOINTS: ' || v_restored_count || ' records' || CHR(10);

            SELECT COUNT(*) INTO v_restored_count
            FROM JSON_TABLE(v_etl_filter_json, '$[*]'
                COLUMNS (filter_id NUMBER PATH '$.filter_id'));
            v_result := v_result || 'ETL_FILTER: ' || v_restored_count || ' records';

        ELSE
            v_result := 'RESTORE COMPLETED:' || CHR(10);

            DELETE FROM CONTROL_SETTINGS;
            DELETE FROM CONTROL_ENDPOINTS;
            DELETE FROM ETL_FILTER;

            INSERT INTO CONTROL_SETTINGS (setting_key, setting_value, description)
            SELECT setting_key, setting_value, description
            FROM JSON_TABLE(v_control_settings_json, '$[*]'
                COLUMNS (
                    setting_key VARCHAR2(100) PATH '$.setting_key',
                    setting_value VARCHAR2(4000) PATH '$.setting_value',
                    description VARCHAR2(4000) PATH '$.description'
                ));
            v_result := v_result || 'CONTROL_SETTINGS: ' || SQL%ROWCOUNT || ' records' || CHR(10);

            INSERT INTO CONTROL_ENDPOINTS (endpoint_id, endpoint_key, endpoint_template, comments)
            SELECT endpoint_id, endpoint_key, endpoint_template, comments
            FROM JSON_TABLE(v_control_endpoints_json, '$[*]'
                COLUMNS (
                    endpoint_id NUMBER PATH '$.endpoint_id',
                    endpoint_key VARCHAR2(100) PATH '$.endpoint_key',
                    endpoint_template VARCHAR2(500) PATH '$.endpoint_template',
                    comments VARCHAR2(500) PATH '$.comments'
                ));
            v_result := v_result || 'CONTROL_ENDPOINTS: ' || SQL%ROWCOUNT || ' records' || CHR(10);

            INSERT INTO ETL_FILTER (filter_id, plant_id, plant_name, issue_revision,
                                   added_date, added_by_user_id, notes)
            SELECT filter_id, plant_id, plant_name, issue_revision,
                   TO_DATE(added_date, 'YYYY-MM-DD HH24:MI:SS'), added_by_user_id, notes
            FROM JSON_TABLE(v_etl_filter_json, '$[*]'
                COLUMNS (
                    filter_id NUMBER PATH '$.filter_id',
                    plant_id VARCHAR2(50) PATH '$.plant_id',
                    plant_name VARCHAR2(100) PATH '$.plant_name',
                    issue_revision VARCHAR2(50) PATH '$.issue_revision',
                    added_date VARCHAR2(30) PATH '$.added_date',
                    added_by_user_id VARCHAR2(50) PATH '$.added_by_user_id',
                    notes VARCHAR2(500) PATH '$.notes'
                ));
            v_result := v_result || 'ETL_FILTER: ' || SQL%ROWCOUNT || ' records';

            COMMIT;
        END IF;

        RETURN v_result;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20004, 'Control data backup ID ' || p_backup_id || ' not found');
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE_APPLICATION_ERROR(-20005, 'Restore failed: ' || SQLERRM);
    END restore_control_data;

    PROCEDURE list_backups(
        p_days_back NUMBER DEFAULT 7,
        p_schema_name VARCHAR2 DEFAULT USER
    ) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('=== DDL Backups for ' || p_schema_name || ' (Last ' || p_days_back || ' days) ===');
        DBMS_OUTPUT.PUT_LINE(RPAD('ID', 5) || RPAD('Timestamp', 25) || RPAD('Type', 20) ||
                           RPAD('Objects', 10) || RPAD('Size(KB)', 10) || 'Notes');
        DBMS_OUTPUT.PUT_LINE(RPAD('-', 100, '-'));

        FOR rec IN (
            SELECT backup_id, backup_timestamp, backup_type, object_count,
                   ROUND(ddl_size_bytes/1024) as size_kb, backup_notes
            FROM DDL_BACKUP_OWNER.DDL_BACKUP
            WHERE schema_name = p_schema_name
            AND backup_timestamp >= SYSTIMESTAMP - p_days_back
            ORDER BY backup_timestamp DESC
        ) LOOP
            DBMS_OUTPUT.PUT_LINE(
                RPAD(rec.backup_id, 5) ||
                RPAD(TO_CHAR(rec.backup_timestamp, 'YYYY-MM-DD HH24:MI:SS'), 25) ||
                RPAD(NVL(rec.backup_type, 'UNKNOWN'), 20) ||
                RPAD(rec.object_count, 10) ||
                RPAD(rec.size_kb || 'KB', 10) ||
                NVL(SUBSTR(rec.backup_notes, 1, 30), 'No notes')
            );
        END LOOP;
    END list_backups;

    PROCEDURE compare_backups(
        p_backup_id_1 NUMBER,
        p_backup_id_2 NUMBER
    ) IS
        v_count1 NUMBER;
        v_count2 NUMBER;
        v_size1 NUMBER;
        v_size2 NUMBER;
        v_date1 TIMESTAMP;
        v_date2 TIMESTAMP;
    BEGIN
        SELECT object_count, ddl_size_bytes, backup_timestamp
        INTO v_count1, v_size1, v_date1
        FROM DDL_BACKUP_OWNER.DDL_BACKUP
        WHERE backup_id = p_backup_id_1;

        SELECT object_count, ddl_size_bytes, backup_timestamp
        INTO v_count2, v_size2, v_date2
        FROM DDL_BACKUP_OWNER.DDL_BACKUP
        WHERE backup_id = p_backup_id_2;

        DBMS_OUTPUT.PUT_LINE('=== Backup Comparison ===');
        DBMS_OUTPUT.PUT_LINE('Backup ' || p_backup_id_1 || ' (' || TO_CHAR(v_date1, 'YYYY-MM-DD HH24:MI') || ')');
        DBMS_OUTPUT.PUT_LINE('    Objects: ' || v_count1 || ', Size: ' || ROUND(v_size1/1024) || 'KB');
        DBMS_OUTPUT.PUT_LINE('Backup ' || p_backup_id_2 || ' (' || TO_CHAR(v_date2, 'YYYY-MM-DD HH24:MI') || ')');
        DBMS_OUTPUT.PUT_LINE('    Objects: ' || v_count2 || ', Size: ' || ROUND(v_size2/1024) || 'KB');
        DBMS_OUTPUT.PUT_LINE('Differences:');
        DBMS_OUTPUT.PUT_LINE('    Object count: ' || (v_count2 - v_count1));
        DBMS_OUTPUT.PUT_LINE('    Size change: ' || ROUND((v_size2 - v_size1)/1024) || 'KB');
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('Error: One or both backup IDs not found');
    END compare_backups;

    FUNCTION get_latest_backup_id(
        p_schema_name VARCHAR2 DEFAULT USER
    ) RETURN NUMBER IS
        v_backup_id NUMBER;
    BEGIN
        SELECT MAX(backup_id) INTO v_backup_id
        FROM DDL_BACKUP_OWNER.DDL_BACKUP
        WHERE schema_name = p_schema_name;

        RETURN v_backup_id;
    END get_latest_backup_id;

END PKG_DDL_BACKUP;
/