-- Control table data export from TR2000_STAGING
-- Generated: 2025-09-05 03:00:01


-- ETL_FILTER data
DELETE FROM ETL_FILTER;
INSERT INTO ETL_FILTER (filter_id, plant_id, plant_name, issue_revision, added_date, added_by_user_id) VALUES (65, '34', 'GRANE', '4.2', TO_DATE('2025-09-05 00:25:32', 'YYYY-MM-DD HH24:MI:SS'), 'TEST_USER');

-- CONTROL_SETTINGS data
DELETE FROM CONTROL_SETTINGS;
            ELSE 'TO_DATE(''' || TO_CHAR(last_modified_date, 'YYYY-MM-DD HH24:MI:SS') || ''', ''YYYY-MM-DD HH24:MI:SS'')' END || ', ' ||
                                         *
ERROR at line 6:
ORA-00904: "LAST_MODIFIED_DATE": invalid identifier



-- CONTROL_ENDPOINTS data
DELETE FROM CONTROL_ENDPOINTS;
            ELSE 'TO_DATE(''' || TO_CHAR(last_modified_date, 'YYYY-MM-DD HH24:MI:SS') || ''', ''YYYY-MM-DD HH24:MI:SS'')' END || ');'
                                         *
ERROR at line 9:
ORA-00904: "LAST_MODIFIED_DATE": invalid identifier



COMMIT;

