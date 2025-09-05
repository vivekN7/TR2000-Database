-- Triggers for TR2000_STAGING
-- Generated: 2025-01-05


  CREATE OR REPLACE EDITIONABLE TRIGGER "TR2000_STAGING"."TRG_BLOCK_EMPTY_PKG_BODY" 
AFTER CREATE ON SCHEMA
DECLARE
    v_lines NUMBER;
    v_non_empty_lines NUMBER;
BEGIN
    -- Only check package bodies
    IF ORA_DICT_OBJ_TYPE = 'PACKAGE BODY' THEN
        -- Count total lines
        SELECT COUNT(*)
        INTO v_lines
        FROM USER_SOURCE
        WHERE name = ORA_DICT_OBJ_NAME
        AND type = 'PACKAGE BODY';

        -- Count non-whitespace lines
        SELECT COUNT(*)
        INTO v_non_empty_lines
        FROM USER_SOURCE
        WHERE name = ORA_DICT_OBJ_NAME
        AND type = 'PACKAGE BODY'
        AND REGEXP_LIKE(text, '\S'); -- any non-whitespace

        -- Block if too small (less than 5 non-empty lines is suspicious)
        IF v_non_empty_lines < 5 THEN
            RAISE_APPLICATION_ERROR(-20001,
                'BLOCKED: Package body ' || ORA_DICT_OBJ_NAME ||
                ' appears empty or minimal (only ' || v_non_empty_lines ||
                ' non-empty lines). Please check your code before creating.');
        END IF;

        -- Log successful package body creation
        DBMS_OUTPUT.PUT_LINE('Package body ' || ORA_DICT_OBJ_NAME ||
                           ' created successfully with ' || v_lines ||
                           ' total lines (' || v_non_empty_lines || ' non-empty).');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        -- Don't block on errors in the trigger itself
        DBMS_OUTPUT.PUT_LINE('Warning: Guard trigger error: ' || SQLERRM);
END;
ALTER TRIGGER "TR2000_STAGING"."TRG_BLOCK_EMPTY_PKG_BODY" ENABLE;
/
