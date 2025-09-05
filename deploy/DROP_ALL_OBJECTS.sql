-- =====================================================
-- DROP ALL OBJECTS Script
-- Purpose: Completely remove all TR2000_STAGING objects
-- WARNING: This will DELETE EVERYTHING!
-- =====================================================

SET ECHO ON
SET SERVEROUTPUT ON

PROMPT ========================================
PROMPT WARNING: DROP ALL OBJECTS
PROMPT ========================================
PROMPT
PROMPT This script will DROP ALL objects in TR2000_STAGING:
PROMPT - All packages
PROMPT - All procedures  
PROMPT - All views
PROMPT - All tables (and their data)
PROMPT - All sequences
PROMPT - All triggers
PROMPT
PROMPT THIS CANNOT BE UNDONE!
PROMPT
PROMPT Press Ctrl+C to abort, Enter to continue...
PAUSE

-- =====================================================
-- Drop Package Bodies First (dependencies)
-- =====================================================
PROMPT
PROMPT Dropping Package Bodies...
PROMPT ========================================

BEGIN
    FOR rec IN (SELECT object_name 
                FROM user_objects 
                WHERE object_type = 'PACKAGE BODY'
                ORDER BY object_name)
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP PACKAGE BODY ' || rec.object_name;
            DBMS_OUTPUT.PUT_LINE('Dropped package body: ' || rec.object_name);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error dropping package body ' || rec.object_name || ': ' || SQLERRM);
        END;
    END LOOP;
END;
/

-- =====================================================
-- Drop Package Specifications
-- =====================================================
PROMPT
PROMPT Dropping Package Specifications...
PROMPT ========================================

BEGIN
    FOR rec IN (SELECT object_name 
                FROM user_objects 
                WHERE object_type = 'PACKAGE'
                ORDER BY object_name)
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP PACKAGE ' || rec.object_name;
            DBMS_OUTPUT.PUT_LINE('Dropped package: ' || rec.object_name);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error dropping package ' || rec.object_name || ': ' || SQLERRM);
        END;
    END LOOP;
END;
/

-- =====================================================
-- Drop Procedures
-- =====================================================
PROMPT
PROMPT Dropping Procedures...
PROMPT ========================================

BEGIN
    FOR rec IN (SELECT object_name 
                FROM user_objects 
                WHERE object_type = 'PROCEDURE'
                ORDER BY object_name)
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP PROCEDURE ' || rec.object_name;
            DBMS_OUTPUT.PUT_LINE('Dropped procedure: ' || rec.object_name);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error dropping procedure ' || rec.object_name || ': ' || SQLERRM);
        END;
    END LOOP;
END;
/

-- =====================================================
-- Drop Functions
-- =====================================================
PROMPT
PROMPT Dropping Functions...
PROMPT ========================================

BEGIN
    FOR rec IN (SELECT object_name 
                FROM user_objects 
                WHERE object_type = 'FUNCTION'
                ORDER BY object_name)
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP FUNCTION ' || rec.object_name;
            DBMS_OUTPUT.PUT_LINE('Dropped function: ' || rec.object_name);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error dropping function ' || rec.object_name || ': ' || SQLERRM);
        END;
    END LOOP;
END;
/

-- =====================================================
-- Drop Views
-- =====================================================
PROMPT
PROMPT Dropping Views...
PROMPT ========================================

BEGIN
    FOR rec IN (SELECT view_name 
                FROM user_views
                ORDER BY view_name)
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP VIEW ' || rec.view_name;
            DBMS_OUTPUT.PUT_LINE('Dropped view: ' || rec.view_name);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error dropping view ' || rec.view_name || ': ' || SQLERRM);
        END;
    END LOOP;
END;
/

-- =====================================================
-- Drop Triggers
-- =====================================================
PROMPT
PROMPT Dropping Triggers...
PROMPT ========================================

BEGIN
    FOR rec IN (SELECT trigger_name 
                FROM user_triggers
                ORDER BY trigger_name)
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP TRIGGER ' || rec.trigger_name;
            DBMS_OUTPUT.PUT_LINE('Dropped trigger: ' || rec.trigger_name);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error dropping trigger ' || rec.trigger_name || ': ' || SQLERRM);
        END;
    END LOOP;
END;
/

-- =====================================================
-- Drop Tables (CASCADE CONSTRAINTS to handle FKs)
-- =====================================================
PROMPT
PROMPT Dropping Tables...
PROMPT ========================================

BEGIN
    -- Drop tables in reverse dependency order
    -- First drop tables with foreign keys
    FOR rec IN (SELECT table_name 
                FROM user_tables
                WHERE table_name NOT LIKE 'BIN$%'
                ORDER BY table_name DESC)
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE ' || rec.table_name || ' CASCADE CONSTRAINTS PURGE';
            DBMS_OUTPUT.PUT_LINE('Dropped table: ' || rec.table_name);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error dropping table ' || rec.table_name || ': ' || SQLERRM);
        END;
    END LOOP;
END;
/

-- =====================================================
-- Drop Sequences
-- =====================================================
PROMPT
PROMPT Dropping Sequences...
PROMPT ========================================

BEGIN
    FOR rec IN (SELECT sequence_name 
                FROM user_sequences
                ORDER BY sequence_name)
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP SEQUENCE ' || rec.sequence_name;
            DBMS_OUTPUT.PUT_LINE('Dropped sequence: ' || rec.sequence_name);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error dropping sequence ' || rec.sequence_name || ': ' || SQLERRM);
        END;
    END LOOP;
END;
/

-- =====================================================
-- Drop Types (if any)
-- =====================================================
PROMPT
PROMPT Dropping Types...
PROMPT ========================================

BEGIN
    FOR rec IN (SELECT type_name 
                FROM user_types
                ORDER BY type_name)
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP TYPE ' || rec.type_name || ' FORCE';
            DBMS_OUTPUT.PUT_LINE('Dropped type: ' || rec.type_name);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error dropping type ' || rec.type_name || ': ' || SQLERRM);
        END;
    END LOOP;
END;
/

-- =====================================================
-- Verify All Objects Dropped
-- =====================================================
PROMPT
PROMPT ========================================
PROMPT Verification: Remaining Objects
PROMPT ========================================

SELECT object_type, COUNT(*) as count
FROM user_objects
WHERE object_type NOT IN ('INDEX', 'LOB')
GROUP BY object_type
ORDER BY object_type;

PROMPT
PROMPT ========================================
PROMPT DROP ALL OBJECTS Complete!
PROMPT ========================================
PROMPT
PROMPT If any objects remain above, they may need manual removal.
PROMPT
PROMPT To recreate everything, run: @DEPLOY_ALL.sql
PROMPT
PROMPT ========================================
/