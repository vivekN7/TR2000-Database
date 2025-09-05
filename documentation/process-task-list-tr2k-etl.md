# Process Guidelines - TR2000 ETL System

## 🔴 CORE PRINCIPLES (Permanent)

### System Philosophy
1. **Simplicity First** - This is a staging system, not master data
2. **Direct Database Development** - Work in DB during development, extract DDL for production
3. **No Unnecessary Complexity** - If it's not needed, don't add it
4. **Clear and Reload** - When in doubt, clear everything and reload
5. **Single Source of Truth** - ETL_FILTER table controls what gets loaded

### Development Workflow
```sql
-- 1. Make changes directly in database
CREATE OR REPLACE PACKAGE ...

-- 2. Test immediately
EXEC procedure_name();

-- 3. When stable, extract DDL
@Database/scripts/extract_full_ddl.sql
```

## 📋 ETL ARCHITECTURE

### Data Flow Pattern
```
ETL_FILTER → Clear All → API → RAW_JSON → STG_* → Final Tables
```

### Package Structure (Target: 3 packages)
- **PKG_ETL_CONTROL** - Orchestration and control
- **PKG_API_CLIENT** - API communication
- **PKG_ETL_PROCESSOR** - Parse and load data

### Table Categories
- **Control**: ETL_FILTER, CONTROL_SETTINGS, ETL_STATS
- **Audit**: RAW_JSON, ETL_RUN_LOG, ETL_ERROR_LOG
- **Staging**: STG_* tables (temporary workspace)
- **Final**: Reference and detail tables

## 🔴 PERMANENT RULES

### DO
✅ Work directly in database during development
✅ Use simple DELETE + INSERT patterns
✅ Test immediately after changes
✅ Keep audit trail in RAW_JSON
✅ Clear all data before loading

### DON'T
❌ Create modular deploy files during development
❌ Implement soft-deletes (is_valid columns)
❌ Add cascade triggers
❌ Use complex MERGE statements
❌ Add hash duplicate detection

## 🎯 STANDARD COMMANDS

### Database Connection
```sql
sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1
```

### Check Configuration
```sql
SELECT * FROM ETL_FILTER;
SELECT * FROM CONTROL_SETTINGS;
```

### Run ETL
```sql
EXEC PKG_ETL_CONTROL.run_full_etl();
```

### Extract Schema
```sql
@Database/scripts/extract_full_ddl.sql
```


## 🔧 ERROR RECOVERY

### Standard Recovery Process
```sql
-- If anything fails, just run again:
EXEC PKG_ETL_CONTROL.run_full_etl();
-- It clears everything first, always safe
```


## Git Workflow
- **Commit locally** frequently
- **Document changes** in commit messages

---

*This document contains permanent guidelines that apply to all sessions. For current tasks and immediate plans, see tasks-tr2k-etl.md*