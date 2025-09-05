# Process Guidelines - TR2000 ETL System

## 🔴 CORE PRINCIPLES (Updated 2025-01-05)

### System Philosophy
1. **Script-Based Deployment** - All changes through deployment scripts, never direct DB edits
2. **Safe Data Conversion** - Use PKG_ETL_VALIDATION for all conversions
3. **Clear and Reload** - When in doubt, clear everything and reload (idempotent)
4. **Single Source of Truth** - ETL_FILTER table controls what gets loaded
5. **Git Version Control** - Every change tracked, easy rollback

### Development Workflow (NEW)
```bash
# 1. Edit deployment script (NEVER the database directly)
vi Database/deploy/04_packages/PKG_NAME.sql

# 2. Deploy the change
sqlplus TR2000_STAGING/piping @Database/deploy/04_packages/PKG_NAME.sql

# 3. Test immediately
EXEC PKG_MAIN_ETL_CONTROL.run_main_etl;

# 4. Commit if successful
git add -A && git commit -m "Fix: description" && git push
```

## 📋 ETL ARCHITECTURE

### Data Flow Pattern (with Safety)
```
ETL_FILTER → Clear All → API (via proxy) → RAW_JSON → STG_* → Validation → Core Tables
                                                           ↓
                                                    ETL_ERROR_LOG
```

### Package Structure
- **PKG_MAIN_ETL_CONTROL** - Orchestration and control
- **PKG_API_CLIENT** - API communication via proxy
- **PKG_ETL_PROCESSOR** - Parse and load reference data
- **PKG_PCS_DETAIL_PROCESSOR** - Parse and load PCS details
- **PKG_ETL_VALIDATION** - Safe data conversions (NEW)
- **PKG_ETL_LOGGING** - Run and statistics logging
- **PKG_DATE_UTILS** - Date parsing utilities

### Table Categories
- **Control**: ETL_FILTER, CONTROL_SETTINGS, CONTROL_ENDPOINTS
- **Audit**: RAW_JSON, ETL_RUN_LOG, ETL_ERROR_LOG, ETL_STATISTICS
- **Staging**: STG_* tables (all VARCHAR2, temporary workspace)
- **Core**: Reference and detail tables (properly typed)

## 🔴 NEW DEPLOYMENT RULES

### DO
✅ Edit deployment scripts in Database/deploy/
✅ Use PKG_ETL_VALIDATION.safe_to_number() for all number conversions
✅ Use PKG_ETL_VALIDATION.safe_to_date() for all date conversions
✅ Test deployment on single package before running DEPLOY_ALL
✅ Commit working changes immediately to Git
✅ Keep audit trail in RAW_JSON
✅ Log all conversion errors to ETL_ERROR_LOG

### DON'T
❌ Edit the database directly (always use scripts)
❌ Use raw TO_NUMBER() or TO_DATE() functions
❌ Deploy without testing
❌ Skip error logging
❌ Ignore conversion errors in ETL_ERROR_LOG
❌ Mix database changes with application code

## 🎯 STANDARD COMMANDS

### Database Connection
```sql
sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1
```

### Deploy Changes
```bash
# Single package
sqlplus TR2000_STAGING/piping @Database/deploy/04_packages/PKG_API_CLIENT.sql

# All packages
sqlplus TR2000_STAGING/piping @Database/deploy/DEPLOY_ALL.sql
```

### Check Configuration
```sql
SELECT * FROM ETL_FILTER;
SELECT * FROM CONTROL_SETTINGS;
SELECT * FROM CONTROL_ENDPOINTS;
```

### Run ETL
```sql
-- Main ETL (references and PCS)
EXEC PKG_MAIN_ETL_CONTROL.run_main_etl;

-- VDS catalog (separate, large)
EXEC PKG_INDEPENDENT_ETL_CONTROL.run_vds_catalog_etl;
```

### Monitor Errors
```sql
-- Check conversion errors
SELECT * FROM ETL_ERROR_LOG 
WHERE error_type LIKE 'DATA_CONVERSION_%'
ORDER BY error_timestamp DESC;

-- Get conversion statistics
SELECT PKG_ETL_VALIDATION.get_conversion_stats FROM dual;

-- Check recent runs
SELECT * FROM ETL_RUN_LOG ORDER BY run_id DESC;
```

## 🔧 ERROR RECOVERY

### Standard Recovery Process
```sql
-- If ETL fails, check errors first:
SELECT * FROM ETL_ERROR_LOG 
WHERE error_timestamp > SYSDATE - 1/24
ORDER BY error_timestamp DESC;

-- Then just run again (idempotent):
EXEC PKG_MAIN_ETL_CONTROL.run_main_etl;
```

### Deployment Rollback
```bash
# Revert last change
git revert HEAD
sqlplus TR2000_STAGING/piping @Database/deploy/04_packages/PKG_NAME.sql

# Or restore from backup
sqlplus TR2000_STAGING/piping @Database/backups/20250105_pre_refactor/all_packages.sql
```

## 🚀 Safe Conversion Examples

### Old Way (DANGEROUS)
```sql
-- Will throw ORA-01722 if not a number
INSERT INTO PCS_LIST (test_pressure)
SELECT TO_NUMBER(test_pressure) FROM STG_PCS_LIST;
```

### New Way (SAFE)
```sql
-- Logs error, returns NULL or default
INSERT INTO PCS_LIST (test_pressure)
SELECT PKG_ETL_VALIDATION.safe_to_number(
    p_value => test_pressure,
    p_default => NULL,
    p_source_table => 'STG_PCS_LIST',
    p_source_field => 'TEST_PRESSURE',
    p_record_id => pcs_name
) FROM STG_PCS_LIST;
```

## 📊 Repository Structure

```
Database/                      # Separate Git repository
├── deploy/                   # Deployment scripts
│   ├── 04_packages/         # Individual package scripts
│   ├── DEPLOY_ALL.sql       # Master deployment
│   └── README.md            # Deployment guide
├── backups/                  # Database backups
│   └── 20250105_pre_refactor/  # Baseline
├── documentation/            # System docs
└── Snapshots/               # Point-in-time snapshots
```

## Git Workflow
```bash
# Always work in Database folder
cd Database

# Make changes
vi deploy/04_packages/PKG_NAME.sql

# Deploy and test
sqlplus TR2000_STAGING/piping @deploy/04_packages/PKG_NAME.sql

# Commit if working
git add -A
git commit -m "Fix: Add timeout handling"
git push origin master
```

## 🔒 Security Notes
- API access ONLY through API_SERVICE proxy
- TR2000_STAGING has NO direct API privileges
- All API calls logged in API_CALL_STATS
- Never log sensitive data

---

**Repository**: https://github.com/vivekN7/TR2000-Database  
**Baseline**: baseline-pre-refactor-20250105  
*Last Updated: 2025-01-05*