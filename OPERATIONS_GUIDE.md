# TR2000 Database Operations Guide

Read this guide first, understand the proxy security model, and ALWAYS use safe conversions from PKG_ETL_VALIDATION.


## 📋 Table of Contents
- [Quick Reference Commands](#-quick-reference-commands)
- [Development Workflow](#-development-workflow)
- [Deployment Guide](#-deployment-guide)
- [Monitoring & Troubleshooting](#-monitoring--troubleshooting)
- [Emergency Recovery](#-emergency-recovery)
- [Process Guidelines](#-process-guidelines)

---

## 🚀 Quick Reference Commands

### Essential Daily Operations

#### Connect to Database
```bash
sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1
```

#### Run ETL
```sql
-- Main ETL (references and PCS)
EXEC PKG_MAIN_ETL_CONTROL.run_main_etl;

-- VDS catalog (separate, large dataset)
EXEC PKG_INDEPENDENT_ETL_CONTROL.run_vds_catalog_etl;
```

#### Check Errors
```sql
-- Recent errors (last hour)
SELECT * FROM ETL_ERROR_LOG 
WHERE error_timestamp > SYSDATE - 1/24 
ORDER BY error_timestamp DESC;

-- Conversion errors specifically
SELECT * FROM ETL_ERROR_LOG 
WHERE error_type LIKE 'DATA_CONVERSION_%'
ORDER BY error_timestamp DESC;
```

#### Deploy a Package
```bash
sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1 @deploy/04_packages/PKG_NAME.sql
```

---

## 🔧 Development Workflow

### Making Changes (4-Step Process)

1. **Edit Deployment Script** (NEVER the database directly)
```bash
vi deploy/04_packages/PKG_API_CLIENT.sql
```

2. **Deploy to Database**
```bash
sqlplus TR2000_STAGING/piping @deploy/04_packages/PKG_API_CLIENT.sql
```

3. **Test the Changes**
```sql
EXEC PKG_MAIN_ETL_CONTROL.run_main_etl;

-- Check for errors
SELECT * FROM ETL_ERROR_LOG 
WHERE error_timestamp > SYSDATE - 1/24;
```

4. **Commit to Git** (if successful)
```bash
git add -A
git commit -m "Fix: Description of change"
git push origin master
```

### Rolling Back Changes

#### Option 1: Revert Last Commit
```bash
git revert HEAD
git push
sqlplus TR2000_STAGING/piping @deploy/04_packages/PKG_NAME.sql
```

#### Option 2: Checkout Previous Version
```bash
git checkout HEAD~1 deploy/04_packages/PKG_API_CLIENT.sql
sqlplus TR2000_STAGING/piping @deploy/04_packages/PKG_API_CLIENT.sql
```

#### Option 3: Full Baseline Restore
```bash
git checkout baseline-pre-refactor-20250105
sqlplus TR2000_STAGING/piping @deploy/DEPLOY_ALL.sql
```

---

## 📦 Deployment Guide

### Directory Structure
```
deploy/
├── 00_prerequisites/     # API_SERVICE user setup
├── 01_tables/           # Table DDL scripts
├── 02_sequences/        # Sequence definitions
├── 03_indexes/          # Index definitions
├── 04_packages/         # PL/SQL packages (one file per package)
├── 05_procedures/       # Standalone procedures
├── 06_views/            # View definitions
├── 07_control_data/     # Initial data load
├── DEPLOY_ALL.sql       # Master deployment script
└── DROP_ALL_OBJECTS.sql # Clean slate script
```

### Full Deployment
⚠️ **CRITICAL: Always run deployment scripts from the `deploy/` directory!**

```bash
# CORRECT: Run from deploy directory (uses relative paths)
cd deploy
sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1 @DEPLOY_ALL.sql

# Or with clean slate first
cd deploy
sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1 @DROP_ALL_OBJECTS.sql
sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1 @DEPLOY_ALL.sql
```

**Why this matters:** DEPLOY_ALL.sql uses relative paths like `@@01_tables/01_control_tables.sql`. If you run it from the wrong directory, it will fail with "unable to open file" errors.

### Key Packages

| Package | Purpose | Key Functions |
|---------|---------|---------------|
| **PKG_ETL_VALIDATION** | Safe data conversions + failure tracking | `safe_to_number()`, `safe_to_date()`, `validate_json()`, `get_conversion_failures()` |
| **PKG_API_CLIENT** | API communication via proxy | `fetch_reference_data()`, `fetch_pcs_list()` |
| **PKG_MAIN_ETL_CONTROL** | ETL orchestration | `run_main_etl()` |
| **PKG_ETL_PROCESSOR** | JSON parsing | `parse_and_load_*()` functions |
| **PKG_PCS_DETAIL_PROCESSOR** | PCS detail processing | `process_pcs_*()` functions |
| **PKG_ETL_LOGGING** | Run and error logging | `start_etl_run()`, `log_error()` |
| **PKG_DATE_UTILS** | Date parsing utilities | `safe_parse_date()` |
| **PKG_ETL_TEST_UTILS** | Testing utilities | `reset_for_testing()`, `show_etl_status()` |
| **PKG_INDEPENDENT_ETL_CONTROL** | VDS catalog ETL | `run_vds_catalog_etl()` |

---

## 📊 Monitoring & Troubleshooting

### Standard Monitoring Queries

#### ETL Run History
```sql
-- Recent runs with status
SELECT run_id, run_type, start_time, end_time, status, 
       ROUND((end_time - start_time) * 24 * 60, 2) as minutes
FROM ETL_RUN_LOG 
ORDER BY run_id DESC
FETCH FIRST 10 ROWS ONLY;
```

#### API Call Statistics
```sql
-- API calls from last run
SELECT endpoint_url, http_status, response_time_ms, 
       payload_size_bytes, call_timestamp
FROM API_SERVICE.API_CALL_STATS 
ORDER BY call_timestamp DESC;
```

#### Conversion Statistics & Error Tracking
```sql
-- Get conversion error summary (detailed breakdown)
SELECT PKG_ETL_VALIDATION.get_conversion_stats FROM dual;

-- Get current session failure count
SELECT PKG_ETL_VALIDATION.get_conversion_failures() as current_failures FROM dual;

-- Check ETL statistics for conversion failures (includes WARNING status)
SELECT run_id, stat_type, operation_name, records_failed, status
FROM ETL_STATISTICS 
WHERE records_failed > 0 OR status = 'WARNING'
ORDER BY run_id DESC;

-- Enhanced error log with accurate field context
SELECT error_timestamp, error_type, 
       SUBSTR(error_message, 1, 80) as error_summary,
       SUBSTR(raw_data, 1, 100) as field_context
FROM ETL_ERROR_LOG 
WHERE error_timestamp > SYSDATE - 1/24
ORDER BY error_timestamp DESC;
```

#### Check Configuration
```sql
-- Current ETL filters
SELECT * FROM ETL_FILTER;

-- Control settings
SELECT * FROM CONTROL_SETTINGS;

-- Endpoint configurations
SELECT * FROM CONTROL_ENDPOINTS;
```

#### Invalid Objects
```sql
-- Check for compilation errors
SELECT object_name, object_type, status 
FROM user_objects 
WHERE status = 'INVALID'
ORDER BY object_type, object_name;
```

### Common Issues & Solutions

#### ETL Fails
```sql
-- 1. Check errors
SELECT * FROM ETL_ERROR_LOG 
WHERE error_timestamp > SYSDATE - 1/24
ORDER BY error_timestamp DESC;

-- 2. Re-run (idempotent - safe to run multiple times)
EXEC PKG_MAIN_ETL_CONTROL.run_main_etl;
```

#### Data Conversion Errors
```sql
-- Find problematic data
SELECT source_table, source_field, source_value, 
       error_message, COUNT(*) as occurrences
FROM ETL_ERROR_LOG 
WHERE error_type LIKE 'DATA_CONVERSION_%'
GROUP BY source_table, source_field, source_value, error_message
ORDER BY occurrences DESC;
```

#### Performance Issues
```sql
-- Check slowest API calls
SELECT endpoint_key, AVG(response_time_ms) as avg_ms, 
       MAX(response_time_ms) as max_ms, COUNT(*) as calls
FROM API_SERVICE.API_CALL_STATS
GROUP BY endpoint_key
ORDER BY avg_ms DESC;
```

---

## 🚨 Emergency Recovery

### Recovery Options (in order of preference)

#### 1. Re-run ETL (Quickest)
```sql
-- ETL is idempotent - just run again
EXEC PKG_MAIN_ETL_CONTROL.run_main_etl;
```

#### 2. Restore from Git (Package Issues)
```bash
# Restore specific package from baseline
git checkout baseline-pre-refactor-20250105 deploy/04_packages/PKG_NAME.sql
sqlplus TR2000_STAGING/piping @deploy/04_packages/PKG_NAME.sql
```

#### 3. SQL Backup Restore (Structure Issues)
```bash
# Restore all packages from backup
sqlplus TR2000_STAGING/piping @backups/20250105_pre_refactor/all_packages.sql
```

#### 4. Data Pump Restore (Complete Recovery)
```bash
# Full schema restore
impdp TR2000_STAGING/piping DIRECTORY=backup_dir \
  DUMPFILE=TR2000_FULL_20250105.dmp \
  SCHEMAS=TR2000_STAGING \
  TABLE_EXISTS_ACTION=REPLACE
```

#### 5. Snapshot Restore (Point-in-Time)
```bash
# Restore to known good state
sqlplus TR2000_STAGING/piping @Snapshots/snapshot_20250905_working_etl/RESTORE_MASTER.sql
```

---

## 📋 Process Guidelines

### Core Principles

1. **Script-Based Deployment** - All changes through deployment scripts
2. **Safe Data Conversion** - Use PKG_ETL_VALIDATION for all conversions
3. **Clear and Reload** - ETL is idempotent, safe to re-run
4. **Single Source of Truth** - ETL_FILTER controls what gets loaded
5. **Git Version Control** - Every change tracked, easy rollback

### DO's ✅

- Edit deployment scripts in `deploy/` folder
- Use `PKG_ETL_VALIDATION.safe_to_number()` for number conversions
- Use `PKG_ETL_VALIDATION.safe_to_date()` for date conversions
- Test deployment on single package before running DEPLOY_ALL
- Commit working changes immediately to Git
- Keep audit trail in RAW_JSON table
- Log all conversion errors to ETL_ERROR_LOG
- Check ETL_ERROR_LOG regularly for issues

### DON'Ts ❌

- Edit the database directly (always use scripts)
- Use raw `TO_NUMBER()` or `TO_DATE()` functions
- Deploy without testing
- Skip error logging
- Ignore conversion errors in ETL_ERROR_LOG
- Store passwords in scripts
- Log sensitive data to error tables

### Safe Conversion Example

#### ❌ Old Way (Dangerous)
```sql
-- Will throw ORA-01722 if not a number
INSERT INTO PCS_LIST (test_pressure)
SELECT TO_NUMBER(test_pressure) FROM STG_PCS_LIST;
```

#### ✅ New Way (Safe)
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

 1. Don't use raw TO_NUMBER() or TO_DATE() - Will crash with ORA-01722
  2. Don't edit database directly - Always use deployment scripts
  3. Don't skip error checking - Always check ETL_ERROR_LOG
  4. Don't try to give TR2000_STAGING direct API access - Use proxy

### 🆕 Enhanced Error Tracking (v2.1)

**Recent Improvements:**
- **Accurate Field Context**: Error log now shows exact field name, table, and record ID
- **Failure Statistics**: ETL_STATISTICS tracks conversion failures with WARNING status  
- **Session Counters**: Can monitor conversion failure rates in real-time

**Example Enhanced Error Message:**
```
Error Type: DATA_CONVERSION_INVALID_NUMBER
Field: LongWeldEff, Table: STG_PCS_HEADER_PROPERTIES, ID: AD100
Value: "24" and less = 0,80, Above 24" = 1,0"
```

**ETL Statistics with Failures:**
- **Status**: `WARNING` (instead of `SUCCESS`) when conversion failures occur
- **records_failed**: Shows exact count of failed conversions
- **Operational Impact**: Operations teams see accurate failure rates

---

## 🔒 Security Notes

- **API Access**: Only through API_SERVICE proxy user
- **No Direct Access**: TR2000_STAGING has NO APEX_WEB_SERVICE privileges
- **Audit Trail**: All API calls logged in API_CALL_STATS
- **No Credentials**: Never store passwords in scripts
- **No Sensitive Data**: Never log sensitive data to ETL_ERROR_LOG

---

## 📈 Performance Tips

- Use `safe_to_number()` with source table/field for better error tracking
- Limit PCS details per run with `MAX_PCS_DETAILS_PER_RUN` setting
- Check ETL_ERROR_LOG regularly for conversion issues
- Monitor API_CALL_STATS for slow endpoints
- Use ETL_STATISTICS for detailed operation metrics



---

## 📁 Important Files

- `deploy/DEPLOY_ALL.sql` - Master deployment script
- `deploy/04_packages/PKG_ETL_VALIDATION.sql` - Safe conversion functions
- `backups/20250105_pre_refactor/` - Baseline backup
- `documentation/ETL_ARCHITECTURE.md` - System design details
- `documentation/TR2000_ERD_AND_MAPPINGS.md` - Database schema

---

**Repository**: https://github.com/vivekN7/TR2000-Database  
**Baseline**: baseline-pre-refactor-20250105  
**Last Updated**: 2025-01-05