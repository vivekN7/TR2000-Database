# TR2000 ETL Database Deployment

## 🚀 Quick Start

### Deploy Everything
```bash
sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1 @Database/deploy/DEPLOY_ALL.sql
```

### Deploy Single Package
```bash
sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1 @Database/deploy/04_packages/PKG_API_CLIENT.sql
```

## 📁 Directory Structure

```
Database/deploy/
├── 00_prerequisites/     # API_SERVICE user setup (if needed)
├── 01_tables/            # Table DDL scripts (grouped by type)
├── 02_sequences/         # Sequence definitions
├── 03_indexes/           # Index definitions
├── 04_packages/          # PL/SQL packages (one file per package)
├── 05_procedures/        # Standalone procedures
├── 06_views/             # View definitions
├── 07_control_data/      # Initial data load scripts
├── DEPLOY_ALL.sql        # Master deployment script
├── ROLLBACK_ALL.sql      # Emergency rollback script
└── README.md             # This file
```

## 📦 Package Deployment Files

Each package has its own deployment script that:
1. Drops existing package (if exists)
2. Creates package specification
3. Creates package body
4. Compiles and verifies

### Core Packages

| Package | Purpose | Status |
|---------|---------|--------|
| `PKG_ETL_VALIDATION` | Safe data conversions with error logging | ✅ Deployed |
| `PKG_API_CLIENT` | API communication via proxy | ✅ Deployed |
| `PKG_DATE_UTILS` | Date parsing utilities | 🔄 Pending |
| `PKG_ETL_LOGGING` | ETL run and statistics logging | 🔄 Pending |
| `PKG_ETL_PROCESSOR` | JSON parsing for reference data | 🔄 Pending |
| `PKG_PCS_DETAIL_PROCESSOR` | PCS detail endpoint processing | 🔄 Pending |
| `PKG_MAIN_ETL_CONTROL` | Main ETL orchestration | 🔄 Pending |
| `PKG_ETL_TEST_UTILS` | Testing utilities | 🔄 Pending |
| `PKG_INDEPENDENT_ETL_CONTROL` | VDS catalog ETL | 🔄 Pending |

## 🔧 Workflow

### Making Changes

1. **Edit the deployment script** (not the database directly):
   ```bash
   vi Database/deploy/04_packages/PKG_API_CLIENT.sql
   ```

2. **Deploy the change**:
   ```bash
   sqlplus TR2000_STAGING/piping @Database/deploy/04_packages/PKG_API_CLIENT.sql
   ```

3. **Test the change**:
   ```sql
   -- Run your tests
   EXEC PKG_MAIN_ETL_CONTROL.run_main_etl;
   ```

4. **Commit if successful**:
   ```bash
   git add -A
   git commit -m "Fix: Add timeout handling to API client"
   git push
   ```

### Rolling Back

If something breaks:

```bash
# Option 1: Revert to previous git version
git checkout HEAD~1 Database/deploy/04_packages/PKG_API_CLIENT.sql
sqlplus TR2000_STAGING/piping @Database/deploy/04_packages/PKG_API_CLIENT.sql

# Option 2: Full rollback to baseline
git checkout baseline-pre-refactor-20250105
sqlplus TR2000_STAGING/piping @Database/deploy/DEPLOY_ALL.sql
```

## 🔍 Key Improvements

### PKG_ETL_VALIDATION (NEW)
Critical package for data integrity:
- `safe_to_number()` - Converts with error logging
- `safe_to_date()` - Multiple format support
- `validate_json()` - Ensures valid JSON before processing
- `validate_row_counts()` - Detects data loss
- All errors logged to `ETL_ERROR_LOG`

### Usage Example
```sql
-- Old (dangerous) way:
TO_NUMBER(test_pressure)  -- Fails with ORA-01722

-- New (safe) way:
PKG_ETL_VALIDATION.safe_to_number(
    p_value => test_pressure,
    p_default => NULL,
    p_source_table => 'STG_PCS_LIST',
    p_source_field => 'TEST_PRESSURE'
)
```

## 📊 Monitoring

### Check for conversion errors:
```sql
SELECT * FROM ETL_ERROR_LOG 
WHERE error_type LIKE 'DATA_CONVERSION_%'
ORDER BY error_timestamp DESC;
```

### Get conversion statistics:
```sql
SELECT PKG_ETL_VALIDATION.get_conversion_stats FROM dual;
```

## ⚠️ Important Notes

1. **Never edit the database directly** - Always modify deployment scripts
2. **Test on single package first** before running DEPLOY_ALL
3. **Check for invalid objects** after deployment:
   ```sql
   SELECT object_name, object_type FROM user_objects WHERE status = 'INVALID';
   ```
4. **Commit working changes immediately** to preserve rollback points

## 🚨 Emergency Recovery

If database is corrupted:

```bash
# Restore from Data Pump backup
impdp TR2000_STAGING/piping DIRECTORY=backup_dir DUMPFILE=TR2000_FULL_20250105.dmp SCHEMAS=TR2000_STAGING TABLE_EXISTS_ACTION=REPLACE

# Or restore from SQL backup
sqlplus TR2000_STAGING/piping @Database/backups/20250105_pre_refactor/all_packages.sql
```

## 📈 Next Steps

1. Extract remaining packages into individual deployment scripts
2. Update all packages to use `PKG_ETL_VALIDATION` for safe conversions
3. Add transaction control (SAVEPOINT/ROLLBACK) to all ETL procedures
4. Implement timeout handling in API calls
5. Add retry logic for network failures

---

*Last Updated: 2025-01-05*
*Baseline Tag: baseline-pre-refactor-20250105*