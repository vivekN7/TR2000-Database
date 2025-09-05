# TR2000 Database - Quick Reference

## 🚀 Essential Commands

### Deploy a Package
```bash
sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1 @deploy/04_packages/PKG_NAME.sql
```

### Run Full ETL
```sql
sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1
EXEC PKG_MAIN_ETL_CONTROL.run_main_etl;
```

### Check Errors
```sql
SELECT * FROM ETL_ERROR_LOG WHERE error_timestamp > SYSDATE - 1/24 ORDER BY error_timestamp DESC;
```

## 📝 Git Workflow

### Make Changes
```bash
# Edit package
vi deploy/04_packages/PKG_API_CLIENT.sql

# Test deployment
sqlplus TR2000_STAGING/piping @deploy/04_packages/PKG_API_CLIENT.sql

# Commit and push
git add -A
git commit -m "Fix: Description of change"
git push origin master
```

### Rollback Changes
```bash
# Revert last commit
git revert HEAD
git push

# Or checkout previous version
git checkout HEAD~1 deploy/04_packages/PKG_API_CLIENT.sql
sqlplus TR2000_STAGING/piping @deploy/04_packages/PKG_API_CLIENT.sql
```

## 🔍 Key Packages

| Package | Purpose | Key Functions |
|---------|---------|---------------|
| **PKG_ETL_VALIDATION** | Safe conversions | `safe_to_number()`, `safe_to_date()` |
| **PKG_API_CLIENT** | API calls | `fetch_reference_data()`, `fetch_pcs_list()` |
| **PKG_MAIN_ETL_CONTROL** | ETL orchestration | `run_main_etl()` |
| **PKG_ETL_PROCESSOR** | JSON parsing | `parse_and_load_*()` functions |

## 📊 Monitoring Queries

```sql
-- Conversion errors
SELECT * FROM ETL_ERROR_LOG WHERE error_type LIKE 'DATA_CONVERSION_%';

-- ETL run history
SELECT * FROM ETL_RUN_LOG ORDER BY run_id DESC;

-- API call stats
SELECT * FROM API_SERVICE.API_CALL_STATS ORDER BY call_timestamp DESC;

-- Get conversion statistics
SELECT PKG_ETL_VALIDATION.get_conversion_stats FROM dual;
```

## 🚨 Emergency Recovery

```bash
# From Data Pump backup
impdp TR2000_STAGING/piping DIRECTORY=backup_dir DUMPFILE=TR2000_FULL_20250105.dmp SCHEMAS=TR2000_STAGING TABLE_EXISTS_ACTION=REPLACE

# From SQL backup
sqlplus TR2000_STAGING/piping @backups/20250105_pre_refactor/all_packages.sql

# From snapshot
sqlplus TR2000_STAGING/piping @Snapshots/snapshot_20250905_working_etl/RESTORE_MASTER.sql
```

## 📁 Important Files

- `deploy/DEPLOY_ALL.sql` - Master deployment script
- `deploy/04_packages/PKG_ETL_VALIDATION.sql` - Safe conversion functions
- `backups/20250105_pre_refactor/` - Baseline backup
- `documentation/ETL_ARCHITECTURE.md` - System design

## 🔒 Security Notes

- **Never** store passwords in scripts
- **Always** use API_SERVICE proxy for API calls
- **Never** log sensitive data to ETL_ERROR_LOG

## 📈 Performance Tips

- Use `safe_to_number()` with source table/field for better logging
- Check `ETL_ERROR_LOG` regularly for conversion issues
- Limit PCS details per run with `MAX_PCS_DETAILS_PER_RUN` setting

---
**Repository**: https://github.com/vivekN7/TR2000-Database  
**Last Updated**: 2025-01-05