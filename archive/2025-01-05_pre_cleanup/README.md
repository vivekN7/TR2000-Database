# Archive - 2025-01-05 Pre-Cleanup

## Purpose
This archive contains files that were removed during the cleanup after establishing the script-based deployment system. These files are no longer needed for normal operations but are preserved for historical reference.

## Archived Items

### 1. PKG_API_CLIENT_raw.sql
- **What**: Raw extraction of PKG_API_CLIENT package
- **Why Archived**: Temporary file from initial extraction, replaced by proper deployment script
- **Original Location**: `deploy/04_packages/`

### 2. deploy_scripts/
- **What**: One-time extraction scripts used to populate deploy/ folder
- **Why Archived**: Job complete, scripts no longer needed
- **Contents**:
  - `extract_all_objects.sql` - Extracted tables, sequences, indexes, views
  - `extract_packages.sql` - Extracted all packages
  - `extract_procedures.sql` - Extracted all procedures
  - `extract_control_data.sql` - Extracted control data
- **Original Location**: `deploy/scripts/`

### 3. snapshots/
- **What**: Old database snapshots
- **Why Archived**: Superseded by current deployment structure
- **Contents**:
  - `snapshot_20250905_with_null_stubs/` - Corrupted version with null stub issue
  - `snapshot_20250905_post_recovery/` - Intermediate recovery snapshot
- **Original Location**: `Snapshots/`
- **Note**: Kept `snapshot_20250905_working_etl/` in main structure as historical reference

### 4. ref/
- **What**: Reference DDL and utility scripts
- **Why Archived**: Examples and utilities superseded by deployment structure
- **Contents**:
  - `Oracle_DB_Safety_Kit.md` - Database safety guidelines
  - `Oracle_DB_Safety_Kit_Data_Addon.sql` - Data safety utilities
  - `ddl_proxy_tr2kloader/` - API proxy DDL examples
  - `tr2000_util_package from dba.sql` - DBA utilities
- **Original Location**: Root `ref/` folder

## Recovery
If any of these files are needed:
1. They remain in this archive folder
2. They are also preserved in git history
3. Can be restored with: `git checkout <commit> <file>`

## Archive Date
- **Date**: 2025-01-05
- **Reason**: Post-deployment structure cleanup
- **Commit**: Will be tagged after archiving

---

*These files can be safely deleted from the archive after reviewing if truly not needed.*