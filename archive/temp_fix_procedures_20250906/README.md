# Archived Temporary Fix Procedures

**Date Archived**: 2025-09-06  
**Reason**: These were temporary development artifacts, not needed for production deployment

## Archived Files:

1. **FIX_EMBEDDED_NOTES_PARSER.sql** - Temporary fix for embedded notes parsing
2. **FIX_PCS_LIST_PARSER.sql** - Temporary fix for PCS list parsing  
3. **FIX_VDS_CATALOG_PARSER.sql** - Temporary fix for VDS catalog parsing
4. **TEMP_FIX_VDS_PARSE.sql** - Temporary VDS parsing procedure

## Why Archived:

- These were causing compilation issues (invalid objects)
- Main ETL functionality is properly handled by:
  - `PKG_ETL_PROCESSOR` - Main JSON parsing
  - `PKG_PCS_DETAIL_PROCESSOR` - PCS detail processing  
  - `PKG_INDEPENDENT_ETL_CONTROL` - VDS catalog processing
- Named with "TEMP" and "FIX" indicating temporary nature
- Not referenced by current production ETL workflows

## Recovery:

If needed, these can be restored by moving back to `deploy/05_procedures/` and updating `DEPLOY_ALL.sql` to reference them.