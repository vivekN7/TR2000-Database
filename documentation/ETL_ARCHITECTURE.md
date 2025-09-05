# ETL Architecture - TR2000 Staging System

## Current System Status (2025-01-05)
**FULLY OPERATIONAL** - Processing GRANE plant (34) issue 4.2
- **8 packages**: All VALID ✅
- **Latest Run**: 58 API calls, 11 seconds, 100% success
- **Data Loaded**: 1,650 references + 362 PCS catalog + 8 PCS details

## Architecture Overview

### 1. API Proxy Security Model

```
TR2000_STAGING.PKG_API_CLIENT
         ↓ calls
API_SERVICE.API_GATEWAY.get_clob()
         ↓ makes API call
    External API (Equinor)
         ↓ tracks stats
API_SERVICE.API_CALL_STATS
```

**Key Security Features:**
- TR2000_STAGING has NO direct API privileges
- API_SERVICE user has APEX_WEB_SERVICE privileges
- Proxy authentication: `GRANT CONNECT THROUGH API_SERVICE`
- All API calls tracked in API_CALL_STATS table

### 2. Data Processing Flow

```
ETL_FILTER → Clear Tables → API Call → RAW_JSON → STG_* → Core Tables
```

**Processing Steps:**
1. **ETL_FILTER** defines what plant/issue combinations to load
2. **Clear** existing data for fresh reload
3. **API Call** via API_SERVICE.API_GATEWAY proxy
4. **RAW_JSON** stores raw API response
5. **STG_* tables** temporary storage (all VARCHAR2)
6. **Core tables** final storage with proper data types

## Core Components

### Control Tables
- **ETL_FILTER**: Controls which plant/issue combinations to process
- **CONTROL_SETTINGS**: Configuration (API_BASE_URL, MAX_PCS_DETAILS_PER_RUN)
- **CONTROL_ENDPOINTS**: API endpoint URL templates

### Data Tables
- **9 Reference Tables**: PCS, VDS, MDS, EDS, VSK, ESK, PIPE_ELEMENT, SC, VSM
- **PCS Catalog**: PCS_LIST (all PCS for a plant)
- **6 PCS Detail Tables**: Header, Temp/Pressures, Pipe Sizes, Pipe Elements, Valve Elements, Embedded Notes
- **VDS Catalog**: VDS_LIST (50,000+ items, separate process)

### Staging Tables (Temporary)
All STG_* tables use VARCHAR2 columns to receive JSON data directly:
- STG_PCS_REFERENCES, STG_VDS_REFERENCES, etc.
- STG_PCS_LIST, STG_PCS_HEADER_PROPERTIES, etc.

### Audit/Logging Tables
- **RAW_JSON**: Stores all API responses
- **ETL_RUN_LOG**: Tracks ETL run status
- **ETL_STATISTICS**: Detailed operation metrics
- **ETL_ERROR_LOG**: Error tracking

## Package Structure

### 1. PKG_API_CLIENT
Handles all API communication via proxy:
```sql
v_response := API_SERVICE.API_GATEWAY.get_clob(
    p_url => 'https://equinor.pipespec-api.presight.com/...',
    p_method => 'GET',
    p_body => NULL,
    p_headers => NULL,
    p_credential_static_id => NULL,
    p_status_code => v_http_status
);
```

### 2. PKG_MAIN_ETL_CONTROL
Orchestrates the ETL process:
- `run_full_etl()` - Main entry point
- `process_references_for_issue()` - Load 9 reference types
- `process_pcs_list()` - Load PCS catalog
- `process_pcs_details()` - Load PCS detail tables

### 3. PKG_ETL_PROCESSOR
Parses JSON and loads reference data:
- `parse_and_load_pcs_references()`
- `parse_and_load_vds_references()`
- Similar procedures for all 9 reference types

### 4. PKG_PCS_DETAIL_PROCESSOR
Processes PCS detail endpoints:
- `process_pcs_header_properties()`
- `process_pcs_temp_pressures()`
- 4 more procedures for other detail types

### 5. PKG_ETL_LOGGING
Comprehensive logging:
- Tracks run start/end times
- Records operation statistics
- Logs errors with full context

### 6. Supporting Packages
- **PKG_DATE_UTILS**: Robust date parsing
- **PKG_ETL_TEST_UTILS**: Testing utilities
- **PKG_INDEPENDENT_ETL_CONTROL**: VDS catalog ETL

## ETL Process Details

### Main ETL Process
1. Read ETL_FILTER for plant/issue combinations
2. For each combination:
   - Fetch 9 reference types (9 API calls)
   - Fetch PCS list (1 API call)
3. Process unique PCS combinations:
   - Fetch 6 detail endpoints per PCS (configurable limit)

### Data Processing Pattern
```sql
-- Step 1: Fetch from API
v_response := API_SERVICE.API_GATEWAY.get_clob(...);

-- Step 2: Store in RAW_JSON
INSERT INTO RAW_JSON (payload, ...) VALUES (v_response, ...);

-- Step 3: Parse to staging
INSERT INTO STG_PCS_REFERENCES
SELECT ... FROM JSON_TABLE(v_json, '$.getIssuePCSList[*]' ...);

-- Step 4: Load to core with type conversion
INSERT INTO PCS_REFERENCES
SELECT SYS_GUID(), ..., safe_parse_date(RevDate), ...
FROM STG_PCS_REFERENCES;
```

## API Endpoints

### Reference Data (9 types per issue)
```
/plants/{plantid}/issues/rev/{issuerev}/pcs
/plants/{plantid}/issues/rev/{issuerev}/vds
/plants/{plantid}/issues/rev/{issuerev}/mds
/plants/{plantid}/issues/rev/{issuerev}/eds
/plants/{plantid}/issues/rev/{issuerev}/vsk
/plants/{plantid}/issues/rev/{issuerev}/esk
/plants/{plantid}/issues/rev/{issuerev}/pipe-elements
/plants/{plantid}/issues/rev/{issuerev}/sc
/plants/{plantid}/issues/rev/{issuerev}/vsm
```

### PCS Catalog
```
/plants/{plantid}/pcs
```

### PCS Details (6 per PCS)
```
/plants/{plantid}/pcs/{pcsname}/rev/{revision}
/plants/{plantid}/pcs/{pcsname}/rev/{revision}/temp-pressures
/plants/{plantid}/pcs/{pcsname}/rev/{revision}/pipe-sizes
/plants/{plantid}/pcs/{pcsname}/rev/{revision}/pipe-elements
/plants/{plantid}/pcs/{pcsname}/rev/{revision}/valve-elements
/plants/{plantid}/pcs/{pcsname}/rev/{revision}/embedded-notes
```

### VDS Catalog (separate process)
```
/vds  (returns 50,000+ items)
```

## Operations

### Running ETL
```sql
-- Connect to database
sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1

-- Reset for testing (optional)
EXEC PKG_ETL_TEST_UTILS.reset_for_testing;

-- Configure PCS detail limit
UPDATE CONTROL_SETTINGS 
SET setting_value = '10'  -- or '0' for all
WHERE setting_key = 'MAX_PCS_DETAILS_PER_RUN';

-- Run full ETL
EXEC PKG_MAIN_ETL_CONTROL.run_full_etl;

-- Check status
EXEC PKG_ETL_TEST_UTILS.show_etl_status;
```

### Monitoring
```sql
-- Check API statistics
SELECT * FROM API_SERVICE.API_CALL_STATS;

-- Check recent runs
SELECT * FROM ETL_RUN_LOG ORDER BY run_id DESC;

-- Check for errors
SELECT * FROM ETL_ERROR_LOG 
WHERE error_timestamp > SYSDATE - 1/24
ORDER BY error_timestamp DESC;
```

### Adding New Plant/Issue
```sql
INSERT INTO ETL_FILTER (filter_id, plant_id, plant_name, issue_revision, added_by_user_id)
VALUES (ETL_FILTER_SEQ.NEXTVAL, '35', 'NEW_PLANT', '5.0', 'SYSTEM');
```

## Performance Metrics

### Current Performance (GRANE 34/4.2)
- **API Calls**: 58 total (10 for references/catalog, 48 for 8 PCS details)
- **Data Volume**: ~605KB JSON responses
- **Records Loaded**: 2,012 total
- **Execution Time**: ~11 seconds
- **Success Rate**: 100%

### Scalability
- For 66 PCS references: ~406 API calls needed
- VDS catalog: Single call returns 50,000+ items
- Configurable detail processing prevents timeouts

## Error Recovery

The system uses a simple "clear and reload" strategy:
```sql
-- If anything fails, just run again:
EXEC PKG_MAIN_ETL_CONTROL.run_full_etl;
```

This is safe because:
- Each run starts by clearing target tables
- RAW_JSON preserves audit trail
- All operations are idempotent

## Summary

The TR2000 ETL system provides:
- **Security**: API access only through secure proxy
- **Reliability**: 100% success rate in production
- **Performance**: Complete ETL in seconds
- **Observability**: Comprehensive logging and statistics
- **Simplicity**: Clear and reload strategy for easy recovery