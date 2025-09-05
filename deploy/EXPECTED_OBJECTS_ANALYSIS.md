# TR2000 DEPLOYMENT SCRIPTS - EXPECTED OBJECTS ANALYSIS

## DEPLOYMENT STRUCTURE ANALYSIS

### 1. TABLES (from 01_tables/*.sql)

#### Control Tables (01_control_tables.sql)
- CONTROL_ENDPOINTS
- CONTROL_SETTINGS  
- ETL_FILTER

#### Audit Tables (02_audit_tables.sql)
- ETL_ERROR_LOG
- ETL_RUN_LOG
- ETL_STATISTICS
- RAW_JSON

#### Staging Tables (03_staging_tables.sql) - ALL VARCHAR2
- STG_EDS_REFERENCES
- STG_ESK_REFERENCES
- STG_MDS_REFERENCES
- STG_PCS_EMBEDDED_NOTES
- STG_PCS_HEADER_PROPERTIES
- STG_PCS_LIST
- STG_PCS_PIPE_ELEMENTS
- STG_PCS_PIPE_SIZES
- STG_PCS_REFERENCES
- STG_PCS_TEMP_PRESSURES
- STG_PCS_VALVE_ELEMENTS
- STG_PIPE_ELEMENT_REFERENCES
- STG_SC_REFERENCES
- STG_VDS_LIST
- STG_VDS_REFERENCES
- STG_VSK_REFERENCES
- STG_VSM_REFERENCES

#### Reference Tables (04_reference_tables.sql) - Issue Level
- EDS_REFERENCES
- ESK_REFERENCES
- MDS_REFERENCES
- PCS_REFERENCES
- PIPE_ELEMENT_REFERENCES
- SC_REFERENCES
- VDS_REFERENCES
- VSK_REFERENCES
- VSM_REFERENCES

#### PCS Detail Tables (05_pcs_detail_tables.sql) 
- PCS_EMBEDDED_NOTES
- PCS_HEADER_PROPERTIES
- PCS_PIPE_ELEMENTS
- PCS_PIPE_SIZES
- PCS_TEMP_PRESSURES
- PCS_VALVE_ELEMENTS

#### Catalog Tables (06_catalog_tables.sql)
- PCS_LIST
- VDS_LIST

**TOTAL TABLES: 41**
- Control: 3
- Audit: 4  
- Staging: 17
- Reference: 9
- PCS Detail: 6
- Catalog: 2

### 2. SEQUENCES (02_sequences/all_sequences.sql)
**NONE** - All tables use IDENTITY columns or SYS_GUID()

### 3. PACKAGES (04_packages/*.sql)
- PKG_API_CLIENT
- PKG_DATE_UTILS
- PKG_ETL_LOGGING
- PKG_ETL_PROCESSOR
- PKG_ETL_TEST_UTILS
- PKG_ETL_VALIDATION
- PKG_INDEPENDENT_ETL_CONTROL
- PKG_MAIN_ETL_CONTROL
- PKG_PCS_DETAIL_PROCESSOR

**TOTAL PACKAGES: 9**

### 4. PROCEDURES (05_procedures/*.sql)
- FIX_EMBEDDED_NOTES_PARSER
- FIX_PCS_LIST_PARSER
- FIX_VDS_CATALOG_PARSER
- TEMP_FIX_VDS_PARSE

**TOTAL PROCEDURES: 4**

### 5. VIEWS (06_views/all_views.sql)
- V_API_CALLS_PER_RUN
- V_API_CALL_STATISTICS  
- V_API_OPTIMIZATION_CANDIDATES
- V_ENDPOINT_TABLE_STATISTICS
- V_ETL_RUN_SUMMARY
- V_ETL_STATISTICS_SUMMARY
- V_OPERATION_STATISTICS
- V_RAW_JSON
- V_RAW_JSON_SUMMARY

**TOTAL VIEWS: 9**

### 6. INDEXES (03_indexes/all_indexes.sql)
Expected performance indexes on:
- Reference tables (plant_id, issue_revision)
- Error logs (timestamp, endpoint_key)
- Raw JSON (batch_id, endpoint_key, created_date)
- Statistics (run_id, endpoint_key)

### 7. CONTROL DATA (07_control_data/load_control_data.sql)
- ETL_FILTER: 1 row (GRANE 34/4.2)
- CONTROL_SETTINGS: 4 rows (API_BASE_URL, MAX_PCS_DETAILS_PER_RUN, API_TIMEOUT_SECONDS, LOG_RETENTION_DAYS)
- CONTROL_ENDPOINTS: 17 rows (9 references + 2 catalogs + 6 PCS details)

## DEPLOYMENT ORDER (from DEPLOY_ALL.sql)
1. Tables (01_tables)
2. Sequences (02_sequences) - NONE
3. Indexes (03_indexes)  
4. Views (06_views)
5. Packages (04_packages) - **Order matters for dependencies**
6. Procedures (05_procedures)
7. Control Data (07_control_data)

## CRITICAL DEPENDENCIES
- PKG_DATE_UTILS (first - used by others)
- PKG_ETL_VALIDATION (second - used by processors)
- PKG_ETL_LOGGING (third - used by control)
- PKG_API_CLIENT (fourth - used by control)
- PKG_ETL_PROCESSOR (fifth - uses validation)
- PKG_PCS_DETAIL_PROCESSOR (sixth - uses validation)
- PKG_MAIN_ETL_CONTROL (seventh - orchestrates all)
- PKG_ETL_TEST_UTILS (eighth - testing)
- PKG_INDEPENDENT_ETL_CONTROL (ninth - VDS catalog)

## VERIFICATION POINTS
1. All 41 tables created successfully
2. All 9 packages VALID status
3. All 4 procedures VALID status  
4. All 9 views created successfully
5. Control data loaded (1 + 4 + 17 = 22 rows)
6. No INVALID objects
7. All indexes created