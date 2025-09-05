# TR2000 Database ERD and Column Mappings

## Data Flow Architecture

The TR2000_STAGING database follows a strict data flow pattern:
```
External API → API_SERVICE.API_GATEWAY → RAW_JSON → STG_* Tables (VARCHAR2) → Core Tables (Typed)
```

### API Proxy Security Model:
- **TR2000_STAGING**: No direct API access (no APEX_WEB_SERVICE privileges)
- **API_SERVICE**: Proxy user with APEX_WEB_SERVICE privileges
- **API_GATEWAY**: Package that makes actual API calls
- **API_CALL_STATS**: Tracks all API calls and statistics
- **Authentication**: `ALTER USER TR2000_STAGING GRANT CONNECT THROUGH API_SERVICE`

## Database Schema Structure

### Schema Categories
1. **Control Tables**: Configuration and filtering
2. **Audit/Log Tables**: Tracking and error logging
3. **Staging Tables**: Temporary storage (all VARCHAR2)
4. **Reference Tables**: Issue-level reference data
5. **PCS Detail Tables**: PCS-specific detailed data
6. **VDS Catalog Table**: Independent VDS catalog

---

## Complete Table Constraints

### Control Tables

#### ETL_FILTER
- **Primary Key**: FILTER_ID
- **Unique**: PLANT_ID, ISSUE_REVISION
- **Foreign Keys**: None

#### CONTROL_SETTINGS
- **Primary Key**: SETTING_KEY
- **Unique**: None
- **Foreign Keys**: None

#### CONTROL_ENDPOINTS
- **Primary Key**: ENDPOINT_ID
- **Unique**: ENDPOINT_KEY
- **Foreign Keys**: None

### Audit/Log Tables

#### RAW_JSON
- **Primary Key**: RAW_JSON_ID
- **Unique**: None
- **Foreign Keys**: None

#### ETL_RUN_LOG
- **Primary Key**: RUN_ID
- **Unique**: None
- **Foreign Keys**: None

#### ETL_ERROR_LOG
- **Primary Key**: ERROR_ID
- **Unique**: None
- **Foreign Keys**: None

#### ETL_PERFORMANCE_METRICS
- **Primary Key**: STATE_ID
- **Unique**: ENDPOINT_KEY, PLANT_ID
- **Foreign Keys**: None

### Reference Tables (Issue-Level)

#### PCS_REFERENCES
- **Primary Key**: PCS_REFERENCES_GUID
- **Unique**: PLANT_ID, ISSUE_REVISION, PCS_NAME
- **Foreign Keys**: None

#### VDS_REFERENCES
- **Primary Key**: VDS_REFERENCES_GUID
- **Unique**: PLANT_ID, ISSUE_REVISION, VDS_NAME
- **Foreign Keys**: None

#### MDS_REFERENCES
- **Primary Key**: MDS_REFERENCES_GUID
- **Unique**: PLANT_ID, ISSUE_REVISION, MDS_NAME
- **Foreign Keys**: None

#### EDS_REFERENCES
- **Primary Key**: EDS_REFERENCES_GUID
- **Unique**: PLANT_ID, ISSUE_REVISION, EDS_NAME
- **Foreign Keys**: None

#### VSK_REFERENCES
- **Primary Key**: VSK_REFERENCES_GUID
- **Unique**: PLANT_ID, ISSUE_REVISION, VSK_NAME
- **Foreign Keys**: None

#### ESK_REFERENCES
- **Primary Key**: ESK_REFERENCES_GUID
- **Unique**: PLANT_ID, ISSUE_REVISION, ESK_NAME
- **Foreign Keys**: None

#### PIPE_ELEMENT_REFERENCES
- **Primary Key**: PIPE_ELEMENT_REFERENCES_GUID
- **Unique**: PLANT_ID, ISSUE_REVISION, ELEMENT_ID
- **Foreign Keys**: None

#### SC_REFERENCES
- **Primary Key**: SC_REFERENCES_GUID
- **Unique**: PLANT_ID, ISSUE_REVISION, SC_NAME
- **Foreign Keys**: None

#### VSM_REFERENCES
- **Primary Key**: VSM_REFERENCES_GUID
- **Unique**: PLANT_ID, ISSUE_REVISION, VSM_NAME
- **Foreign Keys**: None

### PCS Detail Tables

#### PCS_LIST (Independent List)
- **Primary Key**: PCS_LIST_GUID
- **Unique**: PLANT_ID, PCS_NAME, REVISION
- **Foreign Keys**: None
- **Note**: General overall list, not referenced by other tables

#### PCS_HEADER_PROPERTIES
- **Primary Key**: PCS_HEADER_PROPERTIES_GUID
- **Unique**: PLANT_ID, PCS_NAME, PCS_REVISION
- **Foreign Keys**: None
- **Note**: PLANT_ID, PCS_NAME, and PCS_REVISION all come from endpoint parameters

#### PCS_TEMP_PRESSURES
- **Primary Key**: PCS_TEMP_PRESSURES_GUID
- **Unique**: PLANT_ID, PCS_NAME, PCS_REVISION, TEMPERATURE, PRESSURE
- **Foreign Keys**: None
- **Note**: PLANT_ID, PCS_NAME, and PCS_REVISION all come from endpoint parameters

#### PCS_PIPE_SIZES
- **Primary Key**: PCS_PIPE_SIZES_GUID
- **Unique**: PLANT_ID, PCS_NAME, PCS_REVISION, NOM_SIZE
- **Foreign Keys**: None
- **Note**: PLANT_ID, PCS_NAME, and PCS_REVISION all come from endpoint parameters

#### PCS_PIPE_ELEMENTS
- **Primary Key**: PCS_PIPE_ELEMENTS_GUID
- **Unique**: PLANT_ID, PCS_NAME, PCS_REVISION, ELEMENT_GROUP_NO, LINE_NO
- **Foreign Keys**: None
- **Note**: PLANT_ID, PCS_NAME, and PCS_REVISION all come from endpoint parameters

#### PCS_VALVE_ELEMENTS
- **Primary Key**: PCS_VALVE_ELEMENTS_GUID
- **Unique**: PLANT_ID, PCS_NAME, PCS_REVISION, VALVE_GROUP_NO, LINE_NO
- **Foreign Keys**: None
- **Note**: PLANT_ID, PCS_NAME, and PCS_REVISION all come from endpoint parameters

#### PCS_EMBEDDED_NOTES
- **Primary Key**: PCS_EMBEDDED_NOTES_GUID
- **Unique**: PLANT_ID, PCS_NAME, PCS_REVISION, TEXT_SECTION_ID
- **Foreign Keys**: None
- **Note**: PLANT_ID, PCS_NAME, and PCS_REVISION all come from endpoint parameters

### VDS Catalog Table

#### VDS_LIST
- **Primary Key**: VDS_LIST_GUID
- **Unique**: VDS_NAME, REVISION
- **Foreign Keys**: None

---

## Complete Column Mapping Tables

### Mapping Legend
- ✅ MATCH: Column exists in both tables with correct mapping (case conversion is expected)
- ➕ CORE_ONLY: Column only exists in core table (GUID, audit columns)
- ❌ MISSING: Column expected but not found
- 🔄 MAPPED: Different column names but correctly mapped (e.g., PCS → PCS_NAME)

---

## REFERENCE TABLES (Issue-Level)

### PCS_REFERENCES Complete Mapping (12 STG → 15 CORE)
| # | JSON Field | STG_PCS_REFERENCES | PCS_REFERENCES | STG Type | Core Type | Match |
|---|------------|-------------------|-----------------|----------|-----------|-------|
| 1 | - | - | PCS_REFERENCES_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | - | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | - | ISSUE_REVISION | ISSUE_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 4 | PCS | PCS | PCS_NAME | VARCHAR2(100) | VARCHAR2(100) | 🔄 MAPPED |
| 5 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 6 | RevDate | RevDate | REV_DATE | VARCHAR2(50) | DATE | ✅ MATCH |
| 7 | Status | Status | STATUS | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 8 | OfficialRevision | OfficialRevision | OFFICIAL_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 9 | RevisionSuffix | RevisionSuffix | REVISION_SUFFIX | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 10 | RatingClass | RatingClass | RATING_CLASS | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 11 | MaterialGroup | MaterialGroup | MATERIAL_GROUP | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 12 | HistoricalPCS | HistoricalPCS | HISTORICAL_PCS | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 13 | Delta | Delta | DELTA | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 14 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 15 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

### VDS_REFERENCES Complete Mapping (8 STG → 11 CORE)
| # | JSON Field | STG_VDS_REFERENCES | VDS_REFERENCES | STG Type | Core Type | Match |
|---|------------|-------------------|-----------------|----------|-----------|-------|
| 1 | - | - | VDS_REFERENCES_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | - | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | - | ISSUE_REVISION | ISSUE_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 4 | VDS | VDS | VDS_NAME | VARCHAR2(100) | VARCHAR2(100) | 🔄 MAPPED |
| 5 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 6 | RevDate | RevDate | REV_DATE | VARCHAR2(50) | DATE | ✅ MATCH |
| 7 | Status | Status | STATUS | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 8 | OfficialRevision | OfficialRevision | OFFICIAL_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 9 | Delta | Delta | DELTA | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 10 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 11 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

### MDS_REFERENCES Complete Mapping (9 STG → 12 CORE)
| # | JSON Field | STG_MDS_REFERENCES | MDS_REFERENCES | STG Type | Core Type | Match |
|---|------------|-------------------|-----------------|----------|-----------|-------|
| 1 | - | - | MDS_REFERENCES_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | - | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | - | ISSUE_REVISION | ISSUE_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 4 | MDS | MDS | MDS_NAME | VARCHAR2(100) | VARCHAR2(100) | 🔄 MAPPED |
| 5 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 6 | Area | Area | AREA | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 7 | RevDate | RevDate | REV_DATE | VARCHAR2(50) | DATE | ✅ MATCH |
| 8 | Status | Status | STATUS | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 9 | OfficialRevision | OfficialRevision | OFFICIAL_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 10 | Delta | Delta | DELTA | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 11 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 12 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

### EDS_REFERENCES Complete Mapping (8 STG → 11 CORE)
| # | JSON Field | STG_EDS_REFERENCES | EDS_REFERENCES | STG Type | Core Type | Match |
|---|------------|-------------------|-----------------|----------|-----------|-------|
| 1 | - | - | EDS_REFERENCES_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | - | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | - | ISSUE_REVISION | ISSUE_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 4 | EDS | EDS | EDS_NAME | VARCHAR2(100) | VARCHAR2(100) | 🔄 MAPPED |
| 5 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 6 | RevDate | RevDate | REV_DATE | VARCHAR2(50) | DATE | ✅ MATCH |
| 7 | Status | Status | STATUS | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 8 | OfficialRevision | OfficialRevision | OFFICIAL_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 9 | Delta | Delta | DELTA | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 10 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 11 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

### VSK_REFERENCES Complete Mapping (8 STG → 11 CORE)
| # | JSON Field | STG_VSK_REFERENCES | VSK_REFERENCES | STG Type | Core Type | Match |
|---|------------|-------------------|-----------------|----------|-----------|-------|
| 1 | - | - | VSK_REFERENCES_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | - | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | - | ISSUE_REVISION | ISSUE_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 4 | VSK | VSK | VSK_NAME | VARCHAR2(100) | VARCHAR2(100) | 🔄 MAPPED |
| 5 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 6 | RevDate | RevDate | REV_DATE | VARCHAR2(50) | DATE | ✅ MATCH |
| 7 | Status | Status | STATUS | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 8 | OfficialRevision | OfficialRevision | OFFICIAL_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 9 | Delta | Delta | DELTA | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 10 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 11 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

### ESK_REFERENCES Complete Mapping (8 STG → 11 CORE)
| # | JSON Field | STG_ESK_REFERENCES | ESK_REFERENCES | STG Type | Core Type | Match |
|---|------------|-------------------|-----------------|----------|-----------|-------|
| 1 | - | - | ESK_REFERENCES_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | - | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | - | ISSUE_REVISION | ISSUE_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 4 | ESK | ESK | ESK_NAME | VARCHAR2(100) | VARCHAR2(100) | 🔄 MAPPED |
| 5 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 6 | RevDate | RevDate | REV_DATE | VARCHAR2(50) | DATE | ✅ MATCH |
| 7 | Status | Status | STATUS | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 8 | OfficialRevision | OfficialRevision | OFFICIAL_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 9 | Delta | Delta | DELTA | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 10 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 11 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

### PIPE_ELEMENT_REFERENCES Complete Mapping (14 STG → 17 CORE)
| # | JSON Field | STG_PIPE_ELEMENT_REFERENCES | PIPE_ELEMENT_REFERENCES | STG Type | Core Type | Match |
|---|------------|----------------------------|-------------------------|----------|-----------|-------|
| 1 | - | - | PIPE_ELEMENT_REFERENCES_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | - | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | - | ISSUE_REVISION | ISSUE_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 4 | ElementID | ElementID | ELEMENT_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 5 | ElementGroup | ElementGroup | ELEMENT_GROUP | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 6 | DimensionStandard | DimensionStandard | DIMENSION_STANDARD | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 7 | ProductForm | ProductForm | PRODUCT_FORM | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 8 | MaterialGrade | MaterialGrade | MATERIAL_GRADE | VARCHAR2(200) | VARCHAR2(200) | ✅ MATCH |
| 9 | MDS | MDS | MDS | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 10 | MDSRevision | MDSRevision | MDS_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 11 | Area | Area | AREA | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 12 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 13 | RevDate | RevDate | REV_DATE | VARCHAR2(50) | DATE | ✅ MATCH |
| 14 | Status | Status | STATUS | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 15 | Delta | Delta | DELTA | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 16 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 17 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

### SC_REFERENCES Complete Mapping (8 STG → 11 CORE)
| # | JSON Field | STG_SC_REFERENCES | SC_REFERENCES | STG Type | Core Type | Match |
|---|------------|-------------------|---------------|----------|-----------|-------|
| 1 | - | - | SC_REFERENCES_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | - | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | - | ISSUE_REVISION | ISSUE_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 4 | SC | SC | SC_NAME | VARCHAR2(100) | VARCHAR2(100) | 🔄 MAPPED |
| 5 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 6 | RevDate | RevDate | REV_DATE | VARCHAR2(50) | DATE | ✅ MATCH |
| 7 | Status | Status | STATUS | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 8 | OfficialRevision | OfficialRevision | OFFICIAL_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 9 | Delta | Delta | DELTA | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 10 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 11 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

### VSM_REFERENCES Complete Mapping (8 STG → 11 CORE)
| # | JSON Field | STG_VSM_REFERENCES | VSM_REFERENCES | STG Type | Core Type | Match |
|---|------------|-------------------|----------------|----------|-----------|-------|
| 1 | - | - | VSM_REFERENCES_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | - | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | - | ISSUE_REVISION | ISSUE_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 4 | VSM | VSM | VSM_NAME | VARCHAR2(100) | VARCHAR2(100) | 🔄 MAPPED |
| 5 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 6 | RevDate | RevDate | REV_DATE | VARCHAR2(50) | DATE | ✅ MATCH |
| 7 | Status | Status | STATUS | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 8 | OfficialRevision | OfficialRevision | OFFICIAL_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 9 | Delta | Delta | DELTA | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 10 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 11 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

---

## PCS DETAIL TABLES

### PCS_PIPE_ELEMENTS Complete Mapping (30 STG → 33 CORE)
| # | JSON Field | STG_PCS_PIPE_ELEMENTS | PCS_PIPE_ELEMENTS | STG Type | Core Type | Match |
|---|------------|----------------------|-------------------|----------|-----------|-------|
| 1 | - | - | PCS_PIPE_ELEMENTS_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | **Endpoint** | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | **Endpoint** | PCS_NAME | PCS_NAME | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 4 | **Endpoint** | PCS_REVISION | PCS_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 5 | PCS | PCS | PCS | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 6 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 7 | MaterialGroupID | MaterialGroupID | MATERIAL_GROUP_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 8 | ElementGroupNo | ElementGroupNo | ELEMENT_GROUP_NO | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 9 | LineNo | LineNo | LINE_NO | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 10 | Element | Element | ELEMENT | VARCHAR2(200) | VARCHAR2(200) | ✅ MATCH |
| 11 | DimStandard | DimStandard | DIM_STANDARD | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 12 | FromSize | FromSize | FROM_SIZE | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 13 | ToSize | ToSize | TO_SIZE | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 14 | ProductForm | ProductForm | PRODUCT_FORM | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 15 | Material | Material | MATERIAL | VARCHAR2(200) | VARCHAR2(200) | ✅ MATCH |
| 16 | MDS | MDS | MDS | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 17 | EDS | EDS | EDS | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 18 | EDSRevision | EDSRevision | EDS_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 19 | ESK | ESK | ESK | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 20 | Revmark | Revmark | REVMARK | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 21 | Remark | Remark | REMARK | VARCHAR2(4000) | VARCHAR2(500) | ✅ MATCH |
| 22 | PageBreak | PageBreak | PAGE_BREAK | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 23 | ElementID | ElementID | ELEMENT_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 24 | FreeText | FreeText | FREE_TEXT | VARCHAR2(500) | VARCHAR2(500) | ✅ MATCH |
| 25 | NoteID | NoteID | NOTE_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 26 | NewDeletedLine | NewDeletedLine | NEW_DELETED_LINE | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 27 | InitialInfo | InitialInfo | INITIAL_INFO | VARCHAR2(4000) | VARCHAR2(200) | ✅ MATCH |
| 28 | InitialRevmark | InitialRevmark | INITIAL_REVMARK | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 29 | MDSVariant | MDSVariant | MDS_VARIANT | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 30 | MDSRevision | MDSRevision | MDS_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 31 | Area | Area | AREA | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 32 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 33 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

### PCS_VALVE_ELEMENTS Complete Mapping (21 STG → 24 CORE)
| # | JSON Field | STG_PCS_VALVE_ELEMENTS | PCS_VALVE_ELEMENTS | STG Type | Core Type | Match |
|---|------------|------------------------|---------------------|----------|-----------|-------|
| 1 | - | - | PCS_VALVE_ELEMENTS_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | **Endpoint** | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | **Endpoint** | PCS_NAME | PCS_NAME | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 4 | **Endpoint** | PCS_REVISION | PCS_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 5 | ValveGroupNo | ValveGroupNo | VALVE_GROUP_NO | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 6 | LineNo | LineNo | LINE_NO | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 7 | ValveType | ValveType | VALVE_TYPE | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 8 | VDS | VDS | VDS | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 9 | ValveDescription | ValveDescription | VALVE_DESCRIPTION | VARCHAR2(4000) | VARCHAR2(500) | ✅ MATCH |
| 10 | FromSize | FromSize | FROM_SIZE | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 11 | ToSize | ToSize | TO_SIZE | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 12 | Revmark | Revmark | REVMARK | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 13 | Remark | Remark | REMARK | VARCHAR2(4000) | VARCHAR2(500) | ✅ MATCH |
| 14 | PageBreak | PageBreak | PAGE_BREAK | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 15 | NoteID | NoteID | NOTE_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 16 | PreviousVDS | PreviousVDS | PREVIOUS_VDS | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 17 | NewDeletedLine | NewDeletedLine | NEW_DELETED_LINE | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 18 | InitialInfo | InitialInfo | INITIAL_INFO | VARCHAR2(4000) | VARCHAR2(200) | ✅ MATCH |
| 19 | InitialRevmark | InitialRevmark | INITIAL_REVMARK | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 20 | SizeRange | SizeRange | SIZE_RANGE | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 21 | Status | Status | STATUS | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 22 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 23 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 24 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

### PCS_EMBEDDED_NOTES Complete Mapping (9 STG → 12 CORE)
| # | JSON Field | STG_PCS_EMBEDDED_NOTES | PCS_EMBEDDED_NOTES | STG Type | Core Type | Match |
|---|------------|-------------------------|---------------------|----------|-----------|-------|
| 1 | - | - | PCS_EMBEDDED_NOTES_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | **Endpoint** | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | **Endpoint** | PCS_NAME | PCS_NAME | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 4 | **Endpoint** | PCS_REVISION | PCS_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 5 | PCSName | PCSName | PCSNAME | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 6 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 7 | TextSectionID | TextSectionID | TEXT_SECTION_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 8 | TextSectionDescription | TextSectionDescription | TEXT_SECTION_DESCRIPTION | VARCHAR2(500) | VARCHAR2(500) | ✅ MATCH |
| 9 | PageBreak | PageBreak | PAGE_BREAK | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 10 | HTMLCLOB | HTMLCLOB | HTML_CLOB | CLOB | CLOB | ✅ MATCH |
| 11 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 12 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

---

## CATALOG TABLES

### VDS_LIST Complete Mapping (18 STG → 21 CORE)
| # | JSON Field | STG_VDS_LIST | VDS_LIST | STG Type | Core Type | Match |
|---|------------|--------------|----------|----------|-----------|-------|
| 1 | - | - | VDS_LIST_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | VDS | VDS | VDS_NAME | VARCHAR2(100) | VARCHAR2(100) | 🔄 MAPPED |
| 3 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 4 | Status | Status | STATUS | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 5 | RevDate | RevDate | REV_DATE | VARCHAR2(50) | DATE | ✅ MATCH |
| 6 | LastUpdate | LastUpdate | LAST_UPDATE | VARCHAR2(50) | DATE | ✅ MATCH |
| 7 | LastUpdateBy | LastUpdateBy | LAST_UPDATE_BY | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 8 | Description | Description | DESCRIPTION | VARCHAR2(500) | VARCHAR2(500) | ✅ MATCH |
| 9 | Notepad | Notepad | NOTEPAD | VARCHAR2(4000) | CLOB | ✅ MATCH |
| 10 | SpecialReqID | SpecialReqID | SPECIAL_REQ_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 11 | ValveTypeID | ValveTypeID | VALVE_TYPE_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 12 | RatingClassID | RatingClassID | RATING_CLASS_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 13 | MaterialGroupID | MaterialGroupID | MATERIAL_GROUP_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 14 | EndConnectionID | EndConnectionID | END_CONNECTION_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 15 | BoreID | BoreID | BORE_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 16 | VDSSizeID | VDSSizeID | VDS_SIZE_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 17 | SizeRange | SizeRange | SIZE_RANGE | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 18 | CustomName | CustomName | CUSTOM_NAME | VARCHAR2(200) | VARCHAR2(200) | ✅ MATCH |
| 19 | SubsegmentList | SubsegmentList | SUBSEGMENT_LIST | VARCHAR2(500) | VARCHAR2(500) | ✅ MATCH |
| 20 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 21 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

### PCS_HEADER_PROPERTIES Complete Mapping (69 STG columns → 71 CORE columns)

| # | JSON Field / Source | STG_PCS_HEADER_PROPERTIES | PCS_HEADER_PROPERTIES | Data Type (STG) | Data Type (Core) | Match Status |
|---|---------------------|---------------------------|------------------------|-----------------|------------------|--------------|
| 1 | - | - | PCS_HEADER_PROPERTIES_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | **Endpoint param** | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | **Endpoint param** | PCS_NAME | PCS_NAME | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 4 | **Endpoint param** | PCS_REVISION | PCS_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 5 | PCS | PCS | PCS | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 6 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 7 | Status | Status | STATUS | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 8 | RevDate | RevDate | REV_DATE | VARCHAR2(50) | DATE | ✅ MATCH |
| 9 | RatingClass | RatingClass | RATING_CLASS | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 10 | TestPressure | TestPressure | TEST_PRESSURE | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 11 | MaterialGroup | MaterialGroup | MATERIAL_GROUP | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 12 | DesignCode | DesignCode | DESIGN_CODE | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 13 | LastUpdate | LastUpdate | LAST_UPDATE | VARCHAR2(50) | DATE | ✅ MATCH |
| 14 | LastUpdateBy | LastUpdateBy | LAST_UPDATE_BY | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 15 | Approver | Approver | APPROVER | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 16 | Notepad | Notepad | NOTEPAD | VARCHAR2(4000) | CLOB | ✅ MATCH |
| 17 | SC | SC | SC | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 18 | VSM | VSM | VSM | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 19 | DesignCodeRevMark | DesignCodeRevMark | DESIGN_CODE_REV_MARK | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 20 | CorrAllowance | CorrAllowance | CORR_ALLOWANCE | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 21 | CorrAllowanceRevMark | CorrAllowanceRevMark | CORR_ALLOWANCE_REV_MARK | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 22 | LongWeldEff | LongWeldEff | LONG_WELD_EFF | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 23 | LongWeldEffRevMark | LongWeldEffRevMark | LONG_WELD_EFF_REV_MARK | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 24 | WallThkTol | WallThkTol | WALL_THK_TOL | VARCHAR2(50) | VARCHAR2(200) | ✅ MATCH |
| 25 | WallThkTolRevMark | WallThkTolRevMark | WALL_THK_TOL_REV_MARK | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 26 | ServiceRemark | ServiceRemark | SERVICE_REMARK | VARCHAR2(500) | VARCHAR2(500) | ✅ MATCH |
| 27 | ServiceRemarkRevMark | ServiceRemarkRevMark | SERVICE_REMARK_REV_MARK | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 28 | DesignPress01 | DesignPress01 | DESIGN_PRESS01 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 29 | DesignPress02 | DesignPress02 | DESIGN_PRESS02 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 30 | DesignPress03 | DesignPress03 | DESIGN_PRESS03 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 31 | DesignPress04 | DesignPress04 | DESIGN_PRESS04 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 32 | DesignPress05 | DesignPress05 | DESIGN_PRESS05 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 33 | DesignPress06 | DesignPress06 | DESIGN_PRESS06 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 34 | DesignPress07 | DesignPress07 | DESIGN_PRESS07 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 35 | DesignPress08 | DesignPress08 | DESIGN_PRESS08 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 36 | DesignPress09 | DesignPress09 | DESIGN_PRESS09 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 37 | DesignPress10 | DesignPress10 | DESIGN_PRESS10 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 38 | DesignPress11 | DesignPress11 | DESIGN_PRESS11 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 39 | DesignPress12 | DesignPress12 | DESIGN_PRESS12 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 40 | DesignPressRevMark | DesignPressRevMark | DESIGN_PRESS_REV_MARK | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 41 | DesignTemp01 | DesignTemp01 | DESIGN_TEMP01 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 42 | DesignTemp02 | DesignTemp02 | DESIGN_TEMP02 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 43 | DesignTemp03 | DesignTemp03 | DESIGN_TEMP03 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 44 | DesignTemp04 | DesignTemp04 | DESIGN_TEMP04 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 45 | DesignTemp05 | DesignTemp05 | DESIGN_TEMP05 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 46 | DesignTemp06 | DesignTemp06 | DESIGN_TEMP06 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 47 | DesignTemp07 | DesignTemp07 | DESIGN_TEMP07 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 48 | DesignTemp08 | DesignTemp08 | DESIGN_TEMP08 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 49 | DesignTemp09 | DesignTemp09 | DESIGN_TEMP09 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 50 | DesignTemp10 | DesignTemp10 | DESIGN_TEMP10 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 51 | DesignTemp11 | DesignTemp11 | DESIGN_TEMP11 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 52 | DesignTemp12 | DesignTemp12 | DESIGN_TEMP12 | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 53 | DesignTempRevMark | DesignTempRevMark | DESIGN_TEMP_REV_MARK | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 54 | NoteIDCorrAllowance | NoteIDCorrAllowance | NOTE_ID_CORR_ALLOWANCE | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 55 | NoteIDServiceCode | NoteIDServiceCode | NOTE_ID_SERVICE_CODE | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 56 | NoteIDWallThkTol | NoteIDWallThkTol | NOTE_ID_WALL_THK_TOL | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 57 | NoteIDLongWeldEff | NoteIDLongWeldEff | NOTE_ID_LONG_WELD_EFF | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 58 | NoteIDGeneralPCS | NoteIDGeneralPCS | NOTE_ID_GENERAL_PCS | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 59 | NoteIDDesignCode | NoteIDDesignCode | NOTE_ID_DESIGN_CODE | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 60 | NoteIDPressTempTable | NoteIDPressTempTable | NOTE_ID_PRESS_TEMP_TABLE | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 61 | NoteIDPipeSizeWthTable | NoteIDPipeSizeWthTable | NOTE_ID_PIPE_SIZE_WTH_TABLE | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 62 | PressElementChange | PressElementChange | PRESS_ELEMENT_CHANGE | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 63 | TempElementChange | TempElementChange | TEMP_ELEMENT_CHANGE | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 64 | MaterialGroupID | MaterialGroupID | MATERIAL_GROUP_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 65 | SpecialReqID | SpecialReqID | SPECIAL_REQ_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 66 | SpecialReq | SpecialReq | SPECIAL_REQ | VARCHAR2(500) | VARCHAR2(500) | ✅ MATCH |
| 67 | NewVDSSection | NewVDSSection | NEW_VDS_SECTION | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 68 | TubePCS | TubePCS | TUBE_PCS | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 69 | EDSMJMatrix | EDSMJMatrix | EDS_MJ_MATRIX | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 70 | MJReductionFactor | MJReductionFactor | MJ_REDUCTION_FACTOR | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 71 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 72 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

---

### PCS_LIST Complete Mapping (16 STG → 19 CORE)
| # | JSON Field | STG_PCS_LIST | PCS_LIST | STG Type | Core Type | Match |
|---|------------|--------------|----------|----------|-----------|-------|
| 1 | - | - | PCS_LIST_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | **Endpoint** | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | PCS | PCS | PCS_NAME | VARCHAR2(100) | VARCHAR2(100) | 🔄 MAPPED |
| 4 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 5 | Status | Status | STATUS | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 6 | RevDate | RevDate | REV_DATE | VARCHAR2(50) | DATE | ✅ MATCH |
| 7 | RatingClass | RatingClass | RATING_CLASS | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 8 | TestPressure | TestPressure | TEST_PRESSURE | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 9 | MaterialGroup | MaterialGroup | MATERIAL_GROUP | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 10 | DesignCode | DesignCode | DESIGN_CODE | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 11 | LastUpdate | LastUpdate | LAST_UPDATE | VARCHAR2(50) | DATE | ✅ MATCH |
| 12 | LastUpdateBy | LastUpdateBy | LAST_UPDATE_BY | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 13 | Approver | Approver | APPROVER | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 14 | Notepad | Notepad | NOTEPAD | VARCHAR2(4000) | CLOB | ✅ MATCH |
| 15 | SpecialReqID | SpecialReqID | SPECIAL_REQ_ID | VARCHAR2(50) | NUMBER(10) | ✅ MATCH |
| 16 | TubePCS | TubePCS | TUBE_PCS | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 17 | NewVDSSection | NewVDSSection | NEW_VDS_SECTION | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 18 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 19 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

---

### PCS_TEMP_PRESSURES Complete Mapping (5 STG → 8 CORE)
| # | JSON Field | STG_PCS_TEMP_PRESSURES | PCS_TEMP_PRESSURES | STG Type | Core Type | Match |
|---|------------|------------------------|---------------------|----------|-----------|-------|
| 1 | - | - | PCS_TEMP_PRESSURES_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | **Endpoint** | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | **Endpoint** | PCS_NAME | PCS_NAME | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 4 | **Endpoint** | PCS_REVISION | PCS_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 5 | Temperature | Temperature | TEMPERATURE | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 6 | Pressure | Pressure | PRESSURE | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 7 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 8 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

### PCS_PIPE_SIZES Complete Mapping (14 STG → 17 CORE)
| # | JSON Field | STG_PCS_PIPE_SIZES | PCS_PIPE_SIZES | STG Type | Core Type | Match |
|---|------------|-------------------|-----------------|----------|-----------|-------|
| 1 | - | - | PCS_PIPE_SIZES_GUID | - | RAW(16) | ➕ CORE_ONLY |
| 2 | **Endpoint** | PLANT_ID | PLANT_ID | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 3 | **Endpoint** | PCS_NAME | PCS_NAME | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 4 | **Endpoint** | PCS_REVISION | PCS_REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 5 | PCS | PCS | PCS | VARCHAR2(100) | VARCHAR2(100) | ✅ MATCH |
| 6 | Revision | Revision | REVISION | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 7 | NomSize | NomSize | NOM_SIZE | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 8 | OuterDiam | OuterDiam | OUTER_DIAM | VARCHAR2(50) | NUMBER(10,2) | ✅ MATCH |
| 9 | WallThickness | WallThickness | WALL_THICKNESS | VARCHAR2(50) | NUMBER(10,3) | ✅ MATCH |
| 10 | Schedule | Schedule | SCHEDULE | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 11 | UnderTolerance | UnderTolerance | UNDER_TOLERANCE | VARCHAR2(50) | NUMBER(10,3) | ✅ MATCH |
| 12 | CorrosionAllowance | CorrosionAllowance | CORROSION_ALLOWANCE | VARCHAR2(50) | NUMBER(10,3) | ✅ MATCH |
| 13 | WeldingFactor | WeldingFactor | WELDING_FACTOR | VARCHAR2(50) | NUMBER(5,3) | ✅ MATCH |
| 14 | DimElementChange | DimElementChange | DIM_ELEMENT_CHANGE | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 15 | ScheduleInMatrix | ScheduleInMatrix | SCHEDULE_IN_MATRIX | VARCHAR2(50) | VARCHAR2(50) | ✅ MATCH |
| 16 | - | - | CREATED_DATE | - | DATE | ➕ CORE_ONLY |
| 17 | - | - | LAST_MODIFIED_DATE | - | DATE | ➕ CORE_ONLY |

## Key Observations

1. **Column Naming Pattern**:
   - **Staging Tables**: CamelCase matching JSON exactly (e.g., "Temperature", "MaterialGroup")
   - **Core Tables**: SNAKE_CASE uppercase (e.g., TEMPERATURE, MATERIAL_GROUP)
   - **This is CORRECT** - The ETL process handles the conversion

2. **Data Type Conversions**:
   - **Staging**: All columns are VARCHAR2 or CLOB (no type conversion)
   - **Core**: Proper data types (NUMBER, DATE, etc.) applied during STG → Core transfer

3. **Additional Columns in Core**:
   - Primary Key GUID (always first column)
   - CREATED_DATE
   - LAST_MODIFIED_DATE

4. **No Foreign Keys**: All tables are independent (simplified architecture)

5. **Important Relationships**:
   - PCS_REFERENCES contains official_revision for each PCS per issue
   - PCS detail tables use PCS_REVISION from endpoint parameters
   - Different issues can point to the same PCS detail revision

---

## API Endpoint to Table Mapping

| API Endpoint | Staging Table | Core Table |
|-------------|---------------|------------|
| /plants/{plantid}/issues/rev/{issuerev}/pcs | STG_PCS_REFERENCES | PCS_REFERENCES |
| /plants/{plantid}/issues/rev/{issuerev}/vds | STG_VDS_REFERENCES | VDS_REFERENCES |
| /plants/{plantid}/issues/rev/{issuerev}/mds | STG_MDS_REFERENCES | MDS_REFERENCES |
| /plants/{plantid}/issues/rev/{issuerev}/eds | STG_EDS_REFERENCES | EDS_REFERENCES |
| /plants/{plantid}/issues/rev/{issuerev}/vsk | STG_VSK_REFERENCES | VSK_REFERENCES |
| /plants/{plantid}/issues/rev/{issuerev}/esk | STG_ESK_REFERENCES | ESK_REFERENCES |
| /plants/{plantid}/issues/rev/{issuerev}/pipe-elements | STG_PIPE_ELEMENT_REFERENCES | PIPE_ELEMENT_REFERENCES |
| /plants/{plantid}/issues/rev/{issuerev}/sc | STG_SC_REFERENCES | SC_REFERENCES |
| /plants/{plantid}/issues/rev/{issuerev}/vsm | STG_VSM_REFERENCES | VSM_REFERENCES |
| /plants/{plantid}/pcs | STG_PCS_LIST | PCS_LIST |
| /plants/{plantid}/pcs/{pcsname}/rev/{revision} | STG_PCS_HEADER_PROPERTIES | PCS_HEADER_PROPERTIES |
| /plants/{plantid}/pcs/{pcsname}/rev/{revision}/temp-pressures | STG_PCS_TEMP_PRESSURES | PCS_TEMP_PRESSURES |
| /plants/{plantid}/pcs/{pcsname}/rev/{revision}/pipe-sizes | STG_PCS_PIPE_SIZES | PCS_PIPE_SIZES |
| /plants/{plantid}/pcs/{pcsname}/rev/{revision}/pipe-elements | STG_PCS_PIPE_ELEMENTS | PCS_PIPE_ELEMENTS |
| /plants/{plantid}/pcs/{pcsname}/rev/{revision}/valve-elements | STG_PCS_VALVE_ELEMENTS | PCS_VALVE_ELEMENTS |
| /plants/{plantid}/pcs/{pcsname}/rev/{revision}/embedded-notes | STG_PCS_EMBEDDED_NOTES | PCS_EMBEDDED_NOTES |
| /vds | STG_VDS_LIST | VDS_LIST |

---

*This document serves as the single source of truth for the TR2000_STAGING database structure.*