# TR2000 Database ETL System

## 📋 Overview

This repository contains the complete database layer for the TR2000 API Data Manager ETL system. It handles extraction, transformation, and loading of piping specification data from Equinor's API endpoints into a structured Oracle database.

## 🏗️ Architecture

```
API (Equinor) → API_SERVICE (Proxy) → RAW_JSON → STG_* Tables → Core Tables
```

### Security Model
- **TR2000_STAGING**: Main schema (no direct API access)
- **API_SERVICE**: Proxy user with APEX_WEB_SERVICE privileges
- All API calls tracked in `API_CALL_STATS`

## 📁 Repository Structure

```
Database/
├── deploy/                   # Deployment scripts
│   ├── 04_packages/         # Individual package deployments
│   ├── DEPLOY_ALL.sql       # Master deployment script
│   └── README.md            # Deployment instructions
├── backups/                  # Database backups
│   ├── 20250105_pre_refactor/  # Baseline SQL extracts
│   ├── datapump/            # Oracle Data Pump exports
│   └── scripts/             # Backup/restore scripts
├── documentation/            # System documentation
│   ├── ETL_ARCHITECTURE.md
│   └── TR2000_ERD_AND_MAPPINGS.md
├── scripts/                  # Utility scripts
└── Snapshots/               # Point-in-time snapshots
    └── snapshot_20250905_working_etl/  # Last known good state
```

## 🚀 Quick Start

### Prerequisites
- Oracle Database 21c XE or higher
- SQL*Plus client
- Git

### Installation

1. Clone this repository:
```bash
git clone https://github.com/vivekN7/TR2000-Database.git
cd TR2000-Database
```

2. Create the database schema:
```bash
sqlplus sys/password@localhost:1521/XEPDB1 as sysdba
CREATE USER TR2000_STAGING IDENTIFIED BY piping;
GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO TR2000_STAGING;
EXIT;
```

3. Deploy the database objects:
```bash
sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1 @deploy/DEPLOY_ALL.sql
```

## 💡 Key Features

### Safe Data Conversion
All data conversions use `PKG_ETL_VALIDATION` for error-free processing:
- `safe_to_number()` - Handles invalid numbers gracefully
- `safe_to_date()` - Supports multiple date formats
- `validate_json()` - Ensures valid JSON before parsing
- All errors logged to `ETL_ERROR_LOG` for monitoring

### Script-Based Deployment
- No direct database modifications
- Each package in its own deployment script
- Clean drop/create prevents corruption
- Full version control with Git

### Idempotent ETL Process
- Clear and reload strategy
- Safe to run multiple times
- Comprehensive error recovery

## 📚 Documentation

### Core Documentation
- **[OPERATIONS_GUIDE.md](OPERATIONS_GUIDE.md)** - Complete operations manual (commands, deployment, monitoring, troubleshooting)
- **[ETL Architecture](documentation/ETL_ARCHITECTURE.md)** - System design and data flow
- **[Database ERD & Mappings](documentation/TR2000_ERD_AND_MAPPINGS.md)** - Table structures and relationships
- **[API Endpoints](documentation/TR2000_API_Endpoints_Documentation.md)** - Available API endpoints

## 📈 Performance

Current metrics (GRANE plant 34/4.2):
- **API Calls**: 58 total
- **Data Volume**: ~605KB JSON
- **Records Loaded**: 2,012
- **Execution Time**: ~11 seconds
- **Success Rate**: 100%

## 🔒 Security

- API access only through `API_SERVICE` proxy
- No credentials stored in code
- All API calls logged and audited
- Sensitive data never logged

## 🏷️ Version History

- **v2.0.0** (2025-01-05): Script-based deployment, safe conversions
- **v1.0.0** (2025-01-05): Initial working ETL system

---

**Repository**: https://github.com/vivekN7/TR2000-Database  
**Status**: Production Ready  
**Baseline**: `baseline-pre-refactor-20250105`  
**Last Updated**: 2025-01-05