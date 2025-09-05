# TR2000 Deployment Scripts

This directory contains all database deployment scripts for the TR2000 ETL system.

## 📚 Full Documentation

For complete deployment instructions, monitoring, and troubleshooting guides, see:
**[../OPERATIONS_GUIDE.md](../OPERATIONS_GUIDE.md)**

## 📁 Directory Structure

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

## 🚀 Quick Commands

### Deploy Everything
```bash
sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1 @DEPLOY_ALL.sql
```

### Deploy Single Package
```bash
sqlplus TR2000_STAGING/piping@localhost:1521/XEPDB1 @04_packages/PKG_NAME.sql
```

---

**See [OPERATIONS_GUIDE.md](../OPERATIONS_GUIDE.md) for detailed deployment workflow, rollback procedures, and monitoring.**