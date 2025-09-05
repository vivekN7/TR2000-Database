-- =====================================================
-- Oracle Data Pump Backup Setup Script
-- Date: 2025-01-05
-- Purpose: Create complete binary backup of TR2000_STAGING
-- =====================================================

-- Connect as SYSDBA to create directory
-- Run this part as: sqlplus sys/justkeepswimming@localhost:1521/XEPDB1 as sysdba

-- Create backup directory (adjust path as needed for your system)
CREATE OR REPLACE DIRECTORY backup_dir AS 'C:\Repos\Docker\DockerContainer\TR2000\TR2K\Database\backups\datapump';

-- Grant permissions to TR2000_STAGING
GRANT READ, WRITE ON DIRECTORY backup_dir TO TR2000_STAGING;

-- Verify directory was created
SELECT directory_name, directory_path 
FROM dba_directories 
WHERE directory_name = 'BACKUP_DIR';

-- Now exit and run the export as TR2000_STAGING user:
-- expdp TR2000_STAGING/piping@localhost:1521/XEPDB1 DIRECTORY=backup_dir DUMPFILE=TR2000_FULL_20250105.dmp LOGFILE=TR2000_FULL_20250105.log SCHEMAS=TR2000_STAGING

-- To import later if needed:
-- impdp TR2000_STAGING/piping@localhost:1521/XEPDB1 DIRECTORY=backup_dir DUMPFILE=TR2000_FULL_20250105.dmp LOGFILE=import_20250105.log SCHEMAS=TR2000_STAGING