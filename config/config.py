# config/config.py

import logging
from logging.handlers import RotatingFileHandler

# ---------------------------
# MongoDB Configuration
# ---------------------------
MongoUri = "mongodb://localhost:27017"

# Metadata database (central store for tracking migration progress)
MetaDb = "MigrationMetadata"

# List of target databases corresponding to each Oracle DB
DatabaseMappings = [
    {
        "SourceDb": "OracleDB1",
        "TargetDb": "MongoTargetDB1"
    },
    {
        "SourceDb": "OracleDB2",
        "TargetDb": "MongoTargetDB2"
    }
]

# ---------------------------
# Oracle Databases Configuration
# ---------------------------
OracleDatabases = [
    {
        "Name": "OracleDB1",
        "User": "user1",
        "Password": "password1",
        "Dsn": "host1:1521/DB1"
    },
    {
        "Name": "OracleDB2",
        "User": "user2",
        "Password": "password2",
        "Dsn": "host2:1521/DB2"
    }
]

# ---------------------------
# Migration Settings
# ---------------------------
DefaultBatchSize = 1000       # Rows per batch
MaxRetries = 3                # Max retries for failed chunks
WorkerPollInterval = 5        # Seconds between worker polls for tasks
ChangeCaptureInterval = 10    # Seconds between polling Oracle for CDC

# ---------------------------
# Logging Configuration
# ---------------------------
LogFilePath = "migration.log"
LogLevel = logging.INFO

Logger = logging.getLogger("MigrationLogger")
Logger.setLevel(LogLevel)

FileHandler = RotatingFileHandler(LogFilePath, maxBytes=5*1024*1024, backupCount=5)
Formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
FileHandler.setFormatter(Formatter)
Logger.addHandler(FileHandler)

ConsoleHandler = logging.StreamHandler()
ConsoleHandler.setFormatter(Formatter)
Logger.addHandler(ConsoleHandler)
