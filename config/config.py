# config/config.py

# ----------------------------
# MongoDB Connection Settings
# ----------------------------
MongoUri = "mongodb://localhost:27017"   # MongoDB URI
MetaDb = "metadata"                       # Central metadata database

# ----------------------------
# Oracle Databases Configuration
# ----------------------------
# Example structure for multiple source Oracle DBs
DatabaseMappings = [
    {
        "SourceDb": "ORACLE_DB1",
        "TargetDb": "MONGO_DB1",
        "Dsn": "oracle_host1:1521/ORCL",  # host:port/service_name
        "User": "oracle_user1",
        "Password": "oracle_password1",
        "PrimaryKey": "ID"                 # Primary key column used for chunking
    },
    {
        "SourceDb": "ORACLE_DB2",
        "TargetDb": "MONGO_DB2",
        "Dsn": "oracle_host2:1521/ORCL",
        "User": "oracle_user2",
        "Password": "oracle_password2",
        "PrimaryKey": "EMP_ID"
    }
]

# ----------------------------
# Migration Settings
# ----------------------------
DefaultBatchSize = 10000      # Number of rows per chunk
MaxRetries = 3                # Max retries per chunk or migration state update
WorkerPollInterval = 5        # Seconds between checking for pending chunks
CdcPollInterval = 10          # Seconds between incremental CDC polling

# ----------------------------
# Worker Settings
# ----------------------------
MaxWorkerThreads = 5          # Number of threads per worker for concurrent chunk processing

# ----------------------------
# Optional: Logging Level Override
# ----------------------------
# LoggingLevel = logging.INFO
