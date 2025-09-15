# Oracle to MongoDB Migrator

**Oracle to MongoDB Migrator** is a scalable, distributed data migration tool designed to transfer large volumes of data from Oracle databases to MongoDB in parallel. The tool supports resumability, fault tolerance, and continuous syncing of changes until a final cutover, making it ideal for production environments where downtime must be minimized.

---

## ⚙️ Features

- ✅ **Parallel Data Migration** – Efficiently migrate terabytes of data using multiple worker processes running on separate servers.
- ✅ **Resumability** – Supports resumption of migration from the last processed chunk in case of failures or restarts.
- ✅ **Change Data Capture (CDC)** – Continuously sync changes from Oracle to MongoDB until the final cutover.
- ✅ **Metadata Tracking** – Centralized MongoDB-based store to track chunk status, retries, and progress.
- ✅ **Coordinator Process** – Assigns data chunks to workers, handles retries, and orchestrates the migration workflow.
- ✅ **Scalable Design** – Designed to scale horizontally and support high availability.

---

## 📦 Architecture Overview

The system includes the following components:

1. **Coordinator Process**  
   Centralized service that manages metadata and assigns migration tasks to workers.

2. **Worker Processes**  
   Extract data from Oracle, transform it, and insert into MongoDB in parallel.

3. **Metadata Store**  
   MongoDB is used as the centralized store for tracking data chunks, processing state, retries, and change logs. Atomic updates and transactions ensure consistency and resumability.

4. **Change Capture Layer**  
   Captures changes in Oracle using triggers or log mining and applies them to MongoDB continuously until the final cutover.

---

## 🛠️ Technology Stack

| Component       | Technology |
|-----------------|-----------|
| Source Database  | Oracle DB |
| Target Database  | MongoDB   |
| Metadata Store   | MongoDB   |
| Programming      | Python (with `pymongo`) |
| Containerization | Docker    |
| Communication   | REST API |

---

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- Docker (optional but recommended for deployment)
- Access credentials for Oracle and MongoDB
- MongoDB replica set configured for transactions and high availability

---

### Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/chaturvedi-prateek/oracle-to-mongodb-migrator.git
   cd oracle-to-mongodb-migrator

2. **Set up Python environment**

  ```bash
  python -m venv venv
  source venv/bin/activate
  pip install -r requirements.txt

3. **Configure MongoDB**

   - Set up a replica set or standalone instance with write concern enabled.
   - Create databases and collections for metadata and change logs.

4. **Configure the application**

   - Update `config.py` with MongoDB connection details and Oracle credentials.

---

### Running the Migration

1. **Start the Coordinator**

   ```bash
   python coordinator.py

2. **Start one or more Worker processes**

  ```bash
   python worker.py

3. **Monitor progress**

  - Use logs and MongoDB queries to track chunk processing, retries, and change log status.

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.


  
