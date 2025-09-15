# coordinator/coordinatorServer.py

# coordinator/coordinator_server.py

import asyncio
import logging
from fastapi import FastAPI, HTTPException
from coordinator.coordinator import Coordinator

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()

# Globals
coordinator_instance: Coordinator | None = None
coordinator_running = False
coordinator_task: asyncio.Task | None = None

@app.on_event("startup")
async def startup_event():
    global coordinator_instance
    coordinator_instance = Coordinator()
    logging.info("Coordinator server started")

@app.post("/start")
async def start_coordinator():
    """Start the migration coordinator loop."""
    global coordinator_running, coordinator_task

    if coordinator_running:
        raise HTTPException(status_code=400, detail="Coordinator is already running")

    async def run_coordinator():
        global coordinator_running
        coordinator_running = True
        try:
            await coordinator_instance.run()
        except asyncio.CancelledError:
            logging.info("Coordinator task cancelled")
        finally:
            coordinator_running = False

    # Schedule the coordinator in the current event loop
    coordinator_task = asyncio.create_task(run_coordinator())
    return {"message": "Coordinator started"}

@app.post("/stop")
async def stop_coordinator():
    """Stop the migration coordinator loop."""
    global coordinator_running, coordinator_task

    if not coordinator_running or coordinator_task is None:
        raise HTTPException(status_code=400, detail="Coordinator is not running")

    coordinator_task.cancel()
    coordinator_running = False
    return {"message": "Coordinator stopping"}

@app.get("/status/{migration_id}")
async def get_status(migration_id: str):
    """
    Get the migration progress for a given migration ID.
    Returns IDLE if the coordinator hasn't started yet.
    """
    if not coordinator_instance:
        return {"state": "IDLE", "progress": None}

    try:
        progress = await coordinator_instance.get_migration_progress(migration_id)
        state = "RUNNING" if coordinator_running else "IDLE"
        return {"state": state, "progress": progress}
    except Exception as e:
        logging.error(f"Error fetching migration progress: {e}")
        return {"state": "RUNNING" if coordinator_running else "IDLE", "progress": None}

@app.get("/list_migrations")
async def list_migrations():
    """
    List all migration IDs with their current status.
    Returns empty list if coordinator hasn't started yet.
    """
    if not coordinator_instance:
        return {"state": "IDLE", "migrations": []}

    try:
        migrations = await coordinator_instance.migration_status.find().to_list(length=100)
        result = [
            {"migration_id": m["_id"], "status": m.get("Status", "Unknown")}
            for m in migrations
        ]
        return {"state": "RUNNING" if coordinator_running else "IDLE", "migrations": result}
    except Exception as e:
        logging.error(f"Error listing migrations: {e}")
        return {"state": "RUNNING" if coordinator_running else "IDLE", "migrations": []}

@app.on_event("shutdown")
async def shutdown_event():
    global coordinator_running, coordinator_task
    if coordinator_task:
        coordinator_task.cancel()
    coordinator_running = False
    logging.info("Coordinator server shutting down")