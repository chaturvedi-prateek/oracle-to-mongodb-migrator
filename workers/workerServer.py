# worker/worker_server.py

import logging
import threading
import asyncio
from fastapi import FastAPI, HTTPException
from config.config import MongoUri, MetaDb, WorkerPollInterval, MaxWorkerThreads
from workers.worker import Worker

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()

# Globals with thread safety
worker_lock = threading.Lock()
worker_instance = None
worker_thread = None
worker_running = False

@app.on_event("startup")
def startup_event():
    global worker_instance
    with worker_lock:
        if not worker_instance:
            worker_instance = Worker(worker_id="worker_1")
    logging.info("Worker server started")

@app.post("/start")
def start_worker():
    global worker_thread, worker_running

    with worker_lock:
        if worker_running:
            raise HTTPException(status_code=400, detail="Worker is already running")

    # Perform coordinator check before starting the worker thread
    try:
        # Run wait_for_coordinator in an event loop temporarily
        asyncio.run(worker_instance.wait_for_coordinator(max_retries=5, delay=5))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Coordinator not ready: {e}")

    with worker_lock:
        worker_running = True

        def run_worker():
            global worker_running
            logging.info("Worker thread starting")
            try:
                asyncio.run(worker_instance.run())
            finally:
                with worker_lock:
                    worker_running = False
                logging.info("Worker thread stopped")

        worker_thread = threading.Thread(target=run_worker, daemon=True)
        worker_thread.start()

    return {"message": "Worker started"}

@app.post("/stop")
def stop_worker():
    global worker_running

    with worker_lock:
        if not worker_running:
            raise HTTPException(status_code=400, detail="Worker is not running")

        # Placeholder for actual stop logic (like sending a cancellation or event)
        worker_running = False

    return {"message": "Worker stopping"}

@app.get("/status")
def get_status():
    with worker_lock:
        if not worker_instance:
            raise HTTPException(status_code=500, detail="Worker not initialized")

        if not worker_running:
            return {"status": "IDLE"}

    # Perform async operations outside of lock to avoid blocking
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    pending = loop.run_until_complete(worker_instance.data_chunks.count_documents({"Status": "Pending"}))
    in_progress = loop.run_until_complete(worker_instance.data_chunks.count_documents({"Status": "InProgress"}))
    completed = loop.run_until_complete(worker_instance.data_chunks.count_documents({"Status": "Completed"}))
    loop.close()

    return {
        "status": "RUNNING",
        "pending": pending,
        "in_progress": in_progress,
        "completed": completed
    }

@app.on_event("shutdown")
def shutdown_event():
    global worker_running
    with worker_lock:
        worker_running = False
    logging.info("Worker server shutting down")