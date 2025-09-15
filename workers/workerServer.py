# worker/worker_server.py

import logging
import threading
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
            worker_instance = Worker(worker_id="worker_1")  # Can be made dynamic if needed
    logging.info("Worker server started")


@app.post("/start")
def start_worker():
    global worker_thread, worker_running

    with worker_lock:
        if worker_running:
            raise HTTPException(status_code=400, detail="Worker is already running")

        def run_worker():
            global worker_running
            logging.info("Worker thread starting")
            with worker_lock:
                worker_running = True
            try:
                worker_instance.run()
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

        # This is a placeholder; ideally, you signal the worker to stop gracefully
        worker_running = False

    return {"message": "Worker stopping"}


@app.get("/status")
def get_status():
    with worker_lock:
        if not worker_instance:
            raise HTTPException(status_code=500, detail="Worker not initialized")

        if not worker_running:
            return {"status": "IDLE"}

        pending = worker_instance.data_chunks.count_documents({"Status": "Pending"})
        in_progress = worker_instance.data_chunks.count_documents({"Status": "InProgress"})
        completed = worker_instance.data_chunks.count_documents({"Status": "Completed"})

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