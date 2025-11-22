# replica_app.py
from fastapi import FastAPI
from pathlib import Path
from datetime import datetime
import json
import asyncio
import threading
import os
from typing import Dict, Any

app = FastAPI(title="Replica Ticket API (Auto-sync)")

# -----------------------
# Config
# -----------------------
WAL_FILE = Path("wal.log")                  # read-only WAL produced by primary
REPLICA_FILE = Path("replica_tickets.json") # persisted replica snapshot
SYNC_INTERVAL_SECONDS = int(os.getenv("REPLICA_SYNC_INTERVAL", "2"))  # poll interval

# -----------------------
# In-memory replica
# -----------------------
replica_tickets: Dict[str, Dict[str, Any]] = {}
# Protect replay and writes with a lock to avoid concurrent replays
_replay_lock = threading.Lock()

# Track last modified time of WAL to avoid unnecessary replays
_last_wal_mtime = 0.0

# -----------------------
# WAL replay function
# -----------------------
def replay_wal_to_replica() -> None:
    """
    Rebuild replica_tickets from WAL and atomically persist to REPLICA_FILE.
    """
    global replica_tickets, _last_wal_mtime

    # Acquire lock to prevent concurrent replays
    acquired = _replay_lock.acquire(timeout=5)
    if not acquired:
        # If lock cannot be acquired, skip this run (avoids blocking)
        return

    try:
        # If WAL doesn't exist, clear replica (but keep previous snapshot if desired)
        if not WAL_FILE.exists():
            replica_tickets = {}
            # Persist empty snapshot atomically
            tmp = REPLICA_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(replica_tickets, indent=2))
            tmp.replace(REPLICA_FILE)
            _last_wal_mtime = 0.0
            return

        # Read WAL and apply operations in order
        new_replica: Dict[str, Dict[str, Any]] = {}
        with WAL_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    # skip invalid WAL line
                    continue

                op = record.get("op")
                ticket_id = record.get("ticket_id")

                if op == "CREATE" and ticket_id:
                    # store as-is (ISO string fields)
                    new_replica[ticket_id] = {
                        "passenger_name": record.get("passenger_name"),
                        "created_at": record.get("created_at"),
                        "expires_at": record.get("expires_at")
                    }
                elif op == "DELETE" and ticket_id:
                    new_replica.pop(ticket_id, None)

        # Replace in-memory and persist atomically
        replica_tickets = new_replica
        tmp = REPLICA_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(replica_tickets, indent=2))
        tmp.replace(REPLICA_FILE)

        # update last mtime
        try:
            _last_wal_mtime = WAL_FILE.stat().st_mtime
        except OSError:
            _last_wal_mtime = 0.0

    finally:
        _replay_lock.release()


# -----------------------
# Background auto-sync task
# -----------------------
async def _auto_sync_loop():
    """
    Background coroutine that polls WAL mtime and triggers replay when WAL changes.
    """
    global _last_wal_mtime

    # On startup, perform an initial sync
    try:
        replay_wal_to_replica()
    except Exception as e:
        # don't crash the background loop; log to console
        print("Initial replica sync failed:", e)

    while True:
        try:
            if WAL_FILE.exists():
                try:
                    mtime = WAL_FILE.stat().st_mtime
                except OSError:
                    mtime = 0.0
                if mtime != _last_wal_mtime:
                    # WAL changed — replay
                    replay_wal_to_replica()
            else:
                # WAL not present: if previously had a mtime, clear replica
                if _last_wal_mtime != 0.0:
                    replay_wal_to_replica()
            # sleep
            await asyncio.sleep(SYNC_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            # task cancelled on shutdown — exit gracefully
            break
        except Exception as e:
            # keep loop alive on unexpected errors
            print("Replica auto-sync loop error:", e)
            await asyncio.sleep(SYNC_INTERVAL_SECONDS)


# -----------------------
# Startup / Shutdown
# -----------------------
@app.on_event("startup")
def startup_event():
    # create and schedule background task in event loop
    loop = asyncio.get_event_loop()
    # store the task on the loop to allow cancellation on shutdown
    task = loop.create_task(_auto_sync_loop())
    # attach reference so FastAPI shutdown can cancel if needed
    setattr(loop, "_replica_auto_sync_task", task)


@app.on_event("shutdown")
def shutdown_event():
    loop = asyncio.get_event_loop()
    task = getattr(loop, "_replica_auto_sync_task", None)
    if task:
        task.cancel()


# -----------------------
# Read-only Endpoints
# -----------------------
@app.get("/replica/tickets")
def get_all_replica_tickets():
    return replica_tickets


@app.get("/replica/tickets/{ticket_id}")
def get_single_replica_ticket(ticket_id: str):
    ticket = replica_tickets.get(ticket_id)
    if not ticket:
        return {"status": "not_found"}
    return ticket


@app.post("/replica/sync")
def manual_sync():
    """
    Manual admin endpoint to force replay now.
    """
    replay_wal_to_replica()
    return {"status": "synced", "count": len(replica_tickets)}

# -----------------------
# Run server
# -----------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

