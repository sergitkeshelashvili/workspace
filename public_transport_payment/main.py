from fastapi import FastAPI, HTTPException, BackgroundTasks
from datetime import datetime, timedelta, UTC
import uuid
import json
from pathlib import Path

app = FastAPI(title="Primary Ticket API")

# ---------------------------------------
# In-memory ticket storage
# ---------------------------------------
tickets = {}

# ---------------------------------------
# WAL (Write-Ahead Log)
# ---------------------------------------
WAL_FILE = Path("wal.log")


def append_wal(entry: dict):
    """Append a JSON line to WAL before modifying memory."""
    with WAL_FILE.open("a") as f:
        f.write(json.dumps(entry) + "\n")


def replay_wal():
    """Rebuild tickets[] from WAL on startup."""
    global tickets

    if not WAL_FILE.exists():
        return

    with WAL_FILE.open("r") as f:
        for line in f:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            op = record.get("op")
            ticket_id = record.get("ticket_id")

            if op == "CREATE":
                tickets[ticket_id] = {
                    "passenger_name": record["passenger_name"],
                    "created_at": datetime.fromisoformat(record["created_at"]),
                    "expires_at": datetime.fromisoformat(record["expires_at"])
                }

            elif op == "DELETE" and ticket_id in tickets:
                del tickets[ticket_id]


# ---------------------------------------
# Startup: restore state from WAL
# ---------------------------------------
@app.on_event("startup")
def load_from_wal():
    replay_wal()


# ---------------------------------------
# PAY: create a ticket
# ---------------------------------------
@app.post("/pay")
def pay(passenger_name: str):
    ticket_id = str(uuid.uuid4())
    created_time = datetime.now(UTC)
    expiry_time = created_time + timedelta(hours=1)

    # WAL FIRST (atomic)
    append_wal({
        "op": "CREATE",
        "ticket_id": ticket_id,
        "passenger_name": passenger_name,
        "created_at": created_time.isoformat(),
        "expires_at": expiry_time.isoformat()
    })

    # THEN in-memory
    tickets[ticket_id] = {
        "passenger_name": passenger_name,
        "created_at": created_time,
        "expires_at": expiry_time
    }

    return {
        "ticket_id": ticket_id,
        "passenger_name": passenger_name,
        "expires_at": expiry_time
    }


# ---------------------------------------
# Remove expired tickets (background)
# ---------------------------------------
def remove_expired_tickets():
    now = datetime.now(UTC)
    expired = [tid for tid, t in tickets.items() if t["expires_at"] < now]

    for tid in expired:
        append_wal({"op": "DELETE", "ticket_id": tid})
        del tickets[tid]


# ---------------------------------------
# VALIDATE ticket + logging
# ---------------------------------------
@app.get("/validate/{ticket_id}")
def validate(ticket_id: str, background_tasks: BackgroundTasks):
    ticket = tickets.get(ticket_id)

    if not ticket:
        background_tasks.add_task(remove_expired_tickets)
        raise HTTPException(status_code=404, detail="Ticket not found")

    if datetime.now(UTC) > ticket["expires_at"]:
        background_tasks.add_task(remove_expired_tickets)
        raise HTTPException(status_code=400, detail="Ticket expired")

    # Log validation
    with open("transaction.log", "a") as f:
        f.write(f"{ticket['passenger_name']} | ticket {ticket_id} validated at {datetime.now(UTC)}\n")

    background_tasks.add_task(remove_expired_tickets)

    return {
        "status": "valid",
        "ticket_id": ticket_id,
        "passenger_name": ticket["passenger_name"],
        "expires_at": ticket["expires_at"]
    }


# ---------------------------------------
# View transaction log
# ---------------------------------------
@app.get("/transactions_log")
def get_transaction_log():
    try:
        with open("transaction.log", "r") as f:
            return {"transactions": f.read()}
    except FileNotFoundError:
        return {"transactions": ""}


# ---------------------------------------
# Run server
# ---------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)


