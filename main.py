from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from typing import Optional, List
import sqlite3, uuid, json, asyncio, secrets, hashlib
from datetime import datetime, date, timedelta
import os

app = FastAPI(title="Operador Status API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBasic()
DB_PATH = os.environ.get("DB_PATH", "operadores.db")
ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("ADMIN_PASS", "admin1234")

# ─── DB SETUP ─────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS operators (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            token TEXT UNIQUE NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operator_id TEXT NOT NULL,
            date TEXT NOT NULL,
            available_minutes INTEGER DEFAULT 240,
            started_at TEXT,
            FOREIGN KEY(operator_id) REFERENCES operators(id)
        );

        CREATE TABLE IF NOT EXISTS status_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operator_id TEXT NOT NULL,
            session_id INTEGER,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            duration_seconds INTEGER,
            FOREIGN KEY(operator_id) REFERENCES operators(id),
            FOREIGN KEY(session_id) REFERENCES sessions(id)
        );
    """)
    conn.commit()
    conn.close()

init_db()

# ─── WEBSOCKET MANAGER ────────────────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.dashboard_connections: List[WebSocket] = []

    async def connect_dashboard(self, ws: WebSocket):
        await ws.accept()
        self.dashboard_connections.append(ws)

    def disconnect_dashboard(self, ws: WebSocket):
        if ws in self.dashboard_connections:
            self.dashboard_connections.remove(ws)

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.dashboard_connections:
            try:
                await ws.send_json(data)
            except:
                dead.append(ws)
        for ws in dead:
            self.disconnect_dashboard(ws)

manager = ConnectionManager()

# ─── AUTH ─────────────────────────────────────────────────────────────────────

def verify_admin(credentials: HTTPBasicCredentials = Depends(security)):
    ok_user = secrets.compare_digest(credentials.username, ADMIN_USER)
    ok_pass = secrets.compare_digest(credentials.password, ADMIN_PASS)
    if not (ok_user and ok_pass):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Invalid credentials",
                            headers={"WWW-Authenticate": "Basic"})
    return credentials.username

def get_operator_by_token(token: str):
    conn = get_db()
    op = conn.execute("SELECT * FROM operators WHERE token=?", (token,)).fetchone()
    conn.close()
    if not op:
        raise HTTPException(status_code=404, detail="Operator not found")
    return dict(op)

# ─── MODELS ───────────────────────────────────────────────────────────────────

class CreateOperator(BaseModel):
    name: str

class StartSession(BaseModel):
    available_minutes: int = 240

class UpdateStatus(BaseModel):
    status: str  # available | busy | offline

# ─── ADMIN ENDPOINTS ──────────────────────────────────────────────────────────

@app.get("/admin/operators", dependencies=[Depends(verify_admin)])
def list_operators():
    conn = get_db()
    ops = conn.execute("SELECT * FROM operators ORDER BY name").fetchall()
    conn.close()
    return [dict(o) for o in ops]

@app.post("/admin/operators", dependencies=[Depends(verify_admin)])
def create_operator(data: CreateOperator):
    conn = get_db()
    op_id = str(uuid.uuid4())
    token = str(uuid.uuid4()).replace("-", "")
    conn.execute("INSERT INTO operators (id, name, token) VALUES (?,?,?)",
                 (op_id, data.name, token))
    conn.commit()
    conn.close()
    return {"id": op_id, "name": data.name, "token": token}

@app.delete("/admin/operators/{op_id}", dependencies=[Depends(verify_admin)])
def delete_operator(op_id: str):
    conn = get_db()
    conn.execute("DELETE FROM operators WHERE id=?", (op_id,))
    conn.commit()
    conn.close()
    return {"ok": True}

# ─── OPERATOR ENDPOINTS ───────────────────────────────────────────────────────

@app.get("/op/{token}/info")
def operator_info(token: str):
    op = get_operator_by_token(token)
    conn = get_db()
    today = date.today().isoformat()
    session = conn.execute(
        "SELECT * FROM sessions WHERE operator_id=? AND date=? ORDER BY id DESC LIMIT 1",
        (op["id"], today)
    ).fetchone()

    current_status = None
    elapsed_available = 0

    if session:
        session = dict(session)
        last_log = conn.execute(
            "SELECT * FROM status_log WHERE session_id=? ORDER BY id DESC LIMIT 1",
            (session["id"],)
        ).fetchone()
        if last_log:
            last_log = dict(last_log)
            current_status = last_log["status"]
            if last_log["ended_at"] is None:
                started = datetime.fromisoformat(last_log["started_at"])
                elapsed_available += int((datetime.utcnow() - started).total_seconds())

        # Sum all available time already consumed
        rows = conn.execute(
            "SELECT SUM(duration_seconds) as total FROM status_log WHERE session_id=? AND status='available' AND ended_at IS NOT NULL",
            (session["id"],)
        ).fetchone()
        if rows["total"]:
            elapsed_available += rows["total"] if current_status == "available" else 0
            if current_status != "available":
                elapsed_available = rows["total"]

    conn.close()
    return {
        "operator": op,
        "session": session,
        "current_status": current_status,
        "elapsed_available_seconds": elapsed_available,
    }

@app.post("/op/{token}/session/start")
async def start_session(token: str, data: StartSession):
    op = get_operator_by_token(token)
    conn = get_db()
    today = date.today().isoformat()
    existing = conn.execute(
        "SELECT * FROM sessions WHERE operator_id=? AND date=?",
        (op["id"], today)
    ).fetchone()
    if existing:
        conn.close()
        return {"session_id": existing["id"], "already_exists": True}

    now = datetime.utcnow().isoformat()
    cur = conn.execute(
        "INSERT INTO sessions (operator_id, date, available_minutes, started_at) VALUES (?,?,?,?)",
        (op["id"], today, data.available_minutes, now)
    )
    session_id = cur.lastrowid
    # Start as available
    conn.execute(
        "INSERT INTO status_log (operator_id, session_id, status, started_at) VALUES (?,?,?,?)",
        (op["id"], session_id, "available", now)
    )
    conn.commit()
    conn.close()
    await manager.broadcast({"event": "status_change", "operator_id": op["id"],
                             "name": op["name"], "status": "available",
                             "available_minutes": data.available_minutes})
    return {"session_id": session_id}

@app.post("/op/{token}/status")
async def update_status(token: str, data: UpdateStatus):
    if data.status not in ("available", "busy", "offline"):
        raise HTTPException(status_code=400, detail="Invalid status")
    op = get_operator_by_token(token)
    conn = get_db()
    today = date.today().isoformat()
    session = conn.execute(
        "SELECT * FROM sessions WHERE operator_id=? AND date=? ORDER BY id DESC LIMIT 1",
        (op["id"], today)
    ).fetchone()
    if not session:
        conn.close()
        raise HTTPException(status_code=400, detail="No active session for today")

    now = datetime.utcnow().isoformat()
    session = dict(session)

    # Close last open log
    last = conn.execute(
        "SELECT * FROM status_log WHERE session_id=? AND ended_at IS NULL ORDER BY id DESC LIMIT 1",
        (session["id"],)
    ).fetchone()
    if last:
        last = dict(last)
        started = datetime.fromisoformat(last["started_at"])
        duration = int((datetime.utcnow() - started).total_seconds())
        conn.execute(
            "UPDATE status_log SET ended_at=?, duration_seconds=? WHERE id=?",
            (now, duration, last["id"])
        )

    # Insert new
    conn.execute(
        "INSERT INTO status_log (operator_id, session_id, status, started_at) VALUES (?,?,?,?)",
        (op["id"], session["id"], data.status, now)
    )
    conn.commit()

    # Get available seconds consumed
    rows = conn.execute(
        "SELECT SUM(duration_seconds) as total FROM status_log WHERE session_id=? AND status='available' AND ended_at IS NOT NULL",
        (session["id"],)
    ).fetchone()
    elapsed = rows["total"] or 0
    if data.status == "available":
        elapsed_available = elapsed
    else:
        elapsed_available = elapsed

    conn.close()
    await manager.broadcast({
        "event": "status_change",
        "operator_id": op["id"],
        "name": op["name"],
        "status": data.status,
        "available_minutes": session["available_minutes"],
        "elapsed_available_seconds": elapsed_available,
    })
    return {"ok": True}

@app.post("/op/{token}/available-time")
async def update_available_time(token: str, minutes: int):
    op = get_operator_by_token(token)
    conn = get_db()
    today = date.today().isoformat()
    conn.execute(
        "UPDATE sessions SET available_minutes=? WHERE operator_id=? AND date=?",
        (minutes, op["id"], today)
    )
    conn.commit()
    conn.close()
    await manager.broadcast({"event": "time_update", "operator_id": op["id"],
                             "name": op["name"], "available_minutes": minutes})
    return {"ok": True}

# ─── DASHBOARD ENDPOINTS ──────────────────────────────────────────────────────

@app.get("/dashboard/current")
def dashboard_current():
    conn = get_db()
    today = date.today().isoformat()
    operators = conn.execute("SELECT * FROM operators").fetchall()
    result = []
    for op in operators:
        op = dict(op)
        session = conn.execute(
            "SELECT * FROM sessions WHERE operator_id=? AND date=? ORDER BY id DESC LIMIT 1",
            (op["id"], today)
        ).fetchone()
        if not session:
            result.append({**op, "status": "offline", "available_minutes": 0,
                           "elapsed_available_seconds": 0, "session": None})
            continue
        session = dict(session)
        last_log = conn.execute(
            "SELECT * FROM status_log WHERE session_id=? ORDER BY id DESC LIMIT 1",
            (session["id"],)
        ).fetchone()
        current_status = dict(last_log)["status"] if last_log else "offline"

        rows = conn.execute(
            "SELECT SUM(duration_seconds) as total FROM status_log WHERE session_id=? AND status='available' AND ended_at IS NOT NULL",
            (session["id"],)
        ).fetchone()
        elapsed = rows["total"] or 0
        if current_status == "available" and last_log:
            started = datetime.fromisoformat(dict(last_log)["started_at"])
            elapsed += int((datetime.utcnow() - started).total_seconds())

        result.append({
            **op,
            "status": current_status,
            "available_minutes": session["available_minutes"],
            "elapsed_available_seconds": elapsed,
            "session": session,
        })
    conn.close()
    return result

@app.get("/dashboard/history")
def dashboard_history(days: int = 30):
    conn = get_db()
    since = (date.today() - timedelta(days=days)).isoformat()
    ops = conn.execute("SELECT * FROM operators").fetchall()
    result = []
    for op in ops:
        op = dict(op)
        sessions = conn.execute(
            "SELECT s.*, COUNT(sl.id) as state_changes FROM sessions s "
            "LEFT JOIN status_log sl ON sl.session_id = s.id "
            "WHERE s.operator_id=? AND s.date >= ? "
            "GROUP BY s.id ORDER BY s.date DESC",
            (op["id"], since)
        ).fetchall()
        daily = []
        for s in sessions:
            s = dict(s)
            stats = conn.execute(
                "SELECT status, SUM(duration_seconds) as total FROM status_log "
                "WHERE session_id=? AND ended_at IS NOT NULL GROUP BY status",
                (s["id"],)
            ).fetchall()
            s["stats"] = {r["status"]: r["total"] or 0 for r in stats}
            daily.append(s)
        result.append({**op, "sessions": daily})
    conn.close()
    return result

# ─── WEBSOCKET ────────────────────────────────────────────────────────────────

@app.websocket("/ws/dashboard")
async def ws_dashboard(ws: WebSocket):
    await manager.connect_dashboard(ws)
    try:
        while True:
            await asyncio.sleep(30)
            await ws.send_json({"event": "ping"})
    except WebSocketDisconnect:
        manager.disconnect_dashboard(ws)

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}
