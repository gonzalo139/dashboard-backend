from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from typing import List
import uuid, asyncio, secrets
from datetime import datetime, date, timedelta, timezone
import os
import psycopg2
import psycopg2.extras

app = FastAPI(title="Operador Status API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBasic()
DATABASE_URL = os.environ.get("DATABASE_URL", "")
ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("ADMIN_PASS", "admin1234")

# ─── DB ───────────────────────────────────────────────────────────────────────

def get_db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    return conn

def utcnow():
    return datetime.now(timezone.utc)

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
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM operators WHERE token=%s", (token,))
            op = cur.fetchone()
    if not op:
        raise HTTPException(status_code=404, detail="Operator not found")
    return dict(op)

# ─── MODELS ───────────────────────────────────────────────────────────────────

class CreateOperator(BaseModel):
    name: str

class StartSession(BaseModel):
    available_minutes: int = 240

class UpdateStatus(BaseModel):
    status: str

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

# ─── ADMIN ────────────────────────────────────────────────────────────────────

@app.get("/admin/operators", dependencies=[Depends(verify_admin)])
def list_operators():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM operators ORDER BY name")
            return [dict(r) for r in cur.fetchall()]

@app.post("/admin/operators", dependencies=[Depends(verify_admin)])
def create_operator(data: CreateOperator):
    op_id = str(uuid.uuid4())
    token = str(uuid.uuid4()).replace("-", "")
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO operators (id, name, token) VALUES (%s,%s,%s)",
                (op_id, data.name, token)
            )
        conn.commit()
    return {"id": op_id, "name": data.name, "token": token}

@app.delete("/admin/operators/{op_id}", dependencies=[Depends(verify_admin)])
def delete_operator(op_id: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM operators WHERE id=%s", (op_id,))
        conn.commit()
    return {"ok": True}

# ─── OPERATOR ────────────────────────────────────────────────────────────────

@app.get("/op/{token}/info")
def operator_info(token: str):
    op = get_operator_by_token(token)
    today = date.today().isoformat()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM sessions WHERE operator_id=%s AND date=%s ORDER BY id DESC LIMIT 1",
                (op["id"], today)
            )
            session = cur.fetchone()
            session = dict(session) if session else None

            elapsed_available = 0
            current_status = None

            if session:
                cur.execute(
                    "SELECT * FROM status_log WHERE session_id=%s ORDER BY id DESC LIMIT 1",
                    (session["id"],)
                )
                last_log = cur.fetchone()
                if last_log:
                    last_log = dict(last_log)
                    current_status = last_log["status"]
                    if last_log["ended_at"] is None and current_status == "available":
                        started = last_log["started_at"]
                        if started.tzinfo is None:
                            started = started.replace(tzinfo=timezone.utc)
                        elapsed_available += int((utcnow() - started).total_seconds())

                cur.execute(
                    "SELECT COALESCE(SUM(duration_seconds),0) as total FROM status_log WHERE session_id=%s AND status='available' AND ended_at IS NOT NULL",
                    (session["id"],)
                )
                row = cur.fetchone()
                elapsed_available += row["total"]

    return {
        "operator": op,
        "session": session,
        "current_status": current_status,
        "elapsed_available_seconds": elapsed_available,
    }

@app.post("/op/{token}/session/start")
async def start_session(token: str, data: StartSession):
    op = get_operator_by_token(token)
    today = date.today().isoformat()
    now = utcnow()

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM sessions WHERE operator_id=%s AND date=%s",
                (op["id"], today)
            )
            existing = cur.fetchone()
            if existing:
                return {"session_id": existing["id"], "already_exists": True}

            cur.execute(
                "INSERT INTO sessions (operator_id, date, available_minutes, started_at) VALUES (%s,%s,%s,%s) RETURNING id",
                (op["id"], today, data.available_minutes, now)
            )
            session_id = cur.fetchone()["id"]
            cur.execute(
                "INSERT INTO status_log (operator_id, session_id, status, started_at) VALUES (%s,%s,%s,%s)",
                (op["id"], session_id, "available", now)
            )
        conn.commit()

    await manager.broadcast({"event": "status_change", "operator_id": op["id"],
                             "name": op["name"], "status": "available",
                             "available_minutes": data.available_minutes})
    return {"session_id": session_id}

@app.post("/op/{token}/status")
async def update_status(token: str, data: UpdateStatus):
    if data.status not in ("available", "busy", "offline"):
        raise HTTPException(status_code=400, detail="Invalid status")
    op = get_operator_by_token(token)
    today = date.today().isoformat()
    now = utcnow()

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM sessions WHERE operator_id=%s AND date=%s ORDER BY id DESC LIMIT 1",
                (op["id"], today)
            )
            session = cur.fetchone()
            if not session:
                raise HTTPException(status_code=400, detail="No active session for today")
            session = dict(session)

            # Close last open log
            cur.execute(
                "SELECT * FROM status_log WHERE session_id=%s AND ended_at IS NULL ORDER BY id DESC LIMIT 1",
                (session["id"],)
            )
            last = cur.fetchone()
            if last:
                last = dict(last)
                started = last["started_at"]
                if started.tzinfo is None:
                    started = started.replace(tzinfo=timezone.utc)
                duration = int((now - started).total_seconds())
                cur.execute(
                    "UPDATE status_log SET ended_at=%s, duration_seconds=%s WHERE id=%s",
                    (now, duration, last["id"])
                )

            cur.execute(
                "INSERT INTO status_log (operator_id, session_id, status, started_at) VALUES (%s,%s,%s,%s)",
                (op["id"], session["id"], data.status, now)
            )

            cur.execute(
                "SELECT COALESCE(SUM(duration_seconds),0) as total FROM status_log WHERE session_id=%s AND status='available' AND ended_at IS NOT NULL",
                (session["id"],)
            )
            elapsed = cur.fetchone()["total"]
        conn.commit()

    await manager.broadcast({
        "event": "status_change",
        "operator_id": op["id"],
        "name": op["name"],
        "status": data.status,
        "available_minutes": session["available_minutes"],
        "elapsed_available_seconds": elapsed,
    })
    return {"ok": True}

@app.post("/op/{token}/available-time")
async def update_available_time(token: str, minutes: int):
    op = get_operator_by_token(token)
    today = date.today().isoformat()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE sessions SET available_minutes=%s WHERE operator_id=%s AND date=%s",
                (minutes, op["id"], today)
            )
        conn.commit()
    await manager.broadcast({"event": "time_update", "operator_id": op["id"],
                             "name": op["name"], "available_minutes": minutes})
    return {"ok": True}

# ─── DASHBOARD ────────────────────────────────────────────────────────────────

@app.get("/dashboard/current")
def dashboard_current():
    today = date.today().isoformat()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM operators")
            operators = [dict(r) for r in cur.fetchall()]
            result = []
            for op in operators:
                cur.execute(
                    "SELECT * FROM sessions WHERE operator_id=%s AND date=%s ORDER BY id DESC LIMIT 1",
                    (op["id"], today)
                )
                session = cur.fetchone()
                if not session:
                    result.append({**op, "status": "offline", "available_minutes": 0,
                                   "elapsed_available_seconds": 0, "session": None})
                    continue
                session = dict(session)

                cur.execute(
                    "SELECT * FROM status_log WHERE session_id=%s ORDER BY id DESC LIMIT 1",
                    (session["id"],)
                )
                last_log = cur.fetchone()
                current_status = dict(last_log)["status"] if last_log else "offline"

                cur.execute(
                    "SELECT COALESCE(SUM(duration_seconds),0) as total FROM status_log WHERE session_id=%s AND status='available' AND ended_at IS NOT NULL",
                    (session["id"],)
                )
                elapsed = cur.fetchone()["total"]

                if current_status == "available" and last_log:
                    started = dict(last_log)["started_at"]
                    if started.tzinfo is None:
                        started = started.replace(tzinfo=timezone.utc)
                    elapsed += int((utcnow() - started).total_seconds())

                result.append({
                    **op,
                    "status": current_status,
                    "available_minutes": session["available_minutes"],
                    "elapsed_available_seconds": elapsed,
                    "session": session,
                })
    return result

@app.get("/dashboard/history")
def dashboard_history(days: int = 30):
    since = (date.today() - timedelta(days=days)).isoformat()
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM operators")
            operators = [dict(r) for r in cur.fetchall()]
            result = []
            for op in operators:
                cur.execute(
                    """SELECT s.*, COUNT(sl.id) as state_changes
                       FROM sessions s
                       LEFT JOIN status_log sl ON sl.session_id = s.id
                       WHERE s.operator_id=%s AND s.date >= %s
                       GROUP BY s.id ORDER BY s.date DESC""",
                    (op["id"], since)
                )
                sessions = [dict(r) for r in cur.fetchall()]
                for s in sessions:
                    cur.execute(
                        "SELECT status, COALESCE(SUM(duration_seconds),0) as total FROM status_log WHERE session_id=%s AND ended_at IS NOT NULL GROUP BY status",
                        (s["id"],)
                    )
                    s["stats"] = {r["status"]: r["total"] for r in cur.fetchall()}
                result.append({**op, "sessions": sessions})
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
    return {"status": "ok", "time": utcnow().isoformat()}
