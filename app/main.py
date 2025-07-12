# main.py  ── fully‑featured FastAPI + Prometheus service
import os
import time
import asyncio
import psutil
import asyncpg
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from secure import Secure

from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

# ────────────────────────────────────────────────────────────────────────────────
#  FastAPI app & core middleware
# ────────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="FastAPI Metrics App")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)
app.add_middleware(GZipMiddleware, minimum_size=1_000)

# Secure headers (equivalent to helmet in Express)
secure_headers = Secure()
class SecureMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        secure_headers.framework.starlette(response)
        return response
app.add_middleware(SecureMiddleware)

# ────────────────────────────────────────────────────────────────────────────────
#  Prometheus default instrumentation
# ────────────────────────────────────────────────────────────────────────────────
Instrumentator().instrument(app).expose(app)   # default /metrics endpoint

# ────────────────────────────────────────────────────────────────────────────────
#  Custom Prometheus metrics  (parity with Node example)
# ────────────────────────────────────────────────────────────────────────────────
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total number of HTTP requests",
    ["method", "route", "status_code"],
)
HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "Request latency in seconds",
    ["method", "route", "status_code"],
    buckets=[0.1, 0.3, 0.5, 0.7, 1, 3, 5, 7, 10],
)

DB_CONNECTIONS_ACTIVE = Gauge(
    "db_connections_active",
    "Number of active connections in asyncpg pool",
)
DB_QUERY_DURATION = Histogram(
    "db_query_duration_seconds",
    "Time spent on database queries",
    ["operation"],
    buckets=[0.01, 0.05, 0.1, 0.3, 0.5, 1, 2, 5],
)
DB_OPERATIONS_TOTAL = Counter(
    "db_operations_total",
    "Total number of database operations",
    ["operation", "status"],
)

CPU_PERCENT_GAUGE = Gauge(
    "process_cpu_percentage",
    "CPU usage percentage of this Python process (sampled every 5 s)",
)

# ────────────────────────────────────────────────────────────────────────────────
#  HTTP metrics middleware (mirrors Express custom middleware)
# ────────────────────────────────────────────────────────────────────────────────
@app.middleware("http")
async def http_metrics_middleware(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)

    # For FastAPI *routes* we can get route.path; fallback to raw URL path
    route_path = request.scope.get("route").path if request.scope.get("route") else request.url.path

    duration = time.time() - start_time
    HTTP_REQUESTS_TOTAL.labels(
        method=request.method, route=route_path, status_code=str(response.status_code)
    ).inc()
    HTTP_REQUEST_DURATION.labels(
        method=request.method, route=route_path, status_code=str(response.status_code)
    ).observe(duration)

    return response

# ────────────────────────────────────────────────────────────────────────────────
#  Database configuration
# ────────────────────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", 5432))
DB_NAME     = os.getenv("DB_NAME", "appdb")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "sagor123")

# ────────────────────────────────────────────────────────────────────────────────
#  Startup & shutdown
# ────────────────────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup() -> None:
    # 1) asyncpg connection pool
    try:
        app.state.pool = await asyncpg.create_pool(
            host=DB_HOST, port=DB_PORT, database=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
            max_size=20, max_inactive_connection_lifetime=30, timeout=2,
        )
        print(f"Connected to Postgres '{DB_NAME}' on {DB_HOST}:{DB_PORT}")
    except Exception as e:
        print(f"⚠️  Could not connect to Postgres: {e}")
        app.state.pool = None

    # 2) background tasks ─ CPU gauge + DB connection gauge
    loop = asyncio.get_event_loop()
    loop.create_task(sample_cpu_usage())
    loop.create_task(sample_db_connections())

@app.on_event("shutdown")
async def shutdown() -> None:
    if getattr(app.state, "pool", None):
        await app.state.pool.close()

# ────────────────────────────────────────────────────────────────────────────────
#  Background tasks for gauges
# ────────────────────────────────────────────────────────────────────────────────
async def sample_cpu_usage() -> None:
    """Set CPU_PERCENT_GAUGE every 5 s using psutil.cpu_percent(None)."""
    while True:
        try:
            CPU_PERCENT_GAUGE.set(psutil.cpu_percent(interval=None))
        finally:
            await asyncio.sleep(5)

async def sample_db_connections() -> None:
    """Set DB_CONNECTIONS_ACTIVE every 5 s using asyncpg pool stats."""
    while True:
        try:
            pool = getattr(app.state, "pool", None)
            if pool:                       # asyncpg >= 0.29 has get_stats()
                acquired, acquiring, idle, total = pool.get_stats()
                DB_CONNECTIONS_ACTIVE.set(acquired)
        finally:
            await asyncio.sleep(5)

# ────────────────────────────────────────────────────────────────────────────────
#  Helper: row → dict with ISO dates (used by CRUD endpoints)
# ────────────────────────────────────────────────────────────────────────────────
def serialize_row(row):
    return {
        k: (v.isoformat() if isinstance(v, datetime) else v)
        for k, v in dict(row).items()
    }

# ────────────────────────────────────────────────────────────────────────────────
#  Health check  (GET /health)
# ────────────────────────────────────────────────────────────────────────────────
@app.get("/health", response_model=None)
async def health_check() -> JSONResponse:
    ts_start = time.time()
    try:
        if not getattr(app.state, "pool", None):
            raise RuntimeError("Database not connected")

        async with app.state.pool.acquire() as conn:
            db_now_row = await conn.fetchrow("SELECT NOW()")
            db_time = db_now_row["now"].isoformat()

        mem = psutil.Process(os.getpid()).memory_info()._asdict()

        return JSONResponse(
            content={
                "status": "healthy",
                "timestamp": datetime.utcnow().isoformat(),
                "database": "connected",
                "uptime": round(time.time() - ts_start, 2),
                "memory": mem,
                "db_time": db_time,
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e),
            },
        )

# ────────────────────────────────────────────────────────────────────────────────
#  CRUD endpoints: /user, /users, /user/{id}          (unchanged logic, new metrics)
# ────────────────────────────────────────────────────────────────────────────────
@app.post("/user", status_code=201)
async def create_user(request: Request):
    ts = time.time()
    conn = None
    try:
        if not getattr(app.state, "pool", None):
            raise HTTPException(503, "Database not available")

        body = await request.json()
        name, email, message = body.get("name"), body.get("email"), body.get("message", "")
        if not name or not email:
            raise HTTPException(400, "Name and email are required")

        conn = await app.state.pool.acquire()
        row = await conn.fetchrow(
            "INSERT INTO user_data (name, email, message, created_at) "
            "VALUES ($1,$2,$3,NOW()) RETURNING *",
            name, email, message
        )

        DB_QUERY_DURATION.labels("insert").observe(time.time() - ts)
        DB_OPERATIONS_TOTAL.labels("insert", "success").inc()
        return {"success": True, "data": serialize_row(row)}
    except HTTPException as he:
        raise he
    except Exception as e:
        DB_QUERY_DURATION.labels("insert").observe(time.time() - ts)
        DB_OPERATIONS_TOTAL.labels("insert", "error").inc()
        raise HTTPException(500, f"Failed to insert data: {e}")
    finally:
        if conn:
            await app.state.pool.release(conn)

@app.get("/users")
async def get_users():
    ts = time.time()
    conn = None
    try:
        if not getattr(app.state, "pool", None):
            raise HTTPException(503, "Database not available")
        conn = await app.state.pool.acquire()
        rows = await conn.fetch("SELECT * FROM user_data ORDER BY created_at DESC LIMIT 100")
        DB_QUERY_DURATION.labels("select").observe(time.time() - ts)
        DB_OPERATIONS_TOTAL.labels("select", "success").inc()
        return {"success": True, "count": len(rows), "data": [serialize_row(r) for r in rows]}
    except HTTPException as he:
        raise he
    except Exception as e:
        DB_QUERY_DURATION.labels("select").observe(time.time() - ts)
        DB_OPERATIONS_TOTAL.labels("select", "error").inc()
        raise HTTPException(500, f"Failed to retrieve data: {e}")
    finally:
        if conn:
            await app.state.pool.release(conn)

@app.put("/user/{id}")
async def update_user(id: int, request: Request):
    ts = time.time()
    conn = None
    body = await request.json()
    if not body:
        raise HTTPException(400, "No payload provided")

    try:
        if not getattr(app.state, "pool", None):
            raise HTTPException(503, "Database not available")
        conn = await app.state.pool.acquire()

        # ensure record exists
        if not await conn.fetchrow("SELECT 1 FROM user_data WHERE id=$1", id):
            raise HTTPException(404, "Record not found")

        cols, vals, idx = [], [], 1
        for field in ("name", "email", "message"):
            if field in body:
                cols.append(f"{field} = ${idx}"); vals.append(body[field]); idx += 1
        if not cols:
            raise HTTPException(400, "No valid fields provided")
        cols.append("updated_at = NOW()")
        vals.append(id)

        updated = await conn.fetchrow(
            f"UPDATE user_data SET {', '.join(cols)} WHERE id = ${idx} RETURNING *", *vals
        )
        DB_QUERY_DURATION.labels("update").observe(time.time() - ts)
        DB_OPERATIONS_TOTAL.labels("update", "success").inc()
        return {"success": True, "data": serialize_row(updated)}
    except HTTPException as he:
        raise he
    except Exception as e:
        DB_QUERY_DURATION.labels("update").observe(time.time() - ts)
        DB_OPERATIONS_TOTAL.labels("update", "error").inc()
        raise HTTPException(500, f"Failed to update data: {e}")
    finally:
        if conn:
            await app.state.pool.release(conn)

@app.delete("/user/{id}")
async def delete_user(id: int):
    ts = time.time()
    conn = None
    try:
        if not getattr(app.state, "pool", None):
            raise HTTPException(503, "Database not available")
        conn = await app.state.pool.acquire()
        deleted = await conn.fetchrow("DELETE FROM user_data WHERE id=$1 RETURNING *", id)
        if not deleted:
            raise HTTPException(404, "Record not found")
        DB_QUERY_DURATION.labels("delete").observe(time.time() - ts)
        DB_OPERATIONS_TOTAL.labels("delete", "success").inc()
        return {"success": True, "data": serialize_row(deleted)}
    except HTTPException as he:
        raise he
    except Exception as e:
        DB_QUERY_DURATION.labels("delete").observe(time.time() - ts)
        DB_OPERATIONS_TOTAL.labels("delete", "error").inc()
        raise HTTPException(500, f"Failed to delete data: {e}")
    finally:
        if conn:
            await app.state.pool.release(conn)

# ────────────────────────────────────────────────────────────────────────────────
#  Optional: explicit /custom‑metrics endpoint (if you’d rather keep Instrumentator at /metrics)
# ────────────────────────────────────────────────────────────────────────────────
@app.get("/custom-metrics")
def custom_metrics() -> PlainTextResponse:
    """Prometheus scrapes here to include our extra gauges/counters."""
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# ────────────────────────────────────────────────────────────────────────────────
#  Run via:  uvicorn main:app --host 0.0.0.0 --port 8000
# ────────────────────────────────────────────────────────────────────────────────
