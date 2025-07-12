import os
import time
import asyncpg
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from secure import Secure
from fastapi.middleware.gzip import GZipMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Histogram
from datetime import datetime
import psutil


app = FastAPI(title="FastAPI Metrics App")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add Secure headers middleware
secure_headers = Secure()

class SecureMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        secure_headers.framework.starlette(response)
        return response

app.add_middleware(SecureMiddleware)

# Add GZip compression
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Add Prometheus metrics
Instrumentator().instrument(app).expose(app)

# Custom Prometheus metrics
db_operations_total = Counter(
    'db_operations_total',
    'Total number of database operations',
    ['operation', 'status']
)

db_query_duration = Histogram(
    'db_query_duration_seconds',
    'Time spent on database queries',
    ['operation']
)

# Database config from env variables with fallbacks
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "appdb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "sagor123")

# Create PostgreSQL connection pool on startup
@app.on_event("startup")
async def startup():
    try:
        app.state.pool = await asyncpg.create_pool(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            max_size=20,
            max_inactive_connection_lifetime=30,
            timeout=2
        )
        print(f"Successfully connected to database: {DB_NAME}")
    except Exception as e:
        print(f"Warning: Could not connect to database: {e}")
        app.state.pool = None

# Close connection pool on shutdown
@app.on_event("shutdown")
async def shutdown():
    if hasattr(app.state, 'pool') and app.state.pool:
        await app.state.pool.close()

# Health check route
@app.get("/")
def root():
    return {"Hello": "World", "status": "healthy"}

# Health check with database status
@app.get("/health", status_code=200)
async def health_check():
    start_time = time.time()
    try:
        # Ensure DB pool is connected
        if not getattr(app.state, "pool", None):
            raise Exception("Database not connected")

        async with app.state.pool.acquire() as conn:
            result = await conn.fetchrow("SELECT NOW()")
            db_time = result["now"].isoformat()

        # Process memory info (similar to Node.js's process.memoryUsage())
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()._asdict()

        return JSONResponse(
            content={
                "status": "healthy",
                "timestamp": datetime.utcnow().isoformat(),
                "database": "connected",
                "uptime": round(time.time() - start_time, 2),
                "memory": memory_info,
                "db_time": db_time,
            }
        )

    except Exception as error:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(error),
            }
        )


# Create user endpoint
@app.post("/user", status_code=status.HTTP_201_CREATED)
async def create_user(request: Request) -> JSONResponse:
    start_time = time.time()
    conn = None

    def serialize_row(row):
        from datetime import datetime
        return {
            key: (value.isoformat() if isinstance(value, datetime) else value)
            for key, value in dict(row).items()
        }

    try:
        # Check if database is available
        if not getattr(app.state, 'pool', None):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database not available",
            )

        body = await request.json()
        name = body.get("name")
        email = body.get("email")
        message = body.get("message", "")

        if not name or not email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Name and email are required",
            )

        conn = await app.state.pool.acquire()

        query = """
            INSERT INTO user_data (name, email, message, created_at)
            VALUES ($1, $2, $3, NOW())
            RETURNING *
        """
        row = await conn.fetchrow(query, name, email, message)

        duration = time.time() - start_time
        db_query_duration.labels(operation="insert").observe(duration)
        db_operations_total.labels(operation="insert", status="success").inc()

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={"success": True, "data": serialize_row(row)},
        )

    except HTTPException as exc:
        raise exc

    except Exception as exc:
        duration = time.time() - start_time
        db_query_duration.labels(operation="insert").observe(duration)
        db_operations_total.labels(operation="insert", status="error").inc()

        print("Database error:", exc)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Failed to insert data", "message": str(exc)},
        )

    finally:
        if conn:
            await app.state.pool.release(conn)

# Get all users endpoint
@app.get("/users", status_code=status.HTTP_200_OK)
async def get_users():
    start_time = time.time()
    conn = None

    def serialize_row(row):
        return {
            key: (value.isoformat() if isinstance(value, datetime) else value)
            for key, value in dict(row).items()
        }

    try:
        if not hasattr(app.state, 'pool') or not app.state.pool:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database not available",
            )

        conn = await app.state.pool.acquire()

        query = "SELECT * FROM user_data ORDER BY created_at DESC LIMIT 100"
        rows = await conn.fetch(query)

        duration = time.time() - start_time
        db_query_duration.labels(operation="select").observe(duration)
        db_operations_total.labels(operation="select", status="success").inc()

        return JSONResponse(
            content={
                "success": True,
                "count": len(rows),
                "data": [serialize_row(row) for row in rows],
            }
        )

    except HTTPException as exc:
        raise exc

    except Exception as error:
        duration = time.time() - start_time
        db_query_duration.labels(operation="select").observe(duration)
        db_operations_total.labels(operation="select", status="error").inc()

        print("Database error:", error)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": "Failed to retrieve data",
                "message": str(error),
            },
        )

    finally:
        if conn:
            await app.state.pool.release(conn)

# ---------------------------------------------------------------------------
# Update user endpoint (Partial update)
# ---------------------------------------------------------------------------
@app.put("/user/{id}", status_code=status.HTTP_200_OK)
async def update_user(id: int, request: Request) -> JSONResponse:
    start_time = time.time()
    conn = None

    def serialize_row(row):
        return {
            key: (value.isoformat() if isinstance(value, datetime) else value)
            for key, value in dict(row).items()
        }

    try:
        if not getattr(app.state, "pool", None):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database not available",
            )

        body = await request.json()

        if not body:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="At least one field (name, email, message) is required for update",
            )

        conn = await app.state.pool.acquire()

        # Fetch existing user to check existence
        existing = await conn.fetchrow("SELECT * FROM user_data WHERE id = $1", id)
        if not existing:
            duration = time.time() - start_time
            db_query_duration.labels(operation="update").observe(duration)
            db_operations_total.labels(operation="update", status="error").inc()
            raise HTTPException(status_code=404, detail="Record not found")

        # Build dynamic SET clause
        allowed_fields = ["name", "email", "message"]
        set_clauses = []
        values = []
        index = 1

        for field in allowed_fields:
            if field in body:
                set_clauses.append(f"{field} = ${index}")
                values.append(body[field])
                index += 1

        if not set_clauses:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid fields provided for update",
            )

        # Add updated_at field
        set_clauses.append(f"updated_at = NOW()")

        # Final query
        update_query = f"""
            UPDATE user_data
            SET {', '.join(set_clauses)}
            WHERE id = ${index}
            RETURNING *
        """
        values.append(id)

        updated_row = await conn.fetchrow(update_query, *values)

        duration = time.time() - start_time
        db_query_duration.labels(operation="update").observe(duration)
        db_operations_total.labels(operation="update", status="success").inc()

        return JSONResponse(
            content={"success": True, "data": serialize_row(updated_row)}
        )

    except HTTPException as exc:
        raise exc

    except Exception as exc:
        duration = time.time() - start_time
        db_query_duration.labels(operation="update").observe(duration)
        db_operations_total.labels(operation="update", status="error").inc()

        print("Database error:", exc)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Failed to update data", "message": str(exc)},
        )

    finally:
        if conn:
            await app.state.pool.release(conn)


# ---------------------------------------------------------------------------
# Delete user endpoint
# ---------------------------------------------------------------------------
@app.delete("/user/{id}", status_code=status.HTTP_200_OK)
async def delete_user(id: int) -> JSONResponse:
    start_time = time.time()
    conn = None

    def serialize_row(row):
        from datetime import datetime
        return {
            key: (value.isoformat() if isinstance(value, datetime) else value)
            for key, value in dict(row).items()
        }

    try:
        # Verify DB pool availability
        if not getattr(app.state, "pool", None):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database not available",
            )

        conn = await app.state.pool.acquire()

        query = "DELETE FROM user_data WHERE id = $1 RETURNING *"
        deleted_row = await conn.fetchrow(query, id)

        duration = time.time() - start_time
        if not deleted_row:
            db_query_duration.labels(operation="delete").observe(duration)
            db_operations_total.labels(operation="delete", status="error").inc()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Record not found",
            )

        db_query_duration.labels(operation="delete").observe(duration)
        db_operations_total.labels(operation="delete", status="success").inc()

        return JSONResponse(
            content={"success": True, "data": serialize_row(deleted_row)}
        )

    except HTTPException as exc:
        raise exc

    except Exception as exc:
        duration = time.time() - start_time
        db_query_duration.labels(operation="delete").observe(duration)
        db_operations_total.labels(operation="delete", status="error").inc()

        print("Database error:", exc)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": "Failed to delete data",
                "message": str(exc),
            },
        )

    finally:
        if conn:
            await app.state.pool.release(conn)



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
