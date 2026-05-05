import os
import signal
import sys
from psycopg_pool import ConnectionPool

# Read connection parameters from environment variables with sensible defaults
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "civic_ledger")
DB_USER = os.getenv("DB_USER", "ledger_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "ledger_pass")

# Create a connection pool
pool = ConnectionPool(
    conninfo=f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}",
    min_size=1,
    max_size=10,
    timeout=30
)

def get_connection():
    """
    Acquire a connection from the pool.
    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    """
    return pool.connection()

def shutdown_pool(*args):
    """Gracefully close the connection pool on shutdown signals."""
    print("Shutting down connection pool...")
    pool.close()
    sys.exit(0)

# Register graceful shutdown handlers
signal.signal(signal.SIGINT, shutdown_pool)
signal.signal(signal.SIGTERM, shutdown_pool)

if __name__ == "__main__":
    # Quick test
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            print("Connected to:", cur.fetchone()[0])
