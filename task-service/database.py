import os
import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATABASE_PATH = DATA_DIR / "tasks.db"


def connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DATABASE_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


@contextmanager
def db_connection() -> Iterator[sqlite3.Connection]:
    connection = connect()
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def initialize_database() -> None:
    with db_connection() as connection:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS task_lists (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                title TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                list_id TEXT REFERENCES task_lists(id) ON DELETE SET NULL,
                status TEXT NOT NULL,
                due_date TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_tasks_user ON tasks(user_id);
            CREATE TABLE IF NOT EXISTS sagas (
                saga_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                before_snapshot TEXT,
                error TEXT,
                created_at REAL NOT NULL,
                deadline_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sagas_pending
                ON sagas(status, deadline_at);
            CREATE TABLE IF NOT EXISTS outbox (
                event_id TEXT PRIMARY KEY,
                exchange_name TEXT NOT NULL,
                routing_key TEXT NOT NULL,
                body TEXT NOT NULL,
                created_at REAL NOT NULL,
                available_at REAL NOT NULL,
                locked_until REAL,
                attempts INTEGER NOT NULL DEFAULT 0,
                published_at REAL
            );
            """
        )


def enqueue_event(
    connection: sqlite3.Connection,
    routing_key: str,
    message: dict,
    event_id: str | None = None
) -> str:
    event_id = event_id or str(uuid.uuid4())
    body = {**message, "event_id": event_id}
    connection.execute(
        """
        INSERT OR IGNORE INTO outbox (
            event_id, exchange_name, routing_key, body, created_at, available_at
        ) VALUES (?, 'todo_exchange', ?, ?, ?, ?)
        """,
        (event_id, routing_key, json.dumps(body), time.time(), time.time())
    )
    return event_id
