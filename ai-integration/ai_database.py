import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from fastapi import HTTPException


DATABASE_PATH = Path(os.getenv("DATA_DIR", "/data")) / "ai_usage.db"


def _connect() -> sqlite3.Connection:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DATABASE_PATH, timeout=30)
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def reserve_tokens(user_id: str, amount: int, daily_limit: int) -> None:
    connection = _connect()
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_token_usage (
                user_id TEXT NOT NULL,
                usage_date TEXT NOT NULL,
                tokens INTEGER NOT NULL,
                PRIMARY KEY (user_id, usage_date)
            )
            """
        )
        row = connection.execute(
            """
            SELECT tokens FROM daily_token_usage
            WHERE user_id = ? AND usage_date = ?
            """,
            (user_id, _today()),
        ).fetchone()
        used = row[0] if row else 0
        if amount <= 0 or used + amount > daily_limit:
            connection.rollback()
            raise HTTPException(
                status_code=429,
                detail="Daily AI token budget reached; try again tomorrow",
            )
        connection.execute(
            """
            INSERT INTO daily_token_usage (user_id, usage_date, tokens)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, usage_date)
            DO UPDATE SET tokens = excluded.tokens
            """,
            (user_id, _today(), used + amount),
        )
        connection.commit()
    finally:
        connection.close()


def settle_tokens(user_id: str, reserved: int, actual: int) -> None:
    connection = _connect()
    try:
        connection.execute(
            """
            UPDATE daily_token_usage
            SET tokens = MAX(0, tokens - ? + ?)
            WHERE user_id = ? AND usage_date = ?
            """,
            (reserved, max(0, actual), user_id, _today()),
        )
        connection.commit()
    finally:
        connection.close()


def release_tokens(user_id: str, reserved: int) -> None:
    settle_tokens(user_id, reserved, 0)
