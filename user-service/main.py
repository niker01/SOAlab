import logging
import sqlite3
import threading
import uuid
from typing import Optional

from fastapi import FastAPI, Header, HTTPException

from database import db_connection, enqueue_event, initialize_database
from rabbitmq import publish_outbox_forever
from schemas import UserCreate, UserUpdate


app = FastAPI(title="User Service")
logger = logging.getLogger(__name__)


@app.on_event("startup")
def startup_event():
    initialize_database()
    threading.Thread(target=publish_outbox_forever, daemon=True).start()


def verify_token(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")

    token = authorization[len("Bearer "):].strip()
    token_prefix = "fake-jwt-token-"
    if not token.startswith(token_prefix) or not token[len(token_prefix):]:
        raise HTTPException(status_code=401, detail="Invalid token")

    user_id = token[len(token_prefix):]
    with db_connection() as connection:
        user = connection.execute(
            "SELECT id FROM users WHERE id = ?",
            (user_id,)
        ).fetchone()
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    return user_id


@app.post("/users/register", status_code=201)
def register_user(user: UserCreate):
    user_id = str(uuid.uuid4())
    try:
        with db_connection() as connection:
            connection.execute(
                "INSERT INTO users (id, email, password_hash) VALUES (?, ?, ?)",
                (user_id, user.email, f"hashed_{user.password}")
            )
    except sqlite3.IntegrityError as exc:
        raise HTTPException(status_code=400, detail="Email already registered") from exc
    return {
        "message": "User registered successfully",
        "user": {"id": user_id, "email": user.email}
    }


@app.post("/users/login", status_code=200)
def login_user(user: UserCreate):
    with db_connection() as connection:
        existing_user = connection.execute(
            "SELECT id FROM users WHERE email = ? AND password_hash = ?",
            (user.email, f"hashed_{user.password}")
        ).fetchone()
    if existing_user is None:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return {
        "message": "Login successful",
        "access_token": f"fake-jwt-token-{existing_user['id']}"
    }


@app.get("/users", status_code=200)
def get_users(authorization: Optional[str] = Header(None)):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        user = connection.execute(
            "SELECT id, email FROM users WHERE id = ?",
            (user_id,)
        ).fetchone()
    return [{"id": user["id"], "email": user["email"]}]


@app.get("/users/{user_id}", status_code=200)
def get_user(user_id: str, authorization: Optional[str] = Header(None)):
    authorized_user_id = verify_token(authorization)
    if user_id != authorized_user_id:
        raise HTTPException(status_code=404, detail="User not found")
    with db_connection() as connection:
        user = connection.execute(
            "SELECT id, email FROM users WHERE id = ?",
            (user_id,)
        ).fetchone()
    return {"id": user["id"], "email": user["email"]}


@app.put("/users/{user_id}", status_code=200)
def update_user(
    user_id: str,
    data: UserUpdate,
    authorization: Optional[str] = Header(None)
):
    authorized_user_id = verify_token(authorization)
    if user_id != authorized_user_id:
        raise HTTPException(status_code=404, detail="User not found")

    try:
        with db_connection() as connection:
            user = connection.execute(
                "SELECT id, email, password_hash FROM users WHERE id = ?",
                (user_id,)
            ).fetchone()
            updated_email = data.email if data.email else user["email"]
            updated_password = (
                f"hashed_{data.password}" if data.password else user["password_hash"]
            )
            connection.execute(
                "UPDATE users SET email = ?, password_hash = ? WHERE id = ?",
                (updated_email, updated_password, user_id)
            )
    except sqlite3.IntegrityError as exc:
        raise HTTPException(status_code=400, detail="Email already registered") from exc

    return {
        "message": "User updated successfully",
        "user": {"id": user_id, "email": updated_email}
    }


@app.delete("/users/{user_id}", status_code=200)
def delete_user(user_id: str, authorization: Optional[str] = Header(None)):
    authorized_user_id = verify_token(authorization)
    if user_id != authorized_user_id:
        raise HTTPException(status_code=404, detail="User not found")

    with db_connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        user = connection.execute(
            "SELECT id FROM users WHERE id = ?",
            (user_id,)
        ).fetchone()
        if user is None:
            raise HTTPException(status_code=404, detail="User not found")
        enqueue_event(
            connection,
            "user.deleted",
            {"event_type": "user.deleted", "user_id": user_id}
        )
        connection.execute("DELETE FROM users WHERE id = ?", (user_id,))

    return {"message": "User deleted successfully"}
