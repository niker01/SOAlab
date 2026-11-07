import json
import logging
import threading
import time
import uuid
from datetime import datetime
from typing import List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from fastapi import FastAPI, Header, HTTPException

from database import db_connection, enqueue_event, initialize_database
from rabbitmq import publish_outbox_forever, start_consumer
from schemas import NotificationSend, ReminderCreate, ReminderUpdate


app = FastAPI(title="Reminder & Notification Service")
logger = logging.getLogger(__name__)
TASK_SERVICE_URL = "http://task-service:8000"


def _reminder_dict(row):
    reminder = dict(row)
    reminder["is_active"] = bool(reminder["is_active"])
    reminder.pop("source")
    return reminder


def _notification_dict(row):
    notification = dict(row)
    if notification["task_id"] is None:
        notification.pop("task_id")
    return notification


def verify_token(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    token = authorization[len("Bearer "):].strip()
    token_prefix = "fake-jwt-token-"
    if not token.startswith(token_prefix) or not token[len(token_prefix):]:
        raise HTTPException(status_code=401, detail="Invalid token")
    return token[len(token_prefix):]


def _verify_task_ownership(task_id: str, authorization: Optional[str]) -> None:
    if not authorization:
        raise HTTPException(status_code=401, detail="Unauthorized")
    request = Request(
        f"{TASK_SERVICE_URL}/tasks/{task_id}",
        headers={"Authorization": authorization},
    )
    try:
        with urlopen(request, timeout=5) as response:
            task = json.loads(response.read())
    except HTTPError as exc:
        if exc.code == 401:
            raise HTTPException(status_code=401, detail="Invalid token") from exc
        if exc.code == 404:
            raise HTTPException(status_code=404, detail="Task not found") from exc
        logger.exception("Task Service rejected reminder ownership verification")
        raise HTTPException(
            status_code=503,
            detail="Could not verify task ownership",
        ) from exc
    except (URLError, TimeoutError, OSError) as exc:
        logger.exception("Task Service unavailable during reminder ownership verification")
        raise HTTPException(
            status_code=503,
            detail="Task Service is unavailable",
        ) from exc
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.exception("Task Service returned an invalid task response")
        raise HTTPException(
            status_code=502,
            detail="Task Service returned an invalid response",
        ) from exc

    if not isinstance(task, dict) or task.get("id") != task_id:
        raise HTTPException(status_code=404, detail="Task not found")


def _get_user_reminder(connection, reminder_id: str, user_id: str):
    reminder = connection.execute(
        "SELECT * FROM reminders WHERE id = ? AND user_id = ?",
        (reminder_id, user_id)
    ).fetchone()
    if reminder is None:
        raise HTTPException(status_code=404, detail="Reminder not found")
    return reminder


def _get_user_notification(connection, notification_id: str, user_id: str):
    notification = connection.execute(
        "SELECT * FROM notification_log WHERE id = ? AND user_id = ?",
        (notification_id, user_id)
    ).fetchone()
    if notification is None:
        raise HTTPException(status_code=404, detail="Notification log not found")
    return notification


def _event_was_processed(connection, event_id: Optional[str]) -> bool:
    if not event_id:
        return False
    return connection.execute(
        "SELECT 1 FROM processed_events WHERE event_id = ?",
        (event_id,)
    ).fetchone() is not None


def _record_processed_event(connection, event_id: Optional[str]) -> None:
    if event_id:
        connection.execute(
            "INSERT OR IGNORE INTO processed_events (event_id, processed_at) VALUES (?, ?)",
            (event_id, time.time())
        )


def _handle_reminder_request(data: dict) -> None:
    task_id = data.get("task_id")
    user_id = data.get("user_id")
    reminder_time = data.get("reminder_time")
    saga_id = data.get("saga_id")
    if not task_id or not user_id or not reminder_time or not saga_id:
        raise ValueError("Reminder command must include task_id, user_id, reminder_time, and saga_id")
    datetime.fromisoformat(reminder_time.replace("Z", "+00:00"))

    event_id = data.get("event_id")
    with db_connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        if connection.execute(
            "SELECT 1 FROM compensated_sagas WHERE saga_id = ?",
            (saga_id,)
        ).fetchone() is not None:
            _record_processed_event(connection, event_id)
            return
        if _event_was_processed(connection, event_id):
            enqueue_event(
                connection,
                "saga.reminder.succeeded",
                {"saga_id": saga_id, "task_id": task_id},
                event_id=f"{saga_id}:succeeded"
            )
            return
        if connection.execute(
            "SELECT 1 FROM deleted_tasks WHERE task_id = ?",
            (task_id,)
        ).fetchone() is not None or connection.execute(
            "SELECT 1 FROM deleted_users WHERE user_id = ?",
            (user_id,)
        ).fetchone() is not None:
            raise ValueError("Cannot create a reminder for a deleted task or user")

        snapshot_exists = connection.execute(
            "SELECT 1 FROM saga_reminder_snapshots WHERE saga_id = ?",
            (saga_id,)
        ).fetchone()
        if snapshot_exists is None:
            previous = connection.execute(
                """
                SELECT id, task_id, user_id, reminder_time, status, is_active, source
                FROM reminders
                WHERE task_id = ? AND source = 'task'
                ORDER BY rowid
                LIMIT 1
                """,
                (task_id,)
            ).fetchone()
            connection.execute(
                """
                INSERT INTO saga_reminder_snapshots (
                    saga_id, task_id, reminder_json, created_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    saga_id,
                    task_id,
                    json.dumps(dict(previous)) if previous is not None else None,
                    time.time()
                )
            )

        existing = connection.execute(
            """
            SELECT id FROM reminders
            WHERE task_id = ? AND source = 'task'
            ORDER BY rowid
            LIMIT 1
            """,
            (task_id,)
        ).fetchone()
        if existing is None:
            connection.execute(
                """
                INSERT INTO reminders (
                    id, task_id, user_id, reminder_time, status, is_active, source
                ) VALUES (?, ?, ?, ?, 'PENDING', 1, 'task')
                """,
                (str(uuid.uuid4()), task_id, user_id, reminder_time)
            )
        else:
            connection.execute(
                """
                UPDATE reminders
                SET user_id = ?, reminder_time = ?, status = 'PENDING', is_active = 1
                WHERE id = ?
                """,
                (user_id, reminder_time, existing["id"])
            )
        _record_processed_event(connection, event_id)
        enqueue_event(
            connection,
            "saga.reminder.succeeded",
            {"saga_id": saga_id, "task_id": task_id},
            event_id=f"{saga_id}:succeeded"
        )


def _handle_reminder_compensation(data: dict) -> None:
    saga_id = data.get("saga_id")
    task_id = data.get("task_id")
    action = data.get("action")
    if not saga_id or not task_id or action not in {"create", "update"}:
        raise ValueError("Reminder compensation must include saga_id and task_id")

    event_id = data.get("event_id")
    with db_connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        if not _event_was_processed(connection, event_id):
            connection.execute(
                """
                INSERT OR IGNORE INTO compensated_sagas (saga_id, compensated_at)
                VALUES (?, ?)
                """,
                (saga_id, time.time())
            )
            snapshot = connection.execute(
                """
                SELECT reminder_json, created_at FROM saga_reminder_snapshots
                WHERE saga_id = ? AND task_id = ?
                """,
                (saga_id, task_id)
            ).fetchone()
            deleted_task = connection.execute(
                "SELECT 1 FROM deleted_tasks WHERE task_id = ?",
                (task_id,)
            ).fetchone() is not None
            previous = None
            if snapshot is not None and snapshot["reminder_json"] is not None:
                previous = json.loads(snapshot["reminder_json"])
            deleted_user = previous is not None and connection.execute(
                "SELECT 1 FROM deleted_users WHERE user_id = ?",
                (previous["user_id"],)
            ).fetchone() is not None
            if deleted_task or deleted_user:
                connection.execute(
                    "DELETE FROM reminders WHERE task_id = ?",
                    (task_id,)
                )
            elif snapshot is not None:
                if previous is None:
                    connection.execute(
                        "DELETE FROM reminders WHERE task_id = ? AND source = 'task'",
                        (task_id,)
                    )
                else:
                    connection.execute(
                        """
                        INSERT INTO reminders (
                            id, task_id, user_id, reminder_time, status, is_active, source
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET
                            task_id = excluded.task_id,
                            user_id = excluded.user_id,
                            reminder_time = excluded.reminder_time,
                            status = excluded.status,
                            is_active = excluded.is_active,
                            source = excluded.source
                        """,
                        (
                            previous["id"],
                            previous["task_id"],
                            previous["user_id"],
                            previous["reminder_time"],
                            previous["status"],
                            previous["is_active"],
                            previous["source"]
                        )
                    )
                connection.execute(
                    """
                    DELETE FROM notification_log
                    WHERE task_id = ? AND created_at >= ?
                    """,
                    (task_id, snapshot["created_at"])
                )
            elif action == "create":
                connection.execute(
                    "DELETE FROM reminders WHERE task_id = ? AND source = 'task'",
                    (task_id,)
                )
            _record_processed_event(connection, event_id)

        enqueue_event(
            connection,
            "saga.reminder.compensated",
            {"saga_id": saga_id, "task_id": task_id},
            event_id=f"{saga_id}:compensated"
        )


def _handle_task_deleted(data: dict) -> None:
    task_id = data.get("task_id")
    if not task_id:
        raise ValueError("Task-deleted event must include task_id")
    with db_connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "INSERT OR IGNORE INTO deleted_tasks (task_id, deleted_at) VALUES (?, ?)",
            (task_id, time.time())
        )
        connection.execute("DELETE FROM reminders WHERE task_id = ?", (task_id,))
        connection.execute("DELETE FROM notification_log WHERE task_id = ?", (task_id,))


def _handle_user_deleted(data: dict) -> None:
    user_id = data.get("user_id")
    if not user_id:
        raise ValueError("User-deleted event must include user_id")
    with db_connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "INSERT OR IGNORE INTO deleted_users (user_id, deleted_at) VALUES (?, ?)",
            (user_id, time.time())
        )
        connection.execute("DELETE FROM reminders WHERE user_id = ?", (user_id,))
        connection.execute("DELETE FROM notification_log WHERE user_id = ?", (user_id,))


def handle_task_event(data: dict, routing_key: str) -> None:
    if routing_key == "task.reminder.requested":
        _handle_reminder_request(data)
    elif routing_key == "task.reminder.compensate":
        _handle_reminder_compensation(data)
    elif routing_key == "task.deleted":
        _handle_task_deleted(data)
    elif routing_key == "user.deleted":
        _handle_user_deleted(data)
    elif routing_key == "task.created":
        task_id = data.get("task_id")
        user_id = data.get("user_id")
        reminder_time = data.get("reminder_time")
        if not task_id or not user_id:
            raise ValueError("Task-created event is missing task_id or user_id")
        if not reminder_time:
            return
        datetime.fromisoformat(reminder_time.replace("Z", "+00:00"))
        with db_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if connection.execute(
                "SELECT 1 FROM deleted_tasks WHERE task_id = ?",
                (task_id,)
            ).fetchone() is not None or connection.execute(
                "SELECT 1 FROM deleted_users WHERE user_id = ?",
                (user_id,)
            ).fetchone() is not None:
                return
            existing = connection.execute(
                "SELECT id FROM reminders WHERE task_id = ? AND source = 'task' LIMIT 1",
                (task_id,)
            ).fetchone()
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO reminders (
                        id, task_id, user_id, reminder_time, status, is_active, source
                    ) VALUES (?, ?, ?, ?, 'PENDING', 1, 'task')
                    """,
                    (str(uuid.uuid4()), task_id, user_id, reminder_time)
                )
            else:
                connection.execute(
                    """
                    UPDATE reminders SET user_id = ?, reminder_time = ?,
                        status = 'PENDING', is_active = 1
                    WHERE id = ?
                    """,
                    (user_id, reminder_time, existing["id"])
                )
    elif routing_key == "task.updated":
        if not data.get("task_id") or not data.get("user_id"):
            raise ValueError("Task-updated event is missing task_id or user_id")


def check_reminders_worker() -> None:
    while True:
        now = datetime.now().astimezone()
        try:
            with db_connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                reminders = connection.execute(
                    """
                    SELECT id, task_id, user_id, reminder_time
                    FROM reminders
                    WHERE status = 'PENDING' AND is_active = 1
                    """
                ).fetchall()
                for reminder in reminders:
                    try:
                        reminder_time = datetime.fromisoformat(
                            reminder["reminder_time"].replace("Z", "+00:00")
                        )
                        if reminder_time.tzinfo is None:
                            reminder_time = reminder_time.replace(tzinfo=now.tzinfo)
                        if reminder_time <= now:
                            updated = connection.execute(
                                """
                                UPDATE reminders SET status = 'TRIGGERED'
                                WHERE id = ? AND status = 'PENDING' AND is_active = 1
                                """,
                                (reminder["id"],)
                            )
                            if updated.rowcount:
                                connection.execute(
                                    """
                                    INSERT INTO notification_log (
                                        id, user_id, task_id, message, channel, status, created_at
                                    ) VALUES (?, ?, ?, ?, 'PUSH', 'SENT', ?)
                                    """,
                                    (
                                        str(uuid.uuid4()),
                                        reminder["user_id"],
                                        reminder["task_id"],
                                        f"Reminder: Task {reminder['task_id']} due time reached!",
                                        time.time()
                                    )
                                )
                    except (KeyError, TypeError, ValueError):
                        logger.exception(
                            "Could not process reminder %s",
                            reminder["id"]
                        )
        except Exception:
            logger.exception("Could not check due reminders")
        time.sleep(10)


@app.on_event("startup")
def startup_event():
    initialize_database()
    threading.Thread(
        target=publish_outbox_forever,
        daemon=True
    ).start()
    threading.Thread(
        target=start_consumer,
        args=(handle_task_event,),
        daemon=True
    ).start()
    threading.Thread(
        target=check_reminders_worker,
        daemon=True
    ).start()


@app.post("/reminders", status_code=201)
def create_reminder(
    reminder: ReminderCreate,
    authorization: Optional[str] = Header(None)
):
    user_id = verify_token(authorization)
    _verify_task_ownership(reminder.task_id, authorization)
    reminder_id = str(uuid.uuid4())
    new_reminder = {
        "id": reminder_id,
        "task_id": reminder.task_id,
        "user_id": user_id,
        "reminder_time": reminder.reminder_time,
        "status": "PENDING",
        "is_active": reminder.is_active
    }
    with db_connection() as connection:
        if connection.execute(
            "SELECT 1 FROM deleted_users WHERE user_id = ?",
            (user_id,)
        ).fetchone() is not None:
            raise HTTPException(status_code=404, detail="User not found")
        connection.execute(
            """
            INSERT INTO reminders (
                id, task_id, user_id, reminder_time, status, is_active, source
            ) VALUES (?, ?, ?, ?, 'PENDING', ?, 'manual')
            """,
            (
                reminder_id,
                reminder.task_id,
                user_id,
                reminder.reminder_time,
                int(reminder.is_active)
            )
        )
    return {"message": "Reminder scheduled successfully", "reminder": new_reminder}


@app.get("/reminders", response_model=List[dict])
def get_reminders(authorization: Optional[str] = Header(None)):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        reminders = connection.execute(
            "SELECT * FROM reminders WHERE user_id = ?",
            (user_id,)
        ).fetchall()
    return [_reminder_dict(reminder) for reminder in reminders]


@app.get("/reminders/{reminder_id}", response_model=dict)
def get_reminder(
    reminder_id: str,
    authorization: Optional[str] = Header(None)
):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        reminder = _get_user_reminder(connection, reminder_id, user_id)
    return _reminder_dict(reminder)


@app.put("/reminders/{reminder_id}", response_model=dict)
def update_reminder(
    reminder_id: str,
    data: ReminderUpdate,
    authorization: Optional[str] = Header(None)
):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        reminder = _get_user_reminder(connection, reminder_id, user_id)
        updated = _reminder_dict(reminder)
        if data.reminder_time is not None:
            updated["reminder_time"] = data.reminder_time
        if data.status is not None:
            updated["status"] = data.status
        if data.is_active is not None:
            updated["is_active"] = data.is_active
        connection.execute(
            """
            UPDATE reminders
            SET reminder_time = ?, status = ?, is_active = ?
            WHERE id = ? AND user_id = ?
            """,
            (
                updated["reminder_time"],
                updated["status"],
                int(updated["is_active"]),
                reminder_id,
                user_id
            )
        )
    return {"message": "Reminder updated", "reminder": updated}


@app.delete("/reminders/{reminder_id}", status_code=200)
def delete_reminder(
    reminder_id: str,
    authorization: Optional[str] = Header(None)
):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        _get_user_reminder(connection, reminder_id, user_id)
        connection.execute(
            "DELETE FROM reminders WHERE id = ? AND user_id = ?",
            (reminder_id, user_id)
        )
    return {"message": "Reminder deleted successfully"}


@app.post("/notifications/send", status_code=200)
def send_notification(
    notification: NotificationSend,
    authorization: Optional[str] = Header(None)
):
    user_id = verify_token(authorization)
    if notification.user_id != user_id:
        raise HTTPException(status_code=404, detail="User not found")
    log_entry = {
        "id": str(uuid.uuid4()),
        "user_id": user_id,
        "message": notification.message,
        "channel": notification.channel,
        "status": "SENT"
    }
    with db_connection() as connection:
        if connection.execute(
            "SELECT 1 FROM deleted_users WHERE user_id = ?",
            (user_id,)
        ).fetchone() is not None:
            raise HTTPException(status_code=404, detail="User not found")
        connection.execute(
            """
            INSERT INTO notification_log (
                id, user_id, task_id, message, channel, status, created_at
            ) VALUES (?, ?, NULL, ?, ?, 'SENT', ?)
            """,
            (
                log_entry["id"],
                user_id,
                notification.message,
                notification.channel,
                time.time()
            )
        )
    return {"message": "Notification dispatched successfully", "details": log_entry}


@app.get("/notifications", response_model=List[dict])
def get_notifications(authorization: Optional[str] = Header(None)):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        notifications = connection.execute(
            "SELECT * FROM notification_log WHERE user_id = ?",
            (user_id,)
        ).fetchall()
    return [_notification_dict(notification) for notification in notifications]


@app.get("/notifications/{notification_id}", response_model=dict)
def get_notification(
    notification_id: str,
    authorization: Optional[str] = Header(None)
):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        notification = _get_user_notification(connection, notification_id, user_id)
    return _notification_dict(notification)


@app.delete("/notifications/{notification_id}", status_code=200)
def delete_notification(
    notification_id: str,
    authorization: Optional[str] = Header(None)
):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        _get_user_notification(connection, notification_id, user_id)
        connection.execute(
            "DELETE FROM notification_log WHERE id = ? AND user_id = ?",
            (notification_id, user_id)
        )
    return {"message": "Notification log deleted successfully"}
