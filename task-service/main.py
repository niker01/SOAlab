import json
import logging
import os
import threading
import time
import uuid
from typing import List, Optional

from fastapi import FastAPI, Header, HTTPException

from database import db_connection, enqueue_event, initialize_database
from rabbitmq import (
    publish_outbox_forever,
    start_saga_consumer,
    start_user_deletion_consumer,
)
from schemas import TaskAssignList, TaskCreate, TaskListCreate, TaskListUpdate, TaskUpdate


app = FastAPI(title="Task Service")
logger = logging.getLogger(__name__)
SAGA_TIMEOUT_SECONDS = float(os.getenv("SAGA_TIMEOUT_SECONDS", "10"))
SAGA_COMPENSATION_WAIT_SECONDS = 3


def _row_dict(row):
    return dict(row) if row is not None else None


def _get_task(connection, task_id: str, user_id: str):
    task = connection.execute(
        "SELECT * FROM tasks WHERE id = ? AND user_id = ?",
        (task_id, user_id)
    ).fetchone()
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


def _get_list(connection, list_id: str, user_id: str):
    task_list = connection.execute(
        "SELECT * FROM task_lists WHERE id = ? AND user_id = ?",
        (list_id, user_id)
    ).fetchone()
    if task_list is None:
        raise HTTPException(status_code=404, detail="List not found")
    return task_list


def _ensure_no_active_saga(connection, task_id: str):
    saga = connection.execute(
        """
        SELECT saga_id FROM sagas
        WHERE task_id = ? AND status IN ('pending', 'compensating')
        LIMIT 1
        """,
        (task_id,)
    ).fetchone()
    if saga is not None:
        raise HTTPException(
            status_code=409,
            detail="A reminder operation for this task is still in progress"
        )


def verify_token(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    token = authorization[len("Bearer "):].strip()
    token_prefix = "fake-jwt-token-"
    if not token.startswith(token_prefix) or not token[len(token_prefix):]:
        raise HTTPException(status_code=401, detail="Invalid token")
    return token[len(token_prefix):]


def compensate_saga(saga_id: str, reason: str) -> None:
    with db_connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        saga = connection.execute(
            "SELECT * FROM sagas WHERE saga_id = ?",
            (saga_id,)
        ).fetchone()
        if saga is None or saga["status"] != "pending":
            return

        before_snapshot = (
            json.loads(saga["before_snapshot"])
            if saga["before_snapshot"] is not None
            else None
        )
        if saga["action"] == "create":
            connection.execute("DELETE FROM tasks WHERE id = ?", (saga["task_id"],))
        elif before_snapshot is not None:
            connection.execute(
                """
                UPDATE tasks
                SET title = ?, description = ?, list_id = ?, status = ?, due_date = ?
                WHERE id = ?
                """,
                (
                    before_snapshot["title"],
                    before_snapshot["description"],
                    before_snapshot["list_id"],
                    before_snapshot["status"],
                    before_snapshot["due_date"],
                    saga["task_id"]
                )
            )

        enqueue_event(
            connection,
            "task.reminder.compensate",
            {
                "event_type": "task.reminder.compensate",
                "saga_id": saga_id,
                "task_id": saga["task_id"],
                "action": saga["action"]
            },
            event_id=f"{saga_id}:compensate"
        )
        connection.execute(
            """
            UPDATE sagas
            SET status = 'compensating', error = ?, updated_at = ?
            WHERE saga_id = ?
            """,
            (reason, time.time(), saga_id)
        )


def handle_saga_message(data: dict, routing_key: str) -> None:
    saga_id = data.get("saga_id")
    if not saga_id:
        logger.error("Ignoring RabbitMQ saga message without saga_id: %s", routing_key)
        return

    if routing_key == "task.reminder.requested.dlq":
        compensate_saga(saga_id, "Notification Service rejected the reminder event")
        return

    with db_connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        saga = connection.execute(
            "SELECT status FROM sagas WHERE saga_id = ?",
            (saga_id,)
        ).fetchone()
        if saga is None:
            logger.warning("Ignoring result for unknown saga %s", saga_id)
            return

        if routing_key == "saga.reminder.succeeded" and saga["status"] == "pending":
            connection.execute(
                "UPDATE sagas SET status = 'completed', updated_at = ? WHERE saga_id = ?",
                (time.time(), saga_id)
            )
        elif routing_key == "saga.reminder.compensated" and saga["status"] == "compensating":
            connection.execute(
                "UPDATE sagas SET status = 'failed', updated_at = ? WHERE saga_id = ?",
                (time.time(), saga_id)
            )


def wait_for_saga(saga_id: str) -> None:
    started_at = time.monotonic()
    compensation_started = False
    while True:
        with db_connection() as connection:
            saga = connection.execute(
                "SELECT status, error, deadline_at, updated_at FROM sagas WHERE saga_id = ?",
                (saga_id,)
            ).fetchone()
        if saga is None:
            raise HTTPException(status_code=503, detail="Task reminder workflow disappeared")
        if saga["status"] == "completed":
            return
        if saga["status"] == "failed":
            raise HTTPException(
                status_code=503,
                detail="Task reminder workflow failed and task changes were rolled back"
            )

        elapsed = time.monotonic() - started_at
        if saga["status"] == "pending" and elapsed >= SAGA_TIMEOUT_SECONDS:
            compensate_saga(saga_id, "Timed out waiting for Notification Service")
            compensation_started = True
        elif (
            saga["status"] == "compensating"
            and time.time() - saga["updated_at"] >= SAGA_COMPENSATION_WAIT_SECONDS
        ):
            compensation_started = True

        if compensation_started and saga["status"] == "compensating":
            raise HTTPException(
                status_code=503,
                detail="Task reminder workflow timed out; compensation is pending"
            )
        time.sleep(0.1)


def handle_user_deleted_event(data: dict, _routing_key: str) -> None:
    user_id = data.get("user_id")
    if not user_id:
        raise ValueError("User-deleted event must include user_id")

    with db_connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        task_ids = [
            row["id"]
            for row in connection.execute(
                "SELECT id FROM tasks WHERE user_id = ?",
                (user_id,)
            ).fetchall()
        ]
        for task_id in task_ids:
            enqueue_event(
                connection,
                "task.deleted",
                {"event_type": "task.deleted", "task_id": task_id},
                event_id=f"user-deleted:{user_id}:task-deleted:{task_id}"
            )
        connection.execute("DELETE FROM task_lists WHERE user_id = ?", (user_id,))
        connection.execute("DELETE FROM tasks WHERE user_id = ?", (user_id,))


def check_saga_timeouts_forever() -> None:
    while True:
        try:
            now = time.time()
            with db_connection() as connection:
                expired = connection.execute(
                    """
                    SELECT saga_id FROM sagas
                    WHERE status = 'pending' AND deadline_at <= ?
                    """,
                    (now,)
                ).fetchall()
            for saga in expired:
                compensate_saga(saga["saga_id"], "Timed out waiting for Notification Service")
        except Exception:
            logger.exception("Could not check for expired reminder sagas")
        time.sleep(0.5)


@app.on_event("startup")
def startup_event():
    initialize_database()
    for target, args in (
        (publish_outbox_forever, ()),
        (start_user_deletion_consumer, (handle_user_deleted_event,)),
        (start_saga_consumer, (handle_saga_message,)),
        (check_saga_timeouts_forever, ())
    ):
        threading.Thread(target=target, args=args, daemon=True).start()


@app.post("/lists", status_code=201)
def create_list(data: TaskListCreate, authorization: Optional[str] = Header(None)):
    user_id = verify_token(authorization)
    list_id = str(uuid.uuid4())
    task_list = {"id": list_id, "user_id": user_id, "title": data.title}
    with db_connection() as connection:
        connection.execute(
            "INSERT INTO task_lists (id, user_id, title) VALUES (?, ?, ?)",
            (list_id, user_id, data.title)
        )
    return {"message": "List created", "list": task_list}


@app.get("/lists", response_model=List[dict])
def get_lists(authorization: Optional[str] = Header(None)):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        lists = connection.execute(
            "SELECT id, user_id, title FROM task_lists WHERE user_id = ?",
            (user_id,)
        ).fetchall()
    return [_row_dict(task_list) for task_list in lists]


@app.get("/lists/{list_id}", response_model=dict)
def get_list(list_id: str, authorization: Optional[str] = Header(None)):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        return _row_dict(_get_list(connection, list_id, user_id))


@app.put("/lists/{list_id}", response_model=dict)
def update_list(
    list_id: str,
    data: TaskListUpdate,
    authorization: Optional[str] = Header(None)
):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        task_list = _get_list(connection, list_id, user_id)
        connection.execute(
            "UPDATE task_lists SET title = ? WHERE id = ?",
            (data.title, list_id)
        )
    task_list = {**_row_dict(task_list), "title": data.title}
    return {"message": "List updated", "list": task_list}


@app.delete("/lists/{list_id}", status_code=200)
def delete_list(list_id: str, authorization: Optional[str] = Header(None)):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        _get_list(connection, list_id, user_id)
        affected_tasks = connection.execute(
            "SELECT id, user_id, title, status FROM tasks WHERE list_id = ?",
            (list_id,)
        ).fetchall()
        for task in affected_tasks:
            _ensure_no_active_saga(connection, task["id"])
            enqueue_event(
                connection,
                "task.updated",
                {
                    "event_type": "task.updated",
                    "task_id": task["id"],
                    "user_id": task["user_id"],
                    "title": task["title"],
                    "status": task["status"],
                    "list_id": None
                }
            )
        connection.execute("DELETE FROM task_lists WHERE id = ?", (list_id,))
    return {"message": "List deleted successfully"}


@app.post("/tasks", status_code=201)
def create_task(task: TaskCreate, authorization: Optional[str] = Header(None)):
    user_id = verify_token(authorization)
    task_id = str(uuid.uuid4())
    new_task = {
        "id": task_id,
        "title": task.title,
        "description": task.description,
        "list_id": task.list_id,
        "status": "PENDING",
        "due_date": task.due_date
    }

    saga_id = None
    with db_connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        if task.list_id:
            _get_list(connection, task.list_id, user_id)
        connection.execute(
            """
            INSERT INTO tasks (id, user_id, title, description, list_id, status, due_date)
            VALUES (?, ?, ?, ?, ?, 'PENDING', ?)
            """,
            (
                task_id,
                user_id,
                task.title,
                task.description,
                task.list_id,
                task.due_date
            )
        )
        if task.due_date:
            saga_id = str(uuid.uuid4())
            now = time.time()
            connection.execute(
                """
                INSERT INTO sagas (
                    saga_id, task_id, action, status, before_snapshot,
                    created_at, deadline_at, updated_at
                ) VALUES (?, ?, 'create', 'pending', NULL, ?, ?, ?)
                """,
                (saga_id, task_id, now, now + SAGA_TIMEOUT_SECONDS, now)
            )
            enqueue_event(
                connection,
                "task.reminder.requested",
                {
                    "event_type": "task.reminder.requested",
                    "saga_id": saga_id,
                    "task_id": task_id,
                    "user_id": user_id,
                    "title": task.title,
                    "reminder_time": task.due_date
                }
            )
        else:
            enqueue_event(
                connection,
                "task.created",
                {
                    "event_type": "task.created",
                    "task_id": task_id,
                    "user_id": user_id,
                    "title": task.title
                }
            )

    if saga_id:
        wait_for_saga(saga_id)
    return {"message": "Task created", "task": new_task}


@app.get("/tasks", response_model=List[dict])
def get_tasks(authorization: Optional[str] = Header(None)):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        tasks = connection.execute(
            """
            SELECT id, title, description, list_id, status, due_date
            FROM tasks WHERE user_id = ?
            """,
            (user_id,)
        ).fetchall()
    return [_row_dict(task) for task in tasks]


@app.get("/tasks/{task_id}", response_model=dict)
def get_task(task_id: str, authorization: Optional[str] = Header(None)):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        task = _get_task(connection, task_id, user_id)
    return {
        key: value
        for key, value in _row_dict(task).items()
        if key != "user_id"
    }


@app.put("/tasks/{task_id}", response_model=dict)
def update_task(
    task_id: str,
    data: TaskUpdate,
    authorization: Optional[str] = Header(None)
):
    user_id = verify_token(authorization)
    saga_id = None
    with db_connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        task = _get_task(connection, task_id, user_id)
        _ensure_no_active_saga(connection, task_id)
        updated = _row_dict(task)
        for field in ("title", "description", "status", "due_date"):
            value = getattr(data, field)
            if value is not None:
                updated[field] = value
        due_date_changed = (
            data.due_date is not None and data.due_date != task["due_date"]
        )
        connection.execute(
            """
            UPDATE tasks
            SET title = ?, description = ?, status = ?, due_date = ?
            WHERE id = ?
            """,
            (
                updated["title"],
                updated["description"],
                updated["status"],
                updated["due_date"],
                task_id
            )
        )
        if due_date_changed:
            saga_id = str(uuid.uuid4())
            now = time.time()
            snapshot = {
                field: task[field]
                for field in ("title", "description", "list_id", "status", "due_date")
            }
            connection.execute(
                """
                INSERT INTO sagas (
                    saga_id, task_id, action, status, before_snapshot,
                    created_at, deadline_at, updated_at
                ) VALUES (?, ?, 'update', 'pending', ?, ?, ?, ?)
                """,
                (
                    saga_id,
                    task_id,
                    json.dumps(snapshot),
                    now,
                    now + SAGA_TIMEOUT_SECONDS,
                    now
                )
            )
            enqueue_event(
                connection,
                "task.reminder.requested",
                {
                    "event_type": "task.reminder.requested",
                    "saga_id": saga_id,
                    "task_id": task_id,
                    "user_id": user_id,
                    "title": updated["title"],
                    "reminder_time": updated["due_date"]
                }
            )
        else:
            enqueue_event(
                connection,
                "task.updated",
                {
                    "event_type": "task.updated",
                    "task_id": task_id,
                    "user_id": user_id,
                    "title": updated["title"],
                    "status": updated["status"]
                }
            )

    if saga_id:
        wait_for_saga(saga_id)
    updated.pop("user_id")
    return {"message": "Task updated", "task": updated}


@app.patch("/tasks/{task_id}/assign-list")
def assign_task_to_list(
    task_id: str,
    data: TaskAssignList,
    authorization: Optional[str] = Header(None)
):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        task = _get_task(connection, task_id, user_id)
        _ensure_no_active_saga(connection, task_id)
        _get_list(connection, data.list_id, user_id)
        connection.execute(
            "UPDATE tasks SET list_id = ? WHERE id = ?",
            (data.list_id, task_id)
        )
        enqueue_event(
            connection,
            "task.updated",
            {
                "event_type": "task.updated",
                "task_id": task_id,
                "user_id": user_id,
                "title": task["title"],
                "status": task["status"],
                "list_id": data.list_id
            }
        )
        updated = {**_row_dict(task), "list_id": data.list_id}
    updated.pop("user_id")
    return {"message": "Task assigned to list", "task": updated}


@app.delete("/tasks/{task_id}", status_code=200)
def delete_task(task_id: str, authorization: Optional[str] = Header(None)):
    user_id = verify_token(authorization)
    with db_connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        _get_task(connection, task_id, user_id)
        _ensure_no_active_saga(connection, task_id)
        enqueue_event(
            connection,
            "task.deleted",
            {"event_type": "task.deleted", "task_id": task_id}
        )
        connection.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
    return {"message": "Task deleted successfully"}
