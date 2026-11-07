import json
import logging
import os
import uuid
from uuid import UUID

import httpx
from fastapi import FastAPI

from function_calling import app as function_calling_app
from a2a.client import A2ACardResolver, create_client
from a2a.client.client import ClientCallContext
from a2a.server.agent_execution.agent_executor import AgentExecutor
from a2a.server.agent_execution.context import RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import (
    add_a2a_routes_to_fastapi,
    create_agent_card_routes,
    create_jsonrpc_routes,
    create_rest_routes,
)
from a2a.server.tasks.inmemory_task_store import InMemoryTaskStore
from a2a.server.tasks.task_updater import TaskUpdater
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    AgentSkill,
    Message,
    Part,
    Role,
    SendMessageRequest,
    Task,
    TaskState,
    TaskStatus,
)
from pydantic import BaseModel, ConfigDict, Field

from api_client import (
    APIRequestError,
    TASK_SERVICE_URL,
    api_request,
    get_current_user,
    require_bearer_token,
)


logger = logging.getLogger(__name__)
AGENT_ROLE = os.getenv("AGENT_ROLE", "producer")
PUBLIC_URL = os.getenv("A2A_PUBLIC_URL", "http://localhost:8010").rstrip("/")
CONSUMER_AGENT_URL = os.getenv(
    "CONSUMER_AGENT_URL", "http://consumer-agent:8000"
).rstrip("/")
CONSUMER_AGENT_INTERNAL_RPC_URL = os.getenv(
    "CONSUMER_AGENT_INTERNAL_RPC_URL",
    "http://consumer-agent:8000/a2a/jsonrpc",
)


class ProduceTaskRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=2000)
    due_date: str | None = Field(default=None, max_length=40)


class ConsumeTaskEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: UUID


def _request_token(context: RequestContext) -> str:
    headers = context.call_context.state.get("headers", {})
    authorization = headers.get("authorization") or headers.get("Authorization")
    return require_bearer_token(authorization)


async def _notify_consumer(task: dict, token: str) -> None:
    async with httpx.AsyncClient(timeout=10) as http_client:
        resolver = A2ACardResolver(http_client, CONSUMER_AGENT_URL)
        card = await resolver.get_agent_card()
    if not card.supported_interfaces:
        raise RuntimeError("Consumer Agent Card does not advertise an interface")
    card.supported_interfaces[0].url = CONSUMER_AGENT_INTERNAL_RPC_URL

    client = await create_client(card)
    payload = json.dumps(
        {
            "task_id": task["id"],
        },
        ensure_ascii=False,
    )
    request = Message(
        role=Role.ROLE_USER,
        message_id=str(uuid.uuid4()),
        context_id=str(uuid.uuid4()),
        parts=[Part(text=payload)],
    )
    completed = False
    try:
        async for event in client.send_message(
            SendMessageRequest(message=request),
            context=ClientCallContext(
                service_parameters={"Authorization": f"Bearer {token}"}
            ),
        ):
            if event.HasField("task"):
                status = event.task.status
                state = status.state
            elif event.HasField("status_update"):
                status = event.status_update.status
                state = event.status_update.status.state
            else:
                continue

            if state == TaskState.TASK_STATE_COMPLETED:
                completed = True
            elif state in {
                TaskState.TASK_STATE_FAILED,
                TaskState.TASK_STATE_CANCELED,
                TaskState.TASK_STATE_REJECTED,
            }:
                details = ""
                if status.HasField("message"):
                    details = " ".join(
                        part.text for part in status.message.parts if part.text
                    )
                message = "Consumer Agent failed to process the task event"
                if details:
                    message = f"{message}: {details}"
                raise RuntimeError(message)
    finally:
        await client.close()
    if not completed:
        raise RuntimeError("Consumer Agent did not confirm task-event processing")


class TodoAgentExecutor(AgentExecutor):
    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        message = context.message
        task_id = context.task_id
        context_id = context.context_id
        if message is None or not task_id or not context_id:
            raise ValueError("A2A request must include a task message")

        await event_queue.enqueue_event(
            Task(
                id=task_id,
                context_id=context_id,
                status=TaskStatus(state=TaskState.TASK_STATE_SUBMITTED),
                history=[message],
            )
        )
        updater = TaskUpdater(event_queue, task_id, context_id)
        await updater.start_work()

        try:
            raw_input = context.get_user_input()
            if len(raw_input) > 4000:
                raise ValueError("A2A input exceeds the 4000-character limit")
            data = json.loads(raw_input)
            if not isinstance(data, dict):
                raise ValueError("A2A input must be a JSON object")
            token = _request_token(context)

            if AGENT_ROLE == "producer":
                result = await self._produce_task(data, token)
            else:
                result = await self._consume_task_event(data, token)

            await updater.add_artifact(
                parts=[Part(text=json.dumps(result, ensure_ascii=False))],
                name="result",
                last_chunk=True,
            )
            await updater.complete()
        except Exception as exc:
            logger.exception("A2A %s agent task failed", AGENT_ROLE)
            status_message = updater.new_agent_message(
                parts=[Part(text=str(exc)[:300])]
            )
            await updater.update_status(
                TaskState.TASK_STATE_FAILED,
                message=status_message,
            )

    async def _produce_task(self, data: dict, token: str) -> dict:
        request = ProduceTaskRequest.model_validate(data)
        user = get_current_user(token)
        task_result = api_request(
            TASK_SERVICE_URL,
            "POST",
            "/tasks",
            token,
            {
                "title": request.title,
                "description": request.description,
                "due_date": request.due_date,
            },
        )
        task = task_result.get("task")
        if not isinstance(task, dict) or not task.get("id"):
            raise RuntimeError("Task Service returned an invalid create-task response")
        try:
            await _notify_consumer(task, token)
        except Exception as notification_error:
            try:
                api_request(
                    TASK_SERVICE_URL,
                    "DELETE",
                    f"/tasks/{task['id']}",
                    token,
                )
            except APIRequestError as compensation_error:
                logger.exception(
                    "A2A notification failed and task compensation failed for %s",
                    task["id"],
                )
                raise RuntimeError(
                    "Consumer notification failed and task compensation failed: "
                    f"{compensation_error.detail}"
                ) from notification_error
            raise RuntimeError(
                "Consumer notification failed; the created task was compensated"
            ) from notification_error
        return {
            "task": task,
            "consumer_agent": "Notification Consumer Agent",
            "reminder": (
                "scheduled; notification will be created when it is due"
                if task.get("due_date")
                else "not scheduled because the task has no due date"
            ),
        }

    async def _consume_task_event(self, data: dict, token: str) -> dict:
        request = ConsumeTaskEvent.model_validate(data)
        task_id = str(request.task_id)
        task = api_request(
            TASK_SERVICE_URL,
            "GET",
            f"/tasks/{task_id}",
            token,
        )
        if task.get("id") != task_id:
            raise ValueError("Task Service returned a different task")
        due_date = task.get("due_date")
        return {
            "processed": True,
            "task_id": task_id,
            "reminder": (
                {
                    "status": "scheduled",
                    "reminder_time": due_date,
                    "notification": "will be created when the reminder is due",
                }
                if due_date
                else {
                    "status": "not_scheduled",
                    "notification": "not created because the task has no due date",
                }
            ),
        }

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        if not context.task_id or not context.context_id:
            return
        updater = TaskUpdater(event_queue, context.task_id, context.context_id)
        await updater.cancel()


def _build_app(role: str) -> FastAPI:
    if role == "producer":
        skill = AgentSkill(
            id="create_task_and_schedule_reminder",
            name="Create task and schedule reminder",
            description=(
                "Create a task through the Task REST API. When due_date is set, "
                "the Task Service schedules a reminder and the Notification "
                "Service creates a notification only when the reminder is due. "
                "Delegate the task-created event to the Consumer Agent for "
                "confirmation. The caller must authenticate with a Bearer token "
                "in the A2A HTTP Authorization header. Input is a JSON object "
                "containing title, optional description, and optional ISO 8601 "
                "due_date."
            ),
            tags=["tasks", "producer", "notifications", "a2a"],
            examples=[
                '{"title":"Prepare assignment","description":"Write report","due_date":null}'
            ],
            input_modes=["text"],
            output_modes=["text"],
        )
    else:
        skill = AgentSkill(
            id="process_task_created_event",
            name="Process task-created event",
            description=(
                "Accept a task-created event from the Producer Agent and confirm "
                "the task's due-date reminder status. The Task Service schedules "
                "the reminder; Notification Service creates a notification only "
                "when it is due. Input is a JSON object containing task_id."
            ),
            tags=["consumer", "notifications", "a2a"],
            examples=[
                '{"task_id":"<task-id>"}'
            ],
            input_modes=["text"],
            output_modes=["text"],
        )

    agent_card = AgentCard(
        name="Todo Producer Agent" if role == "producer" else "Todo Consumer Agent",
        description=(
            "Creates tasks, schedules due-date reminders, and delegates task-created "
            "events to another A2A agent for confirmation."
            if role == "producer"
            else "Consumes task-created events and confirms due-date reminder status."
        ),
        version="1.0.0",
        capabilities=AgentCapabilities(streaming=False, push_notifications=False),
        default_input_modes=["text"],
        default_output_modes=["text"],
        skills=[skill],
        supported_interfaces=[
            AgentInterface(
                protocol_binding="JSONRPC",
                protocol_version="1.0",
                url=f"{PUBLIC_URL}/a2a/jsonrpc",
            )
        ],
    )
    handler = DefaultRequestHandler(
        agent_executor=TodoAgentExecutor(),
        task_store=InMemoryTaskStore(),
        agent_card=agent_card,
    )
    app = FastAPI(title=agent_card.name)
    if role == "producer":
        app.mount("/ai", function_calling_app)
    add_a2a_routes_to_fastapi(
        app,
        agent_card_routes=create_agent_card_routes(agent_card),
        jsonrpc_routes=create_jsonrpc_routes(
            request_handler=handler,
            rpc_url="/a2a/jsonrpc",
        ),
        rest_routes=create_rest_routes(
            request_handler=handler,
            path_prefix="/a2a/rest",
        ),
    )

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "agent": role}

    return app


producer_app = _build_app("producer")
consumer_app = _build_app("consumer")
app = producer_app if AGENT_ROLE == "producer" else consumer_app
