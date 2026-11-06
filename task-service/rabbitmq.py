import json
import logging
import time

import pika

from database import db_connection


logger = logging.getLogger(__name__)
RABBITMQ_HOST = "rabbitmq"


def publish_event(exchange: str, routing_key: str, message: dict) -> None:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    try:
        channel = connection.channel()
        channel.exchange_declare(
            exchange=exchange,
            exchange_type="topic",
            durable=True
        )
        channel.confirm_delivery()
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(message),
            mandatory=True,
            properties=pika.BasicProperties(delivery_mode=2)
        )
    finally:
        if connection.is_open:
            connection.close()


def publish_outbox_forever() -> None:
    while True:
        event = None
        try:
            now = time.time()
            with db_connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                event = connection.execute(
                    """
                    SELECT event_id, exchange_name, routing_key, body
                    FROM outbox
                    WHERE published_at IS NULL
                      AND available_at <= ?
                      AND (locked_until IS NULL OR locked_until <= ?)
                    ORDER BY created_at
                    LIMIT 1
                    """,
                    (now, now)
                ).fetchone()
                if event is not None:
                    connection.execute(
                        "UPDATE outbox SET locked_until = ?, attempts = attempts + 1 WHERE event_id = ?",
                        (now + 30, event["event_id"])
                    )
        except Exception:
            logger.exception("Could not claim a Task Service outbox event")
            time.sleep(2)
            continue

        if event is None:
            time.sleep(0.5)
            continue

        try:
            publish_event(
                event["exchange_name"],
                event["routing_key"],
                json.loads(event["body"])
            )
            with db_connection() as connection:
                connection.execute(
                    "UPDATE outbox SET published_at = ?, locked_until = NULL WHERE event_id = ?",
                    (time.time(), event["event_id"])
                )
        except Exception:
            logger.exception("Could not publish Task Service outbox event %s", event["event_id"])
            try:
                with db_connection() as connection:
                    connection.execute(
                        """
                        UPDATE outbox
                        SET available_at = ?, locked_until = NULL
                        WHERE event_id = ?
                        """,
                        (time.time() + 5, event["event_id"])
                    )
            except Exception:
                logger.exception("Could not reschedule Task Service outbox event")
            time.sleep(1)


def _consume_forever(queue_setup, callback, consumer_name: str) -> None:
    while True:
        connection = None
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST)
            )
            channel = connection.channel()
            queue_setup(channel)
            channel.basic_qos(prefetch_count=1)

            def wrapper(ch, method, properties, body):
                try:
                    data = json.loads(body)
                    callback(data, method.routing_key)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception:
                    logger.exception(
                        "Could not process %s message from %s",
                        consumer_name,
                        method.routing_key
                    )
                    ch.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=consumer_name == "task-saga"
                    )

            if consumer_name == "user-deleted":
                channel.basic_consume(
                    queue="user_deleted_queue",
                    on_message_callback=wrapper
                )
            else:
                channel.basic_consume(
                    queue="task_saga_results",
                    on_message_callback=wrapper
                )
                channel.basic_consume(
                    queue="task_reminder_dlq",
                    on_message_callback=wrapper
                )
            channel.start_consuming()
        except Exception:
            logger.exception("%s RabbitMQ consumer disconnected; retrying", consumer_name)
            time.sleep(5)
        finally:
            if connection is not None and connection.is_open:
                connection.close()


def start_user_deletion_consumer(callback_function) -> None:
    def setup(channel):
        channel.exchange_declare(
            exchange="todo_exchange",
            exchange_type="topic",
            durable=True
        )
        channel.queue_declare(queue="user_deleted_queue", durable=True)
        channel.queue_bind(
            exchange="todo_exchange",
            queue="user_deleted_queue",
            routing_key="user.deleted"
        )

    _consume_forever(setup, callback_function, "user-deleted")


def start_saga_consumer(callback_function) -> None:
    def setup(channel):
        channel.exchange_declare(
            exchange="todo_exchange",
            exchange_type="topic",
            durable=True
        )
        channel.exchange_declare(
            exchange="todo_dead_letter_exchange",
            exchange_type="topic",
            durable=True
        )
        channel.queue_declare(queue="task_saga_results", durable=True)
        channel.queue_bind(
            exchange="todo_exchange",
            queue="task_saga_results",
            routing_key="saga.reminder.*"
        )
        channel.queue_declare(queue="task_reminder_dlq", durable=True)
        channel.queue_bind(
            exchange="todo_dead_letter_exchange",
            queue="task_reminder_dlq",
            routing_key="task.reminder.requested.dlq"
        )

    _consume_forever(setup, callback_function, "task-saga")
