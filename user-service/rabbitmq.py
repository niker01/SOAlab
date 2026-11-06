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
            logger.exception("Could not claim a User Service outbox event")
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
            logger.exception("Could not publish User Service outbox event %s", event["event_id"])
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
                logger.exception("Could not reschedule User Service outbox event")
            time.sleep(1)
