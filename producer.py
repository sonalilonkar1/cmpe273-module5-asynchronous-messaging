"""Publish order messages to RabbitMQ."""

from __future__ import annotations

import argparse
import uuid

import pika

from messages import encode_order, make_order
from rabbit_config import QUEUE_NAME, RABBITMQ_HOST, RABBITMQ_PASSWORD, RABBITMQ_PORT, RABBITMQ_USER


def connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials,
        heartbeat=60, blocked_connection_timeout=30,
    ))


def publish_orders(count: int, start: int = 1) -> list[str]:
    if count < 1:
        raise ValueError("count must be at least 1")
    ids: list[str] = []
    with connection() as conn:
        channel = conn.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        for number in range(start, start + count):
            event_id = f"evt-{uuid.uuid4().hex[:10]}"
            order = make_order(event_id, f"order-{number}", "sku-123", 1 + number % 3)
            channel.basic_publish(
                exchange="", routing_key=QUEUE_NAME, body=encode_order(order),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    content_type="application/json", message_id=event_id,
                ),
            )
            ids.append(event_id)
            print(f"[producer] published event_id={event_id} order_id={order['order_id']} quantity={order['quantity']}", flush=True)
    return ids


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=8)
    parser.add_argument("--start", type=int, default=1)
    args = parser.parse_args()
    publish_orders(args.count, args.start)


if __name__ == "__main__":
    main()
