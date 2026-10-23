"""Competing RabbitMQ consumer with manual ACK and duplicate protection."""

from __future__ import annotations

import argparse
import os
import sys
import time

import pika

from event_store import claim_event
from messages import decode_order
from rabbit_config import QUEUE_NAME, EVENT_STORE_PATH, RABBITMQ_HOST, RABBITMQ_PASSWORD, RABBITMQ_PORT, RABBITMQ_USER


def connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials,
        heartbeat=60, blocked_connection_timeout=30,
    ))


def run_worker(name: str, crash_event: str | None = None, process_seconds: float = 0.25) -> None:
    with connection() as conn:
        channel = conn.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.basic_qos(prefetch_count=1)

        def on_message(ch, method, properties, body):
            try:
                order = decode_order(body)
            except (ValueError, TypeError) as exc:
                print(f"[{name}] invalid message; rejecting: {exc}", flush=True)
                ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
                return
            event_id = order["event_id"]
            print(f"[{name}] received event_id={event_id} order_id={order['order_id']} redelivered={method.redelivered}", flush=True)
            if not claim_event(event_id, EVENT_STORE_PATH):
                print(f"[{name}] duplicate detected event_id={event_id}; business action skipped", flush=True)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            print(f"[{name}] processing event_id={event_id}", flush=True)
            time.sleep(process_seconds)
            print(f"[{name}] business action complete event_id={event_id}", flush=True)
            if crash_event and event_id == crash_event:
                print(f"[{name}] SIMULATED CRASH before ACK event_id={event_id}", flush=True)
                os._exit(17)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[{name}] ACK event_id={event_id}", flush=True)

        print(f"[{name}] listening queue={QUEUE_NAME} pid={os.getpid()}", flush=True)
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            print(f"[{name}] stopping", flush=True)
            channel.stop_consuming()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default=os.getenv("WORKER_NAME", "worker"))
    parser.add_argument("--crash-event", default=os.getenv("CRASH_EVENT_ID"))
    parser.add_argument("--process-seconds", type=float, default=0.25)
    args = parser.parse_args()
    run_worker(args.name, args.crash_event, args.process_seconds)


if __name__ == "__main__":
    try:
        main()
    except pika.exceptions.AMQPConnectionError as exc:
        print(f"RabbitMQ connection failed: {exc}", file=sys.stderr)
        sys.exit(2)
