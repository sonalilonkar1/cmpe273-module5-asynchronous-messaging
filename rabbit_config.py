import os

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "127.0.0.1")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "cmpe273")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "cmpe273")
QUEUE_NAME = os.getenv("QUEUE_NAME", "orders")
EVENT_STORE_PATH = os.getenv("EVENT_STORE_PATH", "processed_events.jsonl")
