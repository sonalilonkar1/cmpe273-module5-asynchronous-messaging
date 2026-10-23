from __future__ import annotations

import json
from typing import Any

REQUIRED_FIELDS = {"event_id", "order_id", "item_id", "quantity"}


def make_order(event_id: str, order_id: str, item_id: str, quantity: int) -> dict[str, Any]:
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    return {"event_id": event_id, "order_id": order_id, "item_id": item_id, "quantity": quantity}


def encode_order(order: dict[str, Any]) -> bytes:
    missing = REQUIRED_FIELDS - order.keys()
    if missing:
        raise ValueError(f"missing fields: {sorted(missing)}")
    return json.dumps(order, separators=(",", ":")).encode("utf-8")


def decode_order(body: bytes) -> dict[str, Any]:
    value = json.loads(body.decode("utf-8"))
    if not isinstance(value, dict) or not REQUIRED_FIELDS.issubset(value):
        raise ValueError("message must be an order object with required fields")
    if not isinstance(value["quantity"], int) or value["quantity"] <= 0:
        raise ValueError("quantity must be a positive integer")
    return value
