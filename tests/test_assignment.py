from __future__ import annotations

import json
import multiprocessing
from pathlib import Path

import pytest

from event_store import claim_event
from messages import decode_order, encode_order, make_order


def test_order_round_trip():
    order = make_order("evt-1", "order-1", "sku-123", 2)
    assert decode_order(encode_order(order)) == order


def test_invalid_quantity_is_rejected():
    with pytest.raises(ValueError):
        make_order("evt-1", "order-1", "sku-123", 0)


def test_duplicate_event_is_claimed_only_once(tmp_path: Path):
    store = tmp_path / "events.jsonl"
    assert claim_event("evt-1", store) is True
    assert claim_event("evt-1", store) is False
    assert claim_event("evt-2", store) is True


def _claim_worker(path: str, connection):
    connection.send(claim_event("same-event", path))
    connection.close()


def test_claim_is_safe_for_competing_processes(tmp_path: Path):
    store = str(tmp_path / "events.jsonl")
    pipes = [multiprocessing.Pipe(False) for _ in range(2)]
    processes = [multiprocessing.Process(target=_claim_worker, args=(store, child)) for _, child in pipes]
    for process in processes:
        process.start()
    results = [parent.recv() for parent, _ in pipes]
    for process in processes:
        process.join()
    assert sorted(results) == [False, True]


def test_payload_is_json():
    payload = encode_order(make_order("evt-2", "order-2", "sku-9", 3))
    assert json.loads(payload) == {"event_id": "evt-2", "order_id": "order-2", "item_id": "sku-9", "quantity": 3}
