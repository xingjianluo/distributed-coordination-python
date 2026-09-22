"""HTTP 入口端到端测试：错误码映射、既有根路径行为与配额接口。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from coordination import FakeClock, LeaseStore
from coordination.api import create_app


@pytest.fixture
def clock():
    return FakeClock(start=1000.0)


@pytest.fixture
def client(clock):
    return TestClient(create_app(LeaseStore(clock=clock)))


def test_root_behavior_preserved():
    # 既有行为不得回归：根路由定义在 main:app 上
    from main import app

    with TestClient(app) as c:
        assert c.get("/").json() == {"Hello": "World"}


def test_acquire_renew_release_flow(client):
    r = client.post("/resources/r1/acquire",
                    json={"client_id": "A", "ttl": 10})
    assert r.status_code == 200
    body = r.json()
    assert body["version"] == 1
    assert body["fence_token"] == "r1#1"

    r = client.post("/resources/r1/renew",
                    json={"client_id": "A", "version": 1, "ttl": 20})
    assert r.status_code == 200 and r.json()["renewed"] is True

    r = client.post("/resources/r1/access",
                    json={"client_id": "A", "version": 1})
    assert r.status_code == 200 and r.json()["allowed"] is True

    r = client.post("/resources/r1/release",
                    json={"client_id": "A", "version": 1})
    assert r.status_code == 200 and r.json()["released"] is True


def test_preempt_returns_distinguishable_error(client):
    client.post("/resources/r1/acquire", json={"client_id": "A", "ttl": 10})
    r = client.post("/resources/r1/acquire", json={"client_id": "B", "ttl": 10})
    assert r.status_code == 409
    assert r.json()["error"] == "resource_held"


def test_forged_and_stale_and_double_release(client, clock):
    client.post("/resources/r1/acquire", json={"client_id": "A", "ttl": 5})

    # 伪造持有者
    r = client.post("/resources/r1/release",
                    json={"client_id": "B", "version": 1})
    assert r.status_code == 403 and r.json()["error"] == "forged_holder"

    clock.advance(6)
    # 过期后 B 获取 -> v2
    r = client.post("/resources/r1/acquire", json={"client_id": "B", "ttl": 5})
    assert r.json()["version"] == 2

    # 旧持有者落后凭证访问
    r = client.post("/resources/r1/access",
                    json={"client_id": "A", "version": 1})
    assert r.status_code == 409 and r.json()["error"] == "stale_fence"

    # 过期续租旧凭证
    r = client.post("/resources/r1/renew",
                    json={"client_id": "A", "version": 1})
    assert r.status_code == 409 and r.json()["error"] == "stale_fence"

    # 正常释放后重复释放
    client.post("/resources/r1/release", json={"client_id": "B", "version": 2})
    r = client.post("/resources/r1/release",
                    json={"client_id": "B", "version": 2})
    assert r.status_code == 409 and r.json()["error"] == "already_released"


def test_idempotent_retry_via_request_id(client):
    payload = {"client_id": "A", "ttl": 10, "request_id": "rid-1"}
    r1 = client.post("/resources/r1/acquire", json=payload).json()
    r2 = client.post("/resources/r1/acquire", json=payload).json()
    assert r1 == r2
    snap = client.get("/snapshot").json()
    assert snap["latest_versions"]["r1"] == 1


def test_quota_endpoints_and_rejection(client):
    r = client.put("/quota/A", json={"quota": 1})
    assert r.status_code == 200
    assert client.get("/quota/A").json()["quota"] == 1
    client.post("/resources/r1/acquire", json={"client_id": "A", "ttl": 10})
    r = client.post("/resources/r2/acquire", json={"client_id": "A", "ttl": 10})
    assert r.status_code == 429
    assert r.json()["error"] == "concurrency_quota_exceeded"


def test_invalid_input_returns_422(client):
    r = client.post("/resources/r1/acquire",
                    json={"client_id": "A", "ttl": -1})
    assert r.status_code == 422
