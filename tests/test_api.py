"""HTTP 入口测试：既有行为不回归 + 错误分类经 API 可区分。"""

from __future__ import annotations

from fastapi.testclient import TestClient

import main

client = TestClient(main.app)


def test_root_behavior_unchanged():
    r = client.get("/")
    print(f"[判定] GET / -> {r.status_code} {r.json()}，既有行为不回归")
    assert r.status_code == 200
    assert r.json() == {"Hello": "World"}


def test_acquire_preempt_release_cycle():
    r = client.post("/leases/acquire", json={"resource": "api-res", "client": "a", "ttl": 30})
    assert r.status_code == 201
    token = r.json()["fencing_token"]
    print(f"[输入] a 获取 api-res -> token={token}")

    r = client.post("/leases/acquire", json={"resource": "api-res", "client": "b", "ttl": 30})
    print(f"[判定] b 抢占 -> {r.status_code} reason={r.json()['error']}")
    assert r.status_code == 409
    assert r.json()["error"] == "held_by_other"

    r = client.post("/leases/release", json={
        "resource": "api-res", "client": "b", "fencing_token": token})
    print(f"[判定] b 伪造身份释放 -> reason={r.json()['error']}")
    assert r.status_code == 409
    assert r.json()["error"] == "not_holder"

    r = client.post("/leases/release", json={
        "resource": "api-res", "client": "a", "fencing_token": token})
    assert r.status_code == 200
    r = client.get("/leases/api-res")
    print(f"[判定] 释放后 holder={r.json()['lease']}，应为 None")
    assert r.json()["lease"] is None


def test_stale_token_via_api():
    r = client.post("/leases/acquire", json={"resource": "api-res2", "client": "a", "ttl": 30})
    old_token = r.json()["fencing_token"]
    r = client.post("/leases/renew", json={
        "resource": "api-res2", "client": "a", "fencing_token": old_token, "ttl": 30})
    assert r.status_code == 200
    r = client.post("/leases/guarded", json={
        "resource": "api-res2", "fencing_token": old_token})
    print(f"[判定] 续租后携带旧 token={old_token} 访问 -> reason={r.json()['error']}")
    assert r.status_code == 409
    assert r.json()["error"] == "stale_fencing_token"
