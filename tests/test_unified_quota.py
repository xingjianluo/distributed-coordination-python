"""统一配额测试：库调用与 HTTP 共享同一 Coordinator，按客户端的并发持有数计费。

覆盖：跨调用方式结论一致、按客户端隔离、释放/过期/失败回收许可、
短时间突发并发不超上限，且 quota_exceeded 可与其它拒绝原因区分。
关键输入与判定依据均打印，pytest -s 可见。
"""

from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

import main
from coordination import (
    Coordinator,
    LeaseRejected,
    ManualClock,
    RejectionReason,
)

shared = main.coordinator  # 库方式与 HTTP 共用的同一单例
http = TestClient(main.app)


def test_quota_consistent_across_library_and_http():
    client_id = "q-unified"
    max_per_client = 8
    print(f"[输入] 客户端 {client_id} 库方式占用 7 个资源，再从 HTTP 占用 1 个")
    lib_leases = [
        shared.acquire(f"q-unified-{i}", client_id, ttl=300) for i in range(7)
    ]
    r = http.post("/leases/acquire", json={
        "resource": "q-unified-http", "client": client_id, "ttl": 300})
    assert r.status_code == 201, r.text
    eighth_token = r.json()["fencing_token"]
    print(f"[判定] 第 8 个占用（HTTP）成功 token={eighth_token}，"
          f"in_flight={shared.in_flight(client_id)}")
    assert shared.in_flight(client_id) == max_per_client

    # 第 9 个：库方式与 HTTP 都必须拒绝，原因同为 quota_exceeded
    with pytest.raises(LeaseRejected) as exc:
        shared.acquire("q-unified-over-lib", client_id, ttl=300)
    print(f"[判定] 库方式第 9 个拒绝原因={exc.value.reason}，应为 QUOTA_EXCEEDED")
    assert exc.value.reason is RejectionReason.QUOTA_EXCEEDED

    r = http.post("/leases/acquire", json={
        "resource": "q-unified-over-http", "client": client_id, "ttl": 300})
    print(f"[判定] HTTP 第 9 个 -> {r.status_code} error={r.json()['error']}")
    assert r.status_code == 409
    assert r.json()["error"] == "quota_exceeded"
    assert shared.in_flight(client_id) == max_per_client  # 拒绝不留残留

    # 经 HTTP 释放第 8 个 -> 许可回收，库方式立即能再占
    r = http.post("/leases/release", json={
        "resource": "q-unified-http", "client": client_id,
        "fencing_token": eighth_token})
    assert r.status_code == 200
    print(f"[判定] HTTP 释放一个后 in_flight={shared.in_flight(client_id)}，应为 7")
    assert shared.in_flight(client_id) == 7
    lease = shared.acquire("q-unified-reclaim", client_id, ttl=300)
    print(f"[判定] 回收后库方式再占成功 token={lease.fencing_token}，in_flight 回到 8")
    assert shared.in_flight(client_id) == 8

    # 清理，避免污染共享单例的后续用例
    for l in lib_leases:
        shared.release(l.resource, client_id, l.fencing_token)
    shared.release("q-unified-reclaim", client_id, lease.fencing_token)
    assert shared.in_flight(client_id) == 0


def test_quota_isolated_per_client():
    clock = ManualClock()
    coord = Coordinator(clock, max_concurrent_per_client=2)
    a1 = coord.acquire("iso-a1", "A", ttl=10)
    a2 = coord.acquire("iso-a2", "A", ttl=10)
    with pytest.raises(LeaseRejected) as exc:
        coord.acquire("iso-a3", "A", ttl=10)
    assert exc.value.reason is RejectionReason.QUOTA_EXCEEDED
    # B 的上限独立：A 打满不影响 B
    b1 = coord.acquire("iso-b1", "B", ttl=10)
    print(f"[输入] A 打满 2 个后再申请被拒；B 独立占用成功 token={b1.fencing_token}")
    print(f"[判定] A 在途={coord.in_flight('A')}，B 在途={coord.in_flight('B')}，互不影响")
    assert coord.in_flight("A") == 2 and coord.in_flight("B") == 1

    # 释放后许可回收
    coord.release("iso-a1", "A", a1.fencing_token)
    coord.release("iso-a2", "A", a2.fencing_token)
    print(f"[判定] A 全部释放后在途={coord.in_flight('A')}，应为 0")
    assert coord.in_flight("A") == 0
    coord.acquire("iso-a1", "A", ttl=10)
    coord.release("iso-b1", "B", b1.fencing_token)


def test_failed_acquire_reclaims_permit():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=2)
    coord.acquire("conflict-local", "holder", ttl=10)

    with pytest.raises(LeaseRejected) as exc:
        coord.acquire("conflict-local", "other", ttl=10)
    assert exc.value.reason is RejectionReason.HELD_BY_OTHER
    print("[输入] other 抢占 holder 的资源失败（库方式）")
    print(f"[判定] 失败后 other 在途={coord.in_flight('other')}，应为 0（无残留）")
    assert coord.in_flight("other") == 0

    # HTTP 路径（共享单例）失败同样不残留，且原因码可区分
    shared.acquire("conflict-shared", "holder", ttl=300)
    r = http.post("/leases/acquire", json={
        "resource": "conflict-shared", "client": "http-other", "ttl": 300})
    print(f"[判定] HTTP 抢占 -> {r.status_code} error={r.json()['error']}")
    assert r.status_code == 409 and r.json()["error"] == "held_by_other"
    assert shared.in_flight("http-other") == 0
    print("[判定] held_by_other 与 quota_exceeded 是不同原因码，可区分")
    held = shared.holder_of("conflict-shared")
    shared.release("conflict-shared", "holder", held.fencing_token)


def test_expired_hold_reclaims_permit():
    clock = ManualClock()
    coord = Coordinator(clock, max_concurrent_per_client=2)
    coord.acquire("exp-1", "A", ttl=5)
    coord.acquire("exp-2", "A", ttl=5)
    clock.advance(6)
    print("[输入] A 打满 2 个，TTL=5，时钟前进 6s")
    assert coord.live_hold_count("A") == 0
    assert coord.in_flight("A") == 0
    print(f"[判定] 过期回收后在途={coord.in_flight('A')}，新占用可立即成功")
    lease = coord.acquire("exp-3", "A", ttl=5)
    assert lease.holder == "A"


def test_same_holder_reacquire_does_not_consume_permit():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=2)
    first = coord.acquire("same", "A", ttl=10)
    second = coord.acquire("same", "A", ttl=10)  # 同持有者重复占用=续租式占位
    print(f"[输入] A 重复占用同一资源：token {first.fencing_token} -> {second.fencing_token}")
    print(f"[判定] 在途={coord.in_flight('A')}，应为 1（不多占）")
    assert coord.in_flight("A") == 1
    coord.release("same", "A", second.fencing_token)
    assert coord.in_flight("A") == 0


def test_burst_concurrency_never_exceeds_quota():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=4)
    peak = 0
    peak_lock = threading.Lock()

    def worker(idx: int) -> None:
        nonlocal peak
        for round_ in range(25):
            resource = f"burst-{idx}-{round_}"
            try:
                coord.acquire(resource, "A", ttl=10)
                with peak_lock:
                    peak = max(peak, coord.in_flight("A"))
            except LeaseRejected as e:
                assert e.reason is RejectionReason.QUOTA_EXCEEDED

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print("[输入] 8 线程短时间并发向 max=4 的 A 申请互不相同的资源")
    print(f"[判定] 峰值在途={peak}，应 <= 4；最终在途={coord.in_flight('A')}")
    assert peak <= 4

    # 全部释放后归零
    live = [
        (r, l)
        for r in (f"burst-{i}-{r_}" for i in range(8) for r_ in range(25))
        if (l := coord.holder_of(r)) is not None
    ]
    for r, l in live:
        coord.release(r, "A", l.fencing_token)
    print(f"[判定] 全部释放后在途={coord.in_flight('A')}，应为 0")
    assert coord.in_flight("A") == 0
