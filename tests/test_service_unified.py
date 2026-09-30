"""统一配额在库直调与 HTTP 两种调用方式下一致：同口径、同拒绝原因、可回收。

关键输入与判定依据均打印（pytest -s 可见）。
"""

from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

import main
from coordination import (
    LeaseRejected,
    LeaseService,
    ManualClock,
    RejectionReason,
)


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def service(clock: ManualClock) -> LeaseService:
    return LeaseService(max_concurrent=2, clock=clock)


@pytest.fixture
def http(service: LeaseService) -> TestClient:
    return TestClient(main.create_app(service))


def test_quota_shared_across_library_and_http(service: LeaseService, http: TestClient):
    # 第一笔占用走库直调
    lease = service.acquire("res-1", "c1", ttl=30)
    print(f"[输入] 库直调占用 res-1 -> token={lease.fencing_token}，used={service.used_quota('c1')}")
    # 第二笔占用走 HTTP：两种入口共享同一上限 2
    r = http.post(
        "/leases/acquire",
        json={"resource": "res-2", "client": "c1", "ttl": 30},
    )
    print(f"[输入] HTTP 占用 res-2 -> {r.status_code}，used={service.used_quota('c1')}")
    assert r.status_code == 201
    assert service.used_quota("c1") == 2

    # 第三笔再走库直调：已到上限，必须拒绝
    with pytest.raises(LeaseRejected) as exc:
        service.acquire("res-3", "c1", ttl=30)
    print(f"[判定] 库直调第三笔 -> {exc.value.reason}，应为 QUOTA_EXCEEDED")
    assert exc.value.reason is RejectionReason.QUOTA_EXCEEDED

    # 同一笔从 HTTP 再试：结论一致，且拒绝原因码一致
    r = http.post(
        "/leases/acquire",
        json={"resource": "res-3", "client": "c1", "ttl": 30},
    )
    body = r.json()
    print(f"[判定] HTTP 第三笔 -> {r.status_code} error={body['error']}，应与库直调一致")
    assert r.status_code == 409
    assert body["error"] == RejectionReason.QUOTA_EXCEEDED.value
    assert service.used_quota("c1") == 2


def test_conflict_and_quota_reasons_are_distinct(service: LeaseService, http: TestClient):
    service.acquire("locked", "owner", ttl=30)
    print("[输入] owner 持有 locked，c2 从 HTTP 抢占（非配额问题）")
    r = http.post(
        "/leases/acquire",
        json={"resource": "locked", "client": "c2", "ttl": 30},
    )
    print(f"[判定] 抢占 -> error={r.json()['error']}，应为 held_by_other")
    assert r.status_code == 409
    assert r.json()["error"] == RejectionReason.HELD_BY_OTHER.value

    # c2 自己占满配额后再申请一个空闲资源：应报 quota_exceeded 而非 held_by_other
    service.acquire("free-a", "c2", ttl=30)
    service.acquire("free-b", "c2", ttl=30)
    with pytest.raises(LeaseRejected) as exc:
        service.acquire("free-c", "c2", ttl=30)
    print(f"[判定] 空闲资源但配额已满 -> {exc.value.reason}，应为 QUOTA_EXCEEDED")
    assert exc.value.reason is RejectionReason.QUOTA_EXCEEDED


def test_quota_reclaimed_after_release_and_expiry(
    service: LeaseService, http: TestClient, clock: ManualClock
):
    lease = service.acquire("rel", "c1", ttl=10)
    r = http.post(
        "/leases/release",
        json={"resource": "rel", "client": "c1", "fencing_token": lease.fencing_token},
    )
    print(f"[输入] HTTP 释放 rel -> {r.status_code}")
    assert r.status_code == 200
    assert service.used_quota("c1") == 0
    # 释放后立刻可再占，无残留
    service.acquire("rel", "c1", ttl=5)
    service.acquire("exp", "c1", ttl=5)
    print(f"[输入] c1 再占 2 个 ttl=5 资源至上限，used={service.used_quota('c1')}")
    clock.advance(6)
    # 过期后经 HTTP 查询触发回收，额度应全部恢复
    assert service.used_quota("c1") == 0
    print(f"[判定] 过期回收后 used={service.used_quota('c1')}，HTTP 仍可占满")
    r = http.post(
        "/leases/acquire",
        json={"resource": "another", "client": "c1", "ttl": 5},
    )
    assert r.status_code == 201


def test_quota_isolated_per_client(service: LeaseService):
    service.acquire("a1", "c1", ttl=30)
    service.acquire("a2", "c1", ttl=30)
    with pytest.raises(LeaseRejected) as exc:
        service.acquire("a3", "c1", ttl=30)
    assert exc.value.reason is RejectionReason.QUOTA_EXCEEDED
    # c2 完全不受 c1 上限影响
    service.acquire("b1", "c2", ttl=30)
    service.acquire("b2", "c2", ttl=30)
    print(f"[判定] c1 满额不影响 c2：used c1={service.used_quota('c1')}, "
          f"c2={service.used_quota('c2')}")
    assert service.used_quota("c1") == 2
    assert service.used_quota("c2") == 2


def test_concurrent_burst_never_exceeds_limit(service: LeaseService):
    acquired: list[str] = []
    rejected: list[RejectionReason] = []
    start = threading.Barrier(8)

    def worker(idx: int) -> None:
        start.wait()
        # 全部申请不同资源，使拒绝只能来自配额上限而非资源冲突
        try:
            service.acquire(f"r-{idx}", "burst", ttl=60)
            acquired.append(f"r-{idx}")
        except LeaseRejected as e:
            rejected.append(e.reason)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    held = service.used_quota("burst")
    print("[输入] 8 线程同时为同一客户端抢占不同资源（上限 2）")
    print(f"[判定] 成功={len(acquired)} 拒绝={len(rejected)} held={held}，"
          f"拒绝原因集合={set(rejected)}")
    assert held == 2
    assert len(acquired) == 2
    assert set(rejected) == {RejectionReason.QUOTA_EXCEEDED}
