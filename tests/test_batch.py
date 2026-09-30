"""批量占用 acquire_batch：全部成功或整体失败（all-or-nothing）。

- 任一资源被他人持有 / 配额不足：整笔失败，不留下半组持有、不占配额；
- 并发下同一资源不会双持有；判定在写入前完成，立即返回不排队干等；
- HTTP 端点 POST /leases/acquire-batch 与库直调结论一致。
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
    return LeaseService(max_concurrent=4, clock=clock)


@pytest.fixture
def http(service: LeaseService) -> TestClient:
    return TestClient(main.create_app(service))


def test_batch_all_success(service: LeaseService):
    leases = service.acquire_batch(["r1", "r2", "r3"], "c1", ttl=10)
    print(f"[输入] 一次申请 r1,r2,r3 -> 返回 {len(leases)} 个租约")
    print(f"[判定] 全部持有者={[l.holder for l in leases]}，used={service.used_quota('c1')}")
    assert [l.resource for l in leases] == ["r1", "r2", "r3"]
    assert all(l.holder == "c1" for l in leases)
    assert service.used_quota("c1") == 3


def test_batch_conflict_rolls_back_everything(service: LeaseService):
    service.acquire("taken", "owner", ttl=10)
    with pytest.raises(LeaseRejected) as exc:
        service.acquire_batch(["free-1", "taken", "free-2"], "c1", ttl=10)
    print("[输入] 批量含被 owner 占用的 taken")
    print(f"[判定] 拒绝={exc.value.reason}，应为 HELD_BY_OTHER；其余资源不得被占")
    assert exc.value.reason is RejectionReason.HELD_BY_OTHER
    assert service.holder_of("free-1") is None
    assert service.holder_of("free-2") is None
    assert service.used_quota("c1") == 0
    assert service.holder_of("taken").holder == "owner"


def test_batch_quota_shortfall_rolls_back_everything(service: LeaseService):
    service.acquire("held", "c1", ttl=10)  # 已用 1/4
    with pytest.raises(LeaseRejected) as exc:
        service.acquire_batch(["q1", "q2", "q3", "q4"], "c1", ttl=10)  # 需 4，只剩 3
    print("[输入] 已占 1 个（上限 4），批量新增需 4 个")
    print(f"[判定] 拒绝={exc.value.reason}，应为 QUOTA_EXCEEDED；整笔不留痕")
    assert exc.value.reason is RejectionReason.QUOTA_EXCEEDED
    for name in ("q1", "q2", "q3", "q4"):
        assert service.holder_of(name) is None
    assert service.used_quota("c1") == 1


def test_batch_with_already_held_resources_counts_only_new(service: LeaseService):
    existing = service.acquire_batch(["r1", "r2"], "c1", ttl=10)  # 用掉 2/4
    # r1/r2 自己仍持有（续租不占新许可），只需为 r3 新增 1 个
    leases = service.acquire_batch(["r1", "r2", "r3"], "c1", ttl=10)
    print(f"[输入] 批量含自己持有的 r1,r2 + 新 r3，返回 {len(leases)} 个")
    print(f"[判定] used={service.used_quota('c1')}，应为 3 而非 5")
    assert len(leases) == 3
    assert service.used_quota("c1") == 3
    # r1/r2 的 token 因续租递增，但仍是 c1 唯一持有
    assert leases[0].fencing_token > existing[0].fencing_token
    assert service.holder_of("r1").holder == "c1"


def test_batch_duplicate_resources_are_deduplicated(service: LeaseService):
    leases = service.acquire_batch(["r1", "r1", "r2"], "c1", ttl=10)
    print(f"[判定] 重复资源去重后返回 {[l.resource for l in leases]}，只占 2 个配额")
    assert [l.resource for l in leases] == ["r1", "r2"]
    assert service.used_quota("c1") == 2


def test_concurrent_batches_never_double_hold_and_always_return(service: LeaseService):
    # 两个客户端同时批量申请一组重叠资源（上限足够，冲突只可能来自重叠资源）
    results: dict[str, object] = {}
    barrier = threading.Barrier(2)

    def run(client: str) -> None:
        barrier.wait()
        try:
            leases = service.acquire_batch(["s1", "s2", "s3"], client, ttl=60)
            results[client] = [l.resource for l in leases]
        except LeaseRejected as e:
            results[client] = e.reason

    threads = [
        threading.Thread(target=run, args=("c1",)),
        threading.Thread(target=run, args=("c2",)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    assert not any(t.is_alive() for t in threads), "不得干等不返回"

    print("[输入] c1/c2 并发批量申请同一组 s1,s2,s3")
    print(f"[判定] c1 结果={results['c1']}；c2 结果={results['c2']}")
    winners = [c for c, v in results.items() if isinstance(v, list)]
    losers = [c for c, v in results.items() if v is RejectionReason.HELD_BY_OTHER]
    assert len(winners) == 1, "整组资源只能被一个客户端拿走"
    assert len(losers) == 1
    for resource in ("s1", "s2", "s3"):
        assert service.holder_of(resource).holder == winners[0]


def test_batch_http_endpoint_matches_library(service: LeaseService, http: TestClient):
    r = http.post(
        "/leases/acquire-batch",
        json={"resources": ["w1", "w2"], "client": "c1", "ttl": 20,
              "idempotency_key": "http-bk"},
    )
    print(f"[输入] HTTP 批量申请 -> {r.status_code}")
    assert r.status_code == 201
    assert [l["resource"] for l in r.json()["leases"]] == ["w1", "w2"]
    # 幂等重放经 HTTP 结果一致
    r2 = http.post(
        "/leases/acquire-batch",
        json={"resources": ["w1", "w2"], "client": "c1", "ttl": 20,
              "idempotency_key": "http-bk"},
    )
    assert r2.json() == r.json()
    # 冲突整体失败：409 + 可区分原因，且 w3 未被占
    r3 = http.post(
        "/leases/acquire-batch",
        json={"resources": ["w1", "w3"], "client": "c2", "ttl": 20},
    )
    print(f"[判定] HTTP 冲突批量 -> {r3.status_code} error={r3.json()['error']}")
    assert r3.status_code == 409
    assert r3.json()["error"] == RejectionReason.HELD_BY_OTHER.value
    assert service.holder_of("w3") is None
