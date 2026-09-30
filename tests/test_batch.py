"""批量占用测试：一组资源全部成功或整体失败。

覆盖：全部成功；任一被他人持有则整单失败、已拿到的部分必须回滚；
配额不足整单失败不留部分占用；并发争抢下原子且无双持有、不无限等待；
批量同样支持幂等重放；库方式与 HTTP 结论一致。
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

shared = main.coordinator
http = TestClient(main.app)


def test_batch_all_success_returns_in_order():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=5)
    leases = coord.acquire_batch(["b-1", "b-2", "b-3"], "A", ttl=10)
    print(f"[输入] A 批量申请 [b-1,b-2,b-3] -> {[l.fencing_token for l in leases]}")
    assert [l.resource for l in leases] == ["b-1", "b-2", "b-3"]
    assert coord.in_flight("A") == 3
    for r in ["b-1", "b-2", "b-3"]:
        assert coord.holder_of(r).holder == "A"


def test_batch_rolls_back_when_one_held_by_other():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=10)
    coord.acquire("taken", "holder", ttl=10)
    with pytest.raises(LeaseRejected) as exc:
        coord.acquire_batch(["free-1", "taken", "free-2"], "A", ttl=10)
    print("[输入] 批量 [free-1, taken, free-2]，其中 taken 被 holder 占着")
    print(f"[判定] 整单拒绝原因={exc.value.reason}，应为 HELD_BY_OTHER")
    assert exc.value.reason is RejectionReason.HELD_BY_OTHER
    # 已判定阶段不触碰状态：free-1/free-2 绝不能留下部分占用，许可也回收
    for r in ["free-1", "free-2"]:
        assert coord.holder_of(r) is None
    print(f"[判定] free-1/free-2 均无人持有，A 在途={coord.in_flight('A')} 应为 0")
    assert coord.in_flight("A") == 0
    assert coord.holder_of("taken").holder == "holder"


def test_batch_rolls_back_when_quota_insufficient():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=2)
    with pytest.raises(LeaseRejected) as exc:
        coord.acquire_batch(["q-a", "q-b", "q-c"], "A", ttl=10)
    print("[输入] 批量申请 3 个，但 A 配额上限为 2")
    print(f"[判定] 整单拒绝原因={exc.value.reason}，应为 QUOTA_EXCEEDED")
    assert exc.value.reason is RejectionReason.QUOTA_EXCEEDED
    for r in ["q-a", "q-b", "q-c"]:
        assert coord.holder_of(r) is None
    print(f"[判定] 无部分占用，A 在途={coord.in_flight('A')} 应为 0")
    assert coord.in_flight("A") == 0
    # 失败后立即可申请一个不超限的批量单
    leases = coord.acquire_batch(["q-a", "q-b"], "A", ttl=10)
    assert len(leases) == 2


def test_batch_with_partially_self_held_consumes_only_new_permits():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=3)
    first_self = coord.acquire("self", "A", ttl=10)
    # 已自持 1 个，再批量 [self, new1, new2]：只新增 2 个许可，合计 3
    leases = coord.acquire_batch(["self", "new1", "new2"], "A", ttl=10)
    print(f"[输入] A 已持 self(token={first_self.fencing_token})，"
          f"批量含 1 个已自持 + 2 个新资源")
    print(f"[判定] 在途={coord.in_flight('A')}，应为 3；无键=新操作，"
          f"self 换发新凭证但不多占")
    assert coord.in_flight("A") == 3
    new_self = next(l for l in leases if l.resource == "self")
    assert new_self.fencing_token > first_self.fencing_token
    assert coord.holder_of("self") == new_self
    # 若新资源使总数超限，整单失败且不影响已自持资源
    with pytest.raises(LeaseRejected) as exc:
        coord.acquire_batch(["new3", "new4"], "A", ttl=10)
    assert exc.value.reason is RejectionReason.QUOTA_EXCEEDED
    assert coord.holder_of("self") is not None
    print("[判定] 后续超限批量失败，已自持 self 保持有效，无回滚误伤")


def test_concurrent_batches_are_atomic_no_double_hold():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=20)
    success: list[str] = []
    rejected: list[RejectionReason] = []
    lock = threading.Lock()
    # 四个不同客户端同时申请同一组重叠资源：只有一个整单成功
    resources = ["c-a", "c-b", "c-c"]

    def attempt(client: str) -> None:
        try:
            coord.acquire_batch(resources, client, ttl=60)
            with lock:
                success.append(client)
        except LeaseRejected as e:
            with lock:
                rejected.append(e.reason)

    clients = ["A", "B", "C", "D"]
    threads = [threading.Thread(target=attempt, args=(c,)) for c in clients]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    holders = {coord.holder_of(r).holder for r in resources}
    print("[输入] 4 个客户端并发批量争抢同一组 3 资源")
    print(f"[判定] 获胜整单={success}，持有者集合={holders}，"
          f"拒绝原因集合={set(rejected)}")
    assert len(holders) == 1  # 每个资源同一持有者，无双持有
    assert holders == {success[0]}
    assert len(success) == 1  # 仅一个整单成功
    assert len(rejected) == 3
    assert set(rejected) == {RejectionReason.HELD_BY_OTHER}
    for r in resources:
        l = coord.holder_of(r)
        coord.release(r, l.holder, l.fencing_token)
    assert all(coord.in_flight(c) == 0 for c in clients)


def test_batch_idempotent_replay_applies_once():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=5)
    first = coord.acquire_batch(["i-1", "i-2"], "A", ttl=10, idem_key="bulk-1")
    replay = coord.acquire_batch(["i-1", "i-2"], "A", ttl=10, idem_key="bulk-1")
    tokens_1 = [l.fencing_token for l in first]
    tokens_2 = [l.fencing_token for l in replay]
    print(f"[输入] 批量同键重放：{tokens_1} vs {tokens_2}")
    print(f"[判定] 结果一致，在途={coord.in_flight('A')} 应为 2（不多占）")
    assert replay == first
    assert coord.in_flight("A") == 2


def test_batch_over_http_success_and_failure():
    ok = http.post("/leases/acquire-batch", json={
        "resources": ["h-1", "h-2"], "client": "bc", "ttl": 300,
        "idem_key": "batch-http-1"})
    assert ok.status_code == 201
    tokens = [l["fencing_token"] for l in ok.json()["leases"]]
    print(f"[判定] HTTP 批量成功 tokens={tokens}，库侧在途={shared.in_flight('bc')}")
    assert shared.in_flight("bc") == 2

    # 重放：同样的凭证，许可不增加
    replay = http.post("/leases/acquire-batch", json={
        "resources": ["h-1", "h-2"], "client": "bc", "ttl": 300,
        "idem_key": "batch-http-1"})
    assert [l["fencing_token"] for l in replay.json()["leases"]] == tokens
    assert shared.in_flight("bc") == 2

    # 别的客户端整单申请含已被占资源 -> 整体失败、无部分占用
    r = http.post("/leases/acquire-batch", json={
        "resources": ["h-1", "h-x"], "client": "bc2", "ttl": 300})
    print(f"[判定] HTTP 冲突批量 -> {r.status_code} error={r.json()['error']}")
    assert r.status_code == 409 and r.json()["error"] == "held_by_other"
    assert shared.holder_of("h-x") is None
    assert shared.in_flight("bc2") == 0

    for l in ok.json()["leases"]:
        shared.release(l["resource"], "bc", l["fencing_token"])
    assert shared.in_flight("bc") == 0


def test_batch_rejects_empty_and_dedupes():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=5)
    with pytest.raises(ValueError):
        coord.acquire_batch([], "A", ttl=10)
    leases = coord.acquire_batch(["dup", "dup", "dup2"], "A", ttl=10)
    print("[输入] 批量含重复资源 [dup, dup, dup2]")
    print(f"[判定] 去重后实际占用 {[l.resource for l in leases]}，"
          f"在途={coord.in_flight('A')} 应为 2")
    assert [l.resource for l in leases] == ["dup", "dup2"]
    assert coord.in_flight("A") == 2
