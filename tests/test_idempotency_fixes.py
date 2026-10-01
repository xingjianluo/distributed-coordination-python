"""幂等修复专项：单个/批量幂等隔离、空标识按未提供处理、首次被拒结论重放。

覆盖本轮三类必须修的问题：
1. 同一客户端用同一标识在单个占用与批量申请间交叉使用，两本账互不影响、
   各自只重放自己的首次结果，跨类型不顶替、不走内部错误路径（HTTP 不 500）；
2. 空串/纯空白标识等同未提供，每次都是新操作，两次占用各自生效不串用；
3. 首次（含批量）被拒的结论原样落账，同标识重放永远回放首次结论，
   不重新执行、不换凭证、不留残留、无双持有。
"""

from __future__ import annotations

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


# ---- 1. 单个占用与批量申请的幂等记录各自独立 ----


def test_single_then_batch_same_key_each_applies_once():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=8)
    key = "cross-1"
    # 先单个占用 r1
    single_first = coord.acquire("r1", "A", ttl=10, idem_key=key)
    # 同键再做批量：必须是一笔全新的批量操作，而不是报错或回放单个结果
    batch_first = coord.acquire_batch(["r2", "r3"], "A", ttl=10, idem_key=key)
    print("[输入] A 同键 cross-1：先单个占 r1，再批量占 [r2,r3]")
    assert [l.resource for l in batch_first] == ["r2", "r3"]
    assert {l.resource for l in batch_first}.isdisjoint({"r1"})
    assert coord.in_flight("A") == 3
    for r in ("r1", "r2", "r3"):
        assert coord.holder_of(r).holder == "A"

    # 之后同键重放：单个只回放单个首次结果，批量只回放批量首次结果
    single_replay = coord.acquire("r1", "A", ttl=10, idem_key=key)
    batch_replay = coord.acquire_batch(["r2", "r3"], "A", ttl=10, idem_key=key)
    assert single_replay == single_first
    assert batch_replay == batch_first
    print("[判定] 交叉重放各自只拿自己的首次凭证，在途仍为 3，不串用不多占")
    assert coord.in_flight("A") == 3
    # 单个重放绝不会顶替成批量那组凭证
    assert single_replay.resource == "r1"
    assert [l.resource for l in batch_replay] == ["r2", "r3"]


def test_batch_then_single_same_key_each_applies_once():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=8)
    key = "cross-2"
    batch_first = coord.acquire_batch(["b1", "b2"], "A", ttl=10, idem_key=key)
    # 同键的单个占用是全新操作，真实占上 b3
    single_first = coord.acquire("b3", "A", ttl=10, idem_key=key)
    print("[输入] A 同键 cross-2：先批量 [b1,b2]，再单个占 b3")
    assert single_first.resource == "b3"
    assert coord.in_flight("A") == 3
    # 两本账各自重放
    assert coord.acquire_batch(["b1", "b2"], "A", ttl=10, idem_key=key) == batch_first
    assert coord.acquire("b3", "A", ttl=10, idem_key=key) == single_first
    assert coord.in_flight("A") == 3
    print("[判定] 批量在前单个在后，同键仍各自独立重放，在途=3")


def test_cross_type_replay_never_replays_other_types_rejection():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=8)
    coord.acquire("hot", "holder", ttl=10)
    # 单个占用首次被拒并落账
    with pytest.raises(LeaseRejected) as exc:
        coord.acquire("hot", "A", ttl=10, idem_key="cross-3")
    assert exc.value.reason is RejectionReason.HELD_BY_OTHER
    # 同键做批量（资源都空闲）：不受单个那笔被拒记录影响，正常成功
    leases = coord.acquire_batch(["f1", "f2"], "A", ttl=10, idem_key="cross-3")
    print("[输入] 单个同键首次被拒后，用同键申请空闲批量资源")
    assert [l.resource for l in leases] == ["f1", "f2"]
    assert coord.in_flight("A") == 2
    # 反过来：单个重放仍回放它自己的被拒结论，不被批量成功覆盖
    with pytest.raises(LeaseRejected) as replay:
        coord.acquire("hot", "A", ttl=10, idem_key="cross-3")
    assert replay.value.reason is RejectionReason.HELD_BY_OTHER
    print("[判定] 单个继续回放 held_by_other；批量独立成功在途=2，互不相顶替")
    assert coord.in_flight("A") == 2


def test_cross_type_same_key_over_http_never_500():
    key = "cross-http-1"
    r_single = http.post("/leases/acquire", json={
        "resource": "xh-1", "client": "xhc", "ttl": 300, "idem_key": key})
    assert r_single.status_code == 201
    r_batch = http.post("/leases/acquire-batch", json={
        "resources": ["xh-2", "xh-3"], "client": "xhc", "ttl": 300,
        "idem_key": key})
    print(f"[输入] HTTP 同键先单个后批量：{r_single.status_code} / {r_batch.status_code}")
    # 旧实现这里会 500（ValueError）或串放单个凭证；现在必须是真实的批量成功
    assert r_batch.status_code == 201
    assert [l["resource"] for l in r_batch.json()["leases"]] == ["xh-2", "xh-3"]
    assert shared.in_flight("xhc") == 3
    # 两本账分别重放
    r_single_replay = http.post("/leases/acquire", json={
        "resource": "xh-1", "client": "xhc", "ttl": 300, "idem_key": key})
    r_batch_replay = http.post("/leases/acquire-batch", json={
        "resources": ["xh-2", "xh-3"], "client": "xhc", "ttl": 300,
        "idem_key": key})
    assert r_single_replay.json() == r_single.json()
    assert r_batch_replay.json() == r_batch.json()
    assert shared.in_flight("xhc") == 3
    print("[判定] HTTP 交叉重放凭证各自一致、无 5xx，在途=3")
    for r in ("xh-1", "xh-2", "xh-3"):
        cur = shared.holder_of(r)
        shared.release(r, "xhc", cur.fencing_token)
    assert shared.in_flight("xhc") == 0


# ---- 2. 空串 / 纯空白标识一律按未提供处理 ----


@pytest.mark.parametrize("blank", ["", "   ", "\t", " \t \n"])
def test_blank_key_each_acquire_is_new_operation(blank):
    coord = Coordinator(ManualClock(), max_concurrent_per_client=8)
    l1 = coord.acquire("e1", "A", ttl=10, idem_key=blank)
    l2 = coord.acquire("e2", "A", ttl=10, idem_key=blank)
    print(f"[输入] 两次不同资源都带空白标识 {blank!r}：tokens="
          f"{l1.fencing_token},{l2.fencing_token}")
    # 两笔都是新操作：两个资源都真实占上，各占一个许可
    assert l1.resource == "e1" and l2.resource == "e2"
    assert coord.holder_of("e1").holder == "A"
    assert coord.holder_of("e2").holder == "A"
    assert coord.live_hold_count("A") == 2
    assert coord.in_flight("A") == 2
    # 对同一资源再来一次空白键：仍是新操作（换发新凭证），但不新增许可
    l3 = coord.acquire("e1", "A", ttl=10, idem_key=blank)
    assert l3.fencing_token > l1.fencing_token
    assert coord.holder_of("e1").fencing_token == l3.fencing_token
    assert coord.in_flight("A") == 2
    print("[判定] 空白键不幂等：三次都是新操作，资源各占各的，在途=2")


def test_blank_key_does_not_replay_prior_result_batch():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=8)
    coord.acquire_batch(["g1", "g2"], "A", ttl=10, idem_key="   ")
    second = coord.acquire_batch(["g3", "g4"], "A", ttl=10, idem_key="   ")
    print("[输入] 两笔不同批量都带纯空白键")
    assert [l.resource for l in second] == ["g3", "g4"]
    for r in ("g1", "g2", "g3", "g4"):
        assert coord.holder_of(r).holder == "A"
    assert coord.in_flight("A") == 4
    print("[判定] 两笔批量各自真实生效，在途=4，没有回放第一批凭证")


def test_blank_key_over_http_each_request_applies():
    r1 = http.post("/leases/acquire", json={
        "resource": "blank-1", "client": "bk", "ttl": 300, "idem_key": ""})
    r2 = http.post("/leases/acquire", json={
        "resource": "blank-2", "client": "bk", "ttl": 300, "idem_key": "   "})
    print(f"[输入] HTTP 两次空/空白键：{r1.status_code}/{r2.status_code}")
    assert r1.status_code == 201 and r2.status_code == 201
    assert r1.json()["resource"] == "blank-1"
    assert r2.json()["resource"] == "blank-2"
    assert shared.in_flight("bk") == 2
    for r in ("blank-1", "blank-2"):
        cur = shared.holder_of(r)
        shared.release(r, "bk", cur.fencing_token)
    assert shared.in_flight("bk") == 0


# ---- 3. 首次被拒结论原样重放（单个 + 批量，冲突 + 配额） ----


def test_single_rejected_first_replays_same_conclusion_even_after_release():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=8)
    holder_lease = coord.acquire("deny-1", "holder", ttl=10)
    with pytest.raises(LeaseRejected) as first:
        coord.acquire("deny-1", "A", ttl=10, idem_key="d-1")
    assert first.value.reason is RejectionReason.HELD_BY_OTHER
    first_detail = first.value.detail

    # 冲突仍在：同键重放，结论一致
    with pytest.raises(LeaseRejected) as replay1:
        coord.acquire("deny-1", "A", ttl=10, idem_key="d-1")
    assert replay1.value.reason is RejectionReason.HELD_BY_OTHER
    assert replay1.value.detail == first_detail

    # 冲突解除后同键重放：仍是首次的拒绝，资源没被 A 占上，不换凭证
    coord.release("deny-1", "holder", holder_lease.fencing_token)
    with pytest.raises(LeaseRejected) as replay2:
        coord.acquire("deny-1", "A", ttl=10, idem_key="d-1")
    print("[输入] 单个首次被拒，冲突解除后同键再试")
    assert replay2.value.reason is RejectionReason.HELD_BY_OTHER
    assert replay2.value.detail == first_detail
    assert coord.holder_of("deny-1") is None
    assert coord.live_hold_count("A") == 0
    assert coord.in_flight("A") == 0
    print("[判定] 重放永远回放首次结论，无残留、无双持有、在途=0")


def test_batch_rejected_first_replays_same_conclusion():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=8)
    coord.acquire("deny-b", "holder", ttl=10)
    with pytest.raises(LeaseRejected) as first:
        coord.acquire_batch(["ok-a", "deny-b", "ok-c"], "A", ttl=10,
                            idem_key="db-1")
    assert first.value.reason is RejectionReason.HELD_BY_OTHER
    first_detail = first.value.detail

    # 同键重放：同样整单拒绝，空闲资源始终没被部分占用
    for _ in range(2):
        with pytest.raises(LeaseRejected) as replay:
            coord.acquire_batch(["ok-a", "deny-b", "ok-c"], "A", ttl=10,
                                idem_key="db-1")
        assert replay.value.reason is RejectionReason.HELD_BY_OTHER
        assert replay.value.detail == first_detail
    for r in ("ok-a", "ok-c"):
        assert coord.holder_of(r) is None
    assert coord.in_flight("A") == 0
    print("[输入] 批量首次整单被拒后同键重放两次")
    print("[判定] 都回放首次 held_by_other，ok-a/ok-c 从未被占，在途=0")

    # holder 释放后重放：仍然回放首次结论，而不是重新执行整单
    cur = coord.holder_of("deny-b")
    coord.release("deny-b", "holder", cur.fencing_token)
    with pytest.raises(LeaseRejected) as after:
        coord.acquire_batch(["ok-a", "deny-b", "ok-c"], "A", ttl=10,
                            idem_key="db-1")
    assert after.value.reason is RejectionReason.HELD_BY_OTHER
    for r in ("ok-a", "deny-b", "ok-c"):
        assert coord.holder_of(r) is None
    assert coord.in_flight("A") == 0
    print("[判定] 冲突解除后批量重放仍是首次结论，无新凭证、无部分占用")


def test_batch_quota_rejection_is_also_replayed():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=2)
    with pytest.raises(LeaseRejected) as first:
        coord.acquire_batch(["q1", "q2", "q3"], "A", ttl=10, idem_key="dq-1")
    assert first.value.reason is RejectionReason.QUOTA_EXCEEDED
    # 同键重放仍回放首次的配额拒绝；换无键新单（不超限）可以正常走
    with pytest.raises(LeaseRejected) as replay:
        coord.acquire_batch(["q1", "q2", "q3"], "A", ttl=10, idem_key="dq-1")
    print("[输入] 批量首次配额超限被拒，同键重放")
    assert replay.value.reason is RejectionReason.QUOTA_EXCEEDED
    assert coord.in_flight("A") == 0
    leases = coord.acquire_batch(["q1", "q2"], "A", ttl=10)
    assert len(leases) == 2 and coord.in_flight("A") == 2
    print("[判定] 配额拒绝也被原样重放；其它操作不受影响，无残留许可")


def test_rejected_replay_over_http_is_409_not_500():
    coord_body = {"resource": "deny-http", "client": "dhc", "ttl": 300,
                  "idem_key": "dh-1"}
    # 让别的客户端先占住
    taken = http.post("/leases/acquire", json={
        "resource": "deny-http", "client": "dhc-other", "ttl": 300})
    assert taken.status_code == 201
    r1 = http.post("/leases/acquire", json=coord_body)
    r2 = http.post("/leases/acquire", json=coord_body)
    print(f"[输入] HTTP 首次被拒后同键重放：{r1.status_code}/{r2.status_code} "
          f"error={r1.json().get('error')}")
    assert r1.status_code == 409 and r2.status_code == 409
    assert r1.json()["error"] == "held_by_other"
    assert r2.json() == r1.json()
    assert shared.in_flight("dhc") == 0
    cur = shared.holder_of("deny-http")
    shared.release("deny-http", "dhc-other", cur.fencing_token)
    assert shared.in_flight("dhc-other") == 0
