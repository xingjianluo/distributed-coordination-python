"""占用幂等测试：客户端生成的 idem_key 保证整笔操作只生效一次。

覆盖：重放不换新凭证、不重复占用、不多占配额、结果与首次一致；
缺键/空键（含纯空白）或换新键按新操作处理；并发双发只有一次生效；
单个占用与批量申请的幂等记录相互独立、跨类型不顶替；首次被拒
（held_by_other/quota_exceeded）的结论也被原样重放，无残留、无双持有；
HTTP 与库方式共享同一套幂等记录。
"""

from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

import main
from coordination import (
    Coordinator,
    Lease,
    LeaseRejected,
    ManualClock,
    RejectionReason,
)

shared = main.coordinator
http = TestClient(main.app)


def test_replay_returns_identical_result_without_new_token():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=5)
    first = coord.acquire("idem-res", "A", ttl=10, idem_key="req-1")
    replay = coord.acquire("idem-res", "A", ttl=10, idem_key="req-1")
    print(f"[输入] 同一 (A, req-1) 重放：首次 token={first.fencing_token}")
    print(f"[判定] 重放 token={replay.fencing_token}，与首次完全一致")
    assert replay == first
    assert coord.in_flight("A") == 1  # 不多占配额
    assert coord.holder_of("idem-res").fencing_token == first.fencing_token


def test_missing_key_treated_as_new_operation():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=5)
    l1 = coord.acquire("idem-res2", "A", ttl=10)
    l2 = coord.acquire("idem-res2", "A", ttl=10)
    print(f"[输入] 不带 idem_key 的重复占用：{l1.fencing_token} -> {l2.fencing_token}")
    print("[判定] 无键按新操作处理：签发新凭证，但同一资源仍只占一个许可")
    assert l2.fencing_token > l1.fencing_token
    assert coord.in_flight("A") == 1


def test_new_key_is_new_operation_but_same_holder_keeps_one_permit():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=5)
    l1 = coord.acquire("idem-res3", "A", ttl=10, idem_key="k-1")
    l2 = coord.acquire("idem-res3", "A", ttl=10, idem_key="k-2")
    print(f"[输入] 换新键 k-1 -> k-2：token {l1.fencing_token} -> {l2.fencing_token}")
    print("[判定] 换新键=新操作（换新凭证）；同客户端同资源仍仅占 1 个许可")
    assert l2.fencing_token > l1.fencing_token
    assert coord.in_flight("A") == 1


def test_replay_does_not_consume_quota_at_limit():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=2)
    coord.acquire("q-1", "A", ttl=10, idem_key="a")
    coord.acquire("q-2", "A", ttl=10, idem_key="b")
    assert coord.in_flight("A") == 2
    # 配额已满：新操作被拒，但带原键的重放仍返回首次结果
    with pytest.raises(LeaseRejected) as exc:
        coord.acquire("q-3", "A", ttl=10, idem_key="c")
    assert exc.value.reason is RejectionReason.QUOTA_EXCEEDED
    replay = coord.acquire("q-1", "A", ttl=10, idem_key="a")
    print(f"[判定] 打满配额后重放原键仍成功 token={replay.fencing_token}，"
          f"在途={coord.in_flight('A')} 仍为 2")
    assert replay.resource == "q-1"
    assert coord.in_flight("A") == 2


def test_concurrent_duplicate_requests_apply_once():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=5)
    results: list[Lease] = []
    errors: list[RejectionReason] = []

    def fire() -> None:
        try:
            results.append(coord.acquire("race-res", "A", ttl=10, idem_key="dup"))
        except LeaseRejected as e:
            errors.append(e.reason)

    threads = [threading.Thread(target=fire) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    tokens = {l.fencing_token for l in results}
    print("[输入] 8 个线程带同一 idem_key=dup 并发占用同一资源")
    print(f"[判定] 成功次数={len(results)}，凭证集合={tokens}，"
          f"在途={coord.in_flight('A')}，拒绝={errors}")
    assert len(results) == 8  # 都拿到首次结果
    assert tokens == {results[0].fencing_token}  # 但凭证只有一个
    assert coord.in_flight("A") == 1
    assert coord.holder_of("race-res").fencing_token == results[0].fencing_token


def test_first_rejection_is_replayed_with_no_residue():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=5)
    holder_lease = coord.acquire("hot", "holder", ttl=10)
    holder_token_before = holder_lease.fencing_token
    # A 首次带键占用失败（被他人持有）：拒绝结论落账、不留许可、无双持有
    with pytest.raises(LeaseRejected) as exc:
        coord.acquire("hot", "A", ttl=10, idem_key="fail-1")
    assert exc.value.reason is RejectionReason.HELD_BY_OTHER
    print("[输入] A 带键 fail-1 首次占用被 holder 拒绝")
    print(f"[判定] A 在途={coord.in_flight('A')} 应为 0；holder 凭证不变 "
          f"token={coord.holder_of('hot').fencing_token}")
    assert coord.in_flight("A") == 0
    assert coord.live_hold_count("A") == 0
    assert coord.holder_of("hot") == holder_lease
    # 同标识重试（holder 仍占着）：原样重放首次结论，不再次执行、不换新凭证
    with pytest.raises(LeaseRejected) as replay_exc:
        coord.acquire("hot", "A", ttl=10, idem_key="fail-1")
    assert replay_exc.value.reason is RejectionReason.HELD_BY_OTHER
    assert replay_exc.value.detail == exc.value.detail
    assert coord.holder_of("hot").fencing_token == holder_token_before
    print("[判定] 同键重放返回与首次一致的拒绝，holder 凭证未变、A 无残留")
    assert coord.in_flight("A") == 0
    # 即使冲突解除（holder 释放），同标识仍重放首次结论，不会变成一次新占用
    coord.release("hot", "holder", holder_lease.fencing_token)
    with pytest.raises(LeaseRejected) as after_release:
        coord.acquire("hot", "A", ttl=10, idem_key="fail-1")
    assert after_release.value.reason is RejectionReason.HELD_BY_OTHER
    assert coord.holder_of("hot") is None
    assert coord.in_flight("A") == 0
    print("[判定] 冲突解除后同键仍重放首次拒绝，资源未被 A 占上、无残留状态")
    # 换新键才是一次全新操作，可以成功
    lease = coord.acquire("hot", "A", ttl=10, idem_key="fail-1-retry-new-key")
    print(f"[判定] 换新键后成功 token={lease.fencing_token}，在途={coord.in_flight('A')}")
    assert lease.holder == "A"
    assert coord.in_flight("A") == 1


def test_single_and_batch_idem_records_are_independent():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=10)
    # 同一标识先单个占用、再批量申请：互不顶替，各自执行自己的首次操作
    solo = coord.acquire("xsolo", "A", ttl=10, idem_key="shared-key")
    bulk = coord.acquire_batch(["xb1", "xb2"], "A", ttl=10, idem_key="shared-key")
    print("[输入] 同标识 shared-key：先单个 xsolo，再批量 [xb1, xb2]")
    assert solo.resource == "xsolo"
    assert [l.resource for l in bulk] == ["xb1", "xb2"]
    for r in ["xb1", "xb2"]:
        assert coord.holder_of(r).holder == "A"
    print(f"[判定] 批量资源真正占上，A 在途={coord.in_flight('A')} 应为 3")
    assert coord.in_flight("A") == 3
    # 各自重放：仍只返回自己的首次结果，不再占用、不多占配额
    solo_replay = coord.acquire("xsolo", "A", ttl=10, idem_key="shared-key")
    bulk_replay = coord.acquire_batch(
        ["xb1", "xb2"], "A", ttl=10, idem_key="shared-key"
    )
    assert solo_replay == solo
    assert bulk_replay == bulk
    assert coord.in_flight("A") == 3
    print("[判定] 两种操作分别重放各自首次结果，在途仍为 3，跨类型不串用")


def test_single_after_batch_with_same_key_is_also_independent():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=10)
    # 反方向：先批量（2 个），再用同一标识做单个占用（旧实现按长度比对，
    # 长度不同会走 ValueError 未处理错误路径；长度相同则被批量凭证顶替）
    bulk = coord.acquire_batch(["yb1", "yb2"], "A", ttl=10, idem_key="same")
    solo = coord.acquire("ysolo", "A", ttl=10, idem_key="same")
    print("[输入] 同标识 same：先批量 [yb1, yb2]，再单个 ysolo")
    assert [l.resource for l in bulk] == ["yb1", "yb2"]
    assert solo.resource == "ysolo"
    assert coord.holder_of("ysolo").holder == "A"
    assert coord.in_flight("A") == 3
    # 批量再重放仍是首次的整组凭证，单个凭证不会顶替批量结果
    bulk_replay = coord.acquire_batch(
        ["yb1", "yb2"], "A", ttl=10, idem_key="same"
    )
    assert bulk_replay == bulk
    assert {l.resource for l in bulk_replay} == {"yb1", "yb2"}
    assert coord.in_flight("A") == 3
    print("[判定] 反向交叉也各自独立，无 ValueError、无顶替，在途=3")


def test_blank_idem_key_treated_as_missing():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=10)
    # 空串与纯空白标识都按“未提供”处理：两次不同资源是两笔独立新操作
    l1 = coord.acquire("blank-1", "A", ttl=10, idem_key="")
    l2 = coord.acquire("blank-2", "A", ttl=10, idem_key="   ")
    print(f"[输入] 空串/纯空白标识各占用一次：tokens={l1.fencing_token},"
          f"{l2.fencing_token}")
    assert l1.resource == "blank-1" and l2.resource == "blank-2"
    assert coord.holder_of("blank-1") == l1
    assert coord.holder_of("blank-2") == l2
    assert coord.in_flight("A") == 2
    # 同一资源带空串重复占用：按新操作处理（换发新凭证），仍只占一个许可
    l3 = coord.acquire("blank-1", "A", ttl=10, idem_key="\t ")
    assert l3.fencing_token > l1.fencing_token
    assert coord.holder_of("blank-1").fencing_token == l3.fencing_token
    assert coord.in_flight("A") == 2
    print("[判定] 空标识不串用：两资源各自占上，在途=2，重复占用不重放")


def test_first_quota_rejection_is_replayed_without_consuming_permits():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=1)
    with pytest.raises(LeaseRejected) as exc:
        coord.acquire_batch(["z1", "z2"], "A", ttl=10, idem_key="q-fail")
    assert exc.value.reason is RejectionReason.QUOTA_EXCEEDED
    # 首次因配额被拒：同键重放原样返回相同拒绝，不尝试申请、不留许可
    with pytest.raises(LeaseRejected) as replay:
        coord.acquire_batch(["z1", "z2"], "A", ttl=10, idem_key="q-fail")
    assert replay.value.reason is RejectionReason.QUOTA_EXCEEDED
    assert coord.in_flight("A") == 0
    for r in ["z1", "z2"]:
        assert coord.holder_of(r) is None
    print("[判定] 批量首次配额拒绝被原样重放，无许可/资源残留")


def test_idempotency_works_over_http():
    body = {"resource": "idem-http", "client": "hc", "ttl": 300, "idem_key": "http-1"}
    r1 = http.post("/leases/acquire", json=body)
    r2 = http.post("/leases/acquire", json=body)
    assert r1.status_code == 201 and r2.status_code == 201
    print(f"[判定] HTTP 两次同键：token {r1.json()['fencing_token']} / "
          f"{r2.json()['fencing_token']}，库侧在途={shared.in_flight('hc')}")
    assert r1.json() == r2.json()
    assert shared.in_flight("hc") == 1
    # 库方式带同一键重放，拿到与 HTTP 首次一致的结果
    via_lib = shared.acquire("idem-http", "hc", 300, "http-1")
    assert via_lib.fencing_token == r1.json()["fencing_token"]
    print("[判定] 库方式重放 HTTP 首单，凭证一致：跨调用方式幂等")
    cur = shared.holder_of("idem-http")
    shared.release("idem-http", "hc", cur.fencing_token)
    assert shared.in_flight("hc") == 0


def test_cross_type_same_key_and_blank_key_over_http():
    # HTTP：同一标识先单个、后批量，不再走 500，各自真正占用
    key = "http-cross-type"
    r_solo = http.post("/leases/acquire", json={
        "resource": "http-xsolo", "client": "hcx", "ttl": 300,
        "idem_key": key})
    r_batch = http.post("/leases/acquire-batch", json={
        "resources": ["http-xb1", "http-xb2"], "client": "hcx", "ttl": 300,
        "idem_key": key})
    print(f"[判定] HTTP 同标识跨类型：单个 {r_solo.status_code} / "
          f"批量 {r_batch.status_code}")
    assert r_solo.status_code == 201 and r_batch.status_code == 201
    assert r_batch.json()["leases"][0]["resource"] == "http-xb1"
    assert shared.in_flight("hcx") == 3
    # 重放各自返回自己的首次结果
    assert http.post("/leases/acquire", json={
        "resource": "http-xsolo", "client": "hcx", "ttl": 300,
        "idem_key": key}).json() == r_solo.json()
    assert shared.in_flight("hcx") == 3

    # 空串标识：两次不同资源各自占用，不互相重放
    r1 = http.post("/leases/acquire", json={
        "resource": "http-blank-1", "client": "hcb", "ttl": 300,
        "idem_key": ""})
    r2 = http.post("/leases/acquire", json={
        "resource": "http-blank-2", "client": "hcb", "ttl": 300,
        "idem_key": "   "})
    print(f"[判定] HTTP 空串标识两次占用：{r1.status_code}/{r2.status_code}，"
          f"在途={shared.in_flight('hcb')}")
    assert r1.status_code == 201 and r2.status_code == 201
    assert r1.json()["resource"] == "http-blank-1"
    assert r2.json()["resource"] == "http-blank-2"
    assert shared.in_flight("hcb") == 2

    for l in [r_solo.json(), *r_batch.json()["leases"],
              r1.json(), r2.json()]:
        shared.release(l["resource"], l["holder"], l["fencing_token"])
    assert shared.in_flight("hcx") == 0 and shared.in_flight("hcb") == 0


def test_first_rejection_replayed_over_http():
    # holder 先占住资源；同键两次 HTTP 占用都得到一致的 409，不出现 500
    http.post("/leases/acquire", json={
        "resource": "http-hot", "client": "owner", "ttl": 300})
    body = {"resource": "http-hot", "client": "loser", "ttl": 300,
            "idem_key": "http-rk"}
    r1 = http.post("/leases/acquire", json=body)
    r2 = http.post("/leases/acquire", json=body)
    print(f"[判定] HTTP 首次被拒 {r1.status_code} 与同键重放 "
          f"{r2.status_code}，error 均为 {r1.json()['error']}")
    assert r1.status_code == 409 and r2.status_code == 409
    assert r1.json() == r2.json()
    assert r1.json()["error"] == "held_by_other"
    assert shared.in_flight("loser") == 0
