"""占用幂等测试：客户端生成的 idem_key 保证整笔操作只生效一次。

覆盖：重放不换新凭证、不重复占用、不多占配额、结果与首次一致；
缺键或换新键按新操作处理；并发双发只有一次生效；中途失败无残留、
无双持有；HTTP 与库方式共享同一套幂等记录。
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


def test_failed_first_attempt_leaves_no_residue_and_replays_same_rejection():
    coord = Coordinator(ManualClock(), max_concurrent_per_client=5)
    holder_lease = coord.acquire("hot", "holder", ttl=10)
    # A 首次带键占用失败（被他人持有）：落账首次结论、不留许可、无双持有
    with pytest.raises(LeaseRejected) as exc:
        coord.acquire("hot", "A", ttl=10, idem_key="fail-1")
    assert exc.value.reason is RejectionReason.HELD_BY_OTHER
    print("[输入] A 带键 fail-1 首次占用被 holder 拒绝")
    print(f"[判定] A 在途={coord.in_flight('A')} 应为 0；holder 凭证不变 "
          f"token={coord.holder_of('hot').fencing_token}")
    assert coord.in_flight("A") == 0
    assert coord.holder_of("hot") == holder_lease

    # 仍然冲突时拿同一个键重试：原样回放首次结论，不重新判定、不换发凭证
    with pytest.raises(LeaseRejected) as replay_exc:
        coord.acquire("hot", "A", ttl=10, idem_key="fail-1")
    assert replay_exc.value.reason is RejectionReason.HELD_BY_OTHER
    assert coord.in_flight("A") == 0
    assert coord.holder_of("hot") == holder_lease

    # 即使 holder 释放、冲突解除，同键重放仍返回首次的拒绝结论，
    # 不会悄悄占上资源、不增加持有数
    coord.release("hot", "holder", holder_lease.fencing_token)
    with pytest.raises(LeaseRejected) as after_release:
        coord.acquire("hot", "A", ttl=10, idem_key="fail-1")
    print("[判定] holder 释放后用 fail-1 重试：仍回放首次 held_by_other 结论")
    assert after_release.value.reason is RejectionReason.HELD_BY_OTHER
    assert coord.holder_of("hot") is None
    assert coord.live_hold_count("A") == 0
    assert coord.in_flight("A") == 0

    # 换一个新键才是全新一次操作，可以成功占用
    lease = coord.acquire("hot", "A", ttl=10, idem_key="fail-2")
    print(f"[判定] 换新键 fail-2 才作为新操作成功 token={lease.fencing_token}，"
          f"在途={coord.in_flight('A')}")
    assert lease.holder == "A"
    assert coord.in_flight("A") == 1


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
