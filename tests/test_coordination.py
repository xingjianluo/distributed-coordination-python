"""协调能力单测。

覆盖：正常获取/释放、抢占被拒、伪造身份、重复释放、过期续租不复活、
落后凭证访问、凭证单调递增、并发争抢、按客户端配额与失败幂等重试。
关键输入与判定依据通过 coordination.store 的 INFO 日志打印（见 caplog）。
"""

from __future__ import annotations

import threading

import pytest

from coordination import (
    AlreadyReleasedError,
    ConcurrencyQuotaExceededError,
    FakeClock,
    FenceToken,
    ForgedHolderError,
    InvalidFenceError,
    LeaseExpiredError,
    LeaseStore,
    ResourceHeldError,
    StaleFenceError,
)
from coordination.models import (
    AccessRequest,
    AcquireRequest,
    ReleaseRequest,
    RenewRequest,
)


@pytest.fixture
def clock():
    return FakeClock(start=1000.0)


@pytest.fixture
def store(clock):
    return LeaseStore(clock=clock, default_quota=1)


# --------------------------------------------------------------------- #
# 1. 正常获取与释放
# --------------------------------------------------------------------- #
def test_acquire_and_release_happy_path(store, caplog):
    caplog.set_level("INFO", logger="coordination.store")
    token = store.acquire(AcquireRequest("doc", "alice", ttl=10.0))
    print(f"[输入] acquire doc/alice ttl=10 -> [判定] {token}")
    assert token == FenceToken("doc", 1)

    # 持有时访问受保护操作被允许
    assert store.access(AccessRequest("doc", "alice", 1)).version == 1

    released = store.release(ReleaseRequest("doc", "alice", 1))
    print(f"[输入] release doc/alice v1 -> [判定] {released}")
    assert released.version == 1

    # 释放后他人可立即获得，版本递增到 2
    token2 = store.acquire(AcquireRequest("doc", "bob", ttl=5.0))
    assert token2.version == 2


def test_versions_strictly_increase_per_resource(store):
    assert store.acquire(AcquireRequest("r", "a", 10)).version == 1
    store.release(ReleaseRequest("r", "a", 1))
    assert store.acquire(AcquireRequest("r", "b", 10)).version == 2
    store.release(ReleaseRequest("r", "b", 2))
    assert store.acquire(AcquireRequest("r", "a", 10)).version == 3


# --------------------------------------------------------------------- #
# 2. 未过期抢占被拒 + 伪造身份
# --------------------------------------------------------------------- #
def test_preempt_rejected_while_lease_active(store):
    store.acquire(AcquireRequest("lock", "alice", ttl=10.0))
    with pytest.raises(ResourceHeldError) as ei:
        store.acquire(AcquireRequest("lock", "bob", ttl=10.0))
    print(f"[输入] bob 在 alice 租约未过期时抢占 -> [判定] {ei.value.code}")
    assert ei.value.code == "resource_held"
    # 持有者仍然是 alice
    snap = store.snapshot()
    assert snap["active"]["lock"]["holder"] == "alice"


def test_forged_holder_release_rejected(store):
    store.acquire(AcquireRequest("lock", "alice", ttl=10.0))
    # bob 伪造当前版本去释放
    with pytest.raises(ForgedHolderError) as ei:
        store.release(ReleaseRequest("lock", "bob", 1))
    print(f"[输入] release lock/bob v1（实际持有者 alice）-> [判定] {ei.value.code}")
    assert ei.value.code == "forged_holder"
    # 租约仍在，没有被误释放
    assert store.snapshot()["active"]["lock"]["holder"] == "alice"


# --------------------------------------------------------------------- #
# 3. 续租语义：成功续租不换版本；过期后续租旧凭证不得复活
# --------------------------------------------------------------------- #
def test_renew_keeps_version_and_extends(store, clock):
    store.acquire(AcquireRequest("lock", "alice", ttl=10.0))
    clock.advance(8)
    token = store.renew(RenewRequest("lock", "alice", 1, ttl=10.0))
    assert token.version == 1  # 续租不提升版本
    clock.advance(9)  # 距首次获取 17s，若无续租早已过期
    assert store.access(AccessRequest("lock", "alice", 1)).version == 1


def test_renew_after_expiry_does_not_revive_old_token(store, clock):
    store.acquire(AcquireRequest("lock", "alice", ttl=10.0))
    clock.advance(11)
    with pytest.raises(LeaseExpiredError) as ei:
        store.renew(RenewRequest("lock", "alice", 1, ttl=10.0))
    print(f"[输入] 过期后续租 lock/alice v1 -> [判定] {ei.value.code}")
    assert ei.value.code == "lease_expired"
    # 他人获取后版本变为 v2，旧 v1 永远失效
    assert store.acquire(AcquireRequest("lock", "bob", 10)).version == 2
    with pytest.raises(StaleFenceError):
        store.renew(RenewRequest("lock", "alice", 1, ttl=10.0))


# --------------------------------------------------------------------- #
# 4. 落后凭证 / 无法识别凭证访问受保护操作
# --------------------------------------------------------------------- #
def test_stale_fence_access_rejected(store, clock):
    store.acquire(AcquireRequest("lock", "alice", ttl=5.0))
    clock.advance(6)
    new = store.acquire(AcquireRequest("lock", "bob", ttl=10.0))
    assert new.version == 2
    with pytest.raises(StaleFenceError) as ei:
        store.access(AccessRequest("lock", "alice", 1))
    print(f"[输入] access lock/alice v1（当前 v2）-> [判定] {ei.value.code}")
    assert ei.value.code == "stale_fence"
    assert store.access(AccessRequest("lock", "bob", 2)).version == 2


def test_unrecognized_fence_rejected(store):
    store.acquire(AcquireRequest("lock", "alice", ttl=5.0))
    with pytest.raises(InvalidFenceError) as ei:
        store.access(AccessRequest("lock", "alice", 99))
    print(f"[输入] access lock/alice v99（从未发放）-> [判定] {ei.value.code}")
    assert ei.value.code == "invalid_fence"


# --------------------------------------------------------------------- #
# 5. 重复释放
# --------------------------------------------------------------------- #
def test_double_release_rejected(store):
    store.acquire(AcquireRequest("lock", "alice", ttl=5.0))
    store.release(ReleaseRequest("lock", "alice", 1))
    with pytest.raises(AlreadyReleasedError) as ei:
        store.release(ReleaseRequest("lock", "alice", 1))
    print(f"[输入] 第二次 release lock/alice v1 -> [判定] {ei.value.code}")
    assert ei.value.code == "already_released"


# --------------------------------------------------------------------- #
# 6. 过期后可被获取，且不残留配额占用
# --------------------------------------------------------------------- #
def test_takeover_after_expiry_and_quota_freed(store, clock):
    store.acquire(AcquireRequest("r1", "alice", ttl=5.0))
    clock.advance(6)
    # 过期后 bob 成功获取（不视为抢占）
    assert store.acquire(AcquireRequest("r1", "bob", 5)).version == 2
    # 过期租约不占 alice 配额：alice 仍可获取新资源
    assert store.acquire(AcquireRequest("r2", "alice", 5)).version == 1


def test_per_client_concurrency_quota(store):
    store.set_quota("alice", 2)
    store.acquire(AcquireRequest("r1", "alice", 10))
    store.acquire(AcquireRequest("r2", "alice", 10))
    with pytest.raises(ConcurrencyQuotaExceededError) as ei:
        store.acquire(AcquireRequest("r3", "alice", 10))
    print(f"[输入] alice 获取第 3 个资源（配额=2）-> [判定] {ei.value.code}")
    assert ei.value.code == "concurrency_quota_exceeded"
    # 配额按客户端隔离，不影响 bob
    assert store.acquire(AcquireRequest("r3", "bob", 10)).version == 1


# --------------------------------------------------------------------- #
# 7. 失败/超时重试：request_id 幂等，不产生重复占用或双持有
# --------------------------------------------------------------------- #
def test_retry_with_same_request_id_is_idempotent(store):
    req = AcquireRequest("lock", "alice", ttl=10.0, request_id="req-7")
    t1 = store.acquire(req)
    t2 = store.acquire(req)  # 模拟客户端超时后原样重试
    t3 = store.acquire(req)
    print(f"[输入] 同一 request_id=req-7 重试 3 次 -> [判定] 始终返回 {t1}")
    assert t1 == t2 == t3
    assert store.snapshot()["latest_versions"]["lock"] == 1


def test_reacquire_while_held_returns_same_token(store):
    # 未带 request_id 的短期抖动重试：自己仍持有时返回同一凭证
    t1 = store.acquire(AcquireRequest("lock", "alice", ttl=10.0))
    t2 = store.acquire(AcquireRequest("lock", "alice", ttl=10.0))
    assert t1 == t2


# --------------------------------------------------------------------- #
# 8. 并发争抢：多线程同时获取同一资源，恰有一个成功
# --------------------------------------------------------------------- #
def test_concurrent_contention_single_winner(store):
    results: list[str | object] = []
    barrier = threading.Barrier(8)

    def worker(client: str):
        barrier.wait()
        try:
            token = store.acquire(
                AcquireRequest("hot", client, ttl=10.0))
            results.append(client)
        except ResourceHeldError as exc:
            results.append(exc.code)

    threads = [threading.Thread(target=worker, args=(f"c{i}",))
               for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    winners = [r for r in results if r != "resource_held"]
    print(f"[输入] 8 线程并发获取 hot -> [判定] 成功者={winners}, 其余被拒")
    assert len(winners) == 1
    assert results.count("resource_held") == 7
    snap = store.snapshot()
    assert snap["active"]["hot"]["holder"] == winners[0]
    assert snap["active"]["hot"]["version"] == 1
