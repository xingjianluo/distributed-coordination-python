"""租约语义测试：获取/释放、抢占拒绝、过期续租、落后凭证、并发争抢、失败重试。

每个用例都打印关键输入与判定依据（pytest -s 可见）。
"""

from __future__ import annotations

import threading

import pytest

from coordination import (
    LeaseRegistry,
    LeaseRejected,
    ManualClock,
    RejectionReason,
)


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def registry(clock: ManualClock) -> LeaseRegistry:
    return LeaseRegistry(clock)


def test_acquire_and_release_happy_path(registry: LeaseRegistry):
    lease = registry.acquire("res", "alice", ttl=10)
    print(f"[输入] acquire(res, alice, ttl=10) -> token={lease.fencing_token}")
    assert lease.holder == "alice"
    assert registry.holder_of("res") == lease

    registry.release("res", "alice", lease.fencing_token)
    print("[判定] 持有者携带当前凭证释放后，holder_of 应为 None")
    assert registry.holder_of("res") is None


def test_preemption_rejected_while_lease_valid(registry: LeaseRegistry):
    lease = registry.acquire("res", "alice", ttl=10)
    with pytest.raises(LeaseRejected) as exc:
        registry.acquire("res", "bob", ttl=10)
    print(f"[输入] alice 持有 token={lease.fencing_token} 未过期；bob 抢占")
    print(f"[判定] 拒绝原因={exc.value.reason}，应为 HELD_BY_OTHER")
    assert exc.value.reason is RejectionReason.HELD_BY_OTHER
    assert registry.holder_of("res").holder == "alice"


def test_forged_holder_identity_rejected(registry: LeaseRegistry):
    lease = registry.acquire("res", "alice", ttl=10)
    with pytest.raises(LeaseRejected) as exc:
        registry.release("res", "mallory", lease.fencing_token)
    print(f"[输入] mallory 伪造 alice 的 token={lease.fencing_token} 释放")
    print(f"[判定] 拒绝原因={exc.value.reason}，应为 NOT_HOLDER")
    assert exc.value.reason is RejectionReason.NOT_HOLDER
    assert registry.holder_of("res").holder == "alice"


def test_double_release_rejected(registry: LeaseRegistry):
    lease = registry.acquire("res", "alice", ttl=10)
    registry.release("res", "alice", lease.fencing_token)
    with pytest.raises(LeaseRejected) as exc:
        registry.release("res", "alice", lease.fencing_token)
    print("[输入] alice 重复释放同一资源")
    print(f"[判定] 拒绝原因={exc.value.reason}，应为 NOT_HELD（与伪造身份可区分）")
    assert exc.value.reason is RejectionReason.NOT_HELD


def test_fencing_tokens_strictly_increasing(registry: LeaseRegistry):
    t1 = registry.acquire("res", "alice", ttl=10).fencing_token
    t2 = registry.renew("res", "alice", t1, ttl=10).fencing_token
    registry.release("res", "alice", t2)
    t3 = registry.acquire("res", "bob", ttl=10).fencing_token
    print(f"[判定] token 序列 {t1} < {t2} < {t3}，释放后也不回退")
    assert t1 < t2 < t3


def test_renew_after_expiry_does_not_revive(registry: LeaseRegistry, clock: ManualClock):
    lease = registry.acquire("res", "alice", ttl=10)
    clock.advance(11)
    with pytest.raises(LeaseRejected) as exc:
        registry.renew("res", "alice", lease.fencing_token, ttl=10)
    print(f"[输入] ttl=10，时钟前进 11s 后续租 token={lease.fencing_token}")
    print(f"[判定] 拒绝原因={exc.value.reason}，应为 LEASE_EXPIRED，旧凭证不得复活")
    assert exc.value.reason is RejectionReason.LEASE_EXPIRED
    assert registry.holder_of("res") is None


def test_stale_token_rejected_on_guarded_access(registry: LeaseRegistry):
    old = registry.acquire("res", "alice", ttl=10)
    new = registry.renew("res", "alice", old.fencing_token, ttl=10)
    with pytest.raises(LeaseRejected) as exc:
        registry.guarded_access("res", old.fencing_token)
    print(f"[输入] 续租后当前 token={new.fencing_token}，携带旧 token={old.fencing_token} 访问")
    print(f"[判定] 拒绝原因={exc.value.reason}，应为 STALE_FENCING_TOKEN")
    assert exc.value.reason is RejectionReason.STALE_FENCING_TOKEN
    registry.guarded_access("res", new.fencing_token)  # 当前凭证放行


def test_guarded_access_after_expiry_rejected(registry: LeaseRegistry, clock: ManualClock):
    lease = registry.acquire("res", "alice", ttl=5)
    clock.advance(6)
    with pytest.raises(LeaseRejected) as exc:
        registry.guarded_access("res", lease.fencing_token)
    print(f"[判定] 过期后访问拒绝原因={exc.value.reason}，应为 LEASE_EXPIRED")
    assert exc.value.reason is RejectionReason.LEASE_EXPIRED


def test_concurrent_contention_single_winner(registry: LeaseRegistry):
    clients = [f"client-{i}" for i in range(16)]
    winners: list[str] = []
    rejections: list[RejectionReason] = []

    def compete(c: str) -> None:
        try:
            registry.acquire("res", c, ttl=60)
            winners.append(c)
        except LeaseRejected as e:
            rejections.append(e.reason)

    threads = [threading.Thread(target=compete, args=(c,)) for c in clients]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f"[输入] {len(clients)} 个客户端并发争抢同一资源")
    print(f"[判定] 获胜者={winners}，拒绝数={len(rejections)}，拒绝原因集合={set(rejections)}")
    assert len(winners) == 1, "同一时刻只能有一个持有者"
    assert len(rejections) == len(clients) - 1
    assert set(rejections) == {RejectionReason.HELD_BY_OTHER}
    assert registry.holder_of("res").holder == winners[0]


def test_retry_after_failure_no_double_hold(registry: LeaseRegistry, clock: ManualClock):
    """客户端超时重试：抖动不得造成重复占用或状态残留。"""
    first = registry.acquire("res", "alice", ttl=5)
    # 模拟 alice 第一次续租请求超时（未到达），租约到期后重试获取
    clock.advance(6)
    retry = registry.acquire("res", "alice", ttl=5)
    print(f"[输入] alice 租约 ttl=5 过期后重试获取")
    print(f"[判定] 旧 token={first.fencing_token} -> 新 token={retry.fencing_token}，严格递增")
    assert retry.fencing_token > first.fencing_token
    # 旧凭证已彻底失效：不能访问、不能释放
    with pytest.raises(LeaseRejected) as exc:
        registry.guarded_access("res", first.fencing_token)
    assert exc.value.reason is RejectionReason.STALE_FENCING_TOKEN
    with pytest.raises(LeaseRejected) as exc:
        registry.release("res", "alice", first.fencing_token)
    assert exc.value.reason is RejectionReason.STALE_FENCING_TOKEN
    # 任意时刻只有一个有效持有者
    assert registry.holder_of("res") == retry
    print("[判定] 重试后 holder_of 唯一且为新凭证，无双持有、无残留")


def test_expired_lease_allows_other_client(registry: LeaseRegistry, clock: ManualClock):
    registry.acquire("res", "alice", ttl=5)
    clock.advance(5)
    lease = registry.acquire("res", "bob", ttl=5)
    print(f"[判定] alice 租约到期后 bob 获取成功，holder={lease.holder}")
    assert lease.holder == "bob"
