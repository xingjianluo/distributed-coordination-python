"""客户端超时重试的幂等性：同一 (client, idempotency_key) 只生效一次。

- 重放不签发新 fencing_token、不重复占用、不多占配额，结果与首次完全一致；
- 标识缺失或换标识按新操作处理；
- 首次失败也被记住，重放返回同一个可区分拒绝原因，且不留状态。
"""

from __future__ import annotations

import pytest

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
    return LeaseService(max_concurrent=3, clock=clock)


def test_replay_returns_identical_lease_without_new_version(service: LeaseService):
    first = service.acquire("res", "c1", ttl=10, idempotency_key="op-1")
    replay = service.acquire("res", "c1", ttl=10, idempotency_key="op-1")
    print(f"[输入] 同键 op-1 重放；首次 token={first.fencing_token} "
          f"expires={first.expires_at}")
    print(f"[判定] 重放 token={replay.fencing_token} expires={replay.expires_at}，"
          "须与首次逐字段一致")
    assert replay == first
    assert replay.fencing_token == first.fencing_token
    assert service.used_quota("c1") == 1


def test_replay_does_not_consume_extra_quota_even_when_full(service: LeaseService):
    service.acquire("r1", "c1", ttl=10, idempotency_key="k1")
    service.acquire("r2", "c1", ttl=10)
    service.acquire("r3", "c1", ttl=10)
    print(f"[输入] c1 已占满配额 used={service.used_quota('c1')}")
    # 配额已耗尽；普通新操作被拒
    with pytest.raises(LeaseRejected) as exc:
        service.acquire("r4", "c1", ttl=10)
    assert exc.value.reason is RejectionReason.QUOTA_EXCEEDED
    # 但带原键的重放必须成功返回首次结果，且不额外占配额
    replay = service.acquire("r1", "c1", ttl=10, idempotency_key="k1")
    print(f"[判定] 满额下幂等重放仍返回 token={replay.fencing_token}，used 仍为 "
          f"{service.used_quota('c1')}")
    assert replay.resource == "r1"
    assert service.used_quota("c1") == 3


def test_missing_key_is_treated_as_new_operation(service: LeaseService):
    first = service.acquire("res", "c1", ttl=10)
    second = service.acquire("res", "c1", ttl=10)  # 无键：新操作（同持有者续租）
    print(f"[输入] 两次无键获取同一资源：token {first.fencing_token} -> "
          f"{second.fencing_token}")
    print("[判定] 无标识按新操作处理：签发新 token，但不重复占配额")
    assert second.fencing_token > first.fencing_token
    assert service.used_quota("c1") == 1


def test_different_key_is_treated_as_new_operation(service: LeaseService):
    first = service.acquire("res", "c1", ttl=10, idempotency_key="k-a")
    second = service.acquire("res", "c1", ttl=10, idempotency_key="k-b")
    print(f"[输入] 同一资源换新键 k-a -> k-b：token "
          f"{first.fencing_token} -> {second.fencing_token}")
    print("[判定] 换标识是新操作：签发新 token，不重复占配额")
    assert second.fencing_token > first.fencing_token
    assert service.used_quota("c1") == 1


def test_failed_operation_replayed_with_same_reason(service: LeaseService):
    service.acquire("locked", "owner", ttl=10)
    with pytest.raises(LeaseRejected) as first:
        service.acquire("locked", "c1", ttl=10, idempotency_key="fail-1")
    with pytest.raises(LeaseRejected) as replay:
        service.acquire("locked", "c1", ttl=10, idempotency_key="fail-1")
    print("[输入] 冲突操作带键 fail-1 重试两次")
    print(f"[判定] 首次={first.value.reason} 重放={replay.value.reason}，"
          "须同一原因；且不占任何配额/持有")
    assert replay.value.reason is first.value.reason
    assert replay.value.reason is RejectionReason.HELD_BY_OTHER
    assert service.used_quota("c1") == 0
    assert service.holder_of("locked").holder == "owner"


def test_quota_failure_is_idempotent_and_leaves_no_residue(service: LeaseService):
    service.acquire("a", "c1", ttl=10)
    service.acquire("b", "c1", ttl=10)
    service.acquire("c", "c1", ttl=10)
    with pytest.raises(LeaseRejected) as first:
        service.acquire("d", "c1", ttl=10, idempotency_key="q-fail")
    with pytest.raises(LeaseRejected) as replay:
        service.acquire("d", "c1", ttl=10, idempotency_key="q-fail")
    print(f"[判定] 配额失败重放 {replay.value.reason} == {first.value.reason}")
    assert replay.value.reason is RejectionReason.QUOTA_EXCEEDED
    assert service.holder_of("d") is None
    assert service.used_quota("c1") == 3


def test_batch_replay_identical_without_extra_quota(service: LeaseService):
    first = service.acquire_batch(["x1", "x2"], "c1", ttl=10, idempotency_key="bk")
    replay = service.acquire_batch(["x1", "x2"], "c1", ttl=10, idempotency_key="bk")
    tokens_first = [l.fencing_token for l in first]
    tokens_replay = [l.fencing_token for l in replay]
    print(f"[判定] 批量重放 token={tokens_replay}，须等于首次 {tokens_first}")
    assert replay == first
    assert tokens_replay == tokens_first
    assert service.used_quota("c1") == 2


def test_single_and_batch_keys_are_namespaced(service: LeaseService):
    single = service.acquire("solo", "c1", ttl=10, idempotency_key="same-key")
    batch = service.acquire_batch(["solo", "other"], "c1", ttl=10,
                                  idempotency_key="same-key")
    print(f"[输入] 单个与批量使用相同字面键 same-key；单个 token={single.fencing_token}")
    print(f"[判定] 两者互不串用：批量返回 {len(batch)} 个租约，且只新增 1 个配额")
    assert len(batch) == 2
    assert service.used_quota("c1") == 2
