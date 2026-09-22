"""租约注册表：基于租约的互斥占用 + 持有权版本（fencing token）。

语义：
- 同一资源同一时刻最多一个持有者；租约到期或被主动释放后才可被他人获取。
- 每次成功获取/续租都签发严格递增的 fencing_token，过期或释放后旧 token 永不复活。
- 所有拒绝都通过 LeaseRejected.reason 给出可区分的原因码。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .clock import Clock, SystemClock
from .errors import LeaseRejected, RejectionReason


@dataclass(frozen=True)
class Lease:
    """一次成功获取的持有凭证。fencing_token 严格递增、不可回退。"""

    resource: str
    holder: str
    fencing_token: int
    expires_at: float


@dataclass
class _LeaseRecord:
    holder: str
    fencing_token: int
    expires_at: float


class LeaseRegistry:
    """进程内模拟多客户端竞争同一组资源的租约协调器（线程安全）。"""

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock: Clock = clock or SystemClock()
        self._lock = threading.Lock()
        self._leases: dict[str, _LeaseRecord] = {}
        # 每个资源历史上签发过的最大 token：单调递增，过期/释放后也不回退。
        self._max_token: dict[str, int] = {}

    # ---- 内部辅助（须在锁内调用） ----

    def _live_record(self, resource: str) -> _LeaseRecord | None:
        rec = self._leases.get(resource)
        if rec is None or rec.expires_at <= self._clock.now():
            return None
        return rec

    def _next_token(self, resource: str) -> int:
        token = self._max_token.get(resource, 0) + 1
        self._max_token[resource] = token
        return token

    # ---- 对外接口 ----

    def acquire(self, resource: str, client: str, ttl: float) -> Lease:
        """获取租约。被他人持有且未过期时拒绝；同一持有者重复获取视为续租。"""
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        with self._lock:
            rec = self._live_record(resource)
            if rec is not None and rec.holder != client:
                raise LeaseRejected(
                    RejectionReason.HELD_BY_OTHER,
                    f"resource={resource} held by {rec.holder}",
                )
            token = self._next_token(resource)
            expires_at = self._clock.now() + ttl
            self._leases[resource] = _LeaseRecord(client, token, expires_at)
            return Lease(resource, client, token, expires_at)

    def release(self, resource: str, client: str, fencing_token: int) -> None:
        """持有者主动释放。伪造身份、重复释放、落后凭证均以不同原因拒绝。"""
        with self._lock:
            rec = self._leases.get(resource)
            if rec is None or rec.expires_at <= self._clock.now():
                raise LeaseRejected(
                    RejectionReason.NOT_HELD, f"resource={resource} is not held"
                )
            if rec.holder != client:
                raise LeaseRejected(
                    RejectionReason.NOT_HOLDER,
                    f"resource={resource} held by {rec.holder}, not {client}",
                )
            if fencing_token != rec.fencing_token:
                raise LeaseRejected(
                    RejectionReason.STALE_FENCING_TOKEN,
                    f"token={fencing_token} != current={rec.fencing_token}",
                )
            del self._leases[resource]

    def renew(self, resource: str, client: str, fencing_token: int, ttl: float) -> Lease:
        """续租：签发新 token 并延长租约。过期后续租不得复活旧凭证。"""
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        with self._lock:
            rec = self._leases.get(resource)
            if rec is None or rec.expires_at <= self._clock.now():
                raise LeaseRejected(
                    RejectionReason.LEASE_EXPIRED,
                    f"resource={resource} lease expired or never held",
                )
            if rec.holder != client:
                raise LeaseRejected(
                    RejectionReason.NOT_HOLDER,
                    f"resource={resource} held by {rec.holder}, not {client}",
                )
            if fencing_token != rec.fencing_token:
                raise LeaseRejected(
                    RejectionReason.STALE_FENCING_TOKEN,
                    f"token={fencing_token} != current={rec.fencing_token}",
                )
            token = self._next_token(resource)
            expires_at = self._clock.now() + ttl
            self._leases[resource] = _LeaseRecord(client, token, expires_at)
            return Lease(resource, client, token, expires_at)

    def guarded_access(self, resource: str, fencing_token: int) -> None:
        """受保护操作：只有携带当前有效凭证才放行，落后凭证必须拒绝。"""
        with self._lock:
            rec = self._leases.get(resource)
            if rec is None:
                raise LeaseRejected(
                    RejectionReason.NOT_HELD, f"resource={resource} is not held"
                )
            if rec.expires_at <= self._clock.now():
                raise LeaseRejected(
                    RejectionReason.LEASE_EXPIRED, f"resource={resource} lease expired"
                )
            if fencing_token != rec.fencing_token:
                raise LeaseRejected(
                    RejectionReason.STALE_FENCING_TOKEN,
                    f"token={fencing_token} != current={rec.fencing_token}",
                )

    def holder_of(self, resource: str) -> Lease | None:
        """查询当前有效持有者（无或已过期则 None，不残留状态）。"""
        with self._lock:
            rec = self._live_record(resource)
            if rec is None:
                return None
            return Lease(resource, rec.holder, rec.fencing_token, rec.expires_at)
