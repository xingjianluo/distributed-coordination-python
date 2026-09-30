"""统一协调服务：在租约互斥之上叠加统一配额、幂等重放与批量原子占用。

所有调用方式（库直调 / HTTP）都经由本服务，因此配额口径、拒绝原因与
幂等结论在各入口下完全一致：

- 配额按客户端隔离，计量口径为"当前有效持有的资源数"；占用成功才计入，
  释放、过期、失败都会回收，绝不残留。
- 占用请求可携带客户端生成的 idempotency_key：同一 (client, key) 的
  重试只生效一次，重放返回与首次完全一致的结果。
- acquire_batch 一次申请一组资源，全部成功才提交，任一冲突/配额不足则
  整体失败并回滚，绝不留下半组持有。
"""

from __future__ import annotations

import threading

from .clock import Clock, SystemClock
from .errors import LeaseRejected, RejectionReason
from .quota import ClientQuota, QuotaPermit
from .registry import Lease, LeaseRegistry


class _IdemRecord:
    """一笔幂等占用操作的首次结论：成功保存结果，失败保存可复现的拒绝。"""

    __slots__ = ("detail", "leases", "ok", "reason")

    def __init__(
        self,
        ok: bool,
        leases: tuple[Lease, ...] | None = None,
        reason: RejectionReason | None = None,
        detail: str = "",
    ) -> None:
        self.ok = ok
        self.leases = leases
        self.reason = reason
        self.detail = detail


class LeaseService:
    """库入口与 HTTP 入口共享的协调门面（线程安全）。

    锁顺序固定为：LeaseService 锁 -> LeaseRegistry 锁，避免死锁。
    """

    def __init__(
        self,
        max_concurrent: int = 8,
        clock: Clock | None = None,
    ) -> None:
        self._clock: Clock = clock or SystemClock()
        self._registry = LeaseRegistry(self._clock)
        self._quota = ClientQuota(max_concurrent=max_concurrent)
        self._quota_limit = max_concurrent
        self._lock = threading.Lock()
        # 经本服务成功占用的资源 -> (所属客户端, 配额许可)。
        # 释放时删除；过期由 sweep 惰性回收。一个资源对应一个许可。
        self._permits: dict[str, tuple[str, QuotaPermit]] = {}
        # 幂等记录：(命名空间, 客户端, key) -> 首次结论。
        self._idem: dict[tuple[str, str, str], _IdemRecord] = {}

    # ---- 内部辅助（调用方须持有 self._lock） ----

    def _sweep_expired(self) -> None:
        """回收所有已失效占用的配额许可，过期不留残留。"""
        dead = [
            resource
            for resource in self._permits
            if self._registry.holder_of(resource) is None
        ]
        for resource in dead:
            _, permit = self._permits.pop(resource)
            permit.close()

    def _live_count(self, client: str) -> int:
        """客户端当前计入配额的有效持有数（调用前应已 sweep）。"""
        return sum(1 for owner, _ in self._permits.values() if owner == client)

    @staticmethod
    def _replay_failure(rec: _IdemRecord) -> LeaseRejected:
        assert rec.reason is not None
        return LeaseRejected(rec.reason, rec.detail)

    # ---- 对外接口 ----

    def acquire(
        self,
        resource: str,
        client: str,
        ttl: float,
        *,
        idempotency_key: str | None = None,
    ) -> Lease:
        """占用单个资源；携带 idempotency_key 时整笔操作只生效一次。

        - 同一持有者对自己仍持有资源的重复获取视为续租，不重复占配额。
        - 先判资源冲突（held_by_other），再判配额（quota_exceeded），
          拒绝原因因此始终可区分。
        - 失败路径不拿许可、不留任何状态。
        """
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        key = idempotency_key.strip() if idempotency_key else None
        with self._lock:
            cached = self._idem.get(("acquire", client, key)) if key else None
            if cached is not None:
                if not cached.ok:
                    raise self._replay_failure(cached)
                assert cached.leases is not None
                return cached.leases[0]

            self._sweep_expired()
            current = self._registry.holder_of(resource)
            if current is not None and current.holder != client:
                rejected = LeaseRejected(
                    RejectionReason.HELD_BY_OTHER,
                    f"resource={resource} held by {current.holder}",
                )
                if key:
                    self._idem[("acquire", client, key)] = _IdemRecord(
                        False, reason=rejected.reason, detail=rejected.detail
                    )
                raise rejected

            # 同持有者续租语义：沿用既有许可，不多占一个配额。
            existing = self._permits.get(resource)
            reused = existing is not None and existing[0] == client
            permit = existing[1] if reused else None
            if permit is None:
                used = self._live_count(client)
                if used >= self._quota_limit:
                    rejected = LeaseRejected(
                        RejectionReason.QUOTA_EXCEEDED,
                        f"client={client} held={used} max={self._quota_limit}",
                    )
                    if key:
                        self._idem[("acquire", client, key)] = _IdemRecord(
                            False, reason=rejected.reason, detail=rejected.detail
                        )
                    raise rejected
                permit = self._quota.acquire(client)

            try:
                lease = self._registry.acquire(resource, client, ttl)
            except BaseException:
                # 注册表失败（如被并发抢占）必须回滚刚拿到的新许可。
                if not reused:
                    permit.close()
                raise
            self._permits[resource] = (client, permit)
            if key:
                self._idem[("acquire", client, key)] = _IdemRecord(
                    True, leases=(lease,)
                )
            return lease

    def acquire_batch(
        self,
        resources: list[str] | tuple[str, ...],
        client: str,
        ttl: float,
        *,
        idempotency_key: str | None = None,
    ) -> tuple[Lease, ...]:
        """原子批量占用：全部成功返回全部租约，否则整体失败不残留。

        - 任一资源被其他客户端持有 -> held_by_other，整体失败；
        - 剔除该客户端仍持有的资源（续租、不占新许可）后，剩余所需许可
          超过剩余配额 -> quota_exceeded，整体失败；
        - 冲突与配额都在任何写入前判定，绝不"占一半"，也不排队干等。
        """
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        names = list(dict.fromkeys(resources))
        if not names:
            raise ValueError("resources must not be empty")
        key = idempotency_key.strip() if idempotency_key else None
        with self._lock:
            cached = self._idem.get(("batch", client, key)) if key else None
            if cached is not None:
                if not cached.ok:
                    raise self._replay_failure(cached)
                assert cached.leases is not None
                return cached.leases

            self._sweep_expired()

            # 阶段一：只读判定。冲突优先于配额，拒绝原因可区分。
            needed: list[str] = []
            for resource in names:
                current = self._registry.holder_of(resource)
                if current is not None and current.holder != client:
                    rejected = LeaseRejected(
                        RejectionReason.HELD_BY_OTHER,
                        f"resource={resource} held by {current.holder}",
                    )
                    if key:
                        self._idem[("batch", client, key)] = _IdemRecord(
                            False, reason=rejected.reason, detail=rejected.detail
                        )
                    raise rejected
                entry = self._permits.get(resource)
                if entry is None or entry[0] != client:
                    needed.append(resource)

            used = self._live_count(client)
            if used + len(needed) > self._quota_limit:
                rejected = LeaseRejected(
                    RejectionReason.QUOTA_EXCEEDED,
                    f"client={client} held={used} need={len(needed)} "
                    f"max={self._quota_limit}",
                )
                if key:
                    self._idem[("batch", client, key)] = _IdemRecord(
                        False, reason=rejected.reason, detail=rejected.detail
                    )
                raise rejected

            # 阶段二：先一次性拿齐所需许可。
            new_permits: list[tuple[str, QuotaPermit]] = []
            for resource in needed:
                new_permits.append((resource, self._quota.acquire(client)))

            # 阶段三：逐个写入注册表；中途失败则回滚本批全部变更，不留半组。
            # 注意：仅回滚本批新增的资源；客户端原本就持有（续租）的保持不动。
            new_map = dict(new_permits)
            leases: list[Lease] = []
            try:
                for resource in names:
                    leases.append(self._registry.acquire(resource, client, ttl))
            except BaseException:
                for lease in leases:
                    if lease.resource in new_map:
                        try:
                            self._registry.release(
                                lease.resource, client, lease.fencing_token
                            )
                        except LeaseRejected:
                            pass
                for resource, permit in new_permits:
                    permit.close()
                    self._permits.pop(resource, None)
                raise

            for resource, permit in new_permits:
                self._permits[resource] = (client, permit)
            result = tuple(leases)
            if key:
                self._idem[("batch", client, key)] = _IdemRecord(True, leases=result)
            return result

    def renew(self, resource: str, client: str, fencing_token: int, ttl: float) -> Lease:
        """持有者续租（语义同 LeaseRegistry.renew）。"""
        with self._lock:
            return self._registry.renew(resource, client, fencing_token, ttl)

    def release(self, resource: str, client: str, fencing_token: int) -> None:
        """持有者释放，同时归还该占用对应的配额许可。"""
        with self._lock:
            self._sweep_expired()
            self._registry.release(resource, client, fencing_token)
            entry = self._permits.pop(resource, None)
            if entry is not None:
                entry[1].close()

    def guarded_access(self, resource: str, fencing_token: int) -> None:
        """受保护操作的凭证校验。"""
        self._registry.guarded_access(resource, fencing_token)

    def holder_of(self, resource: str) -> Lease | None:
        """查询当前有效持有者。"""
        return self._registry.holder_of(resource)

    def used_quota(self, client: str) -> int:
        """某客户端当前计入配额的有效持有数（过期持有先回收再统计）。"""
        with self._lock:
            self._sweep_expired()
            return self._live_count(client)

    def quota_limit(self) -> int:
        return self._quota_limit
