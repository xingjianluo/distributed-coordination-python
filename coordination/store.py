"""进程内租约状态机（核心协调逻辑）。

用一把锁 + 内存字典模拟多客户端竞争同一组资源：

- 互斥：同一资源同一时刻只有一个活跃持有者；租约未过期时其他客户端
  的抢占会被拒绝（``ResourceHeldError``）。
- 凭证：资源维度单调递增的 fence version。每次成功 *获取* 产生新版本；
  续租不换版本，因此旧版本在租约过期后不可能通过续租"复活"。
- 配额：按客户端统计活跃租约数（默认 1）；结合 ``request_id`` 幂等去重，
  超时重试 / 短期抖动返回同一凭证，不会造成重复占用或双持有。
"""

from __future__ import annotations

import logging
import threading
from collections import defaultdict

from .clock import Clock, SystemClock
from .errors import (
    AlreadyReleasedError,
    ConcurrencyQuotaExceededError,
    ForgedHolderError,
    InvalidFenceError,
    LeaseExpiredError,
    ResourceHeldError,
    ResourceNotFoundError,
    StaleFenceError,
)
from .models import (
    AccessRequest,
    AcquireRequest,
    FenceToken,
    Lease,
    ReleaseRequest,
    RenewRequest,
)

logger = logging.getLogger("coordination.store")


class LeaseStore:
    def __init__(self, clock: Clock | None = None,
                 default_quota: int = 1) -> None:
        self._clock = clock or SystemClock()
        self._default_quota = default_quota
        # resource -> 当前持有的租约（过期后仍保留，直到被新获取/显式清理）
        self._leases: dict[str, Lease] = {}
        # resource -> 已发放的最大凭证版本（单调递增，不可回退）
        self._latest_version: dict[str, int] = defaultdict(int)
        # client_id -> 覆盖默认值的并发配额
        self._quotas: dict[str, int] = {}
        # f"{client_id}:{request_id}" -> 首次成功获取返回的凭证（幂等重放）
        self._recent_acquires: dict[str, FenceToken] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ #
    # 内部辅助（调用方须持锁）
    # ------------------------------------------------------------------ #
    def _active_lease(self, resource: str) -> Lease | None:
        """返回资源当前的活跃租约；已过期则惰性清除并返回 None。"""
        lease = self._leases.get(resource)
        if lease is None:
            return None
        if lease.is_active(self._clock.now()):
            return lease
        logger.info("判定依据 resource=%s 租约 v%s 已过期(now=%.3f expires=%.3f)",
                    resource, lease.version, self._clock.now(), lease.expires_at)
        del self._leases[resource]
        return None

    def _active_count_for(self, client_id: str) -> int:
        now = self._clock.now()
        return sum(
            1 for lease in self._leases.values()
            if lease.holder == client_id and lease.is_active(now)
        )

    def _check_holder_and_version(self, resource: str, client_id: str,
                                  version: int) -> Lease:
        """校验"持有者身份 + 凭证版本"，返回活跃租约或抛出可区分错误。"""
        lease = self._active_lease(resource)
        if lease is None:
            # 不活跃：无法判断持有者，统一按过期处理（旧凭证不得复活）
            raise LeaseExpiredError(
                f"资源 {resource} 没有活跃租约，凭证 v{version} 已失效",
                resource=resource, client_id=client_id)
        latest = self._latest_version[resource]
        # 先判凭证本身：无法识别的版本号
        if version > latest:
            raise InvalidFenceError(
                f"凭证 v{version} 无法识别（资源 {resource} 当前最大版本 v{latest}）",
                resource=resource, client_id=client_id)
        # 再判凭证新鲜度：落后版本（旧持有者的旧凭证也走这里）
        if version < lease.version:
            raise StaleFenceError(
                f"凭证 v{version} 落后于当前版本 v{lease.version}，拒绝访问",
                resource=resource, client_id=client_id)
        # 凭证是当前版本但持有者身份不符 -> 伪造身份
        if lease.holder != client_id:
            raise ForgedHolderError(
                f"客户端 {client_id} 不是资源 {resource} 的持有者"
                f"（持有者为 {lease.holder}）",
                resource=resource, client_id=client_id)
        return lease

    # ------------------------------------------------------------------ #
    # 配额管理
    # ------------------------------------------------------------------ #
    def set_quota(self, client_id: str, quota: int) -> None:
        if quota < 1:
            raise ValueError("并发配额必须 >= 1")
        with self._lock:
            self._quotas[client_id] = quota
            logger.info("配置 client=%s 并发配额=%s", client_id, quota)

    def get_quota(self, client_id: str) -> int:
        with self._lock:
            return self._quotas.get(client_id, self._default_quota)

    # ------------------------------------------------------------------ #
    # 获取（acquire）
    # ------------------------------------------------------------------ #
    def acquire(self, req: AcquireRequest) -> FenceToken:
        if req.ttl <= 0:
            raise ValueError("ttl 必须为正数")
        with self._lock:
            now = self._clock.now()

            # 1) 同 request_id 的重试：幂等返回首次凭证，不产生重复占用
            if req.request_id is not None:
                key = f"{req.client_id}:{req.request_id}"
                cached = self._recent_acquires.get(key)
                if cached is not None:
                    logger.info("输入 client=%s resource=%s request_id=%s "
                                "命中幂等重放 -> v%s",
                                req.client_id, req.resource, req.request_id,
                                cached.version)
                    return cached

            lease = self._active_lease(req.resource)

            # 2) 资源被他人活跃持有 -> 抢占被拒
            if lease is not None and lease.holder != req.client_id:
                logger.info("判定依据 resource=%s 被 client=%s 以 v%s 持有至 %.3f，"
                            "client=%s 抢占被拒",
                            req.resource, lease.holder, lease.version,
                            lease.expires_at, req.client_id)
                raise ResourceHeldError(
                    f"资源 {req.resource} 已被 {lease.holder} 持有"
                    f"（凭证 v{lease.version}，剩余 {lease.remaining(now):.3f}s）",
                    resource=req.resource, client_id=req.client_id)

            # 3) 自己已活跃持有该资源：不换凭证（防止抖动重试造成双持有）
            if lease is not None:
                token = FenceToken(req.resource, lease.version)
                logger.info("输入 client=%s resource=%s 已持有 v%s，"
                            "重复获取返回同一凭证",
                            req.client_id, req.resource, lease.version)
                return token

            # 4) 并发配额校验（按活跃租约计数，过期租约不占名额）
            quota = self.get_quota(req.client_id)
            active = self._active_count_for(req.client_id)
            if active >= quota:
                logger.info("判定依据 client=%s 活跃持有=%s 已达配额=%s，拒绝",
                            req.client_id, active, quota)
                raise ConcurrencyQuotaExceededError(
                    f"客户端 {req.client_id} 并发持有 {active} 已达配额 {quota}",
                    resource=req.resource, client_id=req.client_id)

            # 5) 发放新凭证：版本严格递增
            version = self._latest_version[req.resource] + 1
            self._latest_version[req.resource] = version
            self._leases[req.resource] = Lease(
                resource=req.resource,
                holder=req.client_id,
                version=version,
                acquired_at=now,
                expires_at=now + req.ttl,
            )
            token = FenceToken(req.resource, version)
            if req.request_id is not None:
                self._recent_acquires[f"{req.client_id}:{req.request_id}"] = token
            logger.info("输入 client=%s resource=%s ttl=%.3f -> 获取成功 v%s",
                        req.client_id, req.resource, req.ttl, version)
            return token

    # ------------------------------------------------------------------ #
    # 续租（renew）：不提升版本；过期租约不得续租
    # ------------------------------------------------------------------ #
    def renew(self, req: RenewRequest) -> FenceToken:
        with self._lock:
            lease = self._check_holder_and_version(
                req.resource, req.client_id, req.version)
            ttl = req.ttl if req.ttl is not None else (
                lease.expires_at - lease.acquired_at)
            if ttl <= 0:
                raise ValueError("ttl 必须为正数")
            lease.expires_at = self._clock.now() + ttl
            logger.info("输入 client=%s resource=%s v%s 续租 ttl=%.3f "
                        "-> 成功，版本不变，新到期=%.3f",
                        req.client_id, req.resource, req.version, ttl,
                        lease.expires_at)
            return FenceToken(req.resource, lease.version)

    # ------------------------------------------------------------------ #
    # 释放（release）
    # ------------------------------------------------------------------ #
    def release(self, req: ReleaseRequest) -> FenceToken:
        with self._lock:
            lease = self._active_lease(req.resource)
            if lease is None:
                logger.info("判定依据 resource=%s 无活跃租约，"
                            "client=%s v%s 重复释放/释放过期租约被拒",
                            req.resource, req.client_id, req.version)
                raise AlreadyReleasedError(
                    f"资源 {req.resource} 不存在活跃租约，无需释放",
                    resource=req.resource, client_id=req.client_id)
            # 顺序与统一校验一致：先凭证（无法识别/落后），后持有者身份
            latest = self._latest_version[req.resource]
            if req.version > latest:
                raise InvalidFenceError(
                    f"凭证 v{req.version} 无法识别（当前最大版本 v{latest}）",
                    resource=req.resource, client_id=req.client_id)
            if req.version < lease.version:
                raise StaleFenceError(
                    f"凭证 v{req.version} 落后于当前版本 v{lease.version}，拒绝释放",
                    resource=req.resource, client_id=req.client_id)
            if lease.holder != req.client_id:
                logger.info("判定依据 resource=%s 持有者=%s，"
                            "client=%s 伪造身份释放被拒",
                            req.resource, lease.holder, req.client_id)
                raise ForgedHolderError(
                    f"客户端 {req.client_id} 不是资源 {req.resource} 的持有者"
                    f"（持有者为 {lease.holder}）",
                    resource=req.resource, client_id=req.client_id)
            del self._leases[req.resource]
            logger.info("输入 client=%s resource=%s v%s -> 释放成功",
                        req.client_id, req.resource, req.version)
            return FenceToken(req.resource, req.version)

    # ------------------------------------------------------------------ #
    # 受保护操作访问：校验持有者身份与凭证新鲜度
    # ------------------------------------------------------------------ #
    def access(self, req: AccessRequest) -> FenceToken:
        with self._lock:
            if req.resource not in self._latest_version:
                raise ResourceNotFoundError(
                    f"资源 {req.resource} 从未发放过凭证",
                    resource=req.resource, client_id=req.client_id)
            lease = self._check_holder_and_version(
                req.resource, req.client_id, req.version)
            logger.info("输入 client=%s resource=%s v%s -> 受保护操作访问允许",
                        req.client_id, req.resource, req.version)
            return FenceToken(req.resource, lease.version)

    # ------------------------------------------------------------------ #
    # 调试 / 测试
    # ------------------------------------------------------------------ #
    def sweep_expired(self) -> int:
        """主动清理所有过期租约，返回清理数量。"""
        with self._lock:
            expired = [r for r, l in self._leases.items()
                       if not l.is_active(self._clock.now())]
            for r in expired:
                del self._leases[r]
            return len(expired)

    def snapshot(self) -> dict:
        with self._lock:
            now = self._clock.now()
            return {
                "now": now,
                "active": {
                    r: {
                        "holder": l.holder,
                        "version": l.version,
                        "expires_in": l.remaining(now),
                    }
                    for r, l in self._leases.items()
                    if l.is_active(now)
                },
                "latest_versions": dict(self._latest_version),
                "quotas": dict(self._quotas),
            }
