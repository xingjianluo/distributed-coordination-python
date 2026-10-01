"""统一协调门面：库方式调用与 HTTP 入口共用同一套配额、幂等与批量语义。

配额口径：按客户端隔离的“并发持有数”。一次成功的占用占一个许可；
同客户端对已持有资源的重复申请不新增许可；释放、过期、占用失败后
许可立即回收。库调用与 HTTP 必须经由同一个 Coordinator 实例，
结论才天然一致。

幂等：占用（单个/批量）可携带客户端生成的 idem_key；同一
(client, idem_key) 的重放不换出新凭证、不重复占用、不多占配额，
返回与首次完全一致的结果。键缺失（含空串/纯空白串）视为新操作。
单个占用与批量申请的幂等记录按操作类型各自隔离、互不顶替；
首次结论无论成功还是被拒都会落账，之后永远原样重放首次结论。

批量：全部成功或整体失败，任何一个资源被他人持有或配额不足都会
在写入前拒绝；异常路径回滚已拿到的资源与许可，不留部分占用。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .clock import Clock
from .errors import LeaseRejected, RejectionReason
from .quota import ClientQuota, QuotaPermit
from .registry import Lease, LeaseRegistry


@dataclass
class _Holding:
    """客户端对某资源的一次在持记录：持有许可与对应凭证版本。"""

    permit: QuotaPermit
    fencing_token: int


@dataclass
class _IdemEntry:
    """幂等操作的首次落账结论：成功凭证或被拒原因，重放原样回放。"""

    leases: tuple[Lease, ...] | None = None
    rejection: LeaseRejected | None = None


# 幂等键按操作类型隔离：单个占用与批量申请同键也互不影响。
_SCOPE_SINGLE = "single"
_SCOPE_BATCH = "batch"


def _normalized_key(idem_key: str | None) -> str | None:
    """空串或纯空白标识一律视为“没带标识”（None），不参与幂等落账。"""
    if idem_key is None or not idem_key.strip():
        return None
    return idem_key


class Coordinator:
    """库调用与 HTTP 共用的进程内协调器（线程安全）。"""

    def __init__(
        self,
        clock: Clock | None = None,
        max_concurrent_per_client: int = 8,
    ) -> None:
        self._registry = LeaseRegistry(clock)
        self._quota = ClientQuota(max_concurrent=max_concurrent_per_client)
        self._lock = threading.RLock()
        # (client, resource) -> 在持记录；许可与资源持有同生命周期。
        self._holdings: dict[tuple[str, str], _Holding] = {}
        # (client, 操作类型, idem_key) -> 首次结论（成功凭证或被拒原因），
        # 永久留档供重放原样回放；单个占用与批量申请的记录互不影响。
        self._idem: dict[tuple[str, str, str], _IdemEntry] = {}

    # ---- 内部辅助（须持 self._lock 调用） ----

    def _reap_client(self, client: str) -> None:
        """回收该客户端已失效占用对应的许可（过期、易主或登记漂移）。"""
        for (c, resource), holding in list(self._holdings.items()):
            if c != client:
                continue
            current = self._registry.holder_of(resource)
            if current is None or current.holder != client:
                holding.permit.close()
                del self._holdings[(c, resource)]
            elif current.fencing_token != holding.fencing_token:
                holding.fencing_token = current.fencing_token

    # ---- 对外接口 ----

    def acquire(
        self,
        resource: str,
        client: str,
        ttl: float,
        idem_key: str | None = None,
    ) -> Lease:
        """占用单个资源；idem_key 非空白时整笔操作幂等，重放返回首次结论。"""
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        key = _normalized_key(idem_key)
        with self._lock:
            if key is not None:
                entry = self._idem.get((client, _SCOPE_SINGLE, key))
                if entry is not None:
                    # 单个占用只重放单个占用自己的首次结论（成功或被拒）。
                    if entry.rejection is not None:
                        raise LeaseRejected(
                            entry.rejection.reason, entry.rejection.detail
                        )
                    assert entry.leases is not None
                    return entry.leases[0]

            try:
                lease = self._acquire_once(resource, client, ttl)
            except LeaseRejected as rejected:
                if key is not None:
                    # 首次被拒同样落账：之后同键重放原样回放该拒绝，
                    # 不重新判定、不换发凭证、不留任何残留状态。
                    self._idem[(client, _SCOPE_SINGLE, key)] = _IdemEntry(
                        rejection=rejected
                    )
                raise
            if key is not None:
                self._idem[(client, _SCOPE_SINGLE, key)] = _IdemEntry(
                    leases=(lease,)
                )
            return lease

    def _acquire_once(self, resource: str, client: str, ttl: float) -> Lease:
        """单个占用的实际执行（须持 self._lock），不做任何幂等处理。"""
        self._reap_client(client)

        current = self._registry.holder_of(resource)
        if current is not None and current.holder != client:
            raise LeaseRejected(
                RejectionReason.HELD_BY_OTHER,
                f"resource={resource} held by {current.holder}",
            )

        existing = self._holdings.get((client, resource))
        permit = existing.permit if existing else self._quota.acquire(client)
        try:
            lease = self._registry.acquire(resource, client, ttl)
        except BaseException:
            # 占用失败（含底层异常）：新拿的许可立即回收，无残留。
            if existing is None:
                permit.close()
            raise
        self._holdings[(client, resource)] = _Holding(
            permit, lease.fencing_token
        )
        return lease

    def acquire_batch(
        self,
        resources: list[str],
        client: str,
        ttl: float,
        idem_key: str | None = None,
    ) -> list[Lease]:
        """批量占用：全部成功或整体失败回滚，不留部分占用。"""
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        if not resources:
            raise ValueError("resources must not be empty")
        wanted = list(dict.fromkeys(resources))  # 去重保序，同一资源不重复占
        key = _normalized_key(idem_key)

        with self._lock:
            if key is not None:
                entry = self._idem.get((client, _SCOPE_BATCH, key))
                if entry is not None:
                    # 批量只重放批量自己的首次结论（成功整组凭证或被拒）。
                    if entry.rejection is not None:
                        raise LeaseRejected(
                            entry.rejection.reason, entry.rejection.detail
                        )
                    assert entry.leases is not None
                    return list(entry.leases)

            try:
                leases = self._acquire_batch_once(wanted, client, ttl)
            except LeaseRejected as rejected:
                if key is not None:
                    # 首次整单被拒也落账：重放永远回放同一拒绝原因，
                    # 不再做冲突/配额判定，不换发凭证、不留部分占用。
                    self._idem[(client, _SCOPE_BATCH, key)] = _IdemEntry(
                        rejection=rejected
                    )
                raise
            if key is not None:
                self._idem[(client, _SCOPE_BATCH, key)] = _IdemEntry(
                    leases=tuple(leases)
                )
            return leases

    def _acquire_batch_once(
        self, wanted: list[str], client: str, ttl: float
    ) -> list[Lease]:
        """批量占用的实际执行（须持 self._lock），不做任何幂等处理。"""
        self._reap_client(client)

        # 写入前统一判定：任一资源被他人持有 -> 整单拒绝，不触碰任何状态。
        for resource in wanted:
            current = self._registry.holder_of(resource)
            if current is not None and current.holder != client:
                raise LeaseRejected(
                    RejectionReason.HELD_BY_OTHER,
                    f"resource={resource} held by {current.holder}",
                )

        # 已自持的资源不新增许可；其余资源各需一个新许可。
        new_resources = [r for r in wanted if (client, r) not in self._holdings]
        if self._quota.in_flight(client) + len(new_resources) > self._quota.limit:
            raise LeaseRejected(
                RejectionReason.QUOTA_EXCEEDED,
                f"client={client} need={len(new_resources)} "
                f"in_flight={self._quota.in_flight(client)} "
                f"max={self._quota.limit}",
            )

        leases: list[Lease] = []
        granted: list[tuple[str, Lease, QuotaPermit]] = []
        try:
            for resource in wanted:
                holding = self._holdings.get((client, resource))
                if holding is not None:
                    # 已自持：无键=新操作，换发新凭证；但沿用原许可，不多占。
                    permit = holding.permit
                    lease = self._registry.acquire(resource, client, ttl)
                    leases.append(lease)
                else:
                    permit = self._quota.acquire(client)
                    lease = self._registry.acquire(resource, client, ttl)
                    granted.append((resource, lease, permit))
                    leases.append(lease)
        except BaseException:
            # 整体失败：仅释放本单新拿到的资源并回收其许可。
            # 批量前就自持的资源不属于本单新增，保留其持有（不回滚误伤）。
            for resource, lease, permit in granted:
                permit.close()
                try:
                    self._registry.release(resource, client, lease.fencing_token)
                except LeaseRejected:
                    pass
            raise

        for resource, lease, permit in granted:
            self._holdings[(client, resource)] = _Holding(
                permit, lease.fencing_token
            )
        return leases

    def release(self, resource: str, client: str, fencing_token: int) -> None:
        """释放占用并回收该客户端的并发许可。"""
        with self._lock:
            try:
                self._registry.release(resource, client, fencing_token)
            except BaseException:
                self._reap_client(client)
                raise
            holding = self._holdings.pop((client, resource), None)
            if holding is not None:
                holding.permit.close()

    def renew(
        self, resource: str, client: str, fencing_token: int, ttl: float
    ) -> Lease:
        """续租（不新增并发许可），并同步内部凭证版本。"""
        with self._lock:
            try:
                lease = self._registry.renew(resource, client, fencing_token, ttl)
            except BaseException:
                self._reap_client(client)
                raise
            holding = self._holdings.get((client, resource))
            if holding is not None:
                holding.fencing_token = lease.fencing_token
            return lease

    def guarded_access(self, resource: str, fencing_token: int) -> None:
        """受保护访问，直接委托租约注册表。"""
        self._registry.guarded_access(resource, fencing_token)

    def holder_of(self, resource: str) -> Lease | None:
        """查询当前有效持有者。"""
        return self._registry.holder_of(resource)

    def live_hold_count(self, client: str) -> int:
        """客户端当前有效持有数（过期/释放/失败后无残留）。"""
        with self._lock:
            self._reap_client(client)
            return sum(1 for c, _ in self._holdings if c == client)

    def in_flight(self, client: str) -> int:
        """客户端当前占用的并发许可数，应与 live_hold_count 一致。"""
        with self._lock:
            self._reap_client(client)
            return self._quota.in_flight(client)
