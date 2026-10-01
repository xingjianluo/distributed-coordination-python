"""统一协调门面：库方式调用与 HTTP 入口共用同一套配额、幂等与批量语义。

配额口径：按客户端隔离的“并发持有数”。一次成功的占用占一个许可；
同客户端对已持有资源的重复申请不新增许可；释放、过期、占用失败后
许可立即回收。库调用与 HTTP 必须经由同一个 Coordinator 实例，
结论才天然一致。

幂等：占用（单个/批量）可携带客户端生成的 idem_key；同一
(client, 操作类型, idem_key) 的重放不换出新凭证、不重复占用、不多占
配额，返回与首次完全一致的结果。单个占用与批量申请各自维护互不影响
的幂等记录：同一标识用于两种操作时各自只重放自己的首次结果，跨类型
互不顶替。标识为 None 或仅含空白时一律视为未提供（每次都是新操作）。
首次操作无论成功还是被拒（held_by_other / quota_exceeded）都会落账，
重放原样重放首次结论；被拒落账不产生任何许可或资源残留。

批量：全部成功或整体失败，任何一个资源被他人持有或配额不足都会
在写入前拒绝；异常路径回滚已拿到的资源与许可，不留部分占用。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

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
    """幂等操作的落账结果：整笔操作只生效一次，重放原样返回。

    成功时保存首次凭证；首次被拒时保存拒绝原因与详情，重放抛出与首次
    完全一致的拒绝。两种结论都不允许第二次真正执行。
    """

    leases: tuple[Lease, ...] = field(default_factory=tuple)
    rejected_reason: RejectionReason | None = None
    rejected_detail: str = ""


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
        # (client, op_kind, idem_key) -> 首次结论（成功或被拒，永久留档）。
        # op_kind 区分单个占用与批量申请，两类记录互不可见、互不顶替。
        self._idem: dict[tuple[str, str, str], _IdemEntry] = {}

    # ---- 内部辅助（须持 self._lock 调用） ----

    @staticmethod
    def _norm_idem_key(idem_key: str | None) -> str | None:
        """空串或仅含空白的标识一律按未提供处理；非空键原样保留。"""
        if idem_key is None:
            return None
        key = idem_key.strip()
        return key or None

    def _idem_replay(
        self, client: str, op_kind: str, idem_key: str | None
    ) -> _IdemEntry | None:
        """命中幂等账则原样重放首次结论：成功返回落账，被拒抛首次拒绝。"""
        if idem_key is None:
            return None
        entry = self._idem.get((client, op_kind, idem_key))
        if entry is None:
            return None
        if entry.rejected_reason is not None:
            raise LeaseRejected(entry.rejected_reason, entry.rejected_detail)
        return entry

    def _record_rejection(
        self,
        client: str,
        op_kind: str,
        idem_key: str | None,
        exc: LeaseRejected,
    ) -> None:
        """把首次拒绝结论落账，供同标识重放原样重放（不产生任何残留）。"""
        if idem_key is not None:
            self._idem[(client, op_kind, idem_key)] = _IdemEntry(
                rejected_reason=exc.reason, rejected_detail=exc.detail
            )

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
        """占用单个资源；idem_key 非空白时整笔操作幂等，重放返回首次结果。

        幂等记录与批量申请相互独立；首次若被拒，重放抛出与首次一致的拒绝。
        """
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        idem_key = self._norm_idem_key(idem_key)
        with self._lock:
            entry = self._idem_replay(client, "acquire", idem_key)
            if entry is not None:
                return entry.leases[0]

            self._reap_client(client)

            try:
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
            except LeaseRejected as rejected:
                # 首次拒绝同样落账：同标识重放只重放结论，不再次执行。
                self._record_rejection(client, "acquire", idem_key, rejected)
                raise
            self._holdings[(client, resource)] = _Holding(
                permit, lease.fencing_token
            )
            if idem_key is not None:
                self._idem[(client, "acquire", idem_key)] = _IdemEntry((lease,))
            return lease

    def acquire_batch(
        self,
        resources: list[str],
        client: str,
        ttl: float,
        idem_key: str | None = None,
    ) -> list[Lease]:
        """批量占用：全部成功或整体失败回滚，不留部分占用。

        幂等记录与单个占用相互独立；首次若被拒，重放抛出与首次一致的拒绝。
        """
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        if not resources:
            raise ValueError("resources must not be empty")
        wanted = list(dict.fromkeys(resources))  # 去重保序，同一资源不重复占
        idem_key = self._norm_idem_key(idem_key)

        with self._lock:
            entry = self._idem_replay(client, "acquire_batch", idem_key)
            if entry is not None:
                return list(entry.leases)

            self._reap_client(client)

            try:
                # 写入前统一判定：任一资源被他人持有 -> 整单拒绝，不触碰任何状态。
                for resource in wanted:
                    current = self._registry.holder_of(resource)
                    if current is not None and current.holder != client:
                        raise LeaseRejected(
                            RejectionReason.HELD_BY_OTHER,
                            f"resource={resource} held by {current.holder}",
                        )

                # 已自持的资源不新增许可；其余资源各需一个新许可。
                new_resources = [
                    r for r in wanted if (client, r) not in self._holdings
                ]
                if (
                    self._quota.in_flight(client) + len(new_resources)
                    > self._quota.limit
                ):
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
                            self._registry.release(
                                resource, client, lease.fencing_token
                            )
                        except LeaseRejected:
                            pass
                    raise
            except LeaseRejected as rejected:
                # 首次拒绝同样落账：同标识重放只重放结论，不再次执行。
                self._record_rejection(client, "acquire_batch", idem_key, rejected)
                raise

            for resource, lease, permit in granted:
                self._holdings[(client, resource)] = _Holding(
                    permit, lease.fencing_token
                )
            if idem_key is not None:
                self._idem[(client, "acquire_batch", idem_key)] = _IdemEntry(
                    tuple(leases)
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
