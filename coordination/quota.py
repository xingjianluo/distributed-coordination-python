"""按客户端维度的并发配额：限制同一客户端同时在途的请求数。

- 超限立即以 QUOTA_EXCEEDED 拒绝，不排队、不隐式等待。
- 许可通过上下文管理器归还，异常路径也会释放，不留状态残留。
"""

from __future__ import annotations

import threading
from types import TracebackType

from .errors import LeaseRejected, RejectionReason


class ClientQuota:
    """每客户端并发上限（线程安全）。"""

    def __init__(self, max_concurrent: int) -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        self._max = max_concurrent
        self._lock = threading.Lock()
        self._in_flight: dict[str, int] = {}

    def acquire(self, client: str) -> "QuotaPermit":
        """获取一个并发许可，超限抛 LeaseRejected(QUOTA_EXCEEDED)。"""
        with self._lock:
            current = self._in_flight.get(client, 0)
            if current >= self._max:
                raise LeaseRejected(
                    RejectionReason.QUOTA_EXCEEDED,
                    f"client={client} in_flight={current} max={self._max}",
                )
            self._in_flight[client] = current + 1
        return QuotaPermit(self, client)

    def _release(self, client: str) -> None:
        with self._lock:
            current = self._in_flight.get(client, 0)
            if current <= 1:
                self._in_flight.pop(client, None)
            else:
                self._in_flight[client] = current - 1

    def in_flight(self, client: str) -> int:
        """当前在途数（用于断言无状态残留）。"""
        with self._lock:
            return self._in_flight.get(client, 0)


class QuotaPermit:
    """上下文管理器：退出时自动归还许可（含异常路径），且只归还一次。"""

    def __init__(self, quota: ClientQuota, client: str) -> None:
        self._quota = quota
        self._client = client
        self._released = False

    def __enter__(self) -> "QuotaPermit":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        if not self._released:
            self._released = True
            self._quota._release(self._client)
