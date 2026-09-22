"""协调能力的数据模型。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FenceToken:
    """持有凭证：``(resource, version)``。

    ``version`` 在单个资源维度严格递增且不可回退；每次成功获取（acquire）
    产生新凭证。续租不提升版本号，因此过期后续租不可能让旧凭证复活。
    """

    resource: str
    version: int

    def encode(self) -> str:
        return f"{self.resource}#{self.version}"


@dataclass
class Lease:
    """一张有效的资源租约。"""

    resource: str
    holder: str
    version: int
    acquired_at: float
    expires_at: float

    def is_active(self, now: float) -> bool:
        return now < self.expires_at

    def remaining(self, now: float) -> float:
        return max(0.0, self.expires_at - now)


@dataclass(frozen=True)
class AcquireRequest:
    resource: str
    client_id: str
    ttl: float
    request_id: str | None = None


@dataclass(frozen=True)
class RenewRequest:
    resource: str
    client_id: str
    version: int
    ttl: float | None = None


@dataclass(frozen=True)
class ReleaseRequest:
    resource: str
    client_id: str
    version: int


@dataclass(frozen=True)
class AccessRequest:
    resource: str
    client_id: str
    version: int
