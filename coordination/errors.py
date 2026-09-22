"""协调能力的错误分类。

每种拒绝原因对应一个独立异常类型，异常上携带 ``code``（稳定的机器可读
错误码）与 HTTP 状态码，便于调用方区分：抢占被拒 / 伪造身份 / 重复释放 /
凭证落后 / 租约过期 / 并发超限 / 资源不存在 等。
"""

from __future__ import annotations


class CoordinationError(Exception):
    """所有协调错误的基类。"""

    code: str = "coordination_error"
    http_status: int = 400

    def __init__(self, message: str, *, resource: str | None = None,
                 client_id: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.resource = resource
        self.client_id = client_id

    def to_dict(self) -> dict:
        return {
            "error": self.code,
            "message": self.message,
            "resource": self.resource,
            "client_id": self.client_id,
        }


class ResourceHeldError(CoordinationError):
    """资源已被其他客户端持有且租约未过期（抢占被拒）。"""

    code = "resource_held"
    http_status = 409


class ForgedHolderError(CoordinationError):
    """操作者并非当前持有者（伪造持有者身份 / 身份不匹配）。"""

    code = "forged_holder"
    http_status = 403


class LeaseExpiredError(CoordinationError):
    """租约已过期（含对过期凭证做续租，旧凭证不得复活）。"""

    code = "lease_expired"
    http_status = 410


class StaleFenceError(CoordinationError):
    """携带的持有凭证（fence token）落后于当前版本。"""

    code = "stale_fence"
    http_status = 409


class InvalidFenceError(CoordinationError):
    """携带的凭证无法识别（格式非法 / 资源上从未发放过该凭证）。"""

    code = "invalid_fence"
    http_status = 422


class AlreadyReleasedError(CoordinationError):
    """重复释放：租约不存在或已被释放。"""

    code = "already_released"
    http_status = 409


class ConcurrencyQuotaExceededError(CoordinationError):
    """超出该客户端允许的并发持有配额。"""

    code = "concurrency_quota_exceeded"
    http_status = 429


class ResourceNotFoundError(CoordinationError):
    """受保护访问时资源不存在（从未有持有者）。"""

    code = "resource_not_found"
    http_status = 404
