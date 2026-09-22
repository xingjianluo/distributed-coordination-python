"""错误分类：所有租约相关拒绝都必须带可区分的原因码。"""

from __future__ import annotations

import enum


class RejectionReason(enum.Enum):
    """租约操作被拒绝的可区分原因。"""

    HELD_BY_OTHER = "held_by_other"            # 资源被其他客户端持有且未过期
    NOT_HOLDER = "not_holder"                  # 伪造持有者身份 / 非持有者操作
    STALE_FENCING_TOKEN = "stale_fencing_token"  # 携带落后凭证访问受保护操作
    LEASE_EXPIRED = "lease_expired"            # 租约已过期，续租/操作无效
    NOT_HELD = "not_held"                      # 资源当前无人持有
    QUOTA_EXCEEDED = "quota_exceeded"          # 客户端并发配额耗尽


class LeaseRejected(Exception):
    """租约操作被拒绝。reason 字段给出可区分的拒绝原因。"""

    def __init__(self, reason: RejectionReason, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason.value}: {detail}" if detail else reason.value)
