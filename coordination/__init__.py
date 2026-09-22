"""本地进程内分布式协调能力：租约互斥、持有凭证、并发配额与失败重试。"""

from .clock import Clock, FakeClock, SystemClock
from .errors import (
    AlreadyReleasedError,
    ConcurrencyQuotaExceededError,
    CoordinationError,
    ForgedHolderError,
    InvalidFenceError,
    LeaseExpiredError,
    ResourceHeldError,
    ResourceNotFoundError,
    StaleFenceError,
)
from .models import (
    AccessRequest,
    FenceToken,
    AcquireRequest,
    Lease,
    ReleaseRequest,
    RenewRequest,
)
from .store import LeaseStore

__all__ = [
    "LeaseStore",
    "Clock",
    "SystemClock",
    "FakeClock",
    "Lease",
    "FenceToken",
    "AcquireRequest",
    "RenewRequest",
    "ReleaseRequest",
    "AccessRequest",
    "CoordinationError",
    "ResourceHeldError",
    "ForgedHolderError",
    "LeaseExpiredError",
    "StaleFenceError",
    "InvalidFenceError",
    "AlreadyReleasedError",
    "ConcurrencyQuotaExceededError",
    "ResourceNotFoundError",
]
