"""进程内分布式协调：租约互斥、fencing token、统一并发配额、幂等、批量占用。"""

from .clock import Clock, ManualClock, SystemClock
from .errors import LeaseRejected, RejectionReason
from .registry import Lease, LeaseRegistry
from .service import LeaseService

__all__ = [
    "Clock",
    "Lease",
    "LeaseRegistry",
    "LeaseRejected",
    "LeaseService",
    "ManualClock",
    "RejectionReason",
    "SystemClock",
]
