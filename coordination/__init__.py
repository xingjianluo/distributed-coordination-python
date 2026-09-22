"""进程内分布式协调：租约互斥、fencing token、并发配额。"""

from .clock import Clock, ManualClock, SystemClock
from .errors import LeaseRejected, RejectionReason
from .registry import Lease, LeaseRegistry

__all__ = [
    "Clock",
    "ManualClock",
    "SystemClock",
    "LeaseRejected",
    "RejectionReason",
    "Lease",
    "LeaseRegistry",
]
