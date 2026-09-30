"""进程内分布式协调：租约互斥、fencing token、并发配额、幂等、批量占用。"""

from .clock import Clock, ManualClock, SystemClock
from .coordinator import Coordinator
from .errors import LeaseRejected, RejectionReason
from .registry import Lease, LeaseRegistry

__all__ = [
    "Clock",
    "Coordinator",
    "Lease",
    "LeaseRegistry",
    "LeaseRejected",
    "ManualClock",
    "RejectionReason",
    "SystemClock",
]
