"""并发配额测试：超限拒绝、异常路径归还、失败重试后无状态残留。"""

from __future__ import annotations

import threading
import time

import pytest

from coordination import LeaseRejected, RejectionReason
from coordination.quota import ClientQuota


def test_quota_limit_enforced_per_client():
    q = ClientQuota(max_concurrent=2)
    p1 = q.acquire("c1")
    p2 = q.acquire("c1")
    with pytest.raises(LeaseRejected) as exc:
        q.acquire("c1")
    print(f"[输入] max=2，c1 已在途 {q.in_flight('c1')} 个，再申请")
    print(f"[判定] 拒绝原因={exc.value.reason}，应为 QUOTA_EXCEEDED")
    assert exc.value.reason is RejectionReason.QUOTA_EXCEEDED
    # 配额按客户端隔离：c2 不受影响
    with q.acquire("c2"):
        pass
    p1.close()
    p2.close()
    assert q.in_flight("c1") == 0


def test_permit_released_on_exception():
    q = ClientQuota(max_concurrent=1)
    with pytest.raises(RuntimeError):
        with q.acquire("c1"):
            raise RuntimeError("simulated failure")
    print("[输入] 许可作用域内抛异常")
    print(f"[判定] 异常路径也归还许可，in_flight={q.in_flight('c1')} 应为 0")
    assert q.in_flight("c1") == 0
    # 失败后重试可立即获得许可：无残留
    with q.acquire("c1"):
        pass


def test_permit_close_is_idempotent():
    q = ClientQuota(max_concurrent=1)
    p = q.acquire("c1")
    p.close()
    p.close()
    print(f"[判定] 重复关闭不重复归还，in_flight={q.in_flight('c1')} 应为 0")
    assert q.in_flight("c1") == 0


def test_concurrent_quota_never_exceeded():
    q = ClientQuota(max_concurrent=4)
    peak = 0
    peak_lock = threading.Lock()

    def worker() -> None:
        nonlocal peak
        for _ in range(50):
            try:
                with q.acquire("c1"):
                    with peak_lock:
                        peak = max(peak, q.in_flight("c1"))
                    time.sleep(0.001)  # 持许可驻留，制造真实重叠
            except LeaseRejected:
                pass

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"[输入] 8 线程并发申请 max=4 的配额")
    print(f"[判定] 峰值在途={peak}，应 <= 4；结束后 in_flight={q.in_flight('c1')} 应为 0")
    assert peak <= 4
    assert q.in_flight("c1") == 0
