"""HTTP 入口：把进程内租约协调器暴露为 REST 接口。

库方式调用与 HTTP 入口共享同一个 Coordinator 单例，因此配额、
幂等、批量语义在两种调用方式下结论一致。

错误分类映射：所有 LeaseRejected 都以 409 返回，body 中 error 字段
给出可区分的拒绝原因码（见 coordination.errors.RejectionReason）。
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from coordination import Coordinator, LeaseRejected

app = FastAPI(title="distributed-coordination")

# 库方式调用可直接 import main.coordinator，与 HTTP 共享同一份状态与配额。
coordinator = Coordinator(max_concurrent_per_client=8)


class AcquireRequest(BaseModel):
    resource: str
    client: str
    ttl: float = Field(gt=0)
    # 客户端生成的幂等标识；缺失则按一次新的占用操作处理。
    idem_key: str | None = None


class BatchAcquireRequest(BaseModel):
    resources: list[str] = Field(min_length=1)
    client: str
    ttl: float = Field(gt=0)
    idem_key: str | None = None


class ReleaseRequest(BaseModel):
    resource: str
    client: str
    fencing_token: int


class RenewRequest(BaseModel):
    resource: str
    client: str
    fencing_token: int
    ttl: float = Field(gt=0)


class GuardedRequest(BaseModel):
    resource: str
    fencing_token: int


@app.exception_handler(LeaseRejected)
async def lease_rejected_handler(_: Request, exc: LeaseRejected) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"error": exc.reason.value, "detail": exc.detail},
    )


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/leases/acquire", status_code=201)
def acquire(req: AcquireRequest):
    lease = coordinator.acquire(req.resource, req.client, req.ttl, req.idem_key)
    return lease.__dict__


@app.post("/leases/acquire-batch", status_code=201)
def acquire_batch(req: BatchAcquireRequest):
    leases = coordinator.acquire_batch(
        req.resources, req.client, req.ttl, req.idem_key
    )
    return {"leases": [lease.__dict__ for lease in leases]}


@app.post("/leases/release")
def release(req: ReleaseRequest):
    coordinator.release(req.resource, req.client, req.fencing_token)
    return {"released": req.resource}


@app.post("/leases/renew")
def renew(req: RenewRequest):
    lease = coordinator.renew(
        req.resource, req.client, req.fencing_token, req.ttl
    )
    return lease.__dict__


@app.post("/leases/guarded")
def guarded(req: GuardedRequest):
    coordinator.guarded_access(req.resource, req.fencing_token)
    return {"ok": True}


@app.get("/leases/{resource}")
def holder(resource: str):
    lease = coordinator.holder_of(resource)
    return {"lease": lease.__dict__ if lease else None}
